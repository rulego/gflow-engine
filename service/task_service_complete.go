// This file contains task completion / approval methods on TaskServiceImpl:
// mergeVariables helper, CompleteWithApproval (and its Internal variant),
// the simpler Complete wrapper, and the Approve / Reject shortcuts.

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	utils2 "github.com/rulego/gflow-engine/utils"
)

// mergeVariables 合并任务变量。既有变量为合法 JSON 但非对象时（fork-join 后 end
// 节点的 variables 是 join 收集的分支消息数组）无法按键合并，跳过既有值只用新变量。
func (s *TaskServiceImpl) mergeVariables(existingVariables *string, newVariables map[string]interface{}) (*string, error) {
	if newVariables == nil {
		return existingVariables, nil
	}

	var merged map[string]interface{}

	// 解析现有变量
	if existingVariables != nil && *existingVariables != "" {
		var existing any
		if err := utils2.FromJSON(*existingVariables, &existing); err != nil {
			return nil, fmt.Errorf("failed to parse existing variables: %w", err)
		}
		if m, ok := existing.(map[string]interface{}); ok {
			merged = m
		} else {
			merged = make(map[string]interface{})
		}
	} else {
		merged = make(map[string]interface{})
	}

	// 合并新变量
	for key, value := range newVariables {
		merged[key] = value
	}

	// 序列化合并后的变量
	mergedJSON, err := utils2.ToJSON(merged)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize merged variables: %w", err)
	}

	return &mergedJSON, nil
}

// CompleteWithApproval 完成任务（带审批意见）
func (s *TaskServiceImpl) CompleteWithApproval(ctx context.Context, actor Actor, request *ApprovalRequest) error {
	// bindActor：显式 actor 绑定进 ctx（内部管道读取身份/派发事件），
	// 未标记调用模式的 ctx 一律升级为 API 入口（缺身份时拒绝，而不是静默跳过校验）。
	// 引擎内部调用（create_task_aspect 等）会先以 WithInternalCallingMode 标记 ctx，
	// 该标记优先级更高，不受本升级影响。
	ctx = bindActor(ctx, actor)
	// 第二重信号：内部 ctx 携带真实用户身份时降级为 API 模式（防宿主误用内部 ctx）
	ctx = forceAPICallingModeForRealUser(ctx)
	if request == nil {
		return fmt.Errorf("approval request cannot be nil")
	}
	if request.TaskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}

	// 读一次 task 用于解析 instanceID（廉价读）
	task, err := s.taskDAO.Get(ctx, request.TaskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		// 没有 instance 关联（孤儿任务）：无行可锁，用 bare scope 跑同样的逻辑；
		// 其 AfterCommit 回调在 bare scope 上立即内联执行（见 InstanceScope.AfterCommit）。
		scope := bareScope(s.taskDAO.Query)
		return s.completeWithApprovalInternal(ctx, scope, request)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.completeWithApprovalInternal(ctx, scope, request)
	})
}

// completeWithApprovalInternal 在已持有实例行锁的事务内执行 Complete 实际逻辑。
// 副作用（ExecuteNext）通过 scope.AfterCommit 推迟到事务提交后执行。
//
// 调用方必须通过 WithInstanceTx 进入；scope 内部 tx 是行锁事务的句柄。
func (s *TaskServiceImpl) completeWithApprovalInternal(ctx context.Context, scope *InstanceScope, request *ApprovalRequest) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, request.TaskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}

	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 幂等：任务已经 completed。
	// 内部调用（aspect 推进、节点回调）直接返回 nil 保持幂等，保证 client retry 安全；
	// API 路径（用户重复提交 / 并发抢占同一任务）必须返回 ErrTaskAlreadyCompleted，
	// 由控制器映射为 400。否则两个并发审批都会收到 200，与“同一任务只能被审批一次”
	// 的语义冲突（TestConcurrentApproval_OnlyOneSucceeds 即覆盖此场景）。
	if task.Status == string(enums.TaskStatusCompleted) {
		if GetCallingMode(ctx) == CallingModeAPI {
			return fmt.Errorf("task already completed: %w", ErrTaskAlreadyCompleted)
		}
		return nil
	}

	// Complete 与 Suspend 的竞态保护：管理员挂起实例时任务会被批量改成 Suspended，
	// 这里拒绝避免 aspect 在已挂起实例上推进 ExecuteNext 导致状态错乱。
	// 包一层 ErrConflict 让宿主映射 409（裸 error 会被当成 500"内部错误"）。
	if task.Status == string(enums.TaskStatusSuspended) {
		return fmt.Errorf("task is suspended, cannot complete until instance is resumed: %w", ErrConflict)
	}
	// Complete 与同节点清理的竞态保护：或签另一分支先通过（cancelSiblingActiveTasks）、
	// 会签/票签阈值达成（cancelRemainingCountersignSubTasks）、认领互斥都会把本任务置
	// Terminated。迟到的 complete 在锁内重读到的就是终态——放行会把已终止任务翻回
	// Completed+approved（审计失真）并二次触发 ExecuteNext。与实例级 ErrInstanceTerminal
	// 同口径，任何调用模式一律拒绝。
	if task.Status == string(enums.TaskStatusTerminated) {
		reason := ""
		if task.EndReason != nil {
			reason = *task.EndReason
		}
		return fmt.Errorf("%w (reason: %s), cannot complete", ErrTaskTerminated, reason)
	}
	// Pending 是"还没轮到"（顺序会签未激活子任务、待认领池），放行会越序审批；
	// 仅拦 API——end/start 等系统节点任务初始即 Pending，After aspect 的内部
	// Complete 靠它收尾归档
	if task.Status == string(enums.TaskStatusPending) && GetCallingMode(ctx) == CallingModeAPI {
		return fmt.Errorf("task is pending, cannot complete until it is activated or claimed: %w", ErrConflict)
	}
	// 检查所属实例状态：实例若处于非活跃态，需区分 API vs 内部调用
	// - API 路径：实例 Suspended/Terminated/Cancelled/Failed 都拒绝（用户操作）
	// - 内部路径（end-node aspect 推进 Completed 实例的尾任务）：允许，否则会卡住流程归档
	// 注意：Completed 不在 API 拒绝列表里——某些节点完成时实例恰好刚 Complete，
	// 内部 aspect 调用 Complete 必须放行，否则 CompleteProcessInstance 后的清理会失败
	if task.ProcessInstanceID != nil && *task.ProcessInstanceID != "" {
		instance, instErr := scope.Instances().Get(ctx, *task.ProcessInstanceID)
		if instErr == nil && instance != nil {
			callingModeForInstanceCheck := GetCallingMode(ctx)
			blockForAPI := callingModeForInstanceCheck == CallingModeAPI
			if blockForAPI {
				switch instance.Status {
				case string(enums.InstanceStatusSuspended):
					return fmt.Errorf("process instance is suspended, please resume it first")
				case string(enums.InstanceStatusTerminated),
					string(enums.InstanceStatusCancelled),
					string(enums.InstanceStatusFailed):
					return fmt.Errorf("process instance is already %s, cannot complete task", instance.Status)
				}
			}
		}
	}

	// 检查任务是否已分配
	// 按 CallingMode 区分：API 入口下任何无 Assignee 的 userTask/ccTask 都必须先 claim；
	// engine 内部调用（completeInternal）不受此限制（aspect 调用合法）。
	if task.Assignee == nil || *task.Assignee == "" {
		if task.TaskType == constants.TaskTypeUserTask && GetCallingMode(ctx) == CallingModeAPI {
			return fmt.Errorf("task is not assigned, please claim it first")
		}
	}

	// 检查当前用户是否有权限完成任务
	// API 入口必须校验 assignee；内部调用（CallingModeInternal，引擎 aspect 等）跳过校验。
	// CompleteWithApproval 入口已把未标记的 ctx 升级为 API 模式。
	callingMode := GetCallingMode(ctx)
	if callingMode == CallingModeAPI {
		// 操作人：bindActor 已把显式 actor 绑定进 ctx，直接读取
		operatorID := ""
		if u := GetUserFromCtx(ctx); u != nil {
			operatorID = u.UserID
		}
		if operatorID == "" {
			return ErrAuthenticationRequired
		}
		if task.Assignee != nil && *task.Assignee != "" && *task.Assignee != operatorID {
			return fmt.Errorf("task assigned to %s, operator %s: %w", *task.Assignee, operatorID, ErrPermissionDenied)
		}
		// 租户隔离：有身份的操作人租户非空时任务必须同租户。
		// 仅靠 assignee 相等校验挡不住"无 assignee 的候选/抄送类任务被跨租户完成"；
		// 放在身份校验之后，保证无身份时仍返回 ErrAuthenticationRequired（fail-closed 语义不变）。
		if err := ensureTenantAccess(ctx, "task", task.TenantID); err != nil {
			return err
		}
	}

	// 委派归还：被委派人(approve/reject)应把任务归还原审批人(Owner)，不直接完成流转。
	// delegatee 若直接 complete+advance，原 owner 将永不审查，因此走 resolveDelegatedApproval。
	if task.Owner != nil && *task.Owner != "" {
		return s.resolveDelegatedApproval(ctx, scope, task, request)
	}

	// 加签守卫：主任务（无 ParentID）存在未决加签子任务时不能直接完成。
	// 否则审批人加签后又自己点通过，主任务会被直接完成并流转——加签人从未参与
	// （实例 completed，加签待办被归档吞掉）。
	// 加签人必须先审；全部子任务完成后原审批人再通过。
	// 不适用于会签/票签子任务路径（它们带 ParentID，在下方分支按阈值判定）。
	if task.ParentID == nil || *task.ParentID == "" {
		// 查询失败必须拒绝完成（fail-closed），否则守卫被静默绕过
		subs, serr := taskDAO.GetByParentID(ctx, task.ID)
		if serr != nil {
			return fmt.Errorf("failed to check pending add-sign sub-tasks: %w", serr)
		}
		for _, st := range subs {
			if st == nil {
				continue
			}
			if st.Status == string(enums.TaskStatusActive) || st.Status == string(enums.TaskStatusPending) {
				signer := ""
				if st.Assignee != nil {
					signer = *st.Assignee
				}
				return fmt.Errorf("task has pending add-sign sub-tasks (signer %s); wait for all signers before completing: %w", signer, ErrTaskNotClaimable)
			}
		}
	}

	// 合并变量（API 审批按表单字段权限过滤：只读/隐藏字段不允许审批人覆盖）
	reqVars := request.Variables
	if callingMode == CallingModeAPI {
		reqVars = s.filterVariablesByFormPermissions(ctx, scope, task, request.Variables)
	}
	mergedVariables, err := s.mergeVariables(task.Variables, reqVars)
	if err != nil {
		return fmt.Errorf("failed to merge variables: %w", err)
	}
	// 同步局部 task.Variables 为合并后的值：下方 ExecuteNext 下传读 task.Variables，
	// 若不同步，会下传审批前的旧变量(审批人提交的覆盖值丢失)。
	task.Variables = mergedVariables

	// 创建更新任务对象，只更新必要字段
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	now := time.Now()
	updatedTask := &model.WfTask{
		ID:        task.ID,
		Status:    string(enums.TaskStatusCompleted), // 更新任务状态
		Variables: mergedVariables,
		UpdatedAt: &now,
		EndedAt:   &now,
		UpdatedBy: &username,
	}
	// 处理时长（毫秒）：以任务创建时间为起点。created_at 在 gorm autoCreateTime 路径下
	// 必然存在；created_at 零值时兜底返回 0，避免负数 duration 污染历史归档。
	durMs := int64(0)
	if !task.CreatedAt.IsZero() {
		durMs = now.Sub(task.CreatedAt).Milliseconds()
		if durMs < 0 {
			durMs = 0
		}
	}
	updatedTask.Duration = &durMs

	if request.ApprovalResult != "" {
		endReason := string(request.ApprovalResult)
		updatedTask.EndReason = &endReason
	}

	if request.Comment != "" {
		updatedTask.Comment = &request.Comment
	}

	// 保存任务更新
	if err := taskDAO.Update(ctx, updatedTask); err != nil {
		return fmt.Errorf("failed to complete task: %w", err)
	}

	// 审批意见同事务落评论表：task.Comment 只是快照字段，评论表才能在任务
	// 归档后继续查询完整时间线
	commentOperator := ""
	if u := GetUserFromCtx(ctx); u != nil {
		commentOperator = u.UserID
	}
	if err := s.recordApprovalComment(ctx, scope, task, commentOperator, request.Comment); err != nil {
		return fmt.Errorf("failed to record approval comment: %w", err)
	}

	// 审批通过 → 通知发起人进度（发起人不能只在驳回/终止时收到通知，
	// 通过方向也要可见）。仅在 API 审批路径派发，避免内部自动完成（ccTask 等）刷通知；
	// 事务提交后才派发（AfterCommit），回滚不产生幽灵通知。
	if request.ApprovalResult == enums.ApprovalResultApproved && callingMode == CallingModeAPI &&
		task.ProcessInstanceID != nil && *task.ProcessInstanceID != "" &&
		s.workflowEngine != nil && s.workflowEngine.GetTaskEventListener() != nil {
		if instance, iErr := scope.Instances().Get(ctx, *task.ProcessInstanceID); iErr == nil && instance != nil && instance.StartUserID != "" {
			approverID := ""
			if u := GetUserFromCtx(ctx); u != nil {
				approverID = u.UserID
			}
			// 自审（发起人批准自己的流程）无需自我通知
			if instance.StartUserID != approverID {
				evtInst := *task.ProcessInstanceID
				scope.AfterCommit(func() error {
					DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), TaskEvent{
						Type:       TaskEventApproved,
						TaskID:     task.ID,
						TaskDefKey: task.TaskDefKey,
						InstanceID: evtInst,
						ProcessID:  task.ProcessID,
						TenantID:   instance.TenantID,
						TaskName:   task.Name,
						ToUsers:    []string{instance.StartUserID},
						FromUser:   approverID,
						Reason:     request.Comment,
						Timestamp:  time.Now(),
					}, ctx)
					return nil
				})
			}
		}
	}
	// 如果子任务都完成，则更新主流程状态（已被 instance 行锁串行化保护）。
	if task.ParentID != nil && *task.ParentID != "" {
		approvalRuleStr := s.getApprovalRuleString(task.ApprovalRule)
		rule, err := s.parseCountersignRule(approvalRuleStr)
		if err != nil {
			return fmt.Errorf("%w: parse approval rule: %v", ErrCountersignRule, err)
		}

		if rule.IsSequential {
			if err := s.activateNextSequentialTaskInternal(ctx, scope, *task.ProcessInstanceID, task.TaskDefKey); err != nil {
				return err
			}
		}

		if task.TaskType == constants.TaskTypeUserTask {
			// 早期一票否决：仅"全员会签"(CountersignTypeAll)——一人 reject 注定不通过，可立即结束父任务。
			// 阈值类(Any/Majority/Percent/Count，如票签 vote)不适用：少数 reject 不终止，落入下方 rule 阈值判定。
			// 注：子任务完成后不重新触发 OnMsg，故此分支承担 userTask 一票否决的触发职责。
			if request.ApprovalResult == enums.ApprovalResultRejected && enums.CountersignType(rule.Type) == enums.CountersignTypeAll {
				parentTask, perr := taskDAO.Get(ctx, *task.ParentID)
				if perr != nil {
					return fmt.Errorf("failed to load parent task for early veto: %w", perr)
				}
				if parentTask != nil &&
					parentTask.Status != string(enums.TaskStatusCompleted) {
					// 先校验父任务变量：损坏数据 fail-closed，回滚事务。
					// merge 会把 variables 重建为合法 JSON，放在校验前会掩盖损坏。
					vars, err := ParseVariablesJSON(parentTask.Variables)
					if err != nil {
						return fmt.Errorf("failed to parse parent task variables on early veto: %w", err)
					}
					now := time.Now()
					parentTask.Status = string(enums.TaskStatusCompleted)
					parentTask.EndedAt = &now
					endReason := string(enums.ApprovalResultRejected)
					parentTask.EndReason = &endReason
					s.mergeCountersignSubTaskVariables(ctx, scope, parentTask)
					if err := taskDAO.Update(ctx, parentTask); err != nil {
						return fmt.Errorf("failed to update parent task on early veto: %w", err)
					}
					// merge 后重新解析：ExecuteNext 必须带上子任务的 approved/comment
					// （与下方完成分支一致），否则下游路由看不到否决结果
					vars, err = ParseVariablesJSON(parentTask.Variables)
					if err != nil {
						return fmt.Errorf("failed to re-parse parent task variables after merge on early veto: %w", err)
					}
					parentInst := parentTask.ProcessInstanceID
					parentKey := parentTask.TaskDefKey
					// 父任务已定局，剩余未决子任务一并终止，否则留下幽灵待办且 fork 分支凑不齐
					if cerr := s.cancelRemainingCountersignSubTasks(ctx, scope, parentTask.ID); cerr != nil {
						logrus.WithError(cerr).WithField("parentTaskID", parentTask.ID).Warn("failed to cancel remaining sub-tasks after early veto")
					}
					scope.AfterCommit(func() error {
						return s.workflowEngine.GetRuntimeServiceInternal().ExecuteNext(ctx, *parentInst, parentKey, vars)
					})
				}
				return nil
			}
			isCompleted, approved, err := s.checkCountersignSubTaskCompletionInternal(ctx, scope, *task.ParentID, approvalRuleStr)
			if err != nil {
				return fmt.Errorf("failed to check countersign completion: %w", err)
			}
			if isCompleted {
				parentTask, perr := taskDAO.Get(ctx, *task.ParentID)
				if perr != nil {
					return fmt.Errorf("failed to load parent task for completion: %w", perr)
				}
				if parentTask != nil {
					// 加签语义（父任务自身有 assignee）：子任务全部完成还不够——
					// 原审批人自己的批准是必要条件。若子任务批完即完成父任务并流转，
					// 原审批人的审批会被静默跳过（反向绕过）。等原审批人随后通过时，
					// 加签守卫（子任务已全部完成）放行，节点才真正结束。
					if parentTask.Assignee != nil && *parentTask.Assignee != "" &&
						parentTask.Status != string(enums.TaskStatusCompleted) {
						logrus.WithFields(logrus.Fields{
							"parentTaskID": *task.ParentID,
							"signer":       task.Assignee,
						}).Info("all add-sign sub-tasks completed; waiting for original assignee approval before node completion")
						return nil
					}
					// 二次确认：行锁保护下再次检查父任务是否已被并发分支完成
					if parentTask.Status == string(enums.TaskStatusCompleted) {
						logrus.WithField("parentTaskID", *task.ParentID).Warn("Countersign parent already completed by concurrent branch, skip duplicate ExecuteNext")
						return nil
					}
					now := time.Now()
					parentTask.Status = string(enums.TaskStatusCompleted)
					parentTask.EndedAt = &now
					if approved {
						endReason := string(enums.ApprovalResultApproved)
						parentTask.EndReason = &endReason
					} else {
						endReason := string(enums.ApprovalResultRejected)
						parentTask.EndReason = &endReason
					}
					s.mergeCountersignSubTaskVariables(ctx, scope, parentTask)
					if err := taskDAO.Update(ctx, parentTask); err != nil {
						return fmt.Errorf("failed to update parent task on countersign completion: %w", err)
					}
					// 阈值达成即终止：会签/票签父任务完成后，同父节点下仍未投票的
					// active/pending 子任务必须一并终止（置 Terminated + 注明原因）。
					// 否则这些"幽灵待办"会永远留在审批人列表里：点同意返回 200 但
					// 无任何效果，审批记录也失真（投票发生在节点结束后）。
					if cerr := s.cancelRemainingCountersignSubTasks(ctx, scope, parentTask.ID); cerr != nil {
						logrus.WithError(cerr).WithField("parentTaskID", parentTask.ID).Warn("failed to cancel remaining countersign sub-tasks; ghost todos may persist")
					}
					vars, err := ParseVariablesJSON(parentTask.Variables)
					if err != nil {
						return fmt.Errorf("failed to parse parent task variables on countersign completion: %w", err)
					}
					parentInst := parentTask.ProcessInstanceID
					parentKey := parentTask.TaskDefKey
					scope.AfterCommit(func() error {
						return s.workflowEngine.GetRuntimeServiceInternal().ExecuteNext(ctx, *parentInst, parentKey, vars)
					})
				}
				return nil
			}
		}
		return nil
	}

	// 如果是userTask节点，则通知下一个节点/或者userTask节点，做是否都审批完检查
	if task.TaskType == constants.TaskTypeUserTask {
		// 或签：任一完成即节点完成，即时终止同节点其他活跃候选任务，避免幽灵待办
		if task.ApprovalType == string(enums.ApprovalTypeOr) {
			if cerr := s.cancelSiblingActiveTasks(ctx, scope, task); cerr != nil {
				logrus.Warnf("failed to cancel sibling tasks after or-sign completion: %v", cerr)
			}
		}
		vars, err := ParseVariablesJSON(task.Variables)
		if err != nil {
			return err
		}
		inst := task.ProcessInstanceID
		key := task.TaskDefKey
		scope.AfterCommit(func() error {
			return s.workflowEngine.GetRuntimeServiceInternal().ExecuteNext(ctx, *inst, key, vars)
		})
	}

	return nil
}

// mergeCountersignSubTaskVariables 把已审批会签/票签子任务的变量合并进 parentTask.Variables。
// 流转前 ExecuteNext 的 vars 取自 parentTask.Variables，原为创建快照、不含子任务审批写入的
// approved/comment，导致会签节点后的 switch(msg.approved) 等条件路由拿不到审批结果。
// 这里在父任务完成时聚合所有已审批(EndReason 非空)子任务的变量，写回 parentTask.Variables。
func (s *TaskServiceImpl) mergeCountersignSubTaskVariables(ctx context.Context, scope *InstanceScope, parentTask *model.WfTask) {
	if parentTask == nil {
		return
	}
	merged, perr := ParseVariablesJSON(parentTask.Variables)
	if perr != nil || merged == nil {
		merged = map[string]interface{}{}
	}
	subTasks, err := scope.Tasks().GetByParentID(ctx, parentTask.ID)
	if err != nil {
		return
	}
	for _, st := range subTasks {
		if st.EndReason == nil || st.Variables == nil || *st.Variables == "" {
			continue
		}
		sv, serr := ParseVariablesJSON(st.Variables)
		if serr != nil {
			continue
		}
		for k, v := range sv {
			merged[k] = v
		}
	}
	out, err := json.Marshal(merged)
	if err != nil {
		return
	}
	str := string(out)
	parentTask.Variables = &str
}

// resolveDelegatedApproval 处理被委派人(approve/reject)的归还：不完成 task、不流转，
// 把 assignee 还原为 Owner 并清空 Owner，合并 delegatee 的意见到变量供原审批人参考。
func (s *TaskServiceImpl) resolveDelegatedApproval(ctx context.Context, scope *InstanceScope, task *model.WfTask, request *ApprovalRequest) error {
	taskDAO := scope.Tasks()
	if merged, err := s.mergeVariables(task.Variables, request.Variables); err == nil {
		task.Variables = merged
	}
	task.Assignee = task.Owner
	// gorm Updates(struct) 忽略 nil 字段,用空串指针强制清空 owner。owner=="" 语义等同无 owner:
	// resolve 触发条件 *Owner != "" 见空串即不再重触发。否则 owner 永不清 → 每次 approve 重走
	// resolveDelegatedApproval 把任务归还给自己 → 节点永不完成、实例卡死。
	emptyOwner := ""
	task.Owner = &emptyOwner
	operator := ""
	if u := GetUserFromCtx(ctx); u != nil {
		operator = u.UserID
	}
	now := time.Now()
	task.UpdatedBy = &operator
	task.UpdatedAt = &now
	// 被委派人的审批意见落两处：task.Comment 快照让详情时间线立即可见（原审批人
	// 带意见完成时按完成语义覆盖）；评论表留档，不随任务归档丢失
	if request.Comment != "" {
		task.Comment = &request.Comment
	}
	if err := taskDAO.Update(ctx, task); err != nil {
		return err
	}
	if err := s.recordApprovalComment(ctx, scope, task, operator, request.Comment); err != nil {
		return err
	}

	// 归还事件：原审批人需感知任务已回到名下（与显式 Resolve 同口径）
	if listener := s.workflowEngine.GetTaskEventListener(); listener != nil && task.Assignee != nil && *task.Assignee != "" {
		instanceID := ""
		if task.ProcessInstanceID != nil {
			instanceID = *task.ProcessInstanceID
		}
		evt := TaskEvent{
			Type:       TaskEventResolved,
			TaskID:     task.ID,
			TaskDefKey: task.TaskDefKey,
			InstanceID: instanceID,
			ProcessID:  task.ProcessID,
			TenantID:   task.TenantID,
			TaskName:   task.Name,
			ToUsers:    []string{*task.Assignee},
			FromUser:   operator,
			Reason:     request.Comment,
			Timestamp:  time.Now(),
		}
		scope.AfterCommit(func() error {
			DispatchTaskEvent(listener, evt, ctx)
			return nil
		})
	}
	return nil
}

// cancelSiblingActiveTasks 终止同实例同节点(TaskDefKey)的其他活跃任务。
// 用于或签节点完成后清理剩余候选任务，避免幽灵待办。
func (s *TaskServiceImpl) cancelSiblingActiveTasks(ctx context.Context, scope *InstanceScope, completed *model.WfTask) error {
	taskDAO := scope.Tasks()
	q := &dto.TaskQuery{
		InstanceID: completed.ProcessInstanceID,
		TaskDefKey: completed.TaskDefKey,
	}
	q.Status = []string{string(enums.TaskStatusActive)}
	siblings, _, err := taskDAO.List(ctx, q)
	if err != nil {
		return err
	}
	now := time.Now()
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	reason := "cancelled by sibling completion"
	for _, st := range siblings {
		if st.ID == completed.ID {
			continue
		}
		st.Status = string(enums.TaskStatusTerminated)
		st.EndedAt = &now
		st.EndReason = &reason
		st.UpdatedBy = &username
		st.UpdatedAt = &now
		if err := taskDAO.Update(ctx, st); err != nil {
			logrus.Warnf("failed to cancel sibling task %s: %v", st.ID, err)
		}
	}
	return nil
}

// cancelRemainingCountersignSubTasks 会签/票签父任务完成后终止其剩余未决子任务。
// 阈值类规则（majority/percent/count/any）与全员会签达标后，未投票子任务留在
// wf_task 会形成幽灵待办；统一置 Terminated 并注明由阈值达成终止，保留审计痕迹。
func (s *TaskServiceImpl) cancelRemainingCountersignSubTasks(ctx context.Context, scope *InstanceScope, parentTaskID string) error {
	if parentTaskID == "" {
		return nil
	}
	subTasks, err := scope.Tasks().GetByParentID(ctx, parentTaskID)
	if err != nil {
		return err
	}
	now := time.Now()
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	reason := "terminated by countersign threshold reached"
	cancelled := 0
	for _, st := range subTasks {
		if st.Status != string(enums.TaskStatusActive) && st.Status != string(enums.TaskStatusPending) {
			continue
		}
		st.Status = string(enums.TaskStatusTerminated)
		st.EndedAt = &now
		st.EndReason = &reason
		st.UpdatedBy = &username
		st.UpdatedAt = &now
		if err := scope.Tasks().Update(ctx, st); err != nil {
			logrus.Warnf("failed to cancel countersign sub-task %s: %v", st.ID, err)
			continue
		}
		cancelled++
	}
	if cancelled > 0 {
		logrus.WithFields(logrus.Fields{
			"parentTaskID": parentTaskID,
			"cancelled":    cancelled,
		}).Info("cancelled remaining countersign sub-tasks after threshold reached")
	}
	return nil
}

// Complete 完成任务（简化版本）
//
// 审批意图解释：variables["approved"] 为 bool 时
// 视为审批意图——true 走 approved、false 走 rejected 的 CompleteWithApproval 路径，
// 保证 EndReason/审批评论按审批语义落库；审批意见取 variables["comment"]（string）。
// 无该键或类型非 bool 时保持纯 Complete 语义（ApprovalResult 为空，不写 EndReason）。
func (s *TaskServiceImpl) Complete(ctx context.Context, actor Actor, taskID string, variables map[string]interface{}) error {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}

	// 构造审批请求对象
	request := &ApprovalRequest{
		TaskID:    taskID,
		Variables: variables,
	}

	// 从variables中提取审批结果
	if approved, ok := variables["approved"]; ok {
		if approvedBool, ok := approved.(bool); ok {
			if approvedBool {
				request.ApprovalResult = enums.ApprovalResultApproved
			} else {
				request.ApprovalResult = enums.ApprovalResultRejected
			}
		}
	}

	// 从variables中提取审批意见
	if comment, ok := variables["comment"]; ok {
		if commentStr, ok := comment.(string); ok {
			request.Comment = commentStr
		}
	}

	// approved/comment 是控制面约定键（表达审批意图），提取完成后从业务变量中
	// 移除，避免作为流程变量下传污染网关条件上下文。在副本上移除：调用方传入的
	// map 往往还要复用（审计、重试、日志），不能被本次调用静默改写。
	businessVars := make(map[string]interface{}, len(variables))
	for k, v := range variables {
		if k == "approved" || k == "comment" {
			continue
		}
		businessVars[k] = v
	}
	request.Variables = businessVars

	// 调用带审批的完成方法
	return s.CompleteWithApproval(ctx, actor, request)
}

// Approve 审批通过任务
func (s *TaskServiceImpl) Approve(ctx context.Context, actor Actor, taskID, comment string, variables map[string]interface{}) error {
	ctx = bindActor(ctx, actor)
	return s.CompleteWithApproval(ctx, actor, &ApprovalRequest{
		TaskID:         taskID,
		ApprovalResult: enums.ApprovalResultApproved,
		Comment:        comment,
		Variables:      variables,
	})
}

// Reject 审批拒绝任务
func (s *TaskServiceImpl) Reject(ctx context.Context, actor Actor, taskID, comment string, variables map[string]interface{}) error {
	ctx = bindActor(ctx, actor)
	return s.CompleteWithApproval(ctx, actor, &ApprovalRequest{
		TaskID:         taskID,
		ApprovalResult: enums.ApprovalResultRejected,
		Comment:        comment,
		Variables:      variables,
	})
}
