// This file implements Recall (审批人收回): an approver revokes their own
// latest completed approval and gets a fresh pending task at the same node.
// Unlike Withdraw (发起人撤回, terminates the whole instance), recall only
// voids the recaller's own vote; peer countersign votes stay intact.

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
	"github.com/sirupsen/logrus"
)

// recallTransparentNodeTypes 收回路径检查中可穿越的节点类型：路由网关与
// fork/join 走向取决于运行期条件但本身无业务副作用（fork/join 见
// user_task_reject.go 的类型判定，链定义中真实存在）；ccTask 是信息类
// （抄送已发不构成业务动作）。
var recallTransparentNodeTypes = map[string]bool{
	constants.NodeTypeSwitch:        true,
	constants.NodeTypeJsSwitch:      true,
	constants.NodeTypeMsgTypeSwitch: true,
	constants.NodeTypeInclusive:     true,
	constants.NodeTypeRouteGateway:  true,
	"condition":                     true,
	"fork":                          true,
	"join":                          true,
	constants.TaskTypeCCTask:        true,
	constants.NodeTypeStart:         true,
	"startTask":                     true,
}

// Recall 收回（审批人撤销自己最近一条已通过的审批，重建自己的待审任务）。
// 已完成(通过)实例走 recallCompleted：窗口期内发起人/管理员整单重开，末节点重审。
func (s *TaskServiceImpl) Recall(ctx context.Context, actor Actor, instanceID, reason string) error {
	ctx = bindActor(ctx, actor)
	if instanceID == "" || actor.UserID == "" {
		return fmt.Errorf("instance ID and user ID cannot be empty")
	}

	// 流程定义与开关解析放在锁外——WithInstanceTx 回调内只允许经 scope 访问 DB，
	// 而定义解析要读流程表。先无锁读实例拿 processID，锁内再以行锁快照复核状态与归属。
	inst, err := s.workflowEngine.GetRuntimeService().GetProcessInstance(ctx, ActorFromCtx(ctx), instanceID)
	if err != nil {
		return fmt.Errorf("failed to get process instance: %w", err)
	}
	if inst == nil {
		return fmt.Errorf("%w: process instance", ErrNotFound)
	}
	if u := GetUserFromCtx(ctx); u != nil && inst.TenantID != u.TenantID {
		return fmt.Errorf("%w: process instance", ErrNotFound)
	}

	switch inst.Status {
	case string(enums.InstanceStatusActive):
		// 在途收回：走行锁事务
	case string(enums.InstanceStatusSuspended):
		return fmt.Errorf("%w: 流程已挂起，无法收回", ErrValidation)
	case string(enums.InstanceStatusCompleted):
		// 终态收回：实例已归档，窗口期内发起人/管理员整单重开
		return s.recallCompleted(ctx, actor, inst, instanceID, reason)
	default:
		return fmt.Errorf("%w: 流程已结束，无法收回", ErrValidation)
	}

	ap, chain, err := resolveProcessActionPermissions(ctx, s.workflowEngine, inst.ProcessID)
	if err != nil {
		return fmt.Errorf("%w: 流程定义不可用，无法收回", ErrValidation)
	}
	if designerDisabled(ap, "recall") {
		return fmt.Errorf("%w: 该流程已关闭审批人收回", ErrPermissionDenied)
	}

	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.recallInternal(ctx, scope, chain, actor.UserID, instanceID, reason)
	})
}

// defaultRecallWindowDays 终态收回窗口默认天数：窗口封顶"审批完成后下游已消费"的业务风险
const defaultRecallWindowDays = 7

// withinRecallWindow 判断已完成实例是否仍在可收回窗口内（天数按流程级
// recallWindowDays 覆盖，缺省 7 天）。
func withinRecallWindow(ap map[string]interface{}, endedAt *time.Time) bool {
	if endedAt == nil {
		return false
	}
	return time.Since(*endedAt) <= time.Duration(recallWindowDays(ap))*24*time.Hour
}

// recallWindowDays 从流程级配置解析终态收回窗口天数，非法或缺省回落 7 天。
func recallWindowDays(ap map[string]interface{}) int {
	if v, ok := ap["recallWindowDays"].(float64); ok && v > 0 && v <= 365 {
		return int(v)
	}
	return defaultRecallWindowDays
}

// recallCompleted 终态收回：已完成(通过)实例在窗口期内由发起人或管理员整单重开——
// 实例复活为运行中，末节点经 ExecuteNext 重入按审批形态重建任务、整轮重审。
// 不做"只撤自己一票"的变体：实例已归档，逐票回迁的复杂度换不来场景收益，
// 末节点整轮重审对发起人反悔（主场景）语义更直白。
func (s *TaskServiceImpl) recallCompleted(ctx context.Context, actor Actor, hi *model.WfInstance, instanceID, reason string) error {
	userID := actor.UserID
	if hi.StartUserID != userID && !isWorkflowAdmin(&actor) {
		return fmt.Errorf("%w: 已完成的审批仅发起人或管理员可收回", ErrPermissionDenied)
	}

	ap, _, err := resolveProcessActionPermissions(ctx, s.workflowEngine, hi.ProcessID)
	if err != nil {
		return fmt.Errorf("%w: 流程定义不可用，无法收回", ErrValidation)
	}
	if designerDisabled(ap, "recall") {
		return fmt.Errorf("%w: 该流程已关闭审批人收回", ErrPermissionDenied)
	}
	if !withinRecallWindow(ap, hi.EndedAt) {
		return fmt.Errorf("%w: 超过可收回窗口（%d 天），无法收回", ErrValidation, recallWindowDays(ap))
	}

	lastNode, voters, err := latestCompletedNodeInHistory(ctx, s.taskDAO.Query, instanceID)
	if err != nil {
		return fmt.Errorf("failed to resolve last node: %w", err)
	}
	if lastNode == "" {
		return fmt.Errorf("%w: 实例无可重开的审批节点", ErrValidation)
	}

	// 重入通道必须先确认可用:复活成功而重入失败会留下 active 但零任务的僵尸实例
	internal := s.workflowEngine.GetRuntimeServiceInternal()
	if internal == nil {
		return fmt.Errorf("%w: 引擎内部服务未注入，无法重开已完成实例", ErrValidation)
	}

	// 复活实例：同 PK 回插运行表并移除归档行，并发双收回靠主键冲突天然互斥
	err = s.taskDAO.Query.Transaction(func(tx *query.Query) error {
		revived := &model.WfInstance{
			ID:              hi.ID,
			ProcessID:       hi.ProcessID,
			BusinessKey:     hi.BusinessKey,
			Name:            hi.Name,
			Status:          string(enums.InstanceStatusActive),
			Variables:       hi.Variables,
			CurrentActivity: hi.CurrentActivity,
			Priority:        hi.Priority,
			ParentID:        hi.ParentID,
			TenantID:        hi.TenantID,
			CreatedBy:       hi.CreatedBy,
			CreatedAt:       hi.CreatedAt,
			StartUserID:     hi.StartUserID,
		}
		if err := tx.WfInstance.WithContext(ctx).Create(revived); err != nil {
			return fmt.Errorf("failed to revive instance: %w", err)
		}
		if _, err := tx.WfHiInstance.WithContext(ctx).Where(tx.WfHiInstance.ID.Eq(instanceID)).Delete(); err != nil {
			return fmt.Errorf("failed to remove archived instance: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to reopen completed instance: %w", err)
	}

	// 末节点投票人收到作废通知；重入后的新任务由组件派发 assigned 事件
	if listener := s.workflowEngine.GetTaskEventListener(); listener != nil {
		DispatchTaskEvent(listener, TaskEvent{
			Type:       TaskEventRecalled,
			InstanceID: instanceID,
			ProcessID:  hi.ProcessID,
			TenantID:   hi.TenantID,
			ToUsers:    voters,
			FromUser:   userID,
			Reason:     reason,
			Timestamp:  time.Now(),
		}, ctx)
	}

	// 重入末节点重建任务。失败则整体退回归档态补偿——不补偿会留下
	// active 但零任务的僵尸实例，且在途收回通道救不了它（无 completed 任务可寻）
	if err := internal.ExecuteNext(ctx, instanceID, lastNode, nil); err != nil {
		if cerr := s.rearchiveCompletedInstance(ctx, s.taskDAO.Query, hi, instanceID); cerr != nil {
			return fmt.Errorf("末节点重入失败且补偿失败，实例 %s 可能停留为无任务运行态需人工处理: 重入错误=%v 补偿错误=%w", instanceID, err, cerr)
		}
		return fmt.Errorf("%w: 末节点重入失败，已回滚为已完成态: %v", ErrValidation, err)
	}
	return nil
}

// rearchiveCompletedInstance 终态收回的补偿：把复活后的实例连同重入期间
// 可能建出的任务一并退回归档态，恢复到收回前的样子。
func (s *TaskServiceImpl) rearchiveCompletedInstance(ctx context.Context, q *query.Query, hi *model.WfInstance, instanceID string) error {
	return q.Transaction(func(tx *query.Query) error {
		tasks, err := tx.WfTask.WithContext(ctx).Where(tx.WfTask.ProcessInstanceID.Eq(instanceID)).Find()
		if err != nil {
			return fmt.Errorf("failed to list tasks: %w", err)
		}
		if len(tasks) > 0 {
			now := time.Now()
			rollbackReason := "收回重开失败回滚"
			his := make([]*model.WfHiTask, 0, len(tasks))
			taskIDs := make([]string, 0, len(tasks))
			for _, tk := range tasks {
				if tk.Status == string(enums.TaskStatusActive) ||
					tk.Status == string(enums.TaskStatusPending) ||
					tk.Status == string(enums.TaskStatusSuspended) {
					tk.Status = string(enums.TaskStatusTerminated)
					tk.EndedAt = &now
					tk.EndReason = &rollbackReason
				}
				his = append(his, taskToHiTask(tk))
				taskIDs = append(taskIDs, tk.ID)
			}
			if err := tx.WfHiTask.WithContext(ctx).CreateInBatches(his, archiveBatchSize); err != nil {
				return fmt.Errorf("failed to archive tasks: %w", err)
			}
			if _, err := tx.WfTaskAssignee.WithContext(ctx).Where(tx.WfTaskAssignee.TaskID.In(taskIDs...)).Delete(); err != nil {
				logrus.Warnf("recall rollback: failed to clean task assignees: %v", err)
			}
			if _, err := tx.WfTask.WithContext(ctx).Where(tx.WfTask.ProcessInstanceID.Eq(instanceID)).Delete(); err != nil {
				return fmt.Errorf("failed to delete tasks: %w", err)
			}
		}
		if _, err := tx.WfInstance.WithContext(ctx).Where(tx.WfInstance.ID.Eq(instanceID)).Delete(); err != nil {
			return fmt.Errorf("failed to delete revived instance: %w", err)
		}
		// hi 参数是运行表模型，归档表按完结归档的字段映射重建
		hiRow := &model.WfHiInstance{
			ID:              hi.ID,
			ProcessID:       hi.ProcessID,
			BusinessKey:     hi.BusinessKey,
			Name:            hi.Name,
			Status:          hi.Status,
			Variables:       hi.Variables,
			CurrentActivity: hi.CurrentActivity,
			Priority:        hi.Priority,
			ParentID:        hi.ParentID,
			TenantID:        hi.TenantID,
			CreatedBy:       hi.CreatedBy,
			CreatedAt:       hi.CreatedAt,
			UpdatedBy:       hi.UpdatedBy,
			UpdatedAt:       hi.UpdatedAt,
			EndReason:       hi.EndReason,
			Duration:        hi.Duration,
			EndedAt:         hi.EndedAt,
			StartUserID:     hi.StartUserID,
		}
		if err := tx.WfHiInstance.WithContext(ctx).Create(hiRow); err != nil {
			return fmt.Errorf("failed to restore archived instance: %w", err)
		}
		return nil
	})
}

// latestCompletedNodeInHistory 在历史任务里找最近完成的 userTask 节点（末节点）
// 及该节点全部投票人（去重，供作废通知）。
func latestCompletedNodeInHistory(ctx context.Context, q *query.Query, instanceID string) (string, []string, error) {
	wt := q.WfHiTask
	var last model.WfHiTask
	err := wt.WithContext(ctx).
		Where(wt.ProcessInstanceID.Eq(instanceID)).
		Where(wt.TaskType.Eq(constants.TaskTypeUserTask)).
		Where(wt.Status.Eq(string(enums.TaskStatusCompleted))).
		Order(wt.EndedAt.Desc()).
		Limit(1).
		Scan(&last)
	if err != nil {
		return "", nil, fmt.Errorf("failed to query last completed node: %w", err)
	}
	if last.ID == "" || last.TaskDefKey == nil || *last.TaskDefKey == "" {
		return "", nil, nil
	}
	var rows []model.WfHiTask
	err = wt.WithContext(ctx).
		Where(wt.ProcessInstanceID.Eq(instanceID)).
		Where(wt.TaskDefKey.Eq(*last.TaskDefKey)).
		Where(wt.Status.Eq(string(enums.TaskStatusCompleted))).
		Where(wt.Assignee.IsNotNull()).
		Where(wt.Assignee.Neq("")).
		Scan(&rows)
	if err != nil {
		return "", nil, fmt.Errorf("failed to query last node voters: %w", err)
	}
	seen := map[string]bool{}
	voters := make([]string, 0, len(rows))
	for _, r := range rows {
		if r.Assignee == nil || seen[*r.Assignee] {
			continue
		}
		seen[*r.Assignee] = true
		voters = append(voters, *r.Assignee)
	}
	return *last.TaskDefKey, voters, nil
}

// recallInternal 在已持有实例行锁的事务内执行 Recall 实际逻辑。
// 顺序：守卫全部通过后才动数据（终止前沿 → 归档 T → 回置父任务 → 重建任务）。
func (s *TaskServiceImpl) recallInternal(ctx context.Context, scope *InstanceScope, chain *types.RuleChain, userID, instanceID, reason string) error {
	instance, err := scope.Instances().Get(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("failed to get process instance: %w", err)
	}
	if instance == nil {
		return fmt.Errorf("%w: process instance", ErrNotFound)
	}
	if u := GetUserFromCtx(ctx); u != nil && instance.TenantID != u.TenantID {
		return fmt.Errorf("%w: process instance", ErrNotFound)
	}
	if instance.Status != string(enums.InstanceStatusActive) {
		return fmt.Errorf("%w: 流程已结束，无法收回", ErrValidation)
	}

	taskDAO := scope.Tasks()
	hiTaskDAO := scope.HiTasks()
	tasks, _, err := taskDAO.List(ctx, &dto.TaskQuery{InstanceID: &instanceID})
	if err != nil {
		return fmt.Errorf("failed to list instance tasks: %w", err)
	}

	t := findRecallableCompletedTask(tasks, userID)
	if t == nil {
		return fmt.Errorf("%w: 当前没有可收回的审批记录", ErrValidation)
	}
	// completed 但 ended_at 为空的异常行无法与任何记录比先后，终止方向也会失向
	// （前沿任务不被终止 → 双停泊），直接拒绝
	if t.EndedAt == nil {
		return fmt.Errorf("%w: 当前没有可收回的审批记录", ErrValidation)
	}
	if err := evaluateRecallGuard(tasks, t, chain); err != nil {
		return err
	}

	now := time.Now()
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}

	// 终止前沿任务：T 完成之后创建的全部在途任务（下一节点的待办、顺序会签
	// 的在途子任务）。created_at 用 >= 是防 datetime(3) 同毫秒截断——本节点
	// 旧任务都创建于 T 完成之前，>= 不会误伤。
	// 两段式执行：先定终止集，再豁免"同节点存活同伴的加签子任务"——同伴的
	// 任务创建于 T 之前不在终止集，其加签子任务虽 created_at 晚于 T，但归属
	// 同伴的在途审批，不随本次收回作废。
	endReason := constants.EndReasonPrefixRecall
	if reason != "" {
		endReason = fmt.Sprintf("%s：%s", constants.EndReasonPrefixRecall, reason)
	}
	termIDs := map[string]bool{}
	for _, x := range tasks {
		if x.ID == t.ID || (x.Status != string(enums.TaskStatusActive) && x.Status != string(enums.TaskStatusPending)) {
			continue
		}
		if t.EndedAt == nil || x.CreatedAt.Before(*t.EndedAt) {
			continue
		}
		termIDs[x.ID] = true
	}
	for _, x := range tasks {
		if !termIDs[x.ID] || x.ParentID == nil || *x.ParentID == "" {
			continue
		}
		for _, p := range tasks {
			if p.ID == *x.ParentID && !termIDs[p.ID] && p.TaskDefKey == t.TaskDefKey {
				delete(termIDs, x.ID)
				break
			}
		}
	}
	notifyUsers := make([]string, 0)
	for _, x := range tasks {
		if !termIDs[x.ID] {
			continue
		}
		if x.Assignee != nil && *x.Assignee != "" {
			notifyUsers = append(notifyUsers, *x.Assignee)
		}
		x.Status = string(enums.TaskStatusTerminated)
		x.EndedAt = &now
		x.EndReason = &endReason
		x.UpdatedBy = &username
		x.UpdatedAt = &now
		if err := taskDAO.Update(ctx, x); err != nil {
			return fmt.Errorf("failed to terminate frontier task: %w", err)
		}
		if herr := hiTaskDAO.Create(ctx, taskToHiTask(x)); herr != nil {
			return fmt.Errorf("failed to archive frontier task: %w", herr)
		}
		if err := taskDAO.Delete(ctx, x.ID); err != nil {
			return fmt.Errorf("failed to delete frontier task: %w", err)
		}
	}

	// T 归档：status 保持 completed、end_reason 置 recalled——审批历史据此显示
	// 「已收回」，与通过/拒绝同属 end_reason 口径
	recallMark := string(enums.EndReasonRecalled)
	t.EndReason = &recallMark
	t.UpdatedBy = &username
	t.UpdatedAt = &now
	if herr := hiTaskDAO.Create(ctx, taskToHiTask(t)); herr != nil {
		return fmt.Errorf("failed to archive recalled task: %w", herr)
	}
	if err := taskDAO.Delete(ctx, t.ID); err != nil {
		return fmt.Errorf("failed to delete recalled task: %w", err)
	}

	// 会签/票签父任务在节点完成时被置 completed；收回后节点重新未决，回置 active。
	// ended_at/end_reason 要清成 NULL，GORM 结构体更新不写零值字段，须用 map Updates
	if t.ParentID != nil && *t.ParentID != "" {
		if p, perr := taskDAO.Get(ctx, *t.ParentID); perr == nil && p != nil &&
			p.Status == string(enums.TaskStatusCompleted) {
			updates := map[string]interface{}{
				"status":     string(enums.TaskStatusActive),
				"ended_at":   nil,
				"end_reason": nil,
				"updated_by": username,
				"updated_at": now,
			}
			if _, uerr := scope.Tx().WfTask.WithContext(ctx).
				Where(scope.Tx().WfTask.ID.Eq(*t.ParentID)).
				Updates(updates); uerr != nil {
				return fmt.Errorf("failed to revert parent task: %w", uerr)
			}
		}
	}

	// 重建收回人的待审任务；重审完成走节点既有完成判定推进，无需链路跳转
	recreated := buildRecalledTask(s.idGenerator, t, userID, username, now)
	if err := taskDAO.Create(ctx, recreated); err != nil {
		return fmt.Errorf("failed to recreate recalled task: %w", err)
	}

	listener := s.workflowEngine.GetTaskEventListener()
	if listener != nil {
		evtTaskID := recreated.ID
		evtDefKey := recreated.TaskDefKey
		evtParentID := ""
		if recreated.ParentID != nil {
			evtParentID = *recreated.ParentID
		}
		evtName := recreated.Name
		evtProcessID := recreated.ProcessID
		evtTenantID := instance.TenantID
		evtToUsers := notifyUsers
		evtFrom := userID
		scope.AfterCommit(func() error {
			DispatchTaskEvent(listener, TaskEvent{
				Type:         TaskEventRecalled,
				TaskID:       evtTaskID,
				TaskDefKey:   evtDefKey,
				ParentTaskID: evtParentID,
				InstanceID:   instanceID,
				ProcessID:    evtProcessID,
				TenantID:     evtTenantID,
				TaskName:     evtName,
				ToUsers:      evtToUsers,
				FromUser:     evtFrom,
				Reason:       reason,
				Timestamp:    time.Now(),
			}, ctx)
			// 重建任务沿用 assigned 事件，通知口径与新建任务一致
			DispatchTaskEvent(listener, TaskEvent{
				Type:         TaskEventAssigned,
				TaskID:       evtTaskID,
				TaskDefKey:   evtDefKey,
				ParentTaskID: evtParentID,
				InstanceID:   instanceID,
				ProcessID:    evtProcessID,
				TenantID:     evtTenantID,
				TaskName:     evtName,
				ToUsers:      []string{evtFrom},
				FromUser:     evtFrom,
				Timestamp:    time.Now(),
			}, ctx)
			return nil
		})
	}
	return nil
}

// findRecallableCompletedTask 返回 userID 最近一条已完成的 userTask 办理记录
// （会签/票签为子任务，父任务行无办理人天然不会命中）。
func findRecallableCompletedTask(tasks []*model.WfTask, userID string) *model.WfTask {
	var latest *model.WfTask
	for _, t := range tasks {
		if t == nil || t.TaskType != constants.TaskTypeUserTask ||
			t.Status != string(enums.TaskStatusCompleted) {
			continue
		}
		if t.Assignee == nil || *t.Assignee != userID {
			continue
		}
		if latest == nil || endedAtAfter(t, latest) {
			latest = t
		}
	}
	return latest
}

// evaluateRecallGuard 收回守卫（写路径与详情权限位共用，只读不改数据）：
// 更晚办理记录 → 停泊 → 自动化路径，任一不过即拒绝。
func evaluateRecallGuard(tasks []*model.WfTask, t *model.WfTask, chain *types.RuleChain) error {
	for _, x := range tasks {
		if x == nil || x.ID == t.ID {
			continue
		}
		if x.TaskType != constants.TaskTypeUserTask || x.Status != string(enums.TaskStatusCompleted) {
			continue
		}
		if !endedAtAfter(x, t) {
			continue
		}
		if inSameCountersignRound(t, x) {
			continue
		}
		return fmt.Errorf("%w: 存在更晚的办理记录，无法收回", ErrValidation)
	}

	parked := false
	for _, x := range tasks {
		if x != nil && x.TaskType == constants.TaskTypeUserTask &&
			(x.Status == string(enums.TaskStatusActive) || x.Status == string(enums.TaskStatusPending)) {
			// 候选池待认领同样是合法停泊（下一节点"无人办理"含未认领）；
			// 自动化在途窗口无任何任务行，仍被此校验挡住
			parked = true
			break
		}
	}
	if !parked {
		return fmt.Errorf("%w: 流程未停留在审批节点，无法收回", ErrValidation)
	}

	return checkRecallPath(chain, t.TaskDefKey)
}

// inSameCountersignRound 判断 x 是否与 T 同属一个会签轮次：同父的并行同侪子任务，
// 或 T 自己的父任务行（父任务归档时间晚于子任务，完成时间不代表有人办理）。
func inSameCountersignRound(t, x *model.WfTask) bool {
	if t.ParentID == nil || *t.ParentID == "" {
		return false
	}
	if x.ID == *t.ParentID {
		return true
	}
	return x.ParentID != nil && *x.ParentID == *t.ParentID
}

func endedAtAfter(a, b *model.WfTask) bool {
	if a.EndedAt == nil {
		return false
	}
	if b.EndedAt == nil {
		return true
	}
	return a.EndedAt.After(*b.EndedAt)
}

// checkRecallPath 从 T 的节点沿 Success 边向前遍历链定义，T 完成后链路已推进，
// 停泊点之前若途经自动化节点说明机器动作已发生，收回会造成副作用重放。
// userTask/end 截断；路由与抄送穿越；未知类型 fail-closed 拒绝。
func checkRecallPath(chain *types.RuleChain, fromDefKey string) error {
	if chain == nil || len(chain.Metadata.Nodes) == 0 {
		return fmt.Errorf("%w: 流程定义不可用，无法收回", ErrValidation)
	}
	nodeByID := make(map[string]*types.RuleNode, len(chain.Metadata.Nodes))
	for _, n := range chain.Metadata.Nodes {
		nodeByID[n.Id] = n
	}
	if _, ok := nodeByID[fromDefKey]; !ok {
		return fmt.Errorf("%w: 流程定义不可用，无法收回", ErrValidation)
	}
	successors := make(map[string][]string)
	for _, conn := range chain.Metadata.Connections {
		if conn.Type == types.Success || conn.Type == "" {
			successors[conn.FromId] = append(successors[conn.FromId], conn.ToId)
		}
	}

	visited := map[string]bool{fromDefKey: true}
	queue := make([]string, 0, len(successors[fromDefKey]))
	for _, id := range successors[fromDefKey] {
		if !visited[id] {
			visited[id] = true
			queue = append(queue, id)
		}
	}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		node, ok := nodeByID[id]
		if !ok {
			// 连接指向不存在的节点属畸形定义，fail-closed
			return fmt.Errorf("%w: 流程定义不可用，无法收回", ErrValidation)
		}
		switch {
		case node.Type == constants.NodeTypeUserTask || node.Type == constants.NodeTypeEnd:
			// 停泊点或终点：流程停在人工任务上，该路径安全
		case recallTransparentNodeTypes[node.Type]:
			for _, next := range successors[id] {
				if !visited[next] {
					visited[next] = true
					queue = append(queue, next)
				}
			}
		default:
			return fmt.Errorf("%w: 审批之后存在自动化节点，无法收回", ErrValidation)
		}
	}
	return nil
}

// buildRecalledTask 从 T 的快照重建收回人的待审任务。到期时间沿用 T（与加签
// 继承同口径）；variables 剔除 approved/comment 约定键——Complete 会消费它们，
// 残留会让重建任务带着旧投票结果被静默自动通过；_sequentialAssignees 缓存
// 保留，顺序会签推进依赖。
func buildRecalledTask(gen IDGenerator, t *model.WfTask, userID, username string, now time.Time) *model.WfTask {
	assignee := userID
	return &model.WfTask{
		ID:                gen.GenerateTaskID(),
		ProcessInstanceID: t.ProcessInstanceID,
		ProcessID:         t.ProcessID,
		ParentID:          t.ParentID,
		TaskDefKey:        t.TaskDefKey,
		Name:              t.Name,
		Description:       t.Description,
		FormKey:           t.FormKey,
		TaskType:          t.TaskType,
		Status:            string(enums.TaskStatusActive),
		Assignee:          &assignee,
		Owner:             t.Owner,
		Priority:          t.Priority,
		DueDate:           t.DueDate,
		Variables:         stripApprovalResultKeys(t.Variables),
		SequenceOrder:     t.SequenceOrder,
		ApprovalType:      t.ApprovalType,
		ApprovalRule:      t.ApprovalRule,
		TenantID:          t.TenantID,
		CreatedBy:         username,
		CreatedAt:         now,
	}
}

func stripApprovalResultKeys(vars *string) *string {
	m, err := ParseVariablesJSON(vars)
	// 解析失败宁可丢掉全部变量（顺序缓存可由候选人配置重解析兜底），
	// 也不能原样放行——残留 approved=true 会让重建任务被旧投票静默自动通过
	if err != nil || len(m) == 0 {
		return nil
	}
	changed := false
	for _, key := range []string{constants.VarsApproved, constants.VarsComment} {
		if _, ok := m[key]; ok {
			delete(m, key)
			changed = true
		}
	}
	if !changed {
		return vars
	}
	if len(m) == 0 {
		return nil
	}
	out, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	s := string(out)
	return &s
}
