// This file contains the withdraw / return operations on TaskServiceImpl:
// Withdraw (with its Internal variant and the shared terminateProcessInstanceInTx
// helper that reuses the caller's already-held instance lock) and Return
// (with its Internal variant that archives the returned task and jumps to
// the target activity).

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// Withdraw 撤回（申请人撤回已提交的申请）
//
// Withdraw 内部调用 RuntimeService.TerminateProcessInstance 终止实例（跨服务级联），
// 通过 withdrawInternal 调用 runtimeService 的 TerminateInTx 内部方法以避免重复加锁——
// 该方法假定调用方已持有实例行锁。
func (s *TaskServiceImpl) Withdraw(ctx context.Context, actor Actor, taskID, reason string) error {
	ctx = bindActor(ctx, actor)
	userID := actor.UserID
	if taskID == "" || userID == "" {
		return fmt.Errorf("task ID and user ID cannot be empty")
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	if task.ProcessInstanceID == nil || *task.ProcessInstanceID == "" {
		return fmt.Errorf("task has no associated process instance")
	}
	// 设计器显式禁用 withdraw → 拒绝。流程级与节点级都校验：开关写在流程级
	// additionalInfo，节点级配置里不存在；两级都在实例行锁事务外解析。
	if err := s.requireWithdrawEnabled(ctx, *task.ProcessInstanceID, task); err != nil {
		return err
	}

	instanceID := *task.ProcessInstanceID
	return WithInstanceTx(ctx, s.taskDAO.Underlying(), instanceID, func(scope *InstanceScope) error {
		return s.withdrawInternal(ctx, scope, taskID, userID, reason, false)
	})
}

// requireWithdrawEnabled 撤回开关的两级校验：流程级（设计器唯一写入层级）对齐
// 按钮位口径，节点级保留给手工配置的节点开关。实例须存在（查不到即拒绝）。
func (s *TaskServiceImpl) requireWithdrawEnabled(ctx context.Context, instanceID string, task *model.WfTask) error {
	instance, err := dao.NewInstanceDAOWithQuery(s.taskDAO.Underlying()).Get(ctx, instanceID)
	if err != nil || instance == nil {
		return fmt.Errorf("%w: process instance", ErrNotFound)
	}
	if err := s.requireProcessActionEnabled(ctx, instance.ProcessID, "withdraw"); err != nil {
		return err
	}
	return s.requireActionEnabled(ctx, task, "withdraw")
}

// WithdrawByInstance 按流程实例撤回（发起人视角入口）。
// 发起人通常不是当前审批节点的办理人，没有 currentUserActivityTask，无法走 task 维度 Withdraw。
// 此方法按 instanceID 取当前 active 任务，复用 withdrawInternal（含 StartUserID 校验 + 终止实例）。
func (s *TaskServiceImpl) WithdrawByInstance(ctx context.Context, actor Actor, instanceID, reason string) error {
	ctx = bindActor(ctx, actor)
	userID, isAdmin := actor.UserID, isWorkflowAdmin(&actor)
	if instanceID == "" || userID == "" {
		return fmt.Errorf("instance ID and user ID cannot be empty")
	}

	// 流程级撤回开关在进锁前校验：流程定义读取走默认连接，锁内调用属于 tx 逃逸
	// （单写库与外层事务互等）。ProcessID 在实例行上不可变，锁外解析足够；
	// 实例状态与属主在锁内仍有权威复核。
	instance, err := dao.NewInstanceDAOWithQuery(s.taskDAO.Underlying()).Get(ctx, instanceID)
	if err != nil || instance == nil {
		return fmt.Errorf("%w: process instance", ErrNotFound)
	}
	if err := s.requireProcessActionEnabled(ctx, instance.ProcessID, "withdraw"); err != nil {
		return err
	}

	return WithInstanceTx(ctx, s.taskDAO.Underlying(), instanceID, func(scope *InstanceScope) error {
		taskDAO := scope.Tasks()
		q := &dto.TaskQuery{
			InstanceID:     &instanceID,
			ParentIDIsNull: true,
		}
		q.Status = []string{string(enums.TaskStatusActive)}
		tasks, _, err := taskDAO.List(ctx, q)
		if err != nil {
			return fmt.Errorf("failed to list active tasks: %w", err)
		}
		if len(tasks) > 0 {
			// 设计器显式禁用 withdraw（节点级手工配置）→ 拒绝；流程级开关已在锁外校验。
			// 逐活跃任务校验：并行分支上任一节点禁用撤回即整体拒绝，只查首个任务
			// 会漏掉其余分支的禁用配置。
			for _, t := range tasks {
				if err := s.requireActionEnabled(ctx, t, "withdraw"); err != nil {
					return err
				}
			}
			return s.withdrawInternal(ctx, scope, tasks[0].ID, userID, reason, isAdmin)
		}
		return s.withdrawBeforeFirstTask(ctx, scope, instanceID, userID, reason, isAdmin)
	})
}

// withdrawBeforeFirstTask 处理“发起后首个任务尚未落库”的撤回：start 提交后异步驱动
// 首节点，提交成功到首任务创建之间存在窗口，此刻撤回不能按无任务拒绝。直接按实例
// 维度终止（与正常撤回同落 terminated 终态、同撤回原因口径）。仅发起人/管理员、
// 且实例仍为 Active 时允许；WithInstanceTx 的终态闸保证终止后链的后续驱动不会再
// 落出任务。
func (s *TaskServiceImpl) withdrawBeforeFirstTask(ctx context.Context, scope *InstanceScope, instanceID, userID, reason string, isAdmin bool) error {
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
	if !isInstanceStarterOrAdmin(instance, userID, isAdmin) {
		return fmt.Errorf("%w: only the process initiator (or admin) can withdraw", ErrPermissionDenied)
	}
	if instance.Status != string(enums.InstanceStatusActive) {
		return fmt.Errorf("%w: only active instances can be withdrawn, current status: %s", ErrValidation, instance.Status)
	}
	// 流程级 withdraw 开关已由 WithdrawByInstance 在进锁前校验（流程定义读取
	// 不能发生在实例行锁事务内），这里只保留状态与属主复核。

	terminateReason := constants.EndReasonPrefixWithdrawn
	if reason != "" {
		terminateReason = fmt.Sprintf("%s：%s", constants.EndReasonPrefixWithdrawn, reason)
	}
	withdrawCtx := WithEventSource(ctx, EventSourceWithdraw)
	terminatedEvts, err := terminateProcessInstanceInTx(withdrawCtx, s.workflowEngine.GetRuntimeService(), scope.Tx(), instanceID, terminateReason)
	if err != nil {
		return fmt.Errorf("failed to terminate process instance after withdraw: %w", err)
	}
	if s.workflowEngine.GetTaskEventListener() != nil {
		for _, terminatedEvt := range terminatedEvts {
			evt := *terminatedEvt
			scope.AfterCommit(func() error {
				DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), evt, withdrawCtx)
				return nil
			})
		}
	}
	return nil
}

// withdrawInternal 在已持有实例行锁的事务内执行 Withdraw 实际逻辑。
// 内部调用 runtimeService 的 TerminateInTx（同样假定持锁）——避免重新进入
// WithInstanceTx 导致重复 FOR UPDATE 或 savepoint 嵌套。
func (s *TaskServiceImpl) withdrawInternal(ctx context.Context, scope *InstanceScope, taskID, userID, reason string, isAdmin bool) error {
	taskDAO := scope.Tasks()
	hiTaskDAO := scope.HiTasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 幂等：已经撤回过
	if task.Status == string(enums.TaskStatusWithdrawn) {
		return nil
	}

	if task.Status != string(enums.TaskStatusActive) {
		return fmt.Errorf("%w: only active tasks can be withdrawn, current status: %s", ErrValidation, task.Status)
	}

	if task.ProcessInstanceID == nil || *task.ProcessInstanceID == "" {
		return fmt.Errorf("task has no associated process instance")
	}
	// 用 tx-bound DAO 读 instance，避免逃出行锁。
	runtimeService := s.workflowEngine.GetRuntimeService()
	instDAO := scope.Instances()
	instance, err := instDAO.Get(ctx, *task.ProcessInstanceID)
	if err != nil {
		return fmt.Errorf("failed to get process instance: %w", err)
	}
	if instance == nil {
		return fmt.Errorf("%w: process instance", ErrNotFound)
	}
	if u := GetUserFromCtx(ctx); u != nil && instance.TenantID != u.TenantID {
		return fmt.Errorf("%w: process instance", ErrNotFound)
	}
	if !isInstanceStarterOrAdmin(instance, userID, isAdmin) {
		return fmt.Errorf("%w: only the process initiator (or admin) can withdraw", ErrPermissionDenied)
	}

	now := time.Now()
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	endReason := string(enums.EndReasonWithdrawn)
	if reason != "" {
		endReason = string(enums.EndReasonWithdrawn) + ": " + reason
	}
	task.Status = string(enums.TaskStatusWithdrawn)
	task.EndedAt = &now
	task.EndReason = &endReason
	task.UpdatedBy = &username
	task.UpdatedAt = &now

	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to withdraw task: %w", err)
	}

	hiTask := taskToHiTask(task)
	if herr := hiTaskDAO.Create(ctx, hiTask); herr != nil {
		return fmt.Errorf("failed to archive withdrawn task: %w", herr)
	}
	if err := taskDAO.Delete(ctx, task.ID); err != nil {
		return fmt.Errorf("failed to delete withdrawn task: %w", err)
	}

	// 终止同节点的其他活跃任务
	if task.ProcessInstanceID != nil && task.TaskDefKey != "" {
		query := &dto.TaskQuery{
			InstanceID: task.ProcessInstanceID,
			TaskDefKey: task.TaskDefKey,
			PageRequest: dto.PageRequest{
				Status: []string{string(enums.TaskStatusActive), string(enums.TaskStatusPending)},
			},
		}
		if otherTasks, err := listAllTasks(ctx, taskDAO, query); err == nil {
			for _, t := range otherTasks {
				if t.ID != task.ID {
					t.Status = string(enums.TaskStatusTerminated)
					t.EndedAt = &now
					t.EndReason = &endReason
					t.UpdatedBy = &username
					t.UpdatedAt = &now
					if err := taskDAO.Update(ctx, t); err != nil {
						logrus.Warnf("failed to terminate sibling task %s after withdraw: %v", t.ID, err)
					}
					if herr := hiTaskDAO.Create(ctx, taskToHiTask(t)); herr != nil {
						logrus.Warnf("failed to archive terminated task %s: %v", t.ID, herr)
					}
					if err := taskDAO.Delete(ctx, t.ID); err != nil {
						logrus.Warnf("failed to delete terminated task %s after withdraw: %v", t.ID, err)
					}
				}
			}
		}
	}

	// 终止整个流程实例：调用 runtimeService 的 InTx 版本，复用当前 tx（已持锁）
	// ctx 标记来源为撤回，terminated 事件据此携带 Source=withdraw
	terminateReason := constants.EndReasonPrefixWithdrawn
	if reason != "" {
		terminateReason = fmt.Sprintf("%s：%s", constants.EndReasonPrefixWithdrawn, reason)
	}
	withdrawCtx := WithEventSource(ctx, EventSourceWithdraw)
	terminatedEvts, err := terminateProcessInstanceInTx(withdrawCtx, runtimeService, scope.Tx(), *task.ProcessInstanceID, terminateReason)
	if err != nil {
		logrus.Warnf("failed to terminate instance %s after withdraw: %v", *task.ProcessInstanceID, err)
		return fmt.Errorf("failed to terminate process instance after withdraw: %w", err)
	}
	// terminated 事件先于 withdrawn 注册，保证监听方按此顺序收到
	if s.workflowEngine.GetTaskEventListener() != nil {
		for _, terminatedEvt := range terminatedEvts {
			evt := *terminatedEvt
			scope.AfterCommit(func() error {
				DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), evt, withdrawCtx)
				return nil
			})
		}
	}

	// 撤回事件：AfterCommit 派发，回滚不产生幽灵事件。
	// 上面 terminateProcessInstanceInTx 已在同一事务终止实例，事件发出时必为终态
	if s.workflowEngine.GetTaskEventListener() != nil {
		evtTaskID := task.ID
		evtTaskDefKey := task.TaskDefKey
		evtInstanceID := *task.ProcessInstanceID
		evtProcessID := task.ProcessID
		evtTenantID := instance.TenantID
		evtProcessName := instance.Name
		evtStartUser := instance.StartUserID
		evtReason := reason
		evtFromUser := userID
		scope.AfterCommit(func() error {
			DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), TaskEvent{
				Type:                TaskEventWithdrawn,
				TaskID:              evtTaskID,
				TaskDefKey:          evtTaskDefKey,
				InstanceID:          evtInstanceID,
				ProcessID:           evtProcessID,
				TenantID:            evtTenantID,
				ProcessName:         evtProcessName,
				StartUserID:         evtStartUser,
				InstanceStatusAfter: string(enums.InstanceStatusTerminated),
				FromUser:            evtFromUser,
				Reason:              evtReason,
				Source:              EventSourceWithdraw,
				Timestamp:           time.Now(),
			}, withdrawCtx)
			return nil
		})
	}
	return nil
}

// terminateProcessInstanceInTx 复用调用方事务调用 RuntimeService 的内部终止逻辑。
// 通过类型断言访问 *RuntimeServiceImpl.TerminateInTx。非该实现的 runtime（mock 等）
// 直接报错拒绝：回退路径会在事务外用全局连接重入同一实例行，与调用方已持有的
// FOR UPDATE 锁互等自锁挂死，宁可显式失败也不走这条路径。
// 返回待派发的 terminated 事件列表（被终止的每个实例各一条）。
func terminateProcessInstanceInTx(ctx context.Context, runtime RuntimeService, tx *query.Query, instanceID, reason string) ([]*TaskEvent, error) {
	impl, ok := runtime.(*RuntimeServiceImpl)
	if !ok {
		return nil, fmt.Errorf("runtime service %T does not support in-tx termination of instance %s", runtime, instanceID)
	}
	return impl.TerminateInTx(ctx, tx, instanceID, reason)
}

// Return 退回（将任务退回到指定节点）。目标口径：本实例中已运行过的 userTask
// 节点、当前节点的拓扑上游且非自身、重执行区域不穿并行网关，与详情接口
// returnableNodes 列表同源。
func (s *TaskServiceImpl) Return(ctx context.Context, actor Actor, taskID, targetActivityID, reason string) error {
	ctx = bindActor(ctx, actor)
	userID := actor.UserID
	if taskID == "" || targetActivityID == "" || userID == "" {
		return fmt.Errorf("task ID, target activity ID and user ID cannot be empty")
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 租户隔离：跨租户任务按 not found 隐藏，与 Withdraw/Claim 同口径，不泄露任务存在性。
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 设计器显式禁用 return → 拒绝（actionPermissions 解析失败同样 fail-closed 拒绝）
	if err := s.requireActionEnabled(ctx, task, "return"); err != nil {
		return err
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return fmt.Errorf("task has no associated process instance")
	}

	// 目标校验依赖流程拓扑：规则链在进事务前加载并构建图视图，解析失败
	// fail-closed 拒绝（事务内走非 tx 连接读流程表会与外层事务互等死锁）
	graph, gerr := s.returnTargetGraph(ctx, task)
	if gerr != nil {
		return gerr
	}

	return WithInstanceTx(ctx, s.taskDAO.Underlying(), instanceID, func(scope *InstanceScope) error {
		return s.returnInternal(ctx, scope, graph, taskID, targetActivityID, userID, reason)
	})
}

func (s *TaskServiceImpl) returnInternal(ctx context.Context, scope *InstanceScope, graph *ChainGraph, taskID, targetActivityID, userID, reason string) error {
	taskDAO := scope.Tasks()
	hiTaskDAO := scope.HiTasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 幂等：已经退回过
	if task.Status == string(enums.TaskStatusReturned) {
		return nil
	}

	if task.Status != string(enums.TaskStatusActive) {
		return fmt.Errorf("only active tasks can be returned, current status: %s", task.Status)
	}

	// 校验退回目标为合法上游节点（已运行/拓扑上游/非自身/区域不跨并行网关），
	// 且操作人为该节点任务的受理人或候选人
	if err := s.requireReturnTarget(ctx, scope, graph, task, targetActivityID, userID); err != nil {
		return err
	}

	task.Status = string(enums.TaskStatusReturned)
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	now := time.Now()
	// 退回原因落 end_reason（withdrawn 同款格式），时间线与打印据此带出退回说明
	endReason := string(enums.EndReasonReturned)
	if reason != "" {
		endReason = string(enums.EndReasonReturned) + ": " + reason
	}
	task.EndReason = &endReason
	task.EndedAt = &now
	task.UpdatedBy = &username
	task.UpdatedAt = &now

	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to return task: %w", err)
	}

	hiTask := taskToHiTask(task)
	if herr := hiTaskDAO.Create(ctx, hiTask); herr != nil {
		return fmt.Errorf("failed to archive returned task: %w", herr)
	}
	if err := taskDAO.Delete(ctx, task.ID); err != nil {
		return fmt.Errorf("failed to delete returned task: %w", err)
	}

	// 终止同节点的其他活跃任务并归档。
	// 必须按 TaskDefKey 过滤（与 withdrawInternal 一致）：fork 并行实例里
	// 其他分支的活跃任务不受本次退回影响，全实例杀会误终止它们
	if task.ProcessInstanceID != nil && task.TaskDefKey != "" {
		activeQuery := &dto.TaskQuery{
			InstanceID: task.ProcessInstanceID,
			TaskDefKey: task.TaskDefKey,
			PageRequest: dto.PageRequest{
				Status: []string{string(enums.TaskStatusActive), string(enums.TaskStatusPending)},
			},
		}
		if activeTasks, aerr := listAllTasks(ctx, taskDAO, activeQuery); aerr == nil {
			for _, t := range activeTasks {
				if t.ID != task.ID {
					t.Status = string(enums.TaskStatusTerminated)
					t.EndedAt = &now
					terminateReason := fmt.Sprintf("%s：流程退回至节点 %s", constants.EndReasonPrefixReturnedVoid, targetActivityID)
					t.EndReason = &terminateReason
					t.UpdatedBy = &username
					t.UpdatedAt = &now
					if err := taskDAO.Update(ctx, t); err != nil {
						logrus.Warnf("failed to terminate sibling task %s after return: %v", t.ID, err)
					}
					if herr := hiTaskDAO.Create(ctx, taskToHiTask(t)); herr != nil {
						logrus.Warnf("failed to archive terminated task %s: %v", t.ID, herr)
					}
					if err := taskDAO.Delete(ctx, t.ID); err != nil {
						logrus.Warnf("failed to delete terminated task %s after return: %v", t.ID, err)
					}
				}
			}
		}
	}

	// 清理重执行区域（目标节点到当前节点）的全部遗留任务：目标与中间节点的旧票
	// 不清，流程重跑经过时会被旧 Completed 任务判为已完成而静默跳过；当前节点
	// 此时只剩已完成旧票（操作人票与在途兄弟票已随 returned/terminated 归档删除），
	// 顺序审批/会签节点的旧票也必须清，否则重入时按旧进度续跑而非整节点重跑。
	// 清理失败不阻断跳转（best-effort），与驳回回跳同口径。
	if task.ProcessInstanceID != nil {
		for _, nodeID := range returnRegionNodes(graph, targetActivityID, task.TaskDefKey) {
			if _, err := s.supersedeNodeTasksInternal(ctx, scope, *task.ProcessInstanceID, nodeID, ""); err != nil {
				logrus.WithError(err).WithField("node", nodeID).
					Warn("supersede stale tasks before return jump failed; stale tasks may cause silent misjudgment")
			}
		}
	}

	// 退回事件：先注册，保证监听器收到 returned 后才可能被 ExecuteNext 的后续事件追上
	if listener := s.workflowEngine.GetTaskEventListener(); listener != nil {
		instID := ""
		if task.ProcessInstanceID != nil {
			instID = *task.ProcessInstanceID
		}
		evtProcessName, evtStartUser := "", ""
		if inst, iErr := scope.Instances().Get(ctx, instID); iErr == nil && inst != nil {
			evtProcessName = inst.Name
			evtStartUser = inst.StartUserID
		}
		evt := TaskEvent{
			Type:                TaskEventReturned,
			TaskID:              task.ID,
			TaskDefKey:          task.TaskDefKey,
			InstanceID:          instID,
			ProcessID:           task.ProcessID,
			TenantID:            task.TenantID,
			ProcessName:         evtProcessName,
			StartUserID:         evtStartUser,
			InstanceStatusAfter: string(enums.InstanceStatusActive),
			TaskName:            task.Name,
			FromUser:            userID,
			Reason:              reason,
			Timestamp:           time.Now(),
		}
		scope.AfterCommit(func() error {
			DispatchTaskEvent(listener, evt, ctx)
			return nil
		})
	}

	// 触发流程跳转：ExecuteNext 推迟到事务提交后执行，避免 rulego OnMsg 同步副作用
	// 重入 WithInstanceTx 抢同一行的 FOR UPDATE 锁。孤儿任务无实例可驱动，跳过注册。
	inst := task.ProcessInstanceID
	if inst != nil {
		scope.AfterCommit(func() error {
			s.driveAfterCommit(ctx, *inst, targetActivityID, nil)
			return nil
		})
	}
	return nil
}

// SupersedeNodeTasks 把 (instanceID, taskDefKey) 命中的全部任务归档到 wf_hi_task
// 并从 wf_task 删除，返回被归档的任务数。用于驳回回跳（toPrev/toStarter/toNode）
// 重新进入目标 userTask 前清理上一轮遗留的 Completed 任务，避免重入时被静默自动通过。
//
// 详见 TaskService 接口注释。归档+删除模式与 returnInternal 一致，仅作用范围不同
// （returnInternal 是按"当前任务→目标节点"清理；这里按显式 (instance,defKey) 清理，
// 供节点层 jumpToNode 在 ExecuteNext 前调用）。
func (s *TaskServiceImpl) SupersedeNodeTasks(ctx context.Context, instanceID, taskDefKey, reason string) (int, error) {
	if instanceID == "" || taskDefKey == "" {
		return 0, fmt.Errorf("instanceID and taskDefKey cannot be empty")
	}
	archived := 0
	if err := WithInstanceTx(ctx, s.taskDAO.Underlying(), instanceID, func(scope *InstanceScope) error {
		n, err := s.supersedeNodeTasksInternal(ctx, scope, instanceID, taskDefKey, reason)
		if err != nil {
			return err
		}
		archived = n
		return nil
	}); err != nil {
		return 0, err
	}
	return archived, nil
}

// supersedeNodeTasksInternal 在已持有实例行锁的事务内执行 SupersedeNodeTasks 实际逻辑。
// 对命中的每个任务：status 落 terminated（与同节点 sibling 终止路径同口径，历史表
// 按 status 维度的消费不得把作废票算成有效在途/已审）→ end_reason 改写作废标记
// （原审批结果跟在冒号后保留供审计追溯，无原值时补充调用方说明）→ 写 wf_hi_task
// 归档 → 从 wf_task 删除。单任务失败只记录告警并跳过（best-effort），不中断整体
// ——一个坏行不应让整次回跳失败。
func (s *TaskServiceImpl) supersedeNodeTasksInternal(ctx context.Context, scope *InstanceScope, instanceID, taskDefKey, reason string) (int, error) {
	taskDAO := scope.Tasks()
	hiTaskDAO := scope.HiTasks()

	query := &dto.TaskQuery{
		InstanceID: &instanceID,
		TaskDefKey: taskDefKey,
	}
	tasks, err := listAllTasks(ctx, taskDAO, query)
	if err != nil {
		return 0, fmt.Errorf("failed to list tasks for supersede: %w", err)
	}

	now := time.Now()
	archived := 0
	for _, t := range tasks {
		voided := constants.EndReasonPrefixReturnedVoid
		if t.EndReason != nil && *t.EndReason != "" {
			voided += "：" + *t.EndReason
		} else if reason != "" {
			voided += "：" + reason
		}
		t.Status = string(enums.TaskStatusTerminated)
		t.EndReason = &voided
		if t.EndedAt == nil {
			t.EndedAt = &now
		}
		if u := GetUserFromCtx(ctx); u != nil {
			userName := u.UserName
			t.UpdatedBy = &userName
		}
		t.UpdatedAt = &now

		if herr := hiTaskDAO.Create(ctx, taskToHiTask(t)); herr != nil {
			logrus.WithError(herr).WithField("taskId", t.ID).
				Warn("failed to archive superseded task before reject jump")
			continue
		}
		if err := taskDAO.Delete(ctx, t.ID); err != nil {
			logrus.WithError(err).WithField("taskId", t.ID).
				Warn("failed to delete superseded task before reject jump")
			continue
		}
		archived++
	}
	return archived, nil
}
