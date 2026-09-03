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
	// 设计器显式禁用 withdraw → 拒绝
	if err := s.requireActionEnabled(ctx, task, "withdraw"); err != nil {
		return err
	}

	instanceID := *task.ProcessInstanceID
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.withdrawInternal(ctx, scope, taskID, userID, reason, false)
	})
}

// WithdrawByInstance 按流程实例撤回（发起人视角入口）。
// 发起人通常不是当前审批节点的办理人，没有 currentUserActivityTask，无法走 task 维度 Withdraw。
// 此方法按 instanceID 取当前 active 任务，复用 withdrawInternal（含 StartUserID 校验 + 终止实例）。
func (s *TaskServiceImpl) WithdrawByInstance(ctx context.Context, actor Actor, instanceID, reason string) error {
	ctx = bindActor(ctx, actor)
	userID, isSuperAdmin := actor.UserID, actor.SuperAdmin
	if instanceID == "" || userID == "" {
		return fmt.Errorf("instance ID and user ID cannot be empty")
	}

	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
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
		if len(tasks) == 0 {
			return fmt.Errorf("%w: no active task to withdraw for instance %s", ErrValidation, instanceID)
		}
		// 设计器显式禁用 withdraw → 拒绝
		if err := s.requireActionEnabled(ctx, tasks[0], "withdraw"); err != nil {
			return err
		}
		return s.withdrawInternal(ctx, scope, tasks[0].ID, userID, reason, isSuperAdmin)
	})
}

// withdrawInternal 在已持有实例行锁的事务内执行 Withdraw 实际逻辑。
// 内部调用 runtimeService 的 TerminateInTx（同样假定持锁）——避免重新进入
// WithInstanceTx 导致重复 FOR UPDATE 或 savepoint 嵌套。
func (s *TaskServiceImpl) withdrawInternal(ctx context.Context, scope *InstanceScope, taskID, userID, reason string, isSuperAdmin bool) error {
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
	if instance.StartUserID != userID && !isSuperAdmin {
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
		}
		query.Status = []string{string(enums.TaskStatusActive), string(enums.TaskStatusPending)}
		otherTasks, _, err := taskDAO.List(ctx, query)
		if err == nil {
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
	terminatedEvt, err := terminateProcessInstanceInTx(withdrawCtx, runtimeService, scope.Tx(), *task.ProcessInstanceID, terminateReason)
	if err != nil {
		logrus.Warnf("failed to terminate instance %s after withdraw: %v", *task.ProcessInstanceID, err)
		return fmt.Errorf("failed to terminate process instance after withdraw: %w", err)
	}
	// terminated 事件先于 withdrawn 注册，保证监听方按此顺序收到
	if terminatedEvt != nil && s.workflowEngine.GetTaskEventListener() != nil {
		evt := *terminatedEvt
		scope.AfterCommit(func() error {
			DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), evt, withdrawCtx)
			return nil
		})
	}

	// 撤回事件：AfterCommit 派发，回滚不产生幽灵事件
	if s.workflowEngine.GetTaskEventListener() != nil {
		evtTaskID := task.ID
		evtTaskDefKey := task.TaskDefKey
		evtInstanceID := *task.ProcessInstanceID
		evtProcessID := task.ProcessID
		evtTenantID := instance.TenantID
		evtReason := reason
		evtFromUser := userID
		scope.AfterCommit(func() error {
			DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), TaskEvent{
				Type:       TaskEventWithdrawn,
				TaskID:     evtTaskID,
				TaskDefKey: evtTaskDefKey,
				InstanceID: evtInstanceID,
				ProcessID:  evtProcessID,
				TenantID:   evtTenantID,
				FromUser:   evtFromUser,
				Reason:     evtReason,
				Source:     EventSourceWithdraw,
				Timestamp:  time.Now(),
			}, withdrawCtx)
			return nil
		})
	}
	return nil
}

// terminateProcessInstanceInTx 复用调用方事务调用 RuntimeService 的内部终止逻辑。
// 通过类型断言访问 *RuntimeServiceImpl.TerminateInTx；如果 runtime 是 mock 或
// 其他实现，回退到重新进入 TerminateProcessInstance（在 GORM savepoint 下会
// 重复 FOR UPDATE，但行为正确）。
// 返回待派发的 terminated 事件（回退路径已在自身提交后派发，返回 nil）。
func terminateProcessInstanceInTx(ctx context.Context, runtime RuntimeService, tx *query.Query, instanceID, reason string) (*TaskEvent, error) {
	if impl, ok := runtime.(*RuntimeServiceImpl); ok {
		return impl.TerminateInTx(ctx, tx, instanceID, reason)
	}
	// 显式 actor：沿用 ctx 已绑定身份（撤回/减签触发人），无身份时按系统动作处理
	actor := ActorFromCtx(ctx)
	if err := runtime.TerminateProcessInstance(ctx, actor, instanceID, reason); err != nil {
		return nil, err
	}
	return nil, nil
}

// Return 退回（将任务退回到指定节点）
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

	// 设计器显式禁用 return → 拒绝（actionPermissions 解析失败时降级放行）
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
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.returnInternal(ctx, scope, taskID, targetActivityID, userID, reason)
	})
}

func (s *TaskServiceImpl) returnInternal(ctx context.Context, scope *InstanceScope, taskID, targetActivityID, userID, reason string) error {
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

	// 校验退回目标必须是上一个 userTask 节点，且操作人为该节点任务的受理人或候选人
	if err := s.requireReturnTarget(ctx, scope, task, targetActivityID, userID); err != nil {
		return err
	}

	task.Status = string(enums.TaskStatusReturned)
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	now := time.Now()
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
		}
		activeQuery.Status = []string{string(enums.TaskStatusActive), string(enums.TaskStatusPending)}
		activeTasks, _, aerr := taskDAO.List(ctx, activeQuery)
		if aerr == nil {
			for _, t := range activeTasks {
				if t.ID != task.ID {
					t.Status = string(enums.TaskStatusTerminated)
					t.EndedAt = &now
					terminateReason := fmt.Sprintf("流程退回到节点 %s", targetActivityID)
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

	// 清理目标节点的所有任务
	if task.ProcessInstanceID != nil {
		targetQuery := &dto.TaskQuery{
			InstanceID: task.ProcessInstanceID,
			TaskDefKey: targetActivityID,
		}
		targetTasks, _, terr := taskDAO.List(ctx, targetQuery)
		if terr == nil {
			for _, t := range targetTasks {
				if herr := hiTaskDAO.Create(ctx, taskToHiTask(t)); herr != nil {
					logrus.Warnf("failed to archive task %s before return: %v", t.ID, herr)
				}
				_ = taskDAO.Delete(ctx, t.ID)
			}
		}
	}

	// 退回事件：先注册，保证监听器收到 returned 后才可能被 ExecuteNext 的后续事件追上
	if listener := s.workflowEngine.GetTaskEventListener(); listener != nil {
		instID := ""
		if task.ProcessInstanceID != nil {
			instID = *task.ProcessInstanceID
		}
		evt := TaskEvent{
			Type:       TaskEventReturned,
			TaskID:     task.ID,
			TaskDefKey: task.TaskDefKey,
			InstanceID: instID,
			ProcessID:  task.ProcessID,
			TenantID:   task.TenantID,
			TaskName:   task.Name,
			FromUser:   userID,
			Reason:     reason,
			Timestamp:  time.Now(),
		}
		scope.AfterCommit(func() error {
			DispatchTaskEvent(listener, evt, ctx)
			return nil
		})
	}

	// 触发流程跳转：ExecuteNext 推迟到事务提交后执行，避免 rulego OnMsg 同步副作用
	// 重入 WithInstanceTx 抢同一行的 FOR UPDATE 锁。
	inst := task.ProcessInstanceID
	scope.AfterCommit(func() error {
		return s.workflowEngine.GetRuntimeServiceInternal().ExecuteNext(ctx, *inst, targetActivityID, nil)
	})
	return nil
}

// SupersedeNodeTasks 把 (instanceID, taskDefKey) 命中的全部任务归档到 wf_hi_task
// 并从 wf_task 删除，返回被归档的任务数。用于驳回回跳（rejectToPrev/Starter/Node）
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
	if err := WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
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
// 对命中的每个任务：写 wf_hi_task 归档（EndReason 标记 superseded）→ 从 wf_task 删除。
// 单个任务归档/删除失败只记录告警并跳过（best-effort），不中断整体——与 returnInternal
// 的 sibling 清理行为一致，避免一个坏行让整个驳回回跳失败。
func (s *TaskServiceImpl) supersedeNodeTasksInternal(ctx context.Context, scope *InstanceScope, instanceID, taskDefKey, reason string) (int, error) {
	taskDAO := scope.Tasks()
	hiTaskDAO := scope.HiTasks()

	query := &dto.TaskQuery{
		InstanceID: &instanceID,
		TaskDefKey: taskDefKey,
	}
	tasks, _, err := taskDAO.List(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to list tasks for supersede: %w", err)
	}

	now := time.Now()
	archived := 0
	for _, t := range tasks {
		// 记录 superseded 归档原因（若已有 EndReason 则保留原审批结果，便于审计区分）
		if t.EndReason == nil || *t.EndReason == "" {
			supersededReason := reason
			if supersededReason == "" {
				supersededReason = "superseded_by_reject_jump"
			}
			t.EndReason = &supersededReason
		}
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
