// This file contains task status / property mutation methods on TaskServiceImpl:
// SetPriority, SetDueDate, SuspendTask, and ActivateTask (each with its
// WithInstanceTx-routed Internal variant).

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/rulego/gflow-engine/types/enums"
)

// SetPriority 设置任务优先级
func (s *TaskServiceImpl) SetPriority(ctx context.Context, actor Actor, taskID string, priority int) error {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	// 租户校验：防止跨租户修改任务属性
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.setPriorityInternal(ctx, bareScope(s.taskDAO.Query), taskID, priority)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.setPriorityInternal(ctx, scope, taskID, priority)
	})
}

func (s *TaskServiceImpl) setPriorityInternal(ctx context.Context, scope *InstanceScope, taskID string, priority int) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	if task.Priority == int32(priority) {
		return nil
	}
	task.Priority = int32(priority)
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	now := time.Now()
	task.UpdatedBy = &username
	task.UpdatedAt = &now
	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to set priority: %w", err)
	}
	return nil
}

// SetDueDate 设置任务到期时间
func (s *TaskServiceImpl) SetDueDate(ctx context.Context, actor Actor, taskID string, dueDate time.Time) error {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	// 租户校验：防止跨租户修改任务属性
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.setDueDateInternal(ctx, bareScope(s.taskDAO.Query), taskID, dueDate)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.setDueDateInternal(ctx, scope, taskID, dueDate)
	})
}

func (s *TaskServiceImpl) setDueDateInternal(ctx context.Context, scope *InstanceScope, taskID string, dueDate time.Time) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	task.DueDate = &dueDate
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	now := time.Now()
	task.UpdatedBy = &username
	task.UpdatedAt = &now
	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to set due date: %w", err)
	}
	return nil
}

// SuspendTask 挂起任务
func (s *TaskServiceImpl) SuspendTask(ctx context.Context, actor Actor, taskID string) error {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	// 租户校验：无 assignee 的任务（如会签父任务）不允许跨租户操作
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.suspendTaskInternal(ctx, bareScope(s.taskDAO.Query), taskID)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.suspendTaskInternal(ctx, scope, taskID)
	})
}

func (s *TaskServiceImpl) suspendTaskInternal(ctx context.Context, scope *InstanceScope, taskID string) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	if task.Status == string(enums.TaskStatusSuspended) {
		return nil
	}
	// 终态任务是既成事实的审批记录：挂起再激活可把已办任务翻回可操作集合，二次推进下游节点
	if isTerminalTaskStatus(task.Status) {
		return fmt.Errorf("task is %s, cannot suspend: %w", task.Status, ErrConflict)
	}
	if u := GetUserFromCtx(ctx); u != nil {
		if task.Assignee != nil && *task.Assignee != "" && *task.Assignee != u.UserID {
			return fmt.Errorf("task assigned to %s, current user %s: %w", *task.Assignee, u.UserID, ErrPermissionDenied)
		}
	}
	task.Status = string(enums.TaskStatusSuspended)
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	now := time.Now()
	task.UpdatedBy = &username
	task.UpdatedAt = &now
	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to suspend task: %w", err)
	}
	return nil
}

// ActivateTask 激活任务
func (s *TaskServiceImpl) ActivateTask(ctx context.Context, actor Actor, taskID string) error {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	// 租户校验：无 assignee 的任务（如会签父任务）不允许跨租户操作
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.activateTaskInternal(ctx, bareScope(s.taskDAO.Query), taskID)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.activateTaskInternal(ctx, scope, taskID)
	})
}

func (s *TaskServiceImpl) activateTaskInternal(ctx context.Context, scope *InstanceScope, taskID string) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 幂等
	if task.Status == string(enums.TaskStatusActive) || task.Status == string(enums.TaskStatusPending) {
		return nil
	}

	// 激活只用于唤醒挂起任务；终态任务翻回 active 会二次推进已流转的节点
	if isTerminalTaskStatus(task.Status) {
		return fmt.Errorf("task is %s, cannot activate: %w", task.Status, ErrConflict)
	}

	if u := GetUserFromCtx(ctx); u != nil {
		if task.Assignee != nil && *task.Assignee != "" && *task.Assignee != u.UserID {
			return fmt.Errorf("task assigned to %s, current user %s: %w", *task.Assignee, u.UserID, ErrPermissionDenied)
		}
	}

	if task.Assignee != nil && *task.Assignee != "" {
		task.Status = string(enums.TaskStatusActive)
	} else {
		task.Status = string(enums.TaskStatusPending)
	}
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	now := time.Now()
	task.UpdatedBy = &username
	task.UpdatedAt = &now
	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to activate task: %w", err)
	}
	return nil
}

// isTerminalTaskStatus 终态任务：completed/terminated/returned/withdrawn，状态不可再变更。
func isTerminalTaskStatus(status string) bool {
	switch enums.TaskStatus(status) {
	case enums.TaskStatusCompleted, enums.TaskStatusTerminated,
		enums.TaskStatusReturned, enums.TaskStatusWithdrawn:
		return true
	}
	return false
}
