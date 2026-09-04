// This file contains assignment-related methods on TaskServiceImpl:
// SetAssignee, SetOwner, Delegate, Resolve, and Transfer (each with its
// WithInstanceTx-routed Internal variant).

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/rulego/gflow-engine/types/enums"
	utils2 "github.com/rulego/gflow-engine/utils"
)

// SetAssignee 设置任务分配人
func (s *TaskServiceImpl) SetAssignee(ctx context.Context, actor Actor, taskID, userID string) error {
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
	// 租户校验：防止跨租户直接改派他人任务
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.setAssigneeInternal(ctx, bareScope(s.taskDAO.Query), taskID, userID)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.setAssigneeInternal(ctx, scope, taskID, userID)
	})
}

func (s *TaskServiceImpl) setAssigneeInternal(ctx context.Context, scope *InstanceScope, taskID, userID string) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 幂等
	if task.Assignee != nil && *task.Assignee == userID {
		return nil
	}

	task.Assignee = &userID
	if userID != "" && task.Status == string(enums.TaskStatusPending) {
		task.Status = string(enums.TaskStatusActive)
	} else if userID == "" {
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
		return fmt.Errorf("failed to set assignee: %w", err)
	}
	return nil
}

// SetOwner 设置任务所有者
func (s *TaskServiceImpl) SetOwner(ctx context.Context, actor Actor, taskID, userID string) error {
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
	// 租户校验：防止跨租户直接改派他人任务
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.setOwnerInternal(ctx, bareScope(s.taskDAO.Query), taskID, userID)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.setOwnerInternal(ctx, scope, taskID, userID)
	})
}

func (s *TaskServiceImpl) setOwnerInternal(ctx context.Context, scope *InstanceScope, taskID, userID string) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	if task.Owner != nil && *task.Owner == userID {
		return nil
	}

	task.Owner = &userID
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	now := time.Now()
	task.UpdatedBy = &username
	task.UpdatedAt = &now

	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to set owner: %w", err)
	}
	return nil
}

// Delegate 委派任务给其他用户
func (s *TaskServiceImpl) Delegate(ctx context.Context, actor Actor, taskID, userID, reason string) error {
	ctx = bindActor(ctx, actor)
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

	// 租户隔离：与 SetAssignee/Resolve 同口径（操作人租户非空时任务必须同租户）
	if err := ensureTenantAccess(ctx, "task", task.TenantID); err != nil {
		return err
	}

	// 设计器显式禁用 delegate → 拒绝（actionPermissions 解析失败时降级放行）
	if err := s.requireActionEnabled(ctx, task, "delegate"); err != nil {
		return err
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.delegateInternal(ctx, bareScope(s.taskDAO.Query), taskID, userID, reason)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.delegateInternal(ctx, scope, taskID, userID, reason)
	})
}

func (s *TaskServiceImpl) delegateInternal(ctx context.Context, scope *InstanceScope, taskID, userID, reason string) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 幂等：已经委派给同一人
	if task.Assignee != nil && *task.Assignee == userID && task.DelegateFrom != nil {
		return nil
	}

	if task.Status != string(enums.TaskStatusActive) {
		return fmt.Errorf("only active tasks can be delegated, current status: %s", task.Status)
	}

	u := GetUserFromCtx(ctx)
	if u == nil || u.UserID == "" {
		return ErrAuthenticationRequired
	}
	// fail-closed：仅当前 assignee 可委派。未分配（pending）任务无从委派——此前
	// assignee 为空时跳过校验，任意同租户用户可把未分配任务委派给他人。
	if task.Assignee == nil || *task.Assignee == "" {
		return fmt.Errorf("task is not assigned, cannot delegate: %w", ErrPermissionDenied)
	}
	if *task.Assignee != u.UserID {
		return fmt.Errorf("task assigned to %s, current user %s: %w", *task.Assignee, u.UserID, ErrPermissionDenied)
	}
	// 禁止委派给自己:self-delegate 会让 Owner==Assignee==DelegateFrom,approve 时
	// resolveDelegatedApproval 把任务"归还给自己"(不完成、不流转),实例永久卡死。
	if u.UserID == userID {
		return fmt.Errorf("cannot delegate task to yourself: %w", ErrValidation)
	}

	originalAssignee := task.Assignee
	if originalAssignee != nil && *originalAssignee != "" {
		task.DelegateFrom = originalAssignee
	} else {
		currentUser := GetUserFromCtx(ctx)
		if currentUser != nil {
			task.DelegateFrom = &currentUser.UserID
		}
	}
	if task.Assignee != nil && *task.Assignee != "" {
		task.Owner = task.Assignee
	}

	task.Assignee = &userID
	task.Status = string(enums.TaskStatusActive)

	if reason != "" {
		task.DelegateReason = &reason
	}

	now := time.Now()
	task.DelegateTime = &now

	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	task.UpdatedBy = &username
	task.UpdatedAt = &now
	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to delegate task: %w", err)
	}
	// 触发委托事件（AfterCommit：回滚不产生幽灵事件）
	if s.workflowEngine.GetTaskEventListener() != nil {
		instanceID := ""
		if task.ProcessInstanceID != nil {
			instanceID = *task.ProcessInstanceID
		}
		fromUser := ""
		if u := GetUserFromCtx(ctx); u != nil {
			fromUser = u.UserID
		}
		evt := TaskEvent{
			Type:       TaskEventForwarded,
			TaskID:     task.ID,
			TaskDefKey: task.TaskDefKey,
			InstanceID: instanceID,
			ProcessID:  task.ProcessID,
			TenantID:   task.TenantID,
			TaskName:   task.Name,
			ToUsers:    []string{userID},
			FromUser:   fromUser,
			Timestamp:  time.Now(),
		}
		scope.AfterCommit(func() error {
			DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), evt, ctx)
			return nil
		})
	}
	return nil
}

// Resolve 解决委派的任务（委派任务完成后返回给原分配人）
func (s *TaskServiceImpl) Resolve(ctx context.Context, actor Actor, taskID string) error {
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
		return s.resolveInternal(ctx, bareScope(s.taskDAO.Query), taskID)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.resolveInternal(ctx, scope, taskID)
	})
}

func (s *TaskServiceImpl) resolveInternal(ctx context.Context, scope *InstanceScope, taskID string) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 幂等：已经没有 Owner（说明已 resolve 过）
	if task.Owner == nil || *task.Owner == "" {
		return nil
	}

	if task.Status != string(enums.TaskStatusActive) {
		return fmt.Errorf("task is not in active status")
	}

	// 归还：assignee 恢复为 owner 并清空 owner（空串指针强制清空，gorm 忽略 nil）
	task.Assignee = task.Owner
	emptyOwner := ""
	task.Owner = &emptyOwner
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	now := time.Now()
	task.UpdatedBy = &username
	task.UpdatedAt = &now

	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to resolve task: %w", err)
	}

	// 委派归还事件：通知原 owner 任务已回到其名下
	if listener := s.workflowEngine.GetTaskEventListener(); listener != nil && task.Assignee != nil && *task.Assignee != "" {
		instanceID := ""
		if task.ProcessInstanceID != nil {
			instanceID = *task.ProcessInstanceID
		}
		fromUser := ""
		if u := GetUserFromCtx(ctx); u != nil {
			fromUser = u.UserID
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
			FromUser:   fromUser,
			Timestamp:  time.Now(),
		}
		scope.AfterCommit(func() error {
			DispatchTaskEvent(listener, evt, ctx)
			return nil
		})
	}
	return nil
}

// Transfer 转办（将任务转给其他人处理）
func (s *TaskServiceImpl) Transfer(ctx context.Context, actor Actor, taskID, toUserID, reason string) error {
	ctx = bindActor(ctx, actor)
	fromUserID := actor.UserID
	if taskID == "" || fromUserID == "" || toUserID == "" {
		return fmt.Errorf("task ID, from user ID and to user ID cannot be empty")
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	if err := s.ensureTargetUserInTenant(ctx, task, toUserID, "transfer"); err != nil {
		return err
	}

	// 设计器显式禁用 transfer → 拒绝（actionPermissions 解析失败时降级放行）
	if err := s.requireActionEnabled(ctx, task, "transfer"); err != nil {
		return err
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.transferInternal(ctx, bareScope(s.taskDAO.Query), taskID, fromUserID, toUserID, reason)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.transferInternal(ctx, scope, taskID, fromUserID, toUserID, reason)
	})
}

func (s *TaskServiceImpl) transferInternal(ctx context.Context, scope *InstanceScope, taskID, fromUserID, toUserID, reason string) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 幂等：已经转给目标用户
	if task.Assignee != nil && *task.Assignee == toUserID {
		return nil
	}

	if task.Status != string(enums.TaskStatusActive) {
		return fmt.Errorf("only active tasks can be transferred, current status: %s: %w", task.Status, ErrValidation)
	}
	// 租户校验：ctx 带身份时任务必须同租户（防跨租户转办）
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	// 操作人以显式参数 fromUserID 为准：只有任务当前 assignee 能转办；
	// 未分配任务（assignee 为空/nil）无从转办，fail-closed 拒绝（此前空 assignee 跳过校验）。
	if task.Assignee == nil || *task.Assignee != fromUserID {
		return fmt.Errorf("only assigned user can transfer task: %w", ErrPermissionDenied)
	}

	task.Assignee = &toUserID
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	now := time.Now()
	task.UpdatedBy = &username
	task.UpdatedAt = &now

	vars := make(map[string]interface{})
	if task.Variables != nil && *task.Variables != "" {
		_ = utils2.FromJSON(*task.Variables, &vars)
	}
	vars["transfer_reason"] = reason
	vars["transfer_from"] = fromUserID
	vars["transfer_time"] = now
	varsJSON, err := utils2.ToJSON(vars)
	if err != nil {
		return fmt.Errorf("failed to serialize task variables: %w", err)
	}
	task.Variables = &varsJSON

	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to transfer task: %w", err)
	}
	// 触发转办事件（AfterCommit：回滚不产生幽灵事件）
	if s.workflowEngine.GetTaskEventListener() != nil {
		instanceID := ""
		if task.ProcessInstanceID != nil {
			instanceID = *task.ProcessInstanceID
		}
		evt := TaskEvent{
			Type:       TaskEventForwarded,
			TaskID:     task.ID,
			TaskDefKey: task.TaskDefKey,
			InstanceID: instanceID,
			ProcessID:  task.ProcessID,
			TenantID:   task.TenantID,
			TaskName:   task.Name,
			ToUsers:    []string{toUserID},
			FromUser:   fromUserID,
			Timestamp:  time.Now(),
		}
		scope.AfterCommit(func() error {
			DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), evt, ctx)
			return nil
		})
	}
	return nil
}
