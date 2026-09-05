// This file contains task-variable management methods on TaskServiceImpl:
// GetTaskVariables, GetTaskVariable, SetTaskVariables / SetTaskVariable,
// and RemoveTaskVariable (each write path with its Internal variant).

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/rulego/gflow-engine/model"
	utils2 "github.com/rulego/gflow-engine/utils"
)

// GetTaskVariables 获取任务变量
func (s *TaskServiceImpl) GetTaskVariables(ctx context.Context, actor Actor, taskID string) (map[string]interface{}, error) {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return nil, fmt.Errorf("task ID cannot be empty")
	}

	// 获取任务
	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	if task == nil {
		return nil, fmt.Errorf("%w: task", ErrNotFound)
	}

	// 租户校验：actor 租户非空时任务必须同租户（跨租户按不存在处理，不泄露任务存在性）
	if u := GetUserFromCtx(ctx); u != nil && u.TenantID != "" && task.TenantID != u.TenantID {
		return nil, fmt.Errorf("%w: task", ErrNotFound)
	}

	return ParseVariablesJSON(task.Variables)
}

// GetTaskVariable 获取指定任务变量
func (s *TaskServiceImpl) GetTaskVariable(ctx context.Context, actor Actor, taskID, variableName string) (interface{}, error) {
	variables, err := s.GetTaskVariables(ctx, actor, taskID)
	if err != nil {
		return nil, err
	}

	value, exists := variables[variableName]
	if !exists {
		return nil, fmt.Errorf("%w: variable %s", ErrNotFound, variableName)
	}

	return value, nil
}

// authorizeTaskOperator 校验操作者是任务 assignee 且同租户（任务变量/操作鉴权，防篡改他人任务）
func (s *TaskServiceImpl) authorizeTaskOperator(ctx context.Context, task *model.WfTask) error {
	u := GetUserFromCtx(ctx)
	if u == nil {
		return fmt.Errorf("authentication required: %w", ErrPermissionDenied)
	}
	if task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	if task.Assignee == nil || *task.Assignee != u.UserID {
		return fmt.Errorf("only assignee can modify task: %w", ErrPermissionDenied)
	}
	return nil
}

// SetTaskVariables 设置任务变量
func (s *TaskServiceImpl) SetTaskVariables(ctx context.Context, actor Actor, taskID string, variables map[string]interface{}) error {
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

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.setTaskVariablesInternal(ctx, bareScope(s.taskDAO.Query), taskID, variables)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.setTaskVariablesInternal(ctx, scope, taskID, variables)
	})
}

func (s *TaskServiceImpl) setTaskVariablesInternal(ctx context.Context, scope *InstanceScope, taskID string, variables map[string]interface{}) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	if err := s.authorizeTaskOperator(ctx, task); err != nil {
		return err
	}
	// 合并而非整体替换：任务变量里存有引擎的运行时状态（如顺序审批的
	// _sequentialAssignees 缓存），整体覆盖会将其冲掉，后续推进丢失进度。
	merged := map[string]interface{}{}
	if task.Variables != nil && *task.Variables != "" {
		_ = utils2.FromJSON(*task.Variables, &merged)
	}
	for k, v := range variables {
		merged[k] = v
	}
	variablesJSON, err := utils2.ToJSON(merged)
	if err != nil {
		return fmt.Errorf("failed to serialize task variables: %w", err)
	}
	task.Variables = &variablesJSON
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	now := time.Now()
	task.UpdatedBy = &username
	task.UpdatedAt = &now
	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to set task variables: %w", err)
	}
	return nil
}

// SetTaskVariable 设置指定任务变量
func (s *TaskServiceImpl) SetTaskVariable(ctx context.Context, actor Actor, taskID, variableName string, value interface{}) error {
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

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.setTaskVariableInternal(ctx, bareScope(s.taskDAO.Query), taskID, variableName, value)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.setTaskVariableInternal(ctx, scope, taskID, variableName, value)
	})
}

func (s *TaskServiceImpl) setTaskVariableInternal(ctx context.Context, scope *InstanceScope, taskID, variableName string, value interface{}) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	if err := s.authorizeTaskOperator(ctx, task); err != nil {
		return err
	}

	variables := map[string]interface{}{}
	if task.Variables != nil && *task.Variables != "" {
		_ = utils2.FromJSON(*task.Variables, &variables)
	}
	variables[variableName] = value
	return s.setTaskVariablesInternal(ctx, scope, taskID, variables)
}

// RemoveTaskVariable 删除任务变量
func (s *TaskServiceImpl) RemoveTaskVariable(ctx context.Context, actor Actor, taskID, variableName string) error {
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

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.removeTaskVariableInternal(ctx, bareScope(s.taskDAO.Query), taskID, variableName)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.removeTaskVariableInternal(ctx, scope, taskID, variableName)
	})
}

func (s *TaskServiceImpl) removeTaskVariableInternal(ctx context.Context, scope *InstanceScope, taskID, variableName string) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	variables := map[string]interface{}{}
	if task.Variables != nil && *task.Variables != "" {
		_ = utils2.FromJSON(*task.Variables, &variables)
	}
	delete(variables, variableName)
	return s.setTaskVariablesInternal(ctx, scope, taskID, variables)
}
