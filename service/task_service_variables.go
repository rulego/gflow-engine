// This file contains task-variable management methods on TaskServiceImpl:
// GetTaskVariables, GetTaskVariable, SetTaskVariables / SetTaskVariable,
// and RemoveTaskVariable (each write path with its Internal variant).

package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
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

	// 租户校验：非系统操作人必须与任务同租户（真实用户空租户 fail-closed；系统身份/
	// 无身份放行），跨租户按不存在处理，不泄露任务存在性。
	if u := GetUserFromCtx(ctx); u != nil && !IsSystemActor(u) && task.TenantID != u.TenantID {
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
		return s.setTaskVariablesInternal(ctx, bareScope(s.taskDAO.Underlying()), taskID, variables)
	}
	return WithInstanceTx(ctx, s.taskDAO.Underlying(), instanceID, func(scope *InstanceScope) error {
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
	// 系统保留键不开放给办理人改写：reassign_* 是改派溯源标记（收回存量据此
	// 判定受管链路，被改写会伪造/抹掉改派来源），proxy_operator/proxy_time 是
	// 代审标记（被清除会让代审出的票重新取得收回资格），fallback_* 是审批人为空
	// 兜底留痕（伪造可骗过自动通过钩子），_sequentialAssignees 是顺序审批推进
	// 缓存（被冲掉会卡死后续节点）。仅校验本次提交的键。
	for k := range variables {
		if isReservedTaskVariableKey(k) {
			return fmt.Errorf("variable %q is reserved by the engine: %w", k, ErrValidation)
		}
	}
	// 合并而非整体替换：任务变量里存有引擎的运行时状态（如顺序审批的
	// _sequentialAssignees 缓存），整体覆盖会将其冲掉，后续推进丢失进度。
	// 损坏 JSON 按错误拒绝：静默清空会连带冲掉引擎运行时状态。
	merged, err := ParseVariablesJSON(task.Variables)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrValidation, err)
	}
	for k, v := range variables {
		merged[k] = v
	}
	return s.writeTaskVariables(ctx, taskDAO, task, merged)
}

// writeTaskVariables 序列化并落库合并后的任务变量。鉴权与保留键校验由调用方完成，
// 各写路径共享此处，保证 UpdatedBy/UpdatedAt 口径一致。
func (s *TaskServiceImpl) writeTaskVariables(ctx context.Context, taskDAO TaskStore, task *model.WfTask, merged map[string]interface{}) error {
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
		return s.setTaskVariableInternal(ctx, bareScope(s.taskDAO.Underlying()), taskID, variableName, value)
	}
	return WithInstanceTx(ctx, s.taskDAO.Underlying(), instanceID, func(scope *InstanceScope) error {
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
	// 只校验被写的键：既有变量集含引擎保留键（如顺序审批推进缓存）属正常状态，
	// 整包校验会让带保留键的任务连任意新变量都写不进
	if isReservedTaskVariableKey(variableName) {
		return fmt.Errorf("variable %q is reserved by the engine: %w", variableName, ErrValidation)
	}
	merged, err := ParseVariablesJSON(task.Variables)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrValidation, err)
	}
	merged[variableName] = value
	return s.writeTaskVariables(ctx, taskDAO, task, merged)
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
		return s.removeTaskVariableInternal(ctx, bareScope(s.taskDAO.Underlying()), taskID, variableName)
	}
	return WithInstanceTx(ctx, s.taskDAO.Underlying(), instanceID, func(scope *InstanceScope) error {
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
	if err := s.authorizeTaskOperator(ctx, task); err != nil {
		return err
	}
	// 只校验被删除的键：既有变量集含引擎保留键（如顺序审批推进缓存）属正常状态，
	// 整包校验会让顺序审批任务删任何变量都报错；删除保留键本身才拒绝
	if isReservedTaskVariableKey(variableName) {
		return fmt.Errorf("variable %q is reserved by the engine: %w", variableName, ErrValidation)
	}
	merged, err := ParseVariablesJSON(task.Variables)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrValidation, err)
	}
	delete(merged, variableName)
	return s.writeTaskVariables(ctx, taskDAO, task, merged)
}

// isReservedTaskVariableKey 引擎托管的任务变量键：办理人经 SetTaskVariables/
// RemoveTaskVariable 不得读写改名派溯源、代审标记、兜底留痕、加签留痕与推进
// 缓存（与审批主路径 stripEngineReservedVars 剥离的键集同源）。前缀保留留给
// 未来的 reassign_* 家族字段。
func isReservedTaskVariableKey(key string) bool {
	return key == constants.VarsProxyOperator ||
		key == constants.VarsProxyTime ||
		key == constants.VarsFallbackPolicy ||
		key == constants.VarsFallbackFrom ||
		key == constants.VarsFallbackReason ||
		key == constants.VarsFallbackTime ||
		key == constants.VarsSignAddedBy ||
		key == constants.KeySequentialAssignees ||
		strings.HasPrefix(key, "reassign_")
}
