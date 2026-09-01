// This file centralizes actionPermissions parsing + the service-layer action
// gates and Return target validation for TaskServiceImpl.
//
// 节点配置解析抽成 resolveNodeActionPermissions，既供 GetProcessInstanceDetail
// 装配（前端按钮显隐），也供五个 service 入口
// （transfer/delegate/return/addSign/reduceSign）强制校验设计器显式 false。

package service

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
)

// resolveNodeActionPermissions 返回某节点 additionalInfo.actionPermissions。
// 复用入口：GetProcessInstanceDetail（详情装配）与 requireActionEnabled（service 校验）。
//
// 任一加载/解析步骤失败都返回空 map（降级放行），避免解析 bug 挡住正常操作。
func resolveNodeActionPermissions(ctx context.Context, engine WorkflowEngine, instanceID, taskDefKey string) map[string]interface{} {
	if instanceID == "" || taskDefKey == "" || engine == nil {
		return map[string]interface{}{}
	}
	// 服务未注入（嵌入式半装配场景）同样降级放行，不允许 panic
	runtimeService := engine.GetRuntimeService()
	if runtimeService == nil {
		return map[string]interface{}{}
	}
	instance, err := runtimeService.GetProcessInstance(ctx, ActorFromCtx(ctx), instanceID)
	if err != nil || instance == nil {
		return map[string]interface{}{}
	}
	if instance.ProcessID == "" {
		return map[string]interface{}{}
	}
	processService := engine.GetProcessService()
	if processService == nil {
		return map[string]interface{}{}
	}
	procDef, err := processService.Get(ctx, instance.ProcessID)
	if err != nil || procDef == nil {
		return map[string]interface{}{}
	}
	rc, err := procDef.ToRuleChain()
	if err != nil || rc == nil {
		return map[string]interface{}{}
	}
	node, ok := rc.GetNode(taskDefKey)
	if !ok {
		return map[string]interface{}{}
	}
	ap, ok := node.GetAdditionalInfo("actionPermissions")
	if !ok {
		return map[string]interface{}{}
	}
	v, ok := ap.(map[string]interface{})
	if !ok {
		return map[string]interface{}{}
	}
	return v
}

// resolveNodeFormPermissions 返回某节点 additionalInfo.formPermissions（字段级权限 r/w/h）。
// 必须传入事务 scope：本函数在 WithInstanceTx 回调内调用，若走全局连接，
// SQLite 等单写锁数据库会与外层事务互等形成死锁。
func resolveNodeFormPermissions(ctx context.Context, scope *InstanceScope, instanceID, taskDefKey string) map[string]interface{} {
	if scope == nil || instanceID == "" || taskDefKey == "" {
		return map[string]interface{}{}
	}
	instance, err := scope.Instances().Get(ctx, instanceID)
	if err != nil || instance == nil || instance.ProcessID == "" {
		return map[string]interface{}{}
	}
	procDef, err := scope.Processes().Get(ctx, instance.ProcessID)
	if err != nil || procDef == nil {
		return map[string]interface{}{}
	}
	rc, err := procDef.ToRuleChain()
	if err != nil || rc == nil {
		return map[string]interface{}{}
	}
	node, ok := rc.GetNode(taskDefKey)
	if !ok {
		return map[string]interface{}{}
	}
	fp, ok := node.GetAdditionalInfo("formPermissions")
	if !ok {
		return map[string]interface{}{}
	}
	v, ok := fp.(map[string]interface{})
	if !ok {
		return map[string]interface{}{}
	}
	return v
}

// requireActionEnabled 校验流程设计器是否在 actionPermissions 中显式禁用了某动作。
// 仅当"明确解析到 actionPermissions 且动作 key 显式 false"才拒绝；其余一律放行。
func (s *TaskServiceImpl) requireActionEnabled(ctx context.Context, task *model.WfTask, actionKey string) error {
	if task == nil || task.ProcessInstanceID == nil || *task.ProcessInstanceID == "" {
		return nil
	}
	ap := resolveNodeActionPermissions(ctx, s.workflowEngine, *task.ProcessInstanceID, task.TaskDefKey)
	if designerDisabled(ap, actionKey) {
		return fmt.Errorf("action %q disabled by designer on node %s: %w",
			actionKey, task.TaskDefKey, ErrPermissionDenied)
	}
	return nil
}

// filterVariablesByFormPermissions 按节点 formPermissions 过滤审批人提交的变量：
// 只读(r)/隐藏(h)字段不允许审批人覆盖；可写(w/缺省)字段放行。无配置时不限制。
func (s *TaskServiceImpl) filterVariablesByFormPermissions(ctx context.Context, scope *InstanceScope, task *model.WfTask, vars map[string]interface{}) map[string]interface{} {
	if len(vars) == 0 || task == nil || task.ProcessInstanceID == nil || *task.ProcessInstanceID == "" {
		return vars
	}
	fp := resolveNodeFormPermissions(ctx, scope, *task.ProcessInstanceID, task.TaskDefKey)
	if len(fp) == 0 {
		return vars
	}
	filtered := make(map[string]interface{}, len(vars))
	for k, v := range vars {
		if perm, ok := fp[k].(string); ok && (perm == "r" || perm == "h") {
			continue
		}
		filtered[k] = v
	}
	return filtered
}

// getPreviousUserTaskDefKey 返回该实例中最近一个已 completed 的 userTask 节点 defKey
// （按 ended_at desc）。用于 Return 目标合法性校验：只允许退回到这一个节点。
// ccTask / 系统节点 / 未完成 userTask 一律不作为合法目标。
//
// 查 wf_task 运行表（单任务审批后留在运行表，实例终结才归档到 wf_hi_task），
// 直走 gen query 以支持 TaskType 过滤，单条 SQL 取目标。
func (s *TaskServiceImpl) getPreviousUserTaskDefKey(ctx context.Context, scope *InstanceScope, instanceID string) (string, error) {
	if instanceID == "" {
		return "", nil
	}
	wt := scope.Tx().WfTask
	var task model.WfTask
	err := wt.WithContext(ctx).
		Where(wt.ProcessInstanceID.Eq(instanceID)).
		Where(wt.TaskType.Eq(constants.TaskTypeUserTask)).
		Where(wt.Status.Eq(string(enums.TaskStatusCompleted))).
		Order(wt.EndedAt.Desc()).
		Limit(1).
		Scan(&task)
	if err != nil {
		return "", fmt.Errorf("failed to query previous userTask: %w", err)
	}
	if task.ID == "" {
		return "", nil
	}
	return task.TaskDefKey, nil
}

// requireReturnTarget 校验 Return 目标：必须是上一 userTask；操作人须为当前任务受理人/候选人。
func (s *TaskServiceImpl) requireReturnTarget(ctx context.Context, scope *InstanceScope, task *model.WfTask, targetActivityID, userID string) error {
	if task == nil || task.ProcessInstanceID == nil {
		return nil
	}
	instanceID := *task.ProcessInstanceID

	// 1. 操作人须为当前任务受理人或候选人
	if !s.isReturnOperator(ctx, task, userID) {
		return fmt.Errorf("return by non-assignee/non-candidate %s: %w", userID, ErrPermissionDenied)
	}

	// 2. 目标必须是上一 userTask（解析失败 fail-closed，防绕过流程约束回退到任意节点）
	prevDefKey, err := s.getPreviousUserTaskDefKey(ctx, scope, instanceID)
	if err != nil {
		logrus.WithError(err).WithField("instanceId", instanceID).
			Warn("failed to resolve previous userTask for return")
		return fmt.Errorf("failed to resolve return target: %w", ErrPermissionDenied)
	}
	if prevDefKey == "" {
		return fmt.Errorf("no completed userTask to return to: %w", ErrValidation)
	}
	if targetActivityID != prevDefKey {
		return fmt.Errorf("return target %q is not the previous userTask %q: %w",
			targetActivityID, prevDefKey, ErrValidation)
	}
	return nil
}

// isReturnOperator 判断 userID 是否为当前任务的受理人或候选人。
func (s *TaskServiceImpl) isReturnOperator(ctx context.Context, task *model.WfTask, userID string) bool {
	if userID == "" {
		return false
	}
	if task.Assignee != nil && *task.Assignee == userID {
		return true
	}
	if task.ProcessInstanceID == nil {
		return false
	}
	candidates, err := s.GetTaskCandidates(ctx, *task.ProcessInstanceID, task.TaskDefKey)
	if err != nil {
		logrus.WithError(err).WithField("taskDefKey", task.TaskDefKey).
			Warn("failed to query task candidates for return operator check; treating as non-candidate")
		return false
	}
	for _, c := range candidates {
		if c.EntityID == userID {
			return true
		}
	}
	return false
}
