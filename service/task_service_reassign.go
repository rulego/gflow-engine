// This file contains the admin Reassign method on TaskServiceImpl.
// reassign 是管理员强制改派：跳过 assignee/候选人校验，仅保留 active 状态校验
// 与 requireActionEnabled("reassign")，落库方式对标 Transfer（写 task.Variables + 发 TaskEvent，
// 不写 history 表）。

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/enums"
	utils2 "github.com/rulego/gflow-engine/utils"
)

// Reassign 管理员强制改派任务给 newAssignee。
// newAssignee - 新办理人ID, reason - 改派原因。
//
// 走 WithInstanceTx：与并发 Complete 串行化，避免整结构 Update
// 覆盖已完成的任务行（复活已办任务）。
func (s *TaskServiceImpl) Reassign(ctx context.Context, actor Actor, taskID, newAssignee, reason string) (string, error) {
	ctx = bindActor(ctx, actor)
	operatorID := actor.UserID
	if taskID == "" {
		return "", fmt.Errorf("task ID cannot be empty")
	}
	if newAssignee == "" {
		return "", fmt.Errorf("new assignee cannot be empty: %w", ErrValidation)
	}

	// 管理操作鉴权：Reassign 跳过 assignee/候选人校验，必须管理员（SuperAdmin）或系统身份，
	// 否则任意同租户用户可强改派任意任务（任务劫持）。
	if err := requireAdminIdentity(&actor); err != nil {
		return "", err
	}

	// 廉价读：解析 instanceID 并做租户校验，无锁
	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return "", fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return "", fmt.Errorf("%w: task", ErrNotFound)
	}
	// 操作者租户校验：管理员只能改派本租户任务（防跨租户改派）
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return "", fmt.Errorf("%w: task", ErrNotFound)
	}
	// 目标用户租户归属校验（IdentityService 实现 TenantMembershipChecker 时生效）
	if err := s.ensureTargetUserInTenant(ctx, task, newAssignee, "reassign"); err != nil {
		return "", err
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}

	var oldAssignee string
	var reassignedTask *model.WfTask
	reassignFn := func(scope *InstanceScope) error {
		t, old, err := s.reassignInternal(ctx, scope, taskID, operatorID, newAssignee, reason)
		if err != nil {
			return err
		}
		oldAssignee = old
		reassignedTask = t
		return nil
	}

	if instanceID == "" {
		// orphan/draft 任务：没有实例行可以锁定，直接走 Internal
		if err := reassignFn(bareScope(s.taskDAO.Query)); err != nil {
			return "", err
		}
	} else if err := WithInstanceTx(ctx, s.taskDAO.Query, instanceID, reassignFn); err != nil {
		return "", err
	}

	// 事务提交后发转办事件（沿用 TaskEventForwarded，避免新增事件类型）
	if reassignedTask != nil && s.workflowEngine.GetTaskEventListener() != nil {
		DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), TaskEvent{
			Type:       TaskEventForwarded,
			TaskID:     reassignedTask.ID,
			InstanceID: instanceID,
			ProcessID:  reassignedTask.ProcessID,
			TenantID:   reassignedTask.TenantID,
			TaskName:   reassignedTask.Name,
			ToUsers:    []string{newAssignee},
			FromUser:   operatorID,
			Reason:     reason,
			Timestamp:  time.Now(),
		}, ctx)
	}

	return oldAssignee, nil
}

// reassignInternal 在已持有实例行锁的事务内执行改派：
// 重新读任务（锁外读可能过期）→ 重校状态与动作开关 → 落库。
func (s *TaskServiceImpl) reassignInternal(ctx context.Context, scope *InstanceScope, taskID, operatorID, newAssignee, reason string) (*model.WfTask, string, error) {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return nil, "", fmt.Errorf("%w: task", ErrNotFound)
	}

	if err := s.requireActionEnabled(ctx, task, "reassign"); err != nil {
		return nil, "", err
	}
	if task.Status != string(enums.TaskStatusActive) {
		return nil, "", fmt.Errorf("only active tasks can be reassigned, current status: %s: %w", task.Status, ErrValidation)
	}

	oldAssignee := ""
	if task.Assignee != nil {
		oldAssignee = *task.Assignee
	}

	now := time.Now()
	if err := s.applyReassign(task, operatorID, newAssignee, reason, now); err != nil {
		return nil, "", err
	}

	// 落库：完整 Update 一并写回 assignee/variables/审计字段，避免两次 UPDATE。
	// 注：不能用 UpdateAssignee——它会把 status 重置为 pending，这里需保留 active。
	if err := taskDAO.Update(ctx, task); err != nil {
		return nil, "", fmt.Errorf("failed to persist reassign: %w", err)
	}
	return task, oldAssignee, nil
}

// applyReassign 把 reassign 元数据写入 task.Variables 并更新内存中的 assignee/审计字段。
// 纯逻辑、可单测；不触 DAO。校验已由调用方完成。
func (s *TaskServiceImpl) applyReassign(task *model.WfTask, operatorID, newAssignee, reason string, now time.Time) error {
	if task == nil {
		return fmt.Errorf("task cannot be nil")
	}
	if task.Status != string(enums.TaskStatusActive) {
		return fmt.Errorf("only active tasks can be reassigned, current status: %s: %w", task.Status, ErrValidation)
	}

	oldAssignee := ""
	if task.Assignee != nil {
		oldAssignee = *task.Assignee
	}

	vars := make(map[string]interface{})
	if task.Variables != nil && *task.Variables != "" {
		_ = utils2.FromJSON(*task.Variables, &vars)
	}
	vars["reassign_operator"] = operatorID
	vars["reassign_from"] = oldAssignee
	vars["reassign_to"] = newAssignee
	vars["reassign_reason"] = reason
	vars["reassign_time"] = now
	varsJSON, err := utils2.ToJSON(vars)
	if err != nil {
		return fmt.Errorf("failed to serialize task variables: %w", err)
	}

	task.Assignee = &newAssignee
	task.Status = string(enums.TaskStatusActive)
	task.Variables = &varsJSON
	// 清委派上下文:reassign 是强制改派,新受理人即最终办理人。若不清 Owner,
	// completeWithApproval 会把新受理人的 approve 当作"归还旧 owner"而不流转。
	// gorm struct Updates 忽略 nil 字段,用空串指针强制写入(owner=="" 语义等同无 owner)。
	emptyStr := ""
	task.Owner = &emptyStr
	task.UpdatedBy = &operatorID
	task.UpdatedAt = &now
	return nil
}
