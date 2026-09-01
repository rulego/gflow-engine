// This file contains small private helpers shared across TaskServiceImpl methods:
// rule-string accessor, task-to-history conversion, and the numeric
// type-coercion helper used by countersign progress aggregation.

package service

import (
	"context"
	"fmt"

	"github.com/rulego/gflow-engine/model"
	"github.com/sirupsen/logrus"
)

// getApprovalRuleString 读取审批规则字符串（nil 安全，空串兜底）。
func (s *TaskServiceImpl) getApprovalRuleString(rule *string) string {
	if rule == nil {
		return ""
	}
	return *rule
}

// taskToHiTask converts a WfTask to WfHiTask for archiving to history.
func taskToHiTask(task *model.WfTask) *model.WfHiTask {
	return &model.WfHiTask{
		ID:                task.ID,
		ProcessInstanceID: task.ProcessInstanceID,
		ProcessID:         task.ProcessID,
		TaskDefKey:        &task.TaskDefKey,
		TaskType:          task.TaskType,
		Name:              task.Name,
		Description:       task.Description,
		ParentID:          task.ParentID,
		Status:            task.Status,
		Assignee:          task.Assignee,
		Owner:             task.Owner,
		DueDate:           task.DueDate,
		Priority:          task.Priority,
		FormKey:           task.FormKey,
		Variables:         task.Variables,
		ClaimedAt:         task.ClaimedAt,
		ApprovalType:      task.ApprovalType,
		ApprovalRule:      task.ApprovalRule,
		DelegateFrom:      task.DelegateFrom,
		DelegateReason:    task.DelegateReason,
		DelegateTime:      task.DelegateTime,
		EndedAt:           task.EndedAt,
		Comment:           task.Comment,
		EndReason:         task.EndReason,
		Duration:          task.Duration,
		TenantID:          task.TenantID,
		CreatedBy:         task.CreatedBy,
		CreatedAt:         task.CreatedAt,
		UpdatedBy:         task.UpdatedBy,
		UpdatedAt:         task.UpdatedAt,
		SequenceOrder:     task.SequenceOrder,
	}
}

// ensureTargetUserInTenant 转办/委派/改派的目标用户租户归属校验。
// 宿主注入的 IdentityService 若同时实现 TenantMembershipChecker 可选接口则执行校验，
// 阻断"把任务转派给其他租户用户"；未实现时引擎无法自行判定（引擎不含用户目录），
// 放行并记 debug 日志——是否允许跨租户转派由宿主实现该接口显式接管。
func (s *TaskServiceImpl) ensureTargetUserInTenant(ctx context.Context, task *model.WfTask, userID, action string) error {
	if s.workflowEngine == nil || task == nil {
		return nil
	}
	checker, ok := s.workflowEngine.GetIdentityService().(TenantMembershipChecker)
	if !ok {
		logrus.WithFields(logrus.Fields{
			"action":   action,
			"taskID":   task.ID,
			"targetID": userID,
		}).Debug("IdentityService does not implement TenantMembershipChecker; target user tenant check skipped")
		return nil
	}
	inTenant, err := checker.IsUserInTenant(ctx, task.TenantID, userID)
	if err != nil {
		return fmt.Errorf("failed to check target user tenant: %w", err)
	}
	if !inTenant {
		return fmt.Errorf("target user %s not in task tenant %s: %w", userID, task.TenantID, ErrPermissionDenied)
	}
	return nil
}
