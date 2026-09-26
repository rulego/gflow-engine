// This file contains small private helpers shared across TaskServiceImpl methods:
// rule-string accessor, task-to-history conversion, and the numeric
// type-coercion helper used by countersign progress aggregation.

package service

import (
	"context"
	"fmt"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
)

// TaskFetchAllPageSize 翻页取全量任务的单页大小：正常一轮取完，循环翻页
// 只为防御极端形态下的慢分页。组件层全量语义查询共用同一口径。
const TaskFetchAllPageSize = 1000

// taskPageLister 由运行表与历史表的任务 DAO 同时满足，listAllTasks 据此翻页取全量。
type taskPageLister interface {
	List(ctx context.Context, query *dto.TaskQuery) ([]*model.WfTask, int64, error)
}

// listAllTasks 按 query 条件翻页取全量任务。task_dao.List 无条件分页，
// pageSize 未设时回落默认 10，同节点清理/恢复、减签、节点置换这类全量语义
// 的调用会被截断：留下幽灵待办，或漏恢复/漏减签该命中的行。
func listAllTasks(ctx context.Context, taskDAO taskPageLister, query *dto.TaskQuery) ([]*model.WfTask, error) {
	var all []*model.WfTask
	for page := 1; ; page++ {
		pageQuery := *query
		pageQuery.Page = page
		pageQuery.PageSize = TaskFetchAllPageSize
		pageTasks, total, err := taskDAO.List(ctx, &pageQuery)
		if err != nil {
			return nil, err
		}
		all = append(all, pageTasks...)
		if int64(len(all)) >= total || len(pageTasks) == 0 {
			return all, nil
		}
	}
}

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

// instanceToHiInstance converts a WfInstance to WfHiInstance for archiving to
// history. 携带字段逐一对应拷贝；status/endReason/endedAt 等终结口径由调用方
// 按各自路径覆写。
func instanceToHiInstance(instance *model.WfInstance) *model.WfHiInstance {
	return &model.WfHiInstance{
		ID:              instance.ID,
		ProcessID:       instance.ProcessID,
		BusinessKey:     instance.BusinessKey,
		Name:            instance.Name,
		Status:          instance.Status,
		Variables:       instance.Variables,
		CurrentActivity: instance.CurrentActivity,
		Priority:        instance.Priority,
		ParentID:        instance.ParentID,
		TenantID:        instance.TenantID,
		CreatedBy:       instance.CreatedBy,
		CreatedAt:       instance.CreatedAt,
		UpdatedBy:       instance.UpdatedBy,
		UpdatedAt:       instance.UpdatedAt,
		EndReason:       instance.EndReason,
		Duration:        instance.Duration,
		EndedAt:         instance.EndedAt,
		StartUserID:     instance.StartUserID,
	}
}

// ensureTargetUserInTenant 转办/委派/改派/加签的目标用户租户归属与启用校验。
// 统一走租户归属鉴权守卫（未实现 TenantMembershipChecker 时跳过，缺口由装配期
// TenantMembershipGuard.Validate 统一告警/严格模式拒绝）；身份服务实现
// ActiveUserChecker 时校验目标启用，停用用户接手任务即无人可办，action 仅用于
// 错误信息标注动作来源。
func (s *TaskServiceImpl) ensureTargetUserInTenant(ctx context.Context, task *model.WfTask, userID, action string) error {
	if s.workflowEngine == nil || task == nil {
		return nil
	}
	identity := s.workflowEngine.GetIdentityService()
	guard := NewTenantMembershipGuard(identity)
	if err := guard.EnsureUserInTenant(ctx, task.TenantID, userID); err != nil {
		return fmt.Errorf("%s: %w", action, err)
	}
	if checker, ok := identity.(ActiveUserChecker); ok && identity != nil {
		active, err := checker.AreActiveUsers(ctx, task.TenantID, []string{userID})
		if err != nil {
			return fmt.Errorf("%s: failed to check target user status: %w", action, err)
		}
		if !active[userID] {
			return fmt.Errorf("%s: target user %s is not active: %w", action, userID, ErrValidation)
		}
	}
	return nil
}
