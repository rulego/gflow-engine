// This file holds the platform-level query extensions on TaskServiceImpl:
// ScanOverdueTasks (cross-tenant overdue sweep for host-side schedulers) and
// GetClaimableInstanceIDs (batch needs-claim marking for todo lists).

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// ScanOverdueTasks 跨租户扫描已过 dueDate 的 active/pending 任务（平台级巡检）。
// 供宿主应用跑定时逾期提醒，替代绕过引擎直接扫 wf_task 的裸 SQL。
// limit<=0 按 500 处理，防止单轮拖垮数据库。
func (s *TaskServiceImpl) ScanOverdueTasks(ctx context.Context, limit int) ([]*model.WfTask, error) {
	if limit <= 0 {
		limit = 500
	}
	now := time.Now()
	q := &dto.TaskQuery{
		// TenantID 留空：不限租户。DAO 对空 TenantID 不加过滤条件。
		DueDateBefore: &now,
		PageRequest: dto.PageRequest{
			Page:     1,
			PageSize: limit,
			OrderBy:  "due_date",
			Status:   []string{string(enums.TaskStatusActive), string(enums.TaskStatusPending)},
		},
	}
	tasks, _, err := s.taskDAO.List(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("failed to scan overdue tasks: %w", err)
	}
	return filterOverdueTasks(tasks, now), nil
}

// GetClaimableInstanceIDs 批量判断哪些实例存在"当前用户可认领"的任务。
// 单次查询拿到用户在本租户的全部候选任务（CandidateUser 机制与待办列表同口径），
// 再过滤出无 assignee 且命中 instanceIDs 的任务；instanceIDs 为空表示不限实例。
func (s *TaskServiceImpl) GetClaimableInstanceIDs(ctx context.Context, actor Actor, instanceIDs []string) ([]string, error) {
	ctx = bindActor(ctx, actor)
	userID, tenantID := actor.UserID, actor.TenantID
	if userID == "" || tenantID == "" {
		return nil, fmt.Errorf("userID and tenantID cannot be empty: %w", ErrValidation)
	}

	q := &dto.TaskQuery{
		TenantID:         tenantID,
		CandidateUser:    userID,
		CandidateRoleIDs: s.candidateRoleIDs(ctx, tenantID, userID),
		PageRequest: dto.PageRequest{
			Page:     1,
			PageSize: 500,
			Status:   []string{string(enums.TaskStatusPending)},
		},
	}
	if len(instanceIDs) > 0 {
		q.InstanceIDs = instanceIDs
	}
	tasks, _, err := s.taskDAO.List(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("failed to query candidate tasks: %w", err)
	}

	wanted := make(map[string]struct{}, len(instanceIDs))
	for _, id := range instanceIDs {
		wanted[id] = struct{}{}
	}

	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, t := range tasks {
		if t == nil || t.ProcessInstanceID == nil || *t.ProcessInstanceID == "" {
			continue
		}
		// 已指派的不算"待认领"
		if t.Assignee != nil && *t.Assignee != "" {
			continue
		}
		instID := *t.ProcessInstanceID
		if len(wanted) > 0 {
			if _, ok := wanted[instID]; !ok {
				continue
			}
		}
		if _, dup := seen[instID]; dup {
			continue
		}
		seen[instID] = struct{}{}
		out = append(out, instID)
	}
	return out, nil
}
