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
		// 只巡检活跃实例：挂起实例的任务冻结在原地，超时提醒与超时自动办理
		// 都不应在其上触发。
		InstanceStatuses: []string{string(enums.InstanceStatusActive)},
		PageRequest: dto.PageRequest{
			OrderBy: "due_date",
			Status:  []string{string(enums.TaskStatusActive), string(enums.TaskStatusPending)},
		},
	}
	// 翻页取到 limit 条为止：DAO 分页被 MaxPageSize=200 硬截断，单页传大
	// limit 会被静默削到 200，逾期任务超量时尾部任务始终扫不到。due_date
	// 同毫秒并列时按任务 ID 去重防跨页重复
	var all []*model.WfTask
	seen := make(map[string]bool)
	for page := 1; len(all) < limit; page++ {
		pageQuery := *q
		pageQuery.Page = page
		pageQuery.PageSize = min(limit-len(all), TaskFetchAllPageSize)
		tasks, total, err := s.taskDAO.List(ctx, &pageQuery)
		if err != nil {
			return nil, fmt.Errorf("failed to scan overdue tasks: %w", err)
		}
		for _, t := range tasks {
			if !seen[t.ID] {
				seen[t.ID] = true
				all = append(all, t)
			}
		}
		if int64(len(all)) >= total || len(tasks) == 0 {
			break
		}
	}
	return filterOverdueTasks(all, now), nil
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
		CandidateDeptIDs: s.candidateDeptIDs(ctx, tenantID, userID),
		// 挂起/失败实例的任务签收会被守卫拒绝，这里同步按实例状态过滤，
		// 避免列表标出「待认领」而签收必败（与超时扫描的实例状态口径一致）
		InstanceStatuses: []string{string(enums.InstanceStatusActive)},
	}
	q.Status = []string{string(enums.TaskStatusPending)}
	if len(instanceIDs) > 0 {
		q.InstanceIDs = instanceIDs
	}
	// 候选任务量不受单页上限约束，按 total 翻页取全量，漏页会把在办实例漏标「可认领」
	var tasks []*model.WfTask
	for page := 1; ; page++ {
		q.Page = page
		q.PageSize = TaskFetchAllPageSize
		batch, total, err := s.taskDAO.List(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("failed to query candidate tasks: %w", err)
		}
		tasks = append(tasks, batch...)
		if int64(len(tasks)) >= total || len(batch) == 0 {
			break
		}
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
