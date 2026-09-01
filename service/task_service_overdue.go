// This file contains the admin global overdue-query method on TaskServiceImpl,
// and the shared filterOverdueTasks pure helper.

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// GetOverdueTasks 全局视角查询已过 dueDate 的 active 任务（管理员监控）。
// 与 countOverdueActiveTasks 区别：不带 userID/assignee 过滤，返回任务切片而非计数。
// 复用 countOverdueFromTasks 同款 DueDate 判定（filterOverdueTasks）。
func (s *TaskServiceImpl) GetOverdueTasks(ctx context.Context, actor Actor, query *dto.TaskQuery) ([]*model.WfTask, int64, error) {
	ctx = bindActor(ctx, actor)
	tenantID := actor.TenantID
	if tenantID == "" {
		return nil, 0, fmt.Errorf("tenant ID cannot be empty")
	}

	q := &dto.TaskQuery{}
	if query != nil {
		*q = *query
	}
	q.TenantID = tenantID
	q.PageRequest.Status = []string{string(enums.TaskStatusPending), string(enums.TaskStatusActive)}
	// 不设 Assignee，管理员全局视角

	// 过期判定下推 DB：due_date 非空且早于 now。
	now := time.Now()
	q.DueDateBefore = &now

	tasks, _, err := s.taskDAO.List(ctx, q)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query overdue tasks: %w", err)
	}

	// 兜底：纯逻辑二次过滤，防御 DAO 条件未命中（如自定义方言）。
	overdue := filterOverdueTasks(tasks, now)
	return overdue, int64(len(overdue)), nil
}

// filterOverdueTasks 返回 DueDate 非 nil 且早于 now 的任务（纯逻辑）。
func filterOverdueTasks(tasks []*model.WfTask, now time.Time) []*model.WfTask {
	result := make([]*model.WfTask, 0, len(tasks))
	for _, t := range tasks {
		if t != nil && t.DueDate != nil && t.DueDate.Before(now) {
			result = append(result, t)
		}
	}
	return result
}
