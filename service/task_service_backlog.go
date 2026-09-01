// This file contains the admin backlog-by-process aggregation on TaskServiceImpl.
// 聚合与名称批量查询均在 DB 层完成，避免全量拉表内存分组与逐条取 processName 的 N+1。

package service

import (
	"context"
	"fmt"
)

// BacklogItem 积压看板单项：某流程定义下的 active 任务数。
type BacklogItem struct {
	ProcessDefID string `json:"processDefID"` // wf_task.process_id（流程定义ID）
	ProcessName  string `json:"processName"`  // 关联 wf_process.name
	ActiveCount  int64  `json:"activeCount"`
}

// GetBacklogByProcess 按流程定义聚合 active 任务数，倒序取 top 10（管理员积压看板）。
// 单条 GROUP BY 聚合 + 单条批量取 processName，共 2 条 SQL。
func (s *TaskServiceImpl) GetBacklogByProcess(ctx context.Context, actor Actor) ([]*BacklogItem, error) {
	ctx = bindActor(ctx, actor)
	tenantID := actor.TenantID
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty")
	}

	rows, err := s.taskDAO.AggregateActiveByProcess(ctx, tenantID, 10)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate backlog: %w", err)
	}

	items := make([]*BacklogItem, 0, len(rows))
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		items = append(items, &BacklogItem{ProcessDefID: r.ProcessID, ActiveCount: r.ActiveCount})
		ids = append(ids, r.ProcessID)
	}

	// 批量取 processName，去 N+1
	names, err := s.taskDAO.ListProcessNamesByID(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("failed to load process names: %w", err)
	}
	for _, item := range items {
		item.ProcessName = names[item.ProcessDefID]
	}
	return items, nil
}
