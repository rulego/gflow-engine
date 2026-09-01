package dao

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
)

// Tests for hi_task_dao.go.

func TestHiTaskDAO_List_InstanceIDs(t *testing.T) {
	q := newTestQuery(t, ddlWfHiTask)
	d := NewHiTaskDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	seed := func(id, instanceID string) {
		entity := &model.WfHiTask{
			ID:        id,
			Name:      id,
			TaskType:  "user_task",
			Status:    "completed",
			TenantID:  "tenant1",
			CreatedAt: now,
		}
		if instanceID != "" {
			entity.ProcessInstanceID = &instanceID
		}
		if err := d.Create(ctx, entity); err != nil {
			t.Fatalf("seed hi task %s: %v", id, err)
		}
	}
	seed("h1", "inst-1")
	seed("h2", "inst-2")
	seed("h3", "inst-3")

	tasks, total, err := d.List(ctx, &dto.TaskQuery{InstanceIDs: []string{"inst-2", "inst-3"}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	ids := taskIDSet(tasks)
	if !ids["h2"] || !ids["h3"] || ids["h1"] {
		t.Errorf("InstanceIDs filter result = %v", ids)
	}
}

// HiTaskDAO.List 时间窗过滤：weekCompleted/aggregateMonthCompleted 等统计
// 依赖 EndedAfter/EndedBefore/CreatedAfter 在历史查询路径上生效。
func TestHiTaskDAO_List_TimeWindowFilters(t *testing.T) {
	q := newTestQuery(t, ddlWfHiTask)
	d := NewHiTaskDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	seed := func(id string, endedAt time.Time) {
		if err := d.Create(ctx, &model.WfHiTask{
			ID:        id,
			Name:      id,
			TaskType:  "user_task",
			Status:    "completed",
			TenantID:  "tenant1",
			CreatedAt: now,
			EndedAt:   &endedAt,
		}); err != nil {
			t.Fatalf("seed hi task %s: %v", id, err)
		}
	}
	weekAgo := now.AddDate(0, 0, -7)
	seed("recent-1", now.Add(-2*time.Hour))
	seed("recent-2", now.Add(-24*time.Hour))
	seed("old-1", weekAgo.Add(-24*time.Hour)) // 一周多以前完成
	seed("old-2", now.AddDate(0, 0, -40))

	// EndedAfter：只保留时间窗内完成的任务
	cutoff := now.AddDate(0, 0, -7)
	tasks, total, err := d.List(ctx, &dto.TaskQuery{EndedAfter: &cutoff})
	if err != nil {
		t.Fatalf("List EndedAfter: %v", err)
	}
	if total != 2 {
		t.Errorf("EndedAfter total = %d, want 2", total)
	}
	ids := taskIDSet(tasks)
	if !ids["recent-1"] || !ids["recent-2"] || ids["old-1"] || ids["old-2"] {
		t.Errorf("EndedAfter filter result = %v", ids)
	}

	// EndedBefore：只保留早于边界完成的任务
	before := now.Add(-30 * time.Hour)
	tasks2, total2, err := d.List(ctx, &dto.TaskQuery{EndedBefore: &before})
	if err != nil {
		t.Fatalf("List EndedBefore: %v", err)
	}
	if total2 != 2 {
		t.Errorf("EndedBefore total = %d, want 2", total2)
	}
	ids2 := taskIDSet(tasks2)
	if !ids2["old-1"] || !ids2["old-2"] || ids2["recent-1"] || ids2["recent-2"] {
		t.Errorf("EndedBefore filter result = %v", ids2)
	}

	// CreatedAfter 同样生效：边界取未来时间 → 全部排除
	future := now.Add(1 * time.Hour)
	_, total3, err := d.List(ctx, &dto.TaskQuery{CreatedAfter: &future})
	if err != nil {
		t.Fatalf("List CreatedAfter: %v", err)
	}
	if total3 != 0 {
		t.Errorf("CreatedAfter(future) total = %d, want 0", total3)
	}
}
