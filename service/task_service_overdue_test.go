package service

import (
	"context"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// filterOverdueTasks 单测：管理员全局视角下过滤 DueDate 过期的 active task。
func TestFilterOverdueTasks_Global(t *testing.T) {
	now := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	past := now.Add(-2 * time.Hour)
	future := now.Add(2 * time.Hour)

	tasks := []*model.WfTask{
		{ID: "t1", DueDate: &past},   // 过期
		{ID: "t2", DueDate: &future}, // 未过期
		{ID: "t3", DueDate: nil},     // 无到期
		{ID: "t4", DueDate: &past},   // 过期
	}

	got := filterOverdueTasks(tasks, now)
	if len(got) != 2 {
		t.Fatalf("expected 2 overdue, got %d", len(got))
	}
	ids := map[string]bool{got[0].ID: true, got[1].ID: true}
	if !ids["t1"] || !ids["t4"] {
		t.Errorf("expected t1+t4, got %v", ids)
	}
}

func TestFilterOverdueTasks_Empty(t *testing.T) {
	if got := filterOverdueTasks(nil, time.Now()); len(got) != 0 {
		t.Errorf("expected 0, got %d", len(got))
	}
}

// overdueTestDB 内存 SQLite 建 wf_task，返回 TaskDAO。
func overdueTestDB(t *testing.T) *dao.TaskDAO {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:overdue_test?mode=memory&cache=shared&_busy_timeout=30000"), &gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if c, e := db.DB(); e == nil {
			_ = c.Close()
		}
	})
	require.NoError(t, db.Exec(`CREATE TABLE IF NOT EXISTS wf_task (
		id TEXT PRIMARY KEY,
		process_instance_id TEXT,
		process_id TEXT,
		parent_id TEXT,
		task_def_key TEXT,
		name TEXT,
		task_type TEXT,
		description TEXT,
		status TEXT,
		assignee TEXT,
		owner TEXT,
		due_date DATETIME,
		priority INTEGER,
		form_key TEXT,
		variables TEXT,
		claimed_at DATETIME,
		sequence_order INTEGER,
		approval_type TEXT,
		approval_rule TEXT,
		delegate_from TEXT,
		delegate_reason TEXT,
		delegate_time DATETIME,
		ended_at DATETIME,
		comment TEXT,
		end_reason TEXT,
		duration INTEGER,
		tenant_id TEXT,
		created_by TEXT,
		created_at DATETIME,
		updated_by TEXT,
		updated_at DATETIME
	)`).Error)
	require.NoError(t, db.Exec("DELETE FROM wf_task").Error)
	return dao.NewTaskDAOWithQuery(query.Use(db))
}

// TestGetOverdueTasks_DBFilter 端到端验证过期判定下推 DB：
// 只返回 due_date 早于 now 的 active/pending 任务，未过期与无 due_date 的行不进内存。
func TestGetOverdueTasks_DBFilter(t *testing.T) {
	d := overdueTestDB(t)
	ctx := context.Background()
	// 用 time.Now() 锚点（服务侧 GetOverdueTasks 用 time.Now() 判过期），避免硬编码日期随时间失效。
	now := time.Now().UTC()
	past := now.Add(-2 * time.Hour)
	future := now.Add(2 * time.Hour)
	statusActive := string(enums.TaskStatusActive)

	seed := []*model.WfTask{
		{ID: "overdue-1", Status: statusActive, DueDate: &past, TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now, CreatedBy: "s"},
		{ID: "overdue-2", Status: statusActive, DueDate: &past, TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now, CreatedBy: "s"},
		{ID: "future", Status: statusActive, DueDate: &future, TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now, CreatedBy: "s"},
		{ID: "no-due", Status: statusActive, DueDate: nil, TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now, CreatedBy: "s"},
		{ID: "other-tenant", Status: statusActive, DueDate: &past, TenantID: "t2", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now, CreatedBy: "s"},
	}
	for _, tk := range seed {
		require.NoError(t, d.Create(ctx, tk))
	}

	svc := &TaskServiceImpl{taskDAO: d, workflowEngine: noopBacklogEngine{}}
	got, total, err := svc.GetOverdueTasks(ctx, Actor{TenantID: "t1"}, &dto.TaskQuery{PageRequest: dto.PageRequest{PageSize: 100}})
	require.NoError(t, err)

	ids := map[string]bool{}
	for _, tk := range got {
		ids[tk.ID] = true
	}
	require.True(t, ids["overdue-1"], "should include overdue-1")
	require.True(t, ids["overdue-2"], "should include overdue-2")
	require.False(t, ids["future"], "should exclude future-due task")
	require.False(t, ids["no-due"], "should exclude task without due_date")
	require.False(t, ids["other-tenant"], "should exclude other tenant")
	require.EqualValues(t, 2, total)
}

func TestGetOverdueTasks_EmptyTenant(t *testing.T) {
	d := overdueTestDB(t)
	svc := &TaskServiceImpl{taskDAO: d, workflowEngine: noopBacklogEngine{}}
	if _, _, err := svc.GetOverdueTasks(context.Background(), Actor{}, nil); err == nil {
		t.Fatal("expected error for empty tenant")
	}
}

// TestScanOverdueTasks_AcrossTenants 跨租户巡检：只返回已过期的 active/pending 任务。
func TestScanOverdueTasks_AcrossTenants(t *testing.T) {
	d := overdueTestDB(t)
	svc := &TaskServiceImpl{taskDAO: d, workflowEngine: noopBacklogEngine{}}
	ctx := context.Background()
	past := time.Now().Add(-2 * time.Hour)
	future := time.Now().Add(2 * time.Hour)

	seed := func(id, tenant, status string, due *time.Time, assignee string) {
		require.NoError(t, d.Create(ctx, &model.WfTask{
			ID: id, Status: status, TenantID: tenant, DueDate: due,
			Assignee: &assignee, Name: "t", TaskType: "user_task",
			CreatedAt: time.Now(), CreatedBy: "sys",
		}))
	}
	seed("od-1", "t1", string(enums.TaskStatusActive), &past, "u1")
	seed("od-2", "t2", string(enums.TaskStatusPending), &past, "")
	seed("od-3", "t1", string(enums.TaskStatusActive), &future, "u1")  // 未到期
	seed("od-4", "t1", string(enums.TaskStatusCompleted), &past, "u1") // 已完成

	tasks, err := svc.ScanOverdueTasks(ctx, 100)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, tk := range tasks {
		ids[tk.ID] = true
	}
	require.True(t, ids["od-1"], "租户1 过期任务应命中")
	require.True(t, ids["od-2"], "租户2 过期任务应命中（跨租户）")
	require.False(t, ids["od-3"], "未到期任务不应命中")
	require.False(t, ids["od-4"], "已完成任务不应命中")
}
