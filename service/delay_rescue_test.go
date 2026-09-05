// 超期 delay 任务检测与救援入口的单元测试；完整救援链路由 test/e2e 真实用例覆盖。
package service

import (
	"context"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/enums"
)

// newDelayRescueTestDB 建 wf_instance / wf_task / wf_process 内存表，返回 query.Query。
func newDelayRescueTestDB(t *testing.T) *query.Query {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:delay_rescue_test?mode=memory&cache=shared&_busy_timeout=30000"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if c, e := db.DB(); e == nil {
			_ = c.Close()
		}
	})
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS wf_instance (
			id TEXT PRIMARY KEY, process_id TEXT NOT NULL, business_key TEXT, name TEXT NOT NULL,
			status TEXT NOT NULL, variables TEXT, current_activity TEXT,
			priority INTEGER NOT NULL DEFAULT 50, parent_id TEXT, tenant_id TEXT NOT NULL,
			created_by TEXT NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_by TEXT, updated_at DATETIME, end_reason TEXT, duration INTEGER,
			ended_at DATETIME, start_user_id TEXT NOT NULL,
			UNIQUE (tenant_id, business_key)
		)`,
		`CREATE TABLE IF NOT EXISTS wf_task (
			id TEXT PRIMARY KEY, process_instance_id TEXT, process_id TEXT, parent_id TEXT,
			task_def_key TEXT, name TEXT, task_type TEXT, description TEXT, status TEXT,
			assignee TEXT, owner TEXT, due_date DATETIME, priority INTEGER, form_key TEXT,
			variables TEXT, claimed_at DATETIME, sequence_order INTEGER, approval_type TEXT,
			approval_rule TEXT, delegate_from TEXT, delegate_reason TEXT, delegate_time DATETIME,
			ended_at DATETIME, comment TEXT, end_reason TEXT, duration INTEGER,
			tenant_id TEXT, created_by TEXT, created_at DATETIME, updated_by TEXT, updated_at DATETIME
		)`,
		`CREATE TABLE IF NOT EXISTS wf_process (
			id TEXT PRIMARY KEY, process_key TEXT NOT NULL, name TEXT NOT NULL,
			version INTEGER NOT NULL DEFAULT 1, category TEXT, description TEXT,
			definition_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
			publish_time DATETIME, tenant_id TEXT NOT NULL, created_by TEXT,
			created_at DATETIME, updated_by TEXT, updated_at DATETIME, ext TEXT,
			process_type TEXT, icon TEXT
		)`,
	} {
		require.NoError(t, db.Exec(ddl).Error, "create table")
	}
	db.Exec("DELETE FROM wf_task")
	db.Exec("DELETE FROM wf_instance")
	db.Exec("DELETE FROM wf_process")
	return query.Use(db)
}

func newDelayRescueRS(q *query.Query) *RuntimeServiceImpl {
	return &RuntimeServiceImpl{
		instanceDAO: dao.NewInstanceDAOWithQuery(q),
		processDAO:  dao.NewProcessDAOWithQuery(q),
		taskDAO:     dao.NewTaskDAOWithQuery(q),
	}
}

// seedDelayTask 插入一条 delay 任务行（due/createdAt/status 可定制）。
func seedDelayTask(t *testing.T, q *query.Query, id, instanceID, status string, due *time.Time, createdAt time.Time) {
	t.Helper()
	task := &model.WfTask{
		ID:                id,
		ProcessInstanceID: &instanceID,
		ProcessID:         "proc-d",
		TaskDefKey:        "delay_node",
		TaskType:          "delay",
		Name:              "delay",
		Status:            status,
		DueDate:           due,
		CreatedAt:         createdAt,
		CreatedBy:         "sys",
		TenantID:          "t1",
	}
	require.NoError(t, dao.NewTaskDAOWithQuery(q).Create(context.Background(), task))
}

func delayStrPtr(s string) *string { return &s }

// 检测口径：超期（过宽限期）的 delay 未终态行命中；未来到期 / 宽限期内 /
// 已完成 / 无到期时间 / 非 delay 类型均不命中。
func TestGetExpiredDelayTasks_Filters(t *testing.T) {
	q := newDelayRescueTestDB(t)
	rs := newDelayRescueRS(q)
	now := time.Now()

	seedDelayTask(t, q, "t-expired", "inst-1", string(enums.TaskStatusPending),
		timePtr(now.Add(-10*time.Minute)), now.Add(-11*time.Minute))
	seedDelayTask(t, q, "t-expired-active", "inst-1", string(enums.TaskStatusActive),
		timePtr(now.Add(-10*time.Minute)), now.Add(-11*time.Minute))
	seedDelayTask(t, q, "t-future", "inst-1", string(enums.TaskStatusPending),
		timePtr(now.Add(time.Hour)), now)
	seedDelayTask(t, q, "t-in-grace", "inst-1", string(enums.TaskStatusPending),
		timePtr(now.Add(-30*time.Second)), now.Add(-time.Minute))
	seedDelayTask(t, q, "t-completed", "inst-1", string(enums.TaskStatusCompleted),
		timePtr(now.Add(-10*time.Minute)), now.Add(-11*time.Minute))
	seedDelayTask(t, q, "t-no-due", "inst-1", string(enums.TaskStatusPending),
		nil, now.Add(-time.Hour))
	// 非 delay 类型（已超期的 userTask）不进入 delay 检测
	ut := &model.WfTask{
		ID: "t-usertask", ProcessInstanceID: delayStrPtr("inst-1"), ProcessID: "proc-d",
		TaskDefKey: "u", TaskType: "user_task", Name: "u",
		Status: string(enums.TaskStatusActive), DueDate: timePtr(now.Add(-time.Hour)),
		CreatedAt: now.Add(-2 * time.Hour), CreatedBy: "sys", TenantID: "t1",
	}
	require.NoError(t, dao.NewTaskDAOWithQuery(q).Create(context.Background(), ut))
	// 其它租户的超期 delay
	seedDelayTask(t, q, "t-other-tenant", "inst-2", string(enums.TaskStatusPending),
		timePtr(now.Add(-10*time.Minute)), now.Add(-11*time.Minute))
	require.NoError(t, dao.NewTaskDAOWithQuery(q).Update(context.Background(),
		&model.WfTask{ID: "t-other-tenant", TenantID: "t2"}))

	got, err := rs.GetExpiredDelayTasks(context.Background(), "t1")
	require.NoError(t, err)
	ids := make([]string, 0, len(got))
	for _, task := range got {
		ids = append(ids, task.ID)
	}
	assert.ElementsMatch(t, []string{"t-expired", "t-expired-active"}, ids)

	// 空租户 = 全租户视角
	all, err := rs.GetExpiredDelayTasks(context.Background(), "")
	require.NoError(t, err)
	assert.Len(t, all, 3)
}

// 参数与状态守卫：空 ID / 不存在 / 非 delay / 已终态 / 未到期 / 实例非 active /
// 跨租户均被拒绝。
func TestRescueExpiredDelayTask_Validation(t *testing.T) {
	q := newDelayRescueTestDB(t)
	rs := newDelayRescueRS(q)
	now := time.Now()
	ctx := context.Background()
	sysActor := Actor{UserID: "sys", TenantID: "t1"}

	// 实例供任务关联
	require.NoError(t, dao.NewInstanceDAOWithQuery(q).Create(ctx, &model.WfInstance{
		ID: "inst-1", ProcessID: "proc-d", Name: "d", Status: string(enums.InstanceStatusActive),
		TenantID: "t1", CreatedBy: "sys", StartUserID: "s", CreatedAt: now,
	}))

	require.Error(t, rs.RescueExpiredDelayTask(ctx, sysActor, ""))
	require.Error(t, rs.RescueExpiredDelayTask(ctx, sysActor, "t-missing"))

	// 未到期：DueDate 在未来
	seedDelayTask(t, q, "t-future", "inst-1", string(enums.TaskStatusPending), timePtr(now.Add(time.Hour)), now)
	err := rs.RescueExpiredDelayTask(ctx, sysActor, "t-future")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not expired")

	// 非 delay 类型
	seedDelayTask(t, q, "t-not-delay", "inst-1", string(enums.TaskStatusPending), timePtr(now.Add(-time.Hour)), now.Add(-2*time.Hour))
	require.NoError(t, dao.NewTaskDAOWithQuery(q).Update(ctx,
		&model.WfTask{ID: "t-not-delay", TaskType: "user_task"}))
	err = rs.RescueExpiredDelayTask(ctx, sysActor, "t-not-delay")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only delay tasks")

	// 已终态
	seedDelayTask(t, q, "t-completed", "inst-1", string(enums.TaskStatusCompleted), timePtr(now.Add(-time.Hour)), now.Add(-2*time.Hour))
	err = rs.RescueExpiredDelayTask(ctx, sysActor, "t-completed")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only waiting tasks")

	// 无到期时间（求值失败的动态 delay）
	seedDelayTask(t, q, "t-no-due", "inst-1", string(enums.TaskStatusPending), nil, now.Add(-2*time.Hour))
	err = rs.RescueExpiredDelayTask(ctx, sysActor, "t-no-due")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no due date")

	// 超期但跨租户
	seedDelayTask(t, q, "t-cross", "inst-1", string(enums.TaskStatusPending), timePtr(now.Add(-time.Hour)), now.Add(-2*time.Hour))
	require.NoError(t, dao.NewTaskDAOWithQuery(q).Update(ctx, &model.WfTask{ID: "t-cross", TenantID: "t9"}))
	err = rs.RescueExpiredDelayTask(ctx, sysActor, "t-cross")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPermissionDenied)

	// 超期但实例已终止
	require.NoError(t, dao.NewInstanceDAOWithQuery(q).Create(ctx, &model.WfInstance{
		ID: "inst-term", ProcessID: "proc-d", Name: "d", Status: string(enums.InstanceStatusTerminated),
		TenantID: "t1", CreatedBy: "sys", StartUserID: "s", CreatedAt: now,
	}))
	seedDelayTask(t, q, "t-inst-term", "inst-term", string(enums.TaskStatusPending), timePtr(now.Add(-time.Hour)), now.Add(-2*time.Hour))
	err = rs.RescueExpiredDelayTask(ctx, sysActor, "t-inst-term")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only active instances")
}

// timePtr 返回时间指针（测试构造 DueDate 用）。
func timePtr(t time.Time) *time.Time { return &t }
