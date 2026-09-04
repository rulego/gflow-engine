package service

// Tests for task_service_crud.go: GetTask cross-tenant rejection and
// DeleteTask fail-closed behavior. Also hosts the shared secFix sqlite
// fixtures used by the other per-method security tests.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/enums"
)

// secFixStrPtr 字符串指针辅助
func secFixStrPtr(s string) *string { return &s }

// secFixDB 在内存 SQLite 上建 wf_instance + wf_task + wf_hi_task，返回 query.Query。
// 三表共享同一连接（file:secfix_test?mode=memory&cache=shared），便于跨表测试。
func secFixDB(t *testing.T) *query.Query {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:secfix_test?mode=memory&cache=shared&_busy_timeout=30000"), &gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if c, e := db.DB(); e == nil {
			_ = c.Close()
		}
	})
	ddls := []string{
		`CREATE TABLE IF NOT EXISTS wf_instance (
			id TEXT PRIMARY KEY, process_id TEXT NOT NULL, business_key TEXT, name TEXT NOT NULL,
			status TEXT NOT NULL, variables TEXT, current_activity TEXT,
			priority INTEGER NOT NULL DEFAULT 50, parent_id TEXT, tenant_id TEXT NOT NULL,
			created_by TEXT NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_by TEXT, updated_at DATETIME, end_reason TEXT, duration INTEGER, ended_at DATETIME,
			start_user_id TEXT NOT NULL
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
		`CREATE TABLE IF NOT EXISTS wf_hi_task (
			id TEXT PRIMARY KEY, process_instance_id TEXT, process_id TEXT, parent_id TEXT,
			task_def_key TEXT, name TEXT, task_type TEXT, description TEXT, status TEXT,
			assignee TEXT, owner TEXT, due_date DATETIME, priority INTEGER, form_key TEXT,
			variables TEXT, claimed_at DATETIME, sequence_order INTEGER, approval_type TEXT,
			approval_rule TEXT, delegate_from TEXT, delegate_reason TEXT, delegate_time DATETIME,
			ended_at DATETIME, comment TEXT, end_reason TEXT, duration INTEGER,
			tenant_id TEXT, created_by TEXT, created_at DATETIME, updated_by TEXT, updated_at DATETIME
		)`,
	}
	for _, ddl := range ddls {
		require.NoError(t, db.Exec(ddl).Error)
	}
	db.Exec("DELETE FROM wf_instance")
	db.Exec("DELETE FROM wf_task")
	db.Exec("DELETE FROM wf_hi_task")
	return query.Use(db)
}

// TestGetTask_RejectsCrossTenant 验证：租户 A 用户按 ID 读取租户 B 的任务被拒绝。
func TestGetTask_RejectsCrossTenant(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()
	now := time.Now()

	// 租户 B 的任务
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:                "task-B",
		ProcessInstanceID: secFixStrPtr("inst-B"),
		TaskDefKey:        "approve",
		Name:              "审批",
		TaskType:          "user_task",
		Status:            string(enums.TaskStatusActive),
		Assignee:          secFixStrPtr("userB"),
		TenantID:          "tenantB",
		CreatedBy:         "system",
		CreatedAt:         now,
	}))

	taskSvc := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), hiTaskDAO: dao.NewHiTaskDAOWithQuery(q)}

	// 租户 A 用户读 B 的任务 → 必须拒绝
	aCtx := SetUserToCtx(ctx, &Actor{UserID: "userA", TenantID: "tenantA", UserName: "A"})
	_, err := taskSvc.GetTask(aCtx, ActorFromCtx(aCtx), "task-B")
	require.Error(t, err, "跨租户读取任务应被拒绝")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)

	// 反向验证：同租户 B 用户可读
	bCtx := SetUserToCtx(ctx, &Actor{UserID: "userB", TenantID: "tenantB", UserName: "B"})
	task, err := taskSvc.GetTask(bCtx, ActorFromCtx(bCtx), "task-B")
	require.NoError(t, err)
	require.Equal(t, "task-B", task.ID)
}

// TestSecFix_DeleteTaskUnassignedFailsClosed: DeleteTask 在任务所属实例查不到时
// fail-closed（拒绝删除而不是放行）。
func TestSecFix_DeleteTaskUnassignedFailsClosed(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()

	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:                "task-del",
		ProcessInstanceID: secFixStrPtr("inst-ghost"), // 实例不存在
		TaskDefKey:        "approve",
		Name:              "审批",
		TaskType:          "user_task",
		Status:            string(enums.TaskStatusPending),
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         time.Now(),
	}))

	taskSvc := &TaskServiceImpl{
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:      dao.NewHiTaskDAOWithQuery(q),
		workflowEngine: &testEngineDouble{runtime: &testRuntimeDouble{}},
	}

	ctxUser := SetUserToCtx(ctx, &Actor{UserID: "starter", UserName: "S", TenantID: "t1"})
	err := taskSvc.DeleteTask(ctxUser, Actor{UserID: "starter", UserName: "S", TenantID: "t1"}, "task-del", "clean")
	require.Error(t, err, "实例不存在时必须拒绝删除（fail-closed）")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)

	// 任务仍在
	count, cerr := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq("task-del")).Count()
	require.NoError(t, cerr)
	require.EqualValues(t, 1, count)
}

// TestSecFix_SystemDeleteActiveInstance 系统身份删除 active 实例上的任务应成功：
// 与普通删除同一条 WithInstanceTx 持锁路径，不绕过实例行锁。
func TestSecFix_SystemDeleteActiveInstance(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	now := time.Now()

	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-sys-active", ProcessID: "p1", Name: "sys_del_active",
		Status: string(enums.InstanceStatusActive), StartUserID: "starter",
		TenantID: "t1", CreatedBy: "starter", CreatedAt: now,
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-sys-active", ProcessInstanceID: secFixStrPtr("inst-sys-active"), TaskDefKey: "approve",
		Name: "审批", TaskType: "user_task", Status: string(enums.TaskStatusActive),
		Assignee: secFixStrPtr("userA"), TenantID: "t1", CreatedBy: "system", CreatedAt: now,
	}))

	taskSvc := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), workflowEngine: &testEngineDouble{}}
	require.NoError(t, taskSvc.DeleteTask(ctx, SystemActor(), "task-sys-active", "cleanup"))

	count, cerr := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq("task-sys-active")).Count()
	require.NoError(t, cerr)
	require.EqualValues(t, 0, count, "系统身份删除 active 实例任务后应已删除")
}

// TestSecFix_SystemDeleteTerminalInstanceRejected 系统身份删除终态实例上的任务应被
// WithInstanceTx 拒绝（ErrInstanceTerminal），证明系统删除与普通删除一样受实例锁与
// 终态守卫约束。
func TestSecFix_SystemDeleteTerminalInstanceRejected(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	now := time.Now()

	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-sys-terminal", ProcessID: "p1", Name: "sys_del_term",
		Status: string(enums.InstanceStatusCompleted), StartUserID: "starter",
		TenantID: "t1", CreatedBy: "starter", CreatedAt: now, EndedAt: &now,
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-sys-terminal", ProcessInstanceID: secFixStrPtr("inst-sys-terminal"), TaskDefKey: "approve",
		Name: "审批", TaskType: "user_task", Status: string(enums.TaskStatusActive),
		Assignee: secFixStrPtr("userA"), TenantID: "t1", CreatedBy: "system", CreatedAt: now,
	}))

	taskSvc := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), workflowEngine: &testEngineDouble{}}
	err := taskSvc.DeleteTask(ctx, SystemActor(), "task-sys-terminal", "cleanup")
	require.Error(t, err, "系统身份删除终态实例任务必须被拒绝")
	require.True(t, errors.Is(err, ErrInstanceTerminal), "期望 ErrInstanceTerminal，got %v", err)

	count, cerr := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq("task-sys-terminal")).Count()
	require.NoError(t, cerr)
	require.EqualValues(t, 1, count, "终态实例任务应保留（未被绕过锁直删）")
}
