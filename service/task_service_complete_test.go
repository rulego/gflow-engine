package service

// Tests for task_service_complete.go: authorization on the
// CompleteWithApproval/Approve/Reject entry points. An explicit UserID (or
// the ctx user) must always be checked against the assignee, even when the
// caller's ctx carries no CallingMode marker.

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

// explicitAuthzDB 内存 SQLite 最小 schema（wf_instance/wf_task/wf_hi_task）。
func explicitAuthzDB(t *testing.T) *query.Query {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:explicit_authz_test?mode=memory&cache=shared&_busy_timeout=30000"), &gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
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
			start_user_id TEXT NOT NULL,
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
		`CREATE TABLE IF NOT EXISTS wf_hi_task (
			id TEXT PRIMARY KEY, process_instance_id TEXT, process_id TEXT, parent_id TEXT,
			task_def_key TEXT, name TEXT, task_type TEXT, description TEXT, status TEXT,
			assignee TEXT, owner TEXT, due_date DATETIME, priority INTEGER, form_key TEXT,
			variables TEXT, claimed_at DATETIME, sequence_order INTEGER, approval_type TEXT,
			approval_rule TEXT, delegate_from TEXT, delegate_reason TEXT, delegate_time DATETIME,
			ended_at DATETIME, comment TEXT, end_reason TEXT, duration INTEGER,
			tenant_id TEXT, created_by TEXT, created_at DATETIME, updated_by TEXT, updated_at DATETIME
		)`,
		`CREATE TABLE IF NOT EXISTS wf_task_comment (
			id TEXT PRIMARY KEY, task_id TEXT NOT NULL, process_instance_id TEXT NOT NULL DEFAULT '',
			tenant_id TEXT NOT NULL DEFAULT '', user_id TEXT NOT NULL,
			user_name TEXT NOT NULL DEFAULT '', content TEXT NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)`,
	}
	for _, ddl := range ddls {
		require.NoError(t, db.Exec(ddl).Error)
	}
	db.Exec("DELETE FROM wf_instance")
	db.Exec("DELETE FROM wf_task")
	db.Exec("DELETE FROM wf_hi_task")
	db.Exec("DELETE FROM wf_task_comment")
	return query.Use(db)
}

// explicitAuthzSeed 建一个 active 实例 + 一个 assignee=userA 的 active 任务。
func explicitAuthzSeed(t *testing.T, q *query.Query) {
	t.Helper()
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID:          "inst-authz",
		ProcessID:   "proc-1",
		Name:        "authz_test",
		Status:      string(enums.InstanceStatusActive),
		StartUserID: "starter",
		TenantID:    "t1",
		CreatedBy:   "starter",
		CreatedAt:   time.Now(),
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:                "task-authz",
		ProcessInstanceID: secFixStrPtr("inst-authz"),
		TaskDefKey:        "approve",
		Name:              "审批",
		TaskType:          "user_task",
		Status:            string(enums.TaskStatusActive),
		Assignee:          secFixStrPtr("userA"),
		ApprovalType:      string(enums.ApprovalTypeSingle),
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         time.Now(),
	}))
}

func explicitAuthzTaskService(q *query.Query) *TaskServiceImpl {
	return &TaskServiceImpl{
		taskDAO:     dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:   dao.NewHiTaskDAOWithQuery(q),
		idGenerator: DefaultIDGenerator,
	}
}

// TestExplicitUserID_RejectsNonAssigneeOnPlainContext:
// ctx=context.Background()（未标记 CallingMode）+ request.UserID=他人 → 必须 ErrPermissionDenied。
func TestExplicitUserID_RejectsNonAssigneeOnPlainContext(t *testing.T) {
	q := explicitAuthzDB(t)
	explicitAuthzSeed(t, q)
	taskSvc := explicitAuthzTaskService(q)

	err := taskSvc.CompleteWithApproval(context.Background(), Actor{UserID: "attacker", TenantID: "t1"}, &ApprovalRequest{
		TaskID:         "task-authz",
		ApprovalResult: enums.ApprovalResultApproved,
	})
	require.Error(t, err, "显式 UserID=非 assignee 时必须拒绝")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)

	// Approve/Reject 快捷方式同样必须拒绝
	err = taskSvc.Approve(context.Background(), Actor{UserID: "attacker", TenantID: "t1"}, "task-authz", "", nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied), "Approve 期望 ErrPermissionDenied，got %v", err)

	err = taskSvc.Reject(context.Background(), Actor{UserID: "attacker", TenantID: "t1"}, "task-authz", "", nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied), "Reject 期望 ErrPermissionDenied，got %v", err)

	// 任务必须仍处于 active（未被越权完成）
	task, gerr := taskSvc.GetTask(context.Background(), SystemActor(), "task-authz")
	require.NoError(t, gerr)
	require.Equal(t, string(enums.TaskStatusActive), task.Status, "被拒绝的审批不得改变任务状态")
}

// TestExplicitUserID_MissingIdentityFailsClosed 无显式 UserID 且 ctx 无用户 → 必须拒绝
// （ErrAuthenticationRequired），而不是静默跳过校验。
func TestExplicitUserID_MissingIdentityFailsClosed(t *testing.T) {
	q := explicitAuthzDB(t)
	explicitAuthzSeed(t, q)
	taskSvc := explicitAuthzTaskService(q)

	err := taskSvc.CompleteWithApproval(context.Background(), Actor{}, &ApprovalRequest{
		TaskID:         "task-authz",
		ApprovalResult: enums.ApprovalResultApproved,
	})
	require.Error(t, err, "无任何身份信息时必须 fail-closed")
	require.True(t, errors.Is(err, ErrAuthenticationRequired), "期望 ErrAuthenticationRequired，got %v", err)
}

// TestExplicitUserID_AssigneeSucceedsOnPlainContext 正向对照：assignee 本人在裸 ctx 下
// 凭显式 UserID 可以完成审批（不能因收紧而误伤正常用法）。
func TestExplicitUserID_AssigneeSucceedsOnPlainContext(t *testing.T) {
	q := explicitAuthzDB(t)
	explicitAuthzSeed(t, q)
	taskSvc := explicitAuthzTaskService(q)

	err := taskSvc.CompleteWithApproval(context.Background(), Actor{UserID: "userA", TenantID: "t1"}, &ApprovalRequest{
		TaskID:         "task-authz",
		ApprovalResult: enums.ApprovalResultApproved,
		Comment:        "ok",
	})
	require.NoError(t, err, "assignee 本人凭显式 UserID 审批应成功")

	task, gerr := taskSvc.GetTask(context.Background(), SystemActor(), "task-authz")
	require.NoError(t, gerr)
	require.Equal(t, string(enums.TaskStatusCompleted), task.Status)
}

// TestExplicitUserID_CtxUserFallbackRejected 裸 ctx 携带用户身份、无显式 UserID：
// 操作人取 ctx 用户，非 assignee 仍必须拒绝。
func TestExplicitUserID_CtxUserFallbackRejected(t *testing.T) {
	q := explicitAuthzDB(t)
	explicitAuthzSeed(t, q)
	taskSvc := explicitAuthzTaskService(q)

	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "userB", TenantID: "t1", UserName: "B"})
	err := taskSvc.CompleteWithApproval(ctx, Actor{UserID: "userB", TenantID: "t1", UserName: "B"}, &ApprovalRequest{
		TaskID:         "task-authz",
		ApprovalResult: enums.ApprovalResultApproved,
	})
	require.Error(t, err, "ctx 用户非 assignee 时必须拒绝")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)
}

// TestExplicitUserID_InternalCallingModePreserved 引擎内部调用（已标记 CallingModeInternal）
// 不受升级影响：无 ctx 用户也能完成（aspect 推进依赖此语义）。
func TestExplicitUserID_InternalCallingModePreserved(t *testing.T) {
	q := explicitAuthzDB(t)
	explicitAuthzSeed(t, q)
	taskSvc := explicitAuthzTaskService(q)

	ctx := WithInternalCallingMode(context.Background())
	err := taskSvc.CompleteWithApproval(ctx, Actor{}, &ApprovalRequest{
		TaskID:         "task-authz",
		ApprovalResult: enums.ApprovalResultApproved,
	})
	require.NoError(t, err, "内部调用路径应跳过 assignee 校验并成功完成")

	task, gerr := taskSvc.GetTask(context.Background(), SystemActor(), "task-authz")
	require.NoError(t, gerr)
	require.Equal(t, string(enums.TaskStatusCompleted), task.Status)
}

// TestExplicitUserID_InternalCtxWithRealUserDowngraded 第二重信号回归：
// 宿主把内部标记过的 ctx（CallingModeInternal）误用进公共入口时，携带真实用户
// 身份的请求必须降级为 API 模式——非 assignee 不能借内部模式绕过 assignee 校验。
// 纯系统上下文（无用户）保持内部模式的行为由 TestExplicitUserID_InternalCallingModePreserved 覆盖。
func TestExplicitUserID_InternalCtxWithRealUserDowngraded(t *testing.T) {
	q := explicitAuthzDB(t)
	explicitAuthzSeed(t, q)
	taskSvc := explicitAuthzTaskService(q)

	// 内部标记 ctx + 真实用户（非 assignee）→ 降级为 API 模式 → assignee 校验拦截
	ctx := WithInternalCallingMode(SetUserToCtx(context.Background(),
		&Actor{UserID: "userB", TenantID: "t1", UserName: "B"}))
	err := taskSvc.CompleteWithApproval(ctx, Actor{UserID: "userB", TenantID: "t1", UserName: "B"}, &ApprovalRequest{
		TaskID:         "task-authz",
		ApprovalResult: enums.ApprovalResultApproved,
	})
	require.Error(t, err, "real user on internal-marked ctx must NOT bypass assignee check")
	require.ErrorIs(t, err, ErrPermissionDenied)

	// 任务必须仍处于 active（未被越权完成）
	task, gerr := taskSvc.GetTask(context.Background(), SystemActor(), "task-authz")
	require.NoError(t, gerr)
	require.Equal(t, string(enums.TaskStatusActive), task.Status)
}
