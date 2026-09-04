package service

// Tests for task_service_assignment.go: Transfer authorization — only the
// current assignee may transfer, and cross-tenant transfers are rejected.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/enums"
)

func TestSecFix_TransferOnlyAssigneeCanTransfer(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()
	now := time.Now()

	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:         "task-tr",
		TaskDefKey: "approve",
		Name:       "审批",
		TaskType:   "user_task",
		Status:     string(enums.TaskStatusActive),
		Assignee:   secFixStrPtr("userA"),
		TenantID:   "t1",
		CreatedBy:  "system",
		CreatedAt:  now,
	}))

	taskSvc := &TaskServiceImpl{
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:      dao.NewHiTaskDAOWithQuery(q),
		workflowEngine: &testEngineDouble{},
	}

	// fromUserID 不是当前 assignee → 拒绝（显式参数即操作人，必须拥有任务）
	err := taskSvc.Transfer(ctx, Actor{UserID: "userB", TenantID: "t1"}, "task-tr", "userC", "reason")
	require.Error(t, err, "非 assignee 转办必须拒绝")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)

	// fromUserID 是 assignee → 放行
	require.NoError(t, taskSvc.Transfer(ctx, Actor{UserID: "userA", TenantID: "t1"}, "task-tr", "userC", "reason"))
}

func TestSecFix_TransferCrossTenantRejected(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()

	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:         "task-tr2",
		TaskDefKey: "approve",
		Name:       "审批",
		TaskType:   "user_task",
		Status:     string(enums.TaskStatusActive),
		Assignee:   secFixStrPtr("userA"),
		TenantID:   "t1",
		CreatedBy:  "system",
		CreatedAt:  time.Now(),
	}))

	taskSvc := &TaskServiceImpl{
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:      dao.NewHiTaskDAOWithQuery(q),
		workflowEngine: &testEngineDouble{},
	}

	ctxOther := SetUserToCtx(ctx, &Actor{UserID: "userA", UserName: "A", TenantID: "t2"})
	err := taskSvc.Transfer(ctxOther, Actor{UserID: "userA", UserName: "A", TenantID: "t2"}, "task-tr2", "userC", "reason")
	require.Error(t, err, "跨租户转办必须拒绝")
	require.True(t, errors.Is(err, ErrNotFound), "跨租户应伪装成 not found，got %v", err)
}

func TestSecFix_SetAssigneeRequiresAdmin(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-sa", TaskDefKey: "approve", Name: "审批", TaskType: "user_task",
		Status: string(enums.TaskStatusActive), Assignee: secFixStrPtr("userA"),
		TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))
	taskSvc := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), workflowEngine: &testEngineDouble{}}

	// 普通同租户用户非管理员 → 拒绝（改派他人任务是管理操作）
	err := taskSvc.SetAssignee(ctx, Actor{UserID: "userB", TenantID: "t1"}, "task-sa", "userC")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)

	// 空身份 → 拒绝
	require.Error(t, taskSvc.SetAssignee(ctx, Actor{TenantID: "t1"}, "task-sa", "userC"))

	// 管理员 → 放行
	require.NoError(t, taskSvc.SetAssignee(ctx, Actor{UserID: "admin1", TenantID: "t1", SuperAdmin: true}, "task-sa", "userC"))
	persisted, err := taskSvc.taskDAO.Get(ctx, "task-sa")
	require.NoError(t, err)
	require.Equal(t, "userC", *persisted.Assignee)
}

func TestSecFix_SetOwnerRequiresAdmin(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-so", TaskDefKey: "approve", Name: "审批", TaskType: "user_task",
		Status: string(enums.TaskStatusActive), Assignee: secFixStrPtr("userA"),
		TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))
	taskSvc := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), workflowEngine: &testEngineDouble{}}

	// 普通同租户用户 → 拒绝
	err := taskSvc.SetOwner(ctx, Actor{UserID: "userB", TenantID: "t1"}, "task-so", "userB")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)

	// 管理员 → 放行
	require.NoError(t, taskSvc.SetOwner(ctx, Actor{UserID: "admin1", TenantID: "t1", SuperAdmin: true}, "task-so", "userB"))
}

func TestSecFix_ResolveRequiresOperator(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()
	owner, delegatee := "owner-1", "delegatee-1"
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-rs", TaskDefKey: "approve", Name: "审批", TaskType: "user_task",
		Status: string(enums.TaskStatusActive), Assignee: &delegatee, Owner: &owner,
		TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))
	taskSvc := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), workflowEngine: &testEngineDouble{}}

	// 无关同租户用户 → 拒绝（既非被委派人、非原办理人、也非管理员）
	err := taskSvc.Resolve(ctx, Actor{UserID: "intruder", TenantID: "t1"}, "task-rs")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)

	// 原办理人（owner）→ 放行
	require.NoError(t, taskSvc.Resolve(ctx, Actor{UserID: owner, TenantID: "t1"}, "task-rs"))
}
