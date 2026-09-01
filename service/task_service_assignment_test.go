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
