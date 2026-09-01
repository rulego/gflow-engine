package service

// Tests for task_service_withdraw.go: Withdraw authorization — only the
// instance initiator may withdraw a task.

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

func TestSecFix_WithdrawOnlyInitiatorCanWithdraw(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()

	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID:          "inst-wd",
		ProcessID:   "proc-1",
		Name:        "wd",
		Status:      string(enums.InstanceStatusActive),
		TenantID:    "t1",
		CreatedBy:   "system",
		StartUserID: "starter",
		CreatedAt:   time.Now(),
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:                "task-wd",
		ProcessInstanceID: secFixStrPtr("inst-wd"),
		TaskDefKey:        "approve",
		Name:              "审批",
		TaskType:          "user_task",
		Status:            string(enums.TaskStatusActive),
		Assignee:          secFixStrPtr("approver"),
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         time.Now(),
	}))

	taskSvc := &TaskServiceImpl{
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:      dao.NewHiTaskDAOWithQuery(q),
		workflowEngine: &testEngineDouble{},
	}

	// 非发起人撤回 → 拒绝（引擎以显式 userID 对照实例 StartUserID 校验）
	err := taskSvc.Withdraw(ctx, Actor{UserID: "imposter", TenantID: "t1"}, "task-wd", "想撤回")
	require.Error(t, err, "非发起人撤回必须拒绝")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)
}
