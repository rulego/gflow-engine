package service

// PR2 租户隔离补齐用例：
// Return / Unclaim 跨租户按 not found 隐藏；
// Delegate / AddSign 的目标用户跨租户按 PermissionDenied 拒绝。

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

// tenantCheckingIdentity 在 IdentityService 之上叠加 TenantMembershipChecker，
// IsUserInTenant 由 membership map 驱动。本组用例只走 IsUserInTenant（委派/加签
// 目标租户校验），其余 IdentityService 方法不会被调用。
type tenantCheckingIdentity struct {
	IdentityService
	membership map[string]bool
}

func (m *tenantCheckingIdentity) IsUserInTenant(_ context.Context, tenantID, userID string) (bool, error) {
	return m.membership[userID], nil
}

func TestSecFix_ReturnCrossTenantRejected(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-ret-xt", TaskDefKey: "approve", Name: "审批", TaskType: "user_task",
		Status: string(enums.TaskStatusActive), Assignee: secFixStrPtr("userA"),
		TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))
	taskSvc := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), workflowEngine: candGroupEngine{}}

	err := taskSvc.Return(ctx, Actor{UserID: "userB", TenantID: "t2"}, "task-ret-xt", "node1", "reason")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrNotFound), "跨租户 Return 应伪装成 not found，got %v", err)
}

func TestSecFix_UnclaimCrossTenantRejected(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-unclaim-xt", TaskDefKey: "approve", Name: "审批", TaskType: "user_task",
		Status: string(enums.TaskStatusActive), Assignee: secFixStrPtr("userA"),
		TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))
	taskSvc := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), workflowEngine: candGroupEngine{}}

	err := taskSvc.Unclaim(ctx, Actor{UserID: "userB", TenantID: "t2"}, "task-unclaim-xt")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrNotFound), "跨租户 Unclaim 应伪装成 not found，got %v", err)
}

func TestSecFix_DelegateTargetCrossTenantRejected(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-del-xt", TaskDefKey: "approve", Name: "审批", TaskType: "user_task",
		Status: string(enums.TaskStatusActive), Assignee: secFixStrPtr("userA"),
		TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))
	ident := &tenantCheckingIdentity{membership: map[string]bool{"userA": true, "userB": false}}
	taskSvc := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), workflowEngine: candGroupEngine{identity: ident}}

	err := taskSvc.Delegate(ctx, Actor{UserID: "userA", TenantID: "t1"}, "task-del-xt", "userB", "reason")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied), "跨租户委派目标应拒绝，got %v", err)
}

func TestSecFix_AddSignTargetCrossTenantRejected(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-addsign-xt", TaskDefKey: "approve", Name: "审批", TaskType: "user_task",
		Status: string(enums.TaskStatusActive), Assignee: secFixStrPtr("userA"),
		TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))
	ident := &tenantCheckingIdentity{membership: map[string]bool{"userA": true, "userB": false}}
	taskSvc := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), workflowEngine: candGroupEngine{identity: ident}}

	err := taskSvc.AddSign(ctx, Actor{UserID: "userA", TenantID: "t1"}, "task-addsign-xt", []string{"userB"}, "reason")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied), "跨租户加签目标应拒绝，got %v", err)
}
