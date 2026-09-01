package service

// Tests for task_service_claim.go: Claim rejects users outside the task's
// candidate pool and allows claims when the pool is empty.

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

// TestClaim_RejectsNonCandidate 验证：wf_task_assignee 池非空且 userID 非成员时，Claim 被拒绝。
// 候选池通过 AddCandidates 写入 role 引用，identity 展开 role→members 命中校验。
func TestClaim_RejectsNonCandidate(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	now := time.Now()

	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID:          "inst-claim",
		ProcessID:   "proc-1",
		Name:        "claim_test",
		Status:      string(enums.InstanceStatusActive),
		StartUserID: "userA",
		TenantID:    "t1",
		CreatedBy:   "userA",
		CreatedAt:   now,
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:                "task-claim",
		ProcessInstanceID: secFixStrPtr("inst-claim"),
		TaskDefKey:        "approve-node",
		Name:              "审批",
		TaskType:          "user_task",
		Status:            string(enums.TaskStatusPending),
		ApprovalType:      string(enums.ApprovalTypeOr),
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         now,
	}))

	identity := &IdentityServiceImpl{}
	identity.AddMockUser(&User{ID: "userA", TenantID: "t1"})
	identity.AddMockUser(&User{ID: "userC", TenantID: "t1"})
	identity.AddMockRoleUsers("role-1", []string{"userA", "userC"})

	taskSvc := &TaskServiceImpl{
		taskDAO:         dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:       dao.NewHiTaskDAOWithQuery(q),
		taskAssigneeDAO: dao.NewTaskAssigneeDAOWithQuery(q),
		idGenerator:     DefaultIDGenerator,
		workflowEngine:  candGroupEngine{identity: identity},
	}

	// 落库 role 候选（模拟 createClaimTask 落库后的状态）
	require.NoError(t, taskSvc.AddCandidates(ctx, Actor{UserID: "system", TenantID: "t1"}, "task-claim", "role", []string{"role-1"}))

	// userB 不是 role-1 成员 → 必须拒绝
	bCtx := SetUserToCtx(ctx, &Actor{UserID: "userB", TenantID: "t1", UserName: "B"})
	err := taskSvc.Claim(bCtx, Actor{UserID: "userB", TenantID: "t1", UserName: "B"}, "task-claim")
	require.Error(t, err, "非候选人认领应被拒绝")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)

	// 反向验证：候选成员 userA 可认领
	aCtx := SetUserToCtx(ctx, &Actor{UserID: "userA", TenantID: "t1", UserName: "A"})
	require.NoError(t, taskSvc.Claim(aCtx, Actor{UserID: "userA", TenantID: "t1", UserName: "A"}, "task-claim"))
	persisted, err := taskSvc.GetTask(aCtx, ActorFromCtx(aCtx), "task-claim")
	require.NoError(t, err)
	require.NotNil(t, persisted.Assignee)
	require.Equal(t, "userA", *persisted.Assignee)
}

// TestClaim_EmptyCandidatePool_AllowsClaim 验证业务规则：wf_task_assignee 池为空时放行
// （对应非 role 节点 / 历史数据兜底）。
func TestClaim_EmptyCandidatePool_AllowsClaim(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	now := time.Now()

	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID:          "inst-empty",
		ProcessID:   "proc-1",
		Name:        "empty_pool_test",
		Status:      string(enums.InstanceStatusActive),
		StartUserID: "userX",
		TenantID:    "t1",
		CreatedBy:   "userX",
		CreatedAt:   now,
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:                "task-empty-pool",
		ProcessInstanceID: secFixStrPtr("inst-empty"),
		TaskDefKey:        "role-approve",
		Name:              "角色审批",
		TaskType:          "user_task",
		Status:            string(enums.TaskStatusPending),
		ApprovalType:      string(enums.ApprovalTypeSingle),
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         now,
	}))

	taskSvc := &TaskServiceImpl{
		taskDAO:         dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:       dao.NewHiTaskDAOWithQuery(q),
		taskAssigneeDAO: dao.NewTaskAssigneeDAOWithQuery(q),
		idGenerator:     DefaultIDGenerator,
		workflowEngine:  candGroupEngine{identity: NewIdentityService()},
	}
	claimerCtx := SetUserToCtx(ctx, &Actor{UserID: "userX", TenantID: "t1", UserName: "X"})
	require.NoError(t, taskSvc.Claim(claimerCtx, Actor{UserID: "userX", TenantID: "t1", UserName: "X"}, "task-empty-pool"), "空候选人池应放行认领")
}
