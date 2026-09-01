package service

// Tests for task_service_history.go: GetHistoryTasksByProcessInstanceID is
// tenant-filtered so tenant A cannot read tenant B's history tasks.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/enums"
)

// TestGetHistoryTasksByProcessInstanceID_RejectsCrossTenant 验证：
// 租户 A 查询租户 B 实例的历史任务，因 TenantID 过滤拿不到 B 的数据（不会泄露）。
func TestGetHistoryTasksByProcessInstanceID_RejectsCrossTenant(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()
	now := time.Now()

	// 租户 B 的历史任务（WfHiTask.TaskDefKey 为 *string）
	require.NoError(t, q.WfHiTask.Create(&model.WfHiTask{
		ID:                "hitask-B",
		ProcessInstanceID: secFixStrPtr("inst-cross"),
		TaskDefKey:        secFixStrPtr("approve"),
		Name:              "审批",
		TaskType:          "user_task",
		Status:            string(enums.TaskStatusCompleted),
		Assignee:          secFixStrPtr("userB"),
		ApprovalType:      string(enums.ApprovalTypeSingle),
		TenantID:          "tenantB",
		CreatedBy:         "system",
		CreatedAt:         now,
	}))

	taskSvc := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), hiTaskDAO: dao.NewHiTaskDAOWithQuery(q)}

	// 租户 A 用户查询 → 因 TenantID 过滤，返回空（不泄露 B 的任务）
	aCtx := SetUserToCtx(ctx, &Actor{UserID: "userA", TenantID: "tenantA", UserName: "A"})
	tasks, err := taskSvc.GetHistoryTasksByProcessInstanceID(aCtx, "inst-cross")
	require.NoError(t, err)
	require.Empty(t, tasks, "跨租户查询历史任务不应返回他租户数据")

	// 反向验证：同租户 B 用户可查到
	bCtx := SetUserToCtx(ctx, &Actor{UserID: "userB", TenantID: "tenantB", UserName: "B"})
	tasks, err = taskSvc.GetHistoryTasksByProcessInstanceID(bCtx, "inst-cross")
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, "hitask-B", tasks[0].ID)
}
