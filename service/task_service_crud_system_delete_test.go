package service

// PR4 用例：DeleteTask 的系统身份分支不再绕过实例行锁。
// 此前系统分支用 userID=="system" 字符串比对后直接 taskDAO.Delete，跳过
// WithInstanceTx——系统侧直删与并发 Complete 竞争会留下重复终止态或丢审批历史。
// 修复后系统身份应与普通删除走同一条持锁路径：active 实例可删、终态实例拒绝。

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

// TestSecFix_SystemDeleteActiveInstance 系统身份删除 active 实例上的任务应成功（走锁 + 删除）。
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
// WithInstanceTx 拒绝（ErrInstanceTerminal）——证明系统分支已不再绕过实例锁。
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
