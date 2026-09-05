package service

// Tests for task lifecycle guards:
//   - DeleteTask refuses live tasks (active/pending/suspended) for user actors,
//     archives terminal tasks to wf_hi_task before hard delete; system rollback
//     paths keep their unguarded hard-delete behavior
//   - Unclaim sibling restore only revives tasks terminated in the same claim
//     round, not leftovers from earlier rounds
//   - SetTaskVariables merges over existing task variables instead of replacing
//     them wholesale (engine runtime state like sequential-assignee caches
//     lives in task variables)

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
)

func guardTaskSvc(t *testing.T) *TaskServiceImpl {
	q := secFixDB(t)
	return &TaskServiceImpl{
		taskDAO:         dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:       dao.NewHiTaskDAOWithQuery(q),
		taskAssigneeDAO: dao.NewTaskAssigneeDAOWithQuery(q),
		workflowEngine:  &testEngineDouble{},
	}
}

func createGuardTask(t *testing.T, q *query.Query, svc *TaskServiceImpl, id, status, assignee string) {
	task := &model.WfTask{
		ID: id, TaskDefKey: "approve", Name: "审批", TaskType: "user_task",
		Status: status, TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}
	task.ProcessInstanceID = secFixStrPtr("inst-guard")
	if assignee != "" {
		task.Assignee = secFixStrPtr(assignee)
	}
	require.NoError(t, q.WfTask.Create(task))
}

// 用户删除活跃待办被拒绝（删掉即无人可完成、实例卡死）；系统回滚路径不受限；
// 终态任务可删且删除前归档到历史表。
func TestDeleteTask_StatusGuardAndArchive(t *testing.T) {
	svc := guardTaskSvc(t)
	q := secFixDB(t)
	ctx := context.Background()
	userCtx := SetUserToCtx(ctx, &Actor{UserID: "userA", TenantID: "t1", UserName: "A"})

	// WithInstanceTx 需要实例行存在（行锁载体）
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-guard", ProcessID: "proc-1", Name: "删除单",
		Status: string(enums.InstanceStatusActive), TenantID: "t1",
		StartUserID: "starter", CreatedBy: "starter", CreatedAt: time.Now(),
	}))
	createGuardTask(t, q, svc, "task-live-del", string(enums.TaskStatusActive), "userA")
	err := svc.DeleteTask(userCtx, Actor{UserID: "userA", TenantID: "t1"}, "task-live-del", "")
	require.Error(t, err, "活跃任务不可被用户删除")
	require.True(t, errors.Is(err, ErrValidation), "拒绝原因必须是状态守卫，got %v", err)
	persisted, err := svc.taskDAO.Get(ctx, "task-live-del")
	require.NoError(t, err)
	require.NotNil(t, persisted, "活跃任务必须保留")

	// 系统身份（引擎内部回滚清理）删活跃任务放行，且不产生历史行
	require.NoError(t, svc.DeleteTask(ctx, SystemActor(), "task-live-del", "candidate write failed"))
	gone, err := svc.taskDAO.Get(ctx, "task-live-del")
	require.NoError(t, err)
	require.Nil(t, gone)

	createGuardTask(t, q, svc, "task-done-del", string(enums.TaskStatusCompleted), "userA")
	require.NoError(t, svc.DeleteTask(userCtx, Actor{UserID: "userA", TenantID: "t1"}, "task-done-del", ""))
	gone, err = svc.taskDAO.Get(ctx, "task-done-del")
	require.NoError(t, err)
	require.Nil(t, gone, "终态任务应被删除")
	hiTasks, err := q.WfHiTask.WithContext(ctx).Where(q.WfHiTask.ID.Eq("task-done-del")).Find()
	require.NoError(t, err)
	require.Len(t, hiTasks, 1, "删除前必须归档到历史表")
}

// 两轮签收：第二轮取消签收只恢复本轮被终止的兄弟任务，上一轮遗留的同原因
// 终止行不得复活（复活即幽灵待办）。
func TestUnclaim_RestoresOnlySameRoundSiblings(t *testing.T) {
	svc := guardTaskSvc(t)
	q := secFixDB(t)
	ctx := context.Background()

	round1 := time.Now().Add(-10 * time.Minute)
	round2 := time.Now()

	// 主任务：第一轮签收，取消后又签收（第二轮）
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-main", TaskDefKey: "approve", Name: "审批", TaskType: "user_task",
		ProcessInstanceID: secFixStrPtr("inst-guard"),
		Status:            string(enums.TaskStatusActive), Assignee: secFixStrPtr("userA"),
		ClaimedAt: &round2,
		TenantID:  "t1", CreatedBy: "system", CreatedAt: round1,
	}))
	// 第一轮兄弟：第一轮被"他人已签收"终止，之后因流转被 supersede 的场景不存在，
	// 这里保留其上一轮的终止时间戳（早于本轮 claimed_at）
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-sib-r1", TaskDefKey: "approve", Name: "审批", TaskType: "user_task",
		ProcessInstanceID: secFixStrPtr("inst-guard"),
		Status:            string(enums.TaskStatusTerminated), Assignee: secFixStrPtr("userB"),
		EndReason: secFixStrPtr(string(enums.EndReasonClaimedByOther)),
		EndedAt:   &round1, UpdatedAt: &round1,
		TenantID: "t1", CreatedBy: "system", CreatedAt: round1,
	}))
	// 第二轮兄弟：本轮签收时被终止
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-sib-r2", TaskDefKey: "approve", Name: "审批", TaskType: "user_task",
		ProcessInstanceID: secFixStrPtr("inst-guard"),
		Status:            string(enums.TaskStatusTerminated), Assignee: secFixStrPtr("userC"),
		EndReason: secFixStrPtr(string(enums.EndReasonClaimedByOther)),
		EndedAt:   &round2, UpdatedAt: &round2,
		TenantID: "t1", CreatedBy: "system", CreatedAt: round1,
	}))

	// WithInstanceTx 需要实例行存在（行锁载体）
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-guard", ProcessID: "proc-1", Name: "签收单",
		Status: string(enums.InstanceStatusActive), TenantID: "t1",
		StartUserID: "starter", CreatedBy: "starter", CreatedAt: round1,
	}))

	require.NoError(t, svc.Unclaim(ctx, Actor{UserID: "userA", TenantID: "t1"}, "task-main"))

	sibR1, err := svc.taskDAO.Get(ctx, "task-sib-r1")
	require.NoError(t, err)
	require.Equal(t, string(enums.TaskStatusTerminated), sibR1.Status,
		"上一轮遗留的终止兄弟不得跨轮次复活")

	sibR2, err := svc.taskDAO.Get(ctx, "task-sib-r2")
	require.NoError(t, err)
	require.Equal(t, string(enums.TaskStatusPending), sibR2.Status, "本轮被终止的兄弟应恢复待认领")
	require.Equal(t, string(constants.UserSystem), *sibR2.UpdatedBy)
}

// SetTaskVariables 合并语义：引擎写入任务变量的运行时状态（如顺序审批人缓存）
// 不被业务变量批量写入冲掉。
func TestSetTaskVariables_MergesNotReplaces(t *testing.T) {
	svc := guardTaskSvc(t)
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "userA", TenantID: "t1", UserName: "A"})

	existing := `{"_sequentialAssignees":["a","b"],"days":3}`
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-vars", TaskDefKey: "approve", Name: "审批", TaskType: "user_task",
		Status: string(enums.TaskStatusActive), Assignee: secFixStrPtr("userA"),
		Variables: &existing,
		TenantID:  "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))

	require.NoError(t, svc.SetTaskVariables(ctx, Actor{UserID: "userA", TenantID: "t1"}, "task-vars",
		map[string]interface{}{"days": 5, "memo": "ok"}))

	persisted, err := svc.taskDAO.Get(ctx, "task-vars")
	require.NoError(t, err)
	vars, err := ParseVariablesJSON(persisted.Variables)
	require.NoError(t, err)
	require.Equal(t, []interface{}{"a", "b"}, vars["_sequentialAssignees"],
		"引擎运行时缓存必须保留")
	require.Equal(t, float64(5), vars["days"], "同名键按传入值覆盖")
	require.Equal(t, "ok", vars["memo"])
}
