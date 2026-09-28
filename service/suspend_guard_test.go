package service

// 实例挂起/失败与任务生命周期的联动守卫：
//   - CreateTask 不得给挂起实例落新任务（挂起窗口与引擎驱动的落库竞态）；
//   - Claim 不得签收挂起/失败实例的候选池任务；
//   - ActivateTask 不得在挂起/失败实例上单独唤醒任务；
//   - 待办列表只认 active 实例（幽灵待办过滤）。

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

func suspendGuardSeed(t *testing.T, q *query.Query, instanceID, instanceStatus, taskID, taskStatus, assignee string) {
	t.Helper()
	now := time.Now()
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID:          instanceID,
		ProcessID:   "proc-1",
		Name:        "suspend-guard-test",
		Status:      instanceStatus,
		StartUserID: "starter",
		TenantID:    "t1",
		CreatedBy:   "starter",
		CreatedAt:   now,
	}))
	task := &model.WfTask{
		ID:                taskID,
		ProcessInstanceID: secFixStrPtr(instanceID),
		ProcessID:         "proc-1",
		TaskDefKey:        "node-a",
		Name:              "审批",
		TaskType:          "user_task",
		Status:            taskStatus,
		ApprovalType:      string(enums.ApprovalTypeSingle),
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         now,
	}
	if assignee != "" {
		task.Assignee = secFixStrPtr(assignee)
	}
	require.NoError(t, q.WfTask.Create(task))
}

// CreateTask：挂起实例上晚到的任务以 suspended 冻结落库（与级联挂起同态，
// 恢复时随级联激活翻回，流程不因缺任务卡死）。
func TestCreateTask_SuspendedInstance_FrozenTask(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "system", TenantID: "t1"})
	suspendGuardSeed(t, q, "inst-sus", string(enums.InstanceStatusSuspended), "t-existing", string(enums.TaskStatusActive), "")

	eng := &recallEngineDouble{
		proc: recallProcessFake{def: "{}"},
		q:    q,
	}
	svc := &TaskServiceImpl{
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:      dao.NewHiTaskDAOWithQuery(q),
		workflowEngine: eng,
		idGenerator:    DefaultIDGenerator,
	}

	taskID, err := svc.CreateTask(ctx, Actor{UserID: "system", TenantID: "t1"}, &model.WfTask{
		ProcessInstanceID: secFixStrPtr("inst-sus"),
		TaskDefKey:        "node-b",
		TaskType:          "user_task",
		Status:            string(enums.TaskStatusActive),
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         time.Now(),
	})
	require.NoError(t, err, "挂起实例的任务应冻结落库而非拒绝")
	row, gErr := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq(taskID)).First()
	require.NoError(t, gErr)
	require.Equal(t, string(enums.TaskStatusSuspended), row.Status, "落库任务应为 suspended 冻结态")
}

// Claim：挂起实例的候选池任务拒绝签收（幂等放行仅限已是本人 active 任务的场景）。
func TestClaim_Blocked_SuspendedInstance(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	suspendGuardSeed(t, q, "inst-sus-claim", string(enums.InstanceStatusSuspended), "task-sus-claim", string(enums.TaskStatusPending), "")

	taskSvc := newCandSvc(q, newMockIdentity())
	require.NoError(t, taskSvc.AddCandidates(ctx, Actor{UserID: "system", TenantID: "t1"}, "task-sus-claim", "person", []string{"p1"}))

	err := taskSvc.Claim(SetUserToCtx(ctx, &Actor{UserID: "p1", TenantID: "t1", UserName: "P1"}),
		Actor{UserID: "p1", TenantID: "t1"}, "task-sus-claim")
	require.Error(t, err, "挂起实例的候选池任务不可签收")

	// 任务保持 pending 且无 assignee：恢复实例后原候选人仍可正常认领
	row, gErr := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq("task-sus-claim")).First()
	require.NoError(t, gErr)
	require.Equal(t, string(enums.TaskStatusPending), row.Status)

	_, uErr := q.WfInstance.WithContext(ctx).
		Where(q.WfInstance.ID.Eq("inst-sus-claim")).
		UpdateSimple(q.WfInstance.Status.Value(string(enums.InstanceStatusActive)))
	require.NoError(t, uErr)
	require.NoError(t, taskSvc.Claim(SetUserToCtx(ctx, &Actor{UserID: "p1", TenantID: "t1", UserName: "P1"}),
		Actor{UserID: "p1", TenantID: "t1"}, "task-sus-claim"), "实例恢复后同一候选人应可认领")
}

// ActivateTask：挂起实例上的任务不得单独唤醒；实例 active 时任务级唤醒不受影响。
func TestActivateTask_Blocked_SuspendedInstance(t *testing.T) {
	q := candGroupDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "worker", TenantID: "t1", UserName: "W"})
	suspendGuardSeed(t, q, "inst-sus-awaken", string(enums.InstanceStatusSuspended), "task-sus-awaken", string(enums.TaskStatusSuspended), "worker")

	taskSvc := newCandSvc(q, newMockIdentity())
	err := taskSvc.ActivateTask(ctx, Actor{UserID: "worker", TenantID: "t1"}, "task-sus-awaken")
	require.Error(t, err, "挂起实例上的任务不得单独唤醒")

	_, uErr := q.WfInstance.WithContext(ctx).
		Where(q.WfInstance.ID.Eq("inst-sus-awaken")).
		UpdateSimple(q.WfInstance.Status.Value(string(enums.InstanceStatusActive)))
	require.NoError(t, uErr)
	require.NoError(t, taskSvc.ActivateTask(ctx, Actor{UserID: "worker", TenantID: "t1"}, "task-sus-awaken"),
		"实例 active 后任务级唤醒应放行")
}

// GetTodoProcessInstanceList：挂起实例的任务不进待办，active 实例任务不受影响。
func TestTodoList_FiltersSuspendedInstance(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()
	suspendGuardSeed(t, q, "inst-todo-active", string(enums.InstanceStatusActive), "t-todo-active", string(enums.TaskStatusActive), "u1")
	suspendGuardSeed(t, q, "inst-todo-sus", string(enums.InstanceStatusSuspended), "t-todo-sus", string(enums.TaskStatusActive), "u1")
	suspendGuardSeed(t, q, "inst-todo-term", string(enums.InstanceStatusTerminated), "t-todo-term", string(enums.TaskStatusActive), "u1")

	s := &RuntimeServiceImpl{
		instanceDAO:    dao.NewInstanceDAOWithQuery(q),
		workflowEngine: candGroupEngine{identity: newMockIdentity()},
	}
	instances, total, err := s.GetTodoProcessInstanceList(ctx, Actor{UserID: "u1", TenantID: "t1"}, 1, 10, "", nil, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 1, total, "挂起/终止实例的任务不应出现在待办")
	require.Len(t, instances, 1)
	require.Equal(t, "inst-todo-active", instances[0].ID)
}

// taskDAO.List 的 InstanceStatuses 过滤：统计口径与待办列表同源。
func TestTaskDAO_List_InstanceStatusesFilter(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	suspendGuardSeed(t, q, "inst-dao-active", string(enums.InstanceStatusActive), "t-dao-active", string(enums.TaskStatusActive), "u2")
	suspendGuardSeed(t, q, "inst-dao-sus", string(enums.InstanceStatusSuspended), "t-dao-sus", string(enums.TaskStatusActive), "u2")

	taskDAO := dao.NewTaskDAOWithQuery(q)
	_, total, err := taskDAO.List(ctx, &dto.TaskQuery{
		Assignee:         "u2",
		TenantID:         "t1",
		InstanceStatuses: []string{string(enums.InstanceStatusActive)},
		PageRequest:      dto.PageRequest{Status: []string{string(enums.TaskStatusActive)}, PageSize: 10},
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, total, "实例状态过滤应剔除挂起实例的任务")

	_, totalAll, err := taskDAO.List(ctx, &dto.TaskQuery{
		Assignee:    "u2",
		TenantID:    "t1",
		PageRequest: dto.PageRequest{Status: []string{string(enums.TaskStatusActive)}, PageSize: 10},
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, totalAll, "不带实例状态过滤时应查到全部")
}

// CreateTask：实例行不存在（已清扫/归档删除）拒绝落库——放行会产生无法溯源的孤儿任务。
func TestCreateTask_MissingInstance_Rejected(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "system", TenantID: "t1"})
	svc := &TaskServiceImpl{
		taskDAO:     dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:   dao.NewHiTaskDAOWithQuery(q),
		idGenerator: DefaultIDGenerator,
	}

	_, err := svc.CreateTask(ctx, Actor{UserID: "system", TenantID: "t1"}, &model.WfTask{
		ProcessInstanceID: secFixStrPtr("inst-gone"),
		TaskDefKey:        "node-x",
		TaskType:          "user_task",
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         time.Now(),
	})
	require.Error(t, err, "实例不存在的落库必须拒绝")
}

// CreateTask：已完成实例放行尾任务补录（end 节点崩溃恢复路径），终态行无并发
// 变更，直接落库不受行锁事务约束。
func TestCreateTask_CompletedInstance_TailTaskAllowed(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "system", TenantID: "t1"})
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-done", ProcessID: "proc-1", Name: "已完成",
		Status:      string(enums.InstanceStatusCompleted),
		StartUserID: "starter", TenantID: "t1", CreatedBy: "starter", CreatedAt: time.Now(),
	}))
	svc := &TaskServiceImpl{
		taskDAO:     dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:   dao.NewHiTaskDAOWithQuery(q),
		idGenerator: DefaultIDGenerator,
	}

	taskID, err := svc.CreateTask(ctx, Actor{UserID: "system", TenantID: "t1"}, &model.WfTask{
		ProcessInstanceID: secFixStrPtr("inst-done"),
		TaskDefKey:        "gflow_end",
		TaskType:          "end",
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         time.Now(),
	})
	require.NoError(t, err, "已完成实例的尾任务补录应放行")
	row, gErr := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq(taskID)).First()
	require.NoError(t, gErr)
	require.Equal(t, string(enums.TaskStatusPending), row.Status)
}
