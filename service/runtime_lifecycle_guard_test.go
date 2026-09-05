package service

// Tests for runtime lifecycle guards on RuntimeServiceImpl:
//   - draft instances cannot be suspended (suspend→activate would skip the draft
//     activation gate and leave an Active zombie with no tasks)
//   - terminate notification only targets assignees of tasks that are still
//     undecided at termination time, not every historical approver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/enums"
)

func lifecycleDB(t *testing.T) (*RuntimeServiceImpl, *TaskServiceImpl) {
	q := secFixDB(t)
	taskSvc := &TaskServiceImpl{
		taskDAO:         dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:       dao.NewHiTaskDAOWithQuery(q),
		taskAssigneeDAO: dao.NewTaskAssigneeDAOWithQuery(q),
		workflowEngine:  &testEngineDouble{},
	}
	// TerminateInTx 构造通知事件前检查监听器已注册；注入 no-op 监听器让事件路径可达。
	rs := &RuntimeServiceImpl{
		instanceDAO:    dao.NewInstanceDAOWithQuery(q),
		hiInstanceDAO:  dao.NewHiInstanceDAOWithQuery(q),
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		workflowEngine: &testEngineDouble{listener: func(_ context.Context, _ TaskEvent) {}},
	}
	return rs, taskSvc
}

// 草稿实例挂起被拒绝：挂起→激活会绕过草稿激活闸（创建者校验、发起人范围、引擎首驱），
// 落下 Active 却无任何任务的僵尸实例。
func TestSuspendProcessInstance_DraftRejected(t *testing.T) {
	rs, _ := lifecycleDB(t)
	ctx := context.Background()
	now := time.Now()

	require.NoError(t, rs.instanceDAO.Create(ctx, &model.WfInstance{
		ID: "inst-draft", ProcessID: "proc-1", Name: "草稿单",
		Status: string(enums.InstanceStatusDraft), TenantID: "t1",
		StartUserID: "userA", CreatedBy: "userA", CreatedAt: now,
	}))

	err := rs.SuspendProcessInstance(ctx, Actor{UserID: "userA", TenantID: "t1"}, "inst-draft")
	require.Error(t, err, "草稿实例不可挂起")

	persisted, err := rs.instanceDAO.Get(ctx, "inst-draft")
	require.NoError(t, err)
	require.Equal(t, string(enums.InstanceStatusDraft), persisted.Status, "状态必须保持 Draft")
}

// 终止事件只携带终止时仍未决任务的办理人：已批准过早期节点的历史审批人不再收到通知。
func TestTerminateInTx_NotifiesOnlyLiveAssignees(t *testing.T) {
	rs, _ := lifecycleDB(t)
	q := secFixDB(t)
	ctx := context.Background()
	now := time.Now()

	require.NoError(t, rs.instanceDAO.Create(ctx, &model.WfInstance{
		ID: "inst-term", ProcessID: "proc-1", Name: "终止单",
		Status: string(enums.InstanceStatusActive), TenantID: "t1",
		StartUserID: "starter", CreatedBy: "starter", CreatedAt: now,
	}))
	completed := "completed"
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-done", ProcessInstanceID: secFixStrPtr("inst-term"), TaskDefKey: "n1",
		Name: "第一节点", TaskType: "user_task",
		Status: string(enums.TaskStatusCompleted), Assignee: secFixStrPtr("early-approver"),
		EndReason: &completed, TenantID: "t1", CreatedBy: "system", CreatedAt: now,
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-live", ProcessInstanceID: secFixStrPtr("inst-term"), TaskDefKey: "n2",
		Name: "第二节点", TaskType: "user_task",
		Status: string(enums.TaskStatusActive), Assignee: secFixStrPtr("current-approver"),
		TenantID: "t1", CreatedBy: "system", CreatedAt: now,
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-pool", ProcessInstanceID: secFixStrPtr("inst-term"), TaskDefKey: "n2",
		Name: "第二节点候选", TaskType: "user_task",
		Status:   string(enums.TaskStatusPending),
		TenantID: "t1", CreatedBy: "system", CreatedAt: now,
	}))

	evt, err := rs.TerminateInTx(ctx, q, "inst-term", "测试终止")
	require.NoError(t, err)
	require.NotNil(t, evt, "存在需通知的收件人时应返回事件")
	require.Equal(t, []string{"starter", "current-approver"}, evt.ToUsers,
		"通知对象应为发起人+活跃办理人，不含历史审批人")

	// 运行表清空、任务与实例均归档
	tasks, err := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.In("task-done", "task-live", "task-pool")).Find()
	require.NoError(t, err)
	require.Empty(t, tasks)
	hiTasks, err := q.WfHiTask.WithContext(ctx).Where(q.WfHiTask.ProcessInstanceID.Eq("inst-term")).Find()
	require.NoError(t, err)
	require.Len(t, hiTasks, 3)
	hiInst, err := q.WfHiInstance.WithContext(ctx).Where(q.WfHiInstance.ID.Eq("inst-term")).First()
	require.NoError(t, err)
	require.Equal(t, string(enums.InstanceStatusTerminated), hiInst.Status)
}
