package service

// Tests for runtime lifecycle guards on RuntimeServiceImpl:
//   - draft instances cannot be suspended (suspend→activate would skip the draft
//     activation gate and leave an Active zombie with no tasks)
//   - terminate notification only targets assignees of tasks that are still
//     undecided at termination time, not every historical approver

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
	// TerminateInTx 是内部 API（生产路径由 TerminateProcessInstance/Withdraw 在已通过属主/
	// 租户校验后调用）；本测试直接驱动收尾逻辑，故标记为引擎内部模式，跳过属主重校验。
	ctx := WithInternalCallingMode(context.Background())
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

// 唤醒挂起实例的身份口径：发起人、管理员，以及持有该实例未收尾任务的办理人
// 可唤醒（挂起使其丢掉待办入口）；无关同租户用户按属主校验拒绝。
func TestActivateProcessInstance_WakeAuthorization(t *testing.T) {
	rs, _ := lifecycleDB(t)
	q := secFixDB(t)
	ctx := context.Background()
	now := time.Now()

	newSuspended := func(instID string) {
		t.Helper()
		require.NoError(t, rs.instanceDAO.Create(ctx, &model.WfInstance{
			ID: instID, ProcessID: "proc-wake", Name: "唤醒单",
			Status: string(enums.InstanceStatusSuspended), TenantID: "t1",
			StartUserID: "userA", CreatedBy: "userA", CreatedAt: now,
		}))
		require.NoError(t, q.WfTask.Create(&model.WfTask{
			ID: "task-" + instID, ProcessInstanceID: secFixStrPtr(instID), TaskDefKey: "n1",
			Name: "审批节点", TaskType: "user_task",
			Status: string(enums.TaskStatusSuspended), Assignee: secFixStrPtr("userB"),
			TenantID: "t1", CreatedBy: "system", CreatedAt: now,
		}))
	}

	// 办理人：非发起人非管理员，但持有挂起任务
	newSuspended("inst-wake-assignee")
	require.NoError(t, rs.ActivateProcessInstance(ctx, Actor{UserID: "userB", TenantID: "t1"}, "inst-wake-assignee"))
	inst, err := rs.instanceDAO.Get(ctx, "inst-wake-assignee")
	require.NoError(t, err)
	require.Equal(t, string(enums.InstanceStatusActive), inst.Status, "办理人唤醒后实例应回到 active")
	task, err := rs.taskDAO.Get(ctx, "task-inst-wake-assignee")
	require.NoError(t, err)
	require.Equal(t, string(enums.TaskStatusActive), task.Status, "挂起任务应随唤醒回到 active")

	// 无关同租户用户：不持有任务
	newSuspended("inst-wake-eve")
	err = rs.ActivateProcessInstance(ctx, Actor{UserID: "eve", TenantID: "t1"}, "inst-wake-eve")
	require.ErrorIs(t, err, ErrPermissionDenied, "无关用户唤醒他人实例必须被拒")

	// 发起人
	newSuspended("inst-wake-owner")
	require.NoError(t, rs.ActivateProcessInstance(ctx, Actor{UserID: "userA", TenantID: "t1"}, "inst-wake-owner"))

	// 管理员
	newSuspended("inst-wake-admin")
	require.NoError(t, rs.ActivateProcessInstance(ctx, Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}, "inst-wake-admin"))
}

// 草稿实例终止被拒绝：与挂起同口径，草稿的生命周期动作是编辑、提交（激活）与删除，
// 终止会把它挪进已结束列表，既不能提交也不能再走草稿删除。
func TestTerminateInTx_DraftRejected(t *testing.T) {
	q := secFixDB(t)
	rs := &RuntimeServiceImpl{
		instanceDAO:    dao.NewInstanceDAOWithQuery(q),
		hiInstanceDAO:  dao.NewHiInstanceDAOWithQuery(q),
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		workflowEngine: &testEngineDouble{listener: func(_ context.Context, _ TaskEvent) {}},
	}
	ctx := context.Background()

	require.NoError(t, rs.instanceDAO.Create(ctx, &model.WfInstance{
		ID: "inst-draft-term", ProcessID: "proc-1", Name: "草稿单",
		Status: string(enums.InstanceStatusDraft), TenantID: "t1",
		StartUserID: "userA", CreatedBy: "userA", CreatedAt: time.Now(),
	}))

	_, err := rs.TerminateInTx(WithInternalCallingMode(ctx), q, "inst-draft-term", "清理")
	require.Error(t, err, "草稿实例不可终止")
	require.True(t, errors.Is(err, ErrValidation), "期望 ErrValidation，got %v", err)

	persisted, err := rs.instanceDAO.Get(ctx, "inst-draft-term")
	require.NoError(t, err)
	require.Equal(t, string(enums.InstanceStatusDraft), persisted.Status, "实例应保持草稿态")
}

// withdrawProcEngineDouble 撤回开关校验需要流程定义可解析（发起即撤回按流程级
// actionPermissions 判定），在 testEngineDouble 上补最小 ProcessService。
type withdrawProcEngineDouble struct {
	testEngineDouble
	proc ProcessService
}

func (e *withdrawProcEngineDouble) GetProcessService() ProcessService { return e.proc }

// 发起后首个任务尚未落库的撤回：start 异步驱动首节点存在窗口，此刻撤回按实例维度
// 终止并落 withdrawn 口径，不再按“无活跃任务”拒绝。
func TestWithdrawByInstance_BeforeFirstTaskTerminates(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()
	now := time.Now()

	rs := &RuntimeServiceImpl{
		instanceDAO:    dao.NewInstanceDAOWithQuery(q),
		hiInstanceDAO:  dao.NewHiInstanceDAOWithQuery(q),
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		workflowEngine: &testEngineDouble{listener: func(_ context.Context, _ TaskEvent) {}},
	}
	taskSvc := &TaskServiceImpl{
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:      dao.NewHiTaskDAOWithQuery(q),
		workflowEngine: &withdrawProcEngineDouble{testEngineDouble{runtime: rs}, recallProcessFake{def: "{}"}},
	}

	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-wd-fresh", ProcessID: "proc-1", Name: "刚提交单",
		Status: string(enums.InstanceStatusActive), TenantID: "t1",
		StartUserID: "starter", CreatedBy: "starter", CreatedAt: now,
	}))

	// 实例 Active 但任务未落库：发起人撤回应直接终止实例
	require.NoError(t, taskSvc.WithdrawByInstance(ctx, Actor{UserID: "starter", TenantID: "t1"}, "inst-wd-fresh", "提交错了"))

	persisted, err := rs.instanceDAO.Get(ctx, "inst-wd-fresh")
	require.NoError(t, err)
	require.Nil(t, persisted, "已终止实例应归档出运行表")
	hiInst, err := q.WfHiInstance.WithContext(ctx).Where(q.WfHiInstance.ID.Eq("inst-wd-fresh")).First()
	require.NoError(t, err)
	require.Equal(t, string(enums.InstanceStatusTerminated), hiInst.Status)

	// 非发起人不可借该路径终止他人实例
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-wd-fresh2", ProcessID: "proc-1", Name: "他人刚提交单",
		Status: string(enums.InstanceStatusActive), TenantID: "t1",
		StartUserID: "starter", CreatedBy: "starter", CreatedAt: now,
	}))
	err = taskSvc.WithdrawByInstance(ctx, Actor{UserID: "imposter", TenantID: "t1"}, "inst-wd-fresh2", "想撤")
	require.Error(t, err, "非发起人撤回必须拒绝")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)
}

// 发起即撤回同样受设计器 withdraw 开关约束：流程级 actionPermissions 禁用撤回时，
// 首任务未落库窗口不允许借实例维度撤回绕过配置。
func TestWithdrawByInstance_BeforeFirstTask_DisabledByDesigner(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()

	rs := &RuntimeServiceImpl{
		instanceDAO:    dao.NewInstanceDAOWithQuery(q),
		hiInstanceDAO:  dao.NewHiInstanceDAOWithQuery(q),
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		workflowEngine: &testEngineDouble{listener: func(_ context.Context, _ TaskEvent) {}},
	}
	taskSvc := &TaskServiceImpl{
		taskDAO:   dao.NewTaskDAOWithQuery(q),
		hiTaskDAO: dao.NewHiTaskDAOWithQuery(q),
		workflowEngine: &withdrawProcEngineDouble{testEngineDouble{runtime: rs},
			recallProcessFake{def: `{"ruleChain":{"additionalInfo":{"actionPermissions":{"withdraw":false}}},"metadata":{"nodes":[],"connections":[]}}`}},
	}

	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-wd-off", ProcessID: "proc-no-withdraw", Name: "禁撤回单",
		Status: string(enums.InstanceStatusActive), TenantID: "t1",
		StartUserID: "starter", CreatedBy: "starter", CreatedAt: time.Now(),
	}))

	err := taskSvc.WithdrawByInstance(ctx, Actor{UserID: "starter", TenantID: "t1"}, "inst-wd-off", "提交错了")
	require.Error(t, err, "设计器禁用撤回时首任务未落库窗口不得终止实例")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)

	persisted, gErr := rs.instanceDAO.Get(ctx, "inst-wd-off")
	require.NoError(t, gErr)
	require.NotNil(t, persisted, "实例应保持 active 未被终止")
}

// 任务维度撤回同样受流程级 withdraw 开关约束：开关写在流程级 additionalInfo，
// 节点级校验拦不住，有任务分支与首任务未落库窗口同口径拒绝。
func TestWithdrawByInstance_TaskDimension_DisabledByProcessSwitch(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()

	rs := &RuntimeServiceImpl{
		instanceDAO:    dao.NewInstanceDAOWithQuery(q),
		hiInstanceDAO:  dao.NewHiInstanceDAOWithQuery(q),
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		workflowEngine: &testEngineDouble{listener: func(_ context.Context, _ TaskEvent) {}},
	}
	taskSvc := &TaskServiceImpl{
		taskDAO:   dao.NewTaskDAOWithQuery(q),
		hiTaskDAO: dao.NewHiTaskDAOWithQuery(q),
		workflowEngine: &withdrawProcEngineDouble{testEngineDouble{runtime: rs},
			recallProcessFake{def: `{"ruleChain":{"additionalInfo":{"actionPermissions":{"withdraw":false}}},"metadata":{"nodes":[],"connections":[]}}`}},
	}

	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-wd-task-off", ProcessID: "proc-no-withdraw-2", Name: "禁撤回有任务单",
		Status: string(enums.InstanceStatusActive), TenantID: "t1",
		StartUserID: "starter", CreatedBy: "starter", CreatedAt: time.Now(),
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-wd-off", ProcessInstanceID: secFixStrPtr("inst-wd-task-off"), TaskDefKey: "n1",
		Name: "审批", TaskType: "userTask", Status: string(enums.TaskStatusActive),
		Assignee: secFixStrPtr("u1"), TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))

	err := taskSvc.WithdrawByInstance(ctx, Actor{UserID: "starter", TenantID: "t1"}, "inst-wd-task-off", "想撤")
	require.Error(t, err, "流程级禁用撤回时有任务分支同样必须拒绝")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)

	persisted, gErr := rs.instanceDAO.Get(ctx, "inst-wd-task-off")
	require.NoError(t, gErr)
	require.NotNil(t, persisted, "实例应保持 active 未被终止")
}
