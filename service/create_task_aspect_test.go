package service

import (
	"context"
	"testing"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/utils/lock"
	"github.com/rulego/rulego/api/types"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// 测试替身：仅覆写被测路径调用的方法。
// ---------------------------------------------------------------------------

type endNodeCtx struct{ types.NodeCtx }

func (f *endNodeCtx) Type() string { return types.NodeTypeEnd }

type fakeRuleContext struct {
	types.RuleContext
	node   types.NodeCtx
	selfID string
}

func (f *fakeRuleContext) Self() types.NodeCtx         { return f.node }
func (f *fakeRuleContext) GetSelfId() string           { return f.selfID }
func (f *fakeRuleContext) GetContext() context.Context { return context.Background() }
func (f *fakeRuleContext) RuleChain() types.NodeCtx    { return nil }

type fakeTaskService struct{ TaskService }

func (f *fakeTaskService) CreateTask(_ context.Context, _ Actor, task *model.WfTask) (string, error) {
	task.ID = "task-1"
	return "task-1", nil
}

func (f *fakeTaskService) Complete(_ context.Context, _ Actor, _ string, _ map[string]interface{}) error {
	return nil
}

type fakeRuntimeService struct {
	RuntimeService
	completed string
	setVarsID string
	setVars   map[string]interface{}
}

func (f *fakeRuntimeService) CompleteProcessInstance(_ context.Context, _ Actor, id, _ string) error {
	f.completed = id
	return nil
}

func (f *fakeRuntimeService) SetProcessInstanceVariables(_ context.Context, _ Actor, id string, variables map[string]interface{}) error {
	f.setVarsID = id
	f.setVars = variables
	return nil
}

type fakeEngine struct {
	WorkflowEngine
	locker  lock.Locker
	taskSvc TaskService
	rtSvc   RuntimeService
}

func (f *fakeEngine) GetLocker() lock.Locker                  { return f.locker }
func (f *fakeEngine) GetTaskService() TaskService             { return f.taskSvc }
func (f *fakeEngine) GetRuntimeService() RuntimeService       { return f.rtSvc }
func (f *fakeEngine) GetRuleChainExecutor() RuleChainExecutor { return nil }

// end 节点去重锁服务异常时，实例仍应完成归档。
func TestTaskCreator_EndDedupLockErrorStillCompletesInstance(t *testing.T) {
	q := rtImplTestDB(t)
	rtSvc := &fakeRuntimeService{}
	aspect := &TaskCreator{
		instanceDAO: dao.NewInstanceDAOWithQuery(q),
		workflowEngine: &fakeEngine{
			locker:  &failingLocker{},
			taskSvc: &fakeTaskService{},
			rtSvc:   rtSvc,
		},
	}

	md := types.NewMetadata()
	md.PutValue(constants.KeyInstanceID, "inst-end")
	md.PutValue(constants.KeyProcessID, "proc-1")
	md.PutValue(constants.KeyTenantID, "t1")
	msg := types.NewMsg(0, "wf", types.JSON, md, `{}`)
	rctx := &fakeRuleContext{node: &endNodeCtx{}, selfID: "end1"}

	// 锁服务异常时 Before 仍须写入非空 KeyEndExecLock。
	out := aspect.Before(rctx, msg, types.Success)
	require.NotEmpty(t, out.GetMetadata().GetValue(constants.KeyEndExecLock),
		"lock service error must still mark KeyEndExecLock")

	// 锁服务异常时 After 仍应完成实例归档。
	aspect.After(rctx, out, nil, types.Success)
	require.Equal(t, "inst-end", rtSvc.completed, "instance must complete even when end-dedup lock errors")
}

type autoNodeCtx struct{ types.NodeCtx }

func (f *autoNodeCtx) Type() string { return "functions" }

// TestTaskCreator_AutoNodeOutputMergedIntoInstanceVariables 自动化节点完成后，节点产出
// （服务函数生成的单号、接口回执、AI 结论等）须合并进实例变量——任务快照只挂在任务行上，
// 实例变量区与业务侧读不到。
func TestTaskCreator_AutoNodeOutputMergedIntoInstanceVariables(t *testing.T) {
	q := rtImplTestDB(t)
	rtSvc := &fakeRuntimeService{}
	aspect := &TaskCreator{
		instanceDAO: dao.NewInstanceDAOWithQuery(q),
		workflowEngine: &fakeEngine{
			taskSvc: &fakeTaskService{},
			rtSvc:   rtSvc,
		},
	}

	md := types.NewMetadata()
	md.PutValue(constants.KeyInstanceID, "inst-auto")
	md.PutValue(constants.KeyProcessID, "proc-1")
	md.PutValue(constants.KeyTenantID, "t1")
	msg := types.NewMsg(0, "wf", types.JSON, md, `{"serialNo":"WX20260927001"}`)
	rctx := &fakeRuleContext{node: &autoNodeCtx{}, selfID: "gen1"}

	aspect.Before(rctx, msg, types.Success)
	aspect.After(rctx, msg, nil, types.Success)

	require.Equal(t, "inst-auto", rtSvc.setVarsID, "产出应合并进所属实例")
	require.Equal(t, "WX20260927001", rtSvc.setVars["serialNo"], "节点产出应进入实例变量")
}

// TestTaskCreator_AutoNodeEmptyOutputSkipsVariableMerge 空载荷（无 JSON 对象产出）不触发合并。
func TestTaskCreator_AutoNodeEmptyOutputSkipsVariableMerge(t *testing.T) {
	q := rtImplTestDB(t)
	rtSvc := &fakeRuntimeService{}
	aspect := &TaskCreator{
		instanceDAO: dao.NewInstanceDAOWithQuery(q),
		workflowEngine: &fakeEngine{
			taskSvc: &fakeTaskService{},
			rtSvc:   rtSvc,
		},
	}

	md := types.NewMetadata()
	md.PutValue(constants.KeyInstanceID, "inst-auto-empty")
	md.PutValue(constants.KeyProcessID, "proc-1")
	md.PutValue(constants.KeyTenantID, "t1")
	msg := types.NewMsg(0, "wf", types.JSON, md, `not-json`)
	rctx := &fakeRuleContext{node: &autoNodeCtx{}, selfID: "gen1"}

	aspect.Before(rctx, msg, types.Success)
	aspect.After(rctx, msg, nil, types.Success)

	require.Empty(t, rtSvc.setVarsID, "非对象载荷不应触发实例变量合并")
}
