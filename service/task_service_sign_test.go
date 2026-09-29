package service

// Tests for task_service_sign.go: 加签留痕（sign_added_by）与减签资格分层——
// 带标记的加签子任务仅加签人本人可减，无标记子任务（流程配置会签人/存量）不受限。

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
)

// signTestDef 流程定义带 a/b 两个 userTask 节点：actionPermissions 缺省即开，
// 节点缺失会让 strict 解析 fail-closed 拒绝加签/撤回
var signTestDef = recallDefinition(true,
	[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
	[]string{recallConn("a", "b")})

// newSignTestSvc 组装可跑 AddSign/ReduceSign 的服务，SQLite 内存库承载运行表。
func newSignTestSvc(t *testing.T) (*TaskServiceImpl, *query.Query) {
	t.Helper()
	q := secFixDB(t)
	eng := &recallEngineDouble{
		testEngineDouble: testEngineDouble{},
		proc:             recallProcessFake{def: signTestDef},
		q:                q,
	}
	svc := &TaskServiceImpl{
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:      dao.NewHiTaskDAOWithQuery(q),
		workflowEngine: eng,
		idGenerator:    &testSeqIDGen{},
	}
	return svc, q
}

func signSeed(t *testing.T, q *query.Query, instID string) {
	t.Helper()
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: instID, ProcessID: "proc-1", Name: "sign-test", Status: string(enums.InstanceStatusActive),
		TenantID: "t1", CreatedBy: "system", StartUserID: "starter", CreatedAt: time.Now(),
	}))
}

func signSeedParentTask(t *testing.T, q *query.Query, id, instID, assignee, vars string) {
	t.Helper()
	task := &model.WfTask{
		ID: id, ProcessInstanceID: secFixStrPtr(instID), ProcessID: "proc-1",
		TaskDefKey: "a", Name: "审批节点", TaskType: constants.TaskTypeUserTask,
		Status: string(enums.TaskStatusActive), Assignee: secFixStrPtr(assignee),
		TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}
	if vars != "" {
		task.Variables = secFixStrPtr(vars)
	}
	require.NoError(t, q.WfTask.Create(task))
}

// 加签子任务留痕加签操作人：sign_added_by 随子任务落库。
func TestAddSign_MarksSignAddedBy(t *testing.T) {
	svc, q := newSignTestSvc(t)
	signSeed(t, q, "inst-1")
	signSeedParentTask(t, q, "t-a", "inst-1", "alice", "")
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "alice", TenantID: "t1", UserName: "甲"})

	require.NoError(t, svc.AddSign(ctx, Actor{UserID: "alice", TenantID: "t1"}, "t-a", []string{"carol"}, "需要会签"))

	subs, err := svc.taskDAO.GetByParentID(ctx, "t-a")
	require.NoError(t, err)
	require.Len(t, subs, 1)
	require.Equal(t, "carol", *subs[0].Assignee)
	require.Equal(t, "alice", signAddedBy(subs[0]), "子任务变量应记录加签操作人")
}

// 带标记的加签子任务仅加签人本人可减：同节点其他参与人（被加签人 bob）减
// carol 必须被拒，加签人 alice 本人减签成功。
func TestReduceSign_OnlySignerCanReduce(t *testing.T) {
	svc, q := newSignTestSvc(t)
	signSeed(t, q, "inst-1")
	signSeedParentTask(t, q, "t-a", "inst-1", "alice", "")
	ctxAlice := SetUserToCtx(context.Background(), &Actor{UserID: "alice", TenantID: "t1", UserName: "甲"})
	require.NoError(t, svc.AddSign(ctxAlice, Actor{UserID: "alice", TenantID: "t1"}, "t-a", []string{"bob", "carol"}, "加两个人"))

	// bob 是节点参与人（子任务 assignee，authorizeSignOperator 放行），
	// 但 carol 的签是 alice 加的 → 减签资格校验拒绝
	ctxBob := SetUserToCtx(context.Background(), &Actor{UserID: "bob", TenantID: "t1", UserName: "乙"})
	err := svc.ReduceSign(ctxBob, Actor{UserID: "bob", TenantID: "t1"}, "t-a", []string{"carol"}, "想移走别人的签")
	require.ErrorIs(t, err, ErrPermissionDenied)

	// alice 本人减自己加的 carol → 成功
	require.NoError(t, svc.ReduceSign(ctxAlice, Actor{UserID: "alice", TenantID: "t1"}, "t-a", []string{"carol"}, "不需要了"))
	subs, err := svc.taskDAO.GetByParentID(ctxAlice, "t-a")
	require.NoError(t, err)
	require.Len(t, subs, 1, "carol 的加签子任务应被删除")
	require.Equal(t, "bob", *subs[0].Assignee)
}

// 无 sign_added_by 标记的子任务（流程配置的会签/票签人、存量加签）不受加签人
// 限制，节点参与人照旧可减——会签减人/减到 0 终止的既有语义保持。
func TestReduceSign_UnmarkedSubTaskNotRestricted(t *testing.T) {
	svc, q := newSignTestSvc(t)
	signSeed(t, q, "inst-1")
	signSeedParentTask(t, q, "t-a", "inst-1", "alice", "")
	// 无标记子任务：模拟流程配置产生的会签子任务
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "t-sub-legacy", ProcessInstanceID: secFixStrPtr("inst-1"), ProcessID: "proc-1",
		TaskDefKey: "a", Name: "会签子任务", TaskType: constants.TaskTypeUserTask,
		ParentID: secFixStrPtr("t-a"), Status: string(enums.TaskStatusActive),
		Assignee: secFixStrPtr("carol"), TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))

	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "alice", TenantID: "t1", UserName: "甲"})
	require.NoError(t, svc.ReduceSign(ctx, Actor{UserID: "alice", TenantID: "t1"}, "t-a", []string{"carol"}, "会签减人"))
	subs, err := svc.taskDAO.GetByParentID(ctx, "t-a")
	require.NoError(t, err)
	require.Empty(t, subs, "无标记子任务应可减")
}

// 撤回级联终止的跨节点任务 end_reason 带 withdrawn 前缀：并行分支在途票显示
// 「已作废（撤回）」而非「已终止」，与同节点作废票同口径。
func TestWithdraw_CrossNodeTaskArchivedWithWithdrawnReason(t *testing.T) {
	q := secFixDB(t)
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
		workflowEngine: &withdrawProcEngineDouble{testEngineDouble{runtime: rs}, recallProcessFake{def: signTestDef}},
	}

	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-wd-par", ProcessID: "proc-1", Name: "并行撤回单",
		Status: string(enums.InstanceStatusActive), TenantID: "t1",
		StartUserID: "starter", CreatedBy: "starter", CreatedAt: now,
	}))
	// 当前节点任务（撤回主目标）与并行分支另一节点任务同时在途
	for _, spec := range []struct{ id, defKey, assignee string }{
		{"t-cur", "a", "jia"},
		{"t-par", "b", "yi"},
	} {
		require.NoError(t, q.WfTask.Create(&model.WfTask{
			ID: spec.id, ProcessInstanceID: secFixStrPtr("inst-wd-par"), ProcessID: "proc-1",
			TaskDefKey: spec.defKey, Name: spec.defKey, TaskType: constants.TaskTypeUserTask,
			Status: string(enums.TaskStatusActive), Assignee: secFixStrPtr(spec.assignee),
			TenantID: "t1", CreatedBy: "system", CreatedAt: now,
		}))
	}

	ctx := context.Background()
	require.NoError(t, taskSvc.WithdrawByInstance(ctx, Actor{UserID: "starter", TenantID: "t1"}, "inst-wd-par", "填错了"))

	// 并行分支任务归档 end_reason 带 withdrawn 前缀（撤回缘由透传），
	// 不再是固定文案「流程实例被终止」
	hiPar, err := q.WfHiTask.WithContext(ctx).Where(q.WfHiTask.ID.Eq("t-par")).First()
	require.NoError(t, err)
	require.NotNil(t, hiPar.EndReason)
	require.Equal(t, "withdrawn: 填错了", *hiPar.EndReason,
		"跨节点作废票应与同节点同口径（withdrawn 前缀 + 缘由）")
}
