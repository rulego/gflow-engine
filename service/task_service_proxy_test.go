package service

// Tests for task_service_proxy.go: 代审出票、鉴权/开关/边界拒绝、会签一票、
// 留痕与系统评论，以及详情装配的 ProxyBy。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
)

// ---- 夹具 ----

// proxyAuditDB 在 secFix 三表之上补建 wf_task_comment（代审意见与系统评论落这里）。
// cache=shared 内存库：另开同 DSN 连接执行 DDL 即作用在同一库上。
func proxyAuditDB(t *testing.T) *query.Query {
	t.Helper()
	q := secFixDB(t)
	db, err := gorm.Open(sqlite.Open("file:secfix_test?mode=memory&cache=shared&_busy_timeout=30000"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`CREATE TABLE IF NOT EXISTS wf_task_comment (
		id TEXT PRIMARY KEY, task_id TEXT NOT NULL, process_instance_id TEXT NOT NULL DEFAULT '',
		tenant_id TEXT NOT NULL DEFAULT '', user_id TEXT NOT NULL,
		user_name TEXT NOT NULL DEFAULT '', content TEXT NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)`).Error)
	if c, e := db.DB(); e == nil {
		_ = c.Close()
	}
	return q
}

// proxyCommentsOf 读取任务的全部评论（代审票应有意见+系统评论两条）。
func proxyCommentsOf(t *testing.T, q *query.Query, taskID string) []*model.WfTaskComment {
	t.Helper()
	rows, err := q.WfTaskComment.WithContext(context.Background()).
		Where(q.WfTaskComment.TaskID.Eq(taskID)).Find()
	require.NoError(t, err)
	return rows
}

// proxyDefinition 构造流程定义 JSON，流程级 actionPermissions.proxyAudit 可开关。
func proxyDefinition(proxyOn bool) string {
	on := "false"
	if proxyOn {
		on = "true"
	}
	return fmt.Sprintf(`{"ruleChain":{"additionalInfo":{"actionPermissions":{"proxyAudit":%s}}},`+
		`"metadata":{"nodes":[{"id":"a","type":"userTask"},{"id":"b","type":"userTask"}],`+
		`"connections":[{"fromId":"a","toId":"b","type":"Success"}]}}`, on)
}

// proxyDefinitionNoKey 构造不带 proxyAudit 键的流程定义（存量流程形态，缺省=开）。
func proxyDefinitionNoKey() string {
	return `{"ruleChain":{},` +
		`"metadata":{"nodes":[{"id":"a","type":"userTask"},{"id":"b","type":"userTask"}],` +
		`"connections":[{"fromId":"a","toId":"b","type":"Success"}]}}`
}

// proxySeedTask 建 active 任务（assignee 为空表示候选池）。
func proxySeedTask(t *testing.T, q *query.Query, id, instID, defKey, assignee, vars string) {
	t.Helper()
	task := &model.WfTask{
		ID:                id,
		ProcessInstanceID: secFixStrPtr(instID),
		ProcessID:         "proc-px",
		TaskDefKey:        defKey,
		Name:              defKey,
		TaskType:          constants.TaskTypeUserTask,
		Status:            string(enums.TaskStatusActive),
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         time.Now().Add(-time.Hour),
	}
	if assignee != "" {
		task.Assignee = secFixStrPtr(assignee)
	}
	if vars != "" {
		task.Variables = secFixStrPtr(vars)
	}
	require.NoError(t, q.WfTask.Create(task))
}

func proxySeedInstance(t *testing.T, q *query.Query, id string) {
	t.Helper()
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID:          id,
		ProcessID:   "proc-px",
		Name:        "proxy-test",
		Status:      string(enums.InstanceStatusActive),
		TenantID:    "t1",
		CreatedBy:   "system",
		StartUserID: "starter",
		CreatedAt:   time.Now(),
	}))
}

// ---- 用例 ----

// 代审通过：票记原办理人名下（assignee 不变）、变量留痕、意见+系统评论两条、事件带 OnBehalfOf。
func TestProxyAudit_CompletesAsAssignee(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-1")
	proxySeedTask(t, q, "t-px1", "px-inst-1", "a", "yi", `{"amount":100}`)

	evtCh := make(chan TaskEvent, 8)
	svc, eng := newRecallSvc(q, proxyDefinition(true), func(_ context.Context, evt TaskEvent) { evtCh <- evt })

	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "admin", TenantID: "t1", UserName: "管理员", WorkflowAdmin: true})
	require.NoError(t, svc.ProxyAudit(ctx, Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}, "t-px1", true, "越级确认"))

	row, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-px1")).First()
	require.NoError(t, err)
	require.Equal(t, string(enums.TaskStatusCompleted), row.Status)
	require.NotNil(t, row.Assignee)
	require.Equal(t, "yi", *row.Assignee, "票面归属必须是原办理人")
	require.NotNil(t, row.EndReason)
	require.Equal(t, string(enums.ApprovalResultApproved), *row.EndReason)

	var vars map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*row.Variables), &vars))
	require.Equal(t, "admin", vars[constants.VarsProxyOperator])
	require.NotEmpty(t, vars[constants.VarsProxyTime])
	require.Equal(t, float64(100), vars["amount"], "业务变量不能被代审标记冲掉")

	comments := proxyCommentsOf(t, q, "t-px1")
	require.Len(t, comments, 2, "意见 + 系统评论两条")
	joined := comments[0].Content + "|" + comments[1].Content
	require.Contains(t, joined, "越级确认")
	require.Contains(t, joined, "代审：管理员 admin 代 yi 通过该审批")

	// 流转推进打到下一节点；approved 事件带被代人标记
	require.Equal(t, "a", eng.internal.execNextNode)
	select {
	case evt := <-evtCh:
		if evt.Type == TaskEventApproved {
			require.Equal(t, "yi", evt.OnBehalfOf)
			require.Equal(t, "admin", evt.FromUser)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("approved 事件未派发")
	}
}

// 代审驳回：end_reason=rejected、标记在。
func TestProxyAudit_RejectWritesMark(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-2")
	proxySeedTask(t, q, "t-px2", "px-inst-2", "a", "yi", "")

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true})
	require.NoError(t, svc.ProxyAudit(ctx, Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}, "t-px2", false, "驳回理由"))

	row, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-px2")).First()
	require.NoError(t, err)
	require.Equal(t, string(enums.ApprovalResultRejected), *row.EndReason)
	require.True(t, hasProxyMark(row), "驳回票同样要带代审标记")
}

// 普通用户（WorkflowAdmin=false）无权代审。
func TestProxyAudit_RequiresAdmin(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-3")
	proxySeedTask(t, q, "t-px3", "px-inst-3", "a", "yi", "")

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1"})
	err := svc.ProxyAudit(ctx, Actor{UserID: "jia", TenantID: "t1"}, "t-px3", true, "x")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied))
}

// 流程显式关闭 proxyAudit（opt-out）拒绝。
func TestProxyAudit_RequiresProcessSwitch(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-4")
	proxySeedTask(t, q, "t-px4", "px-inst-4", "a", "yi", "")

	svc, _ := newRecallSvc(q, proxyDefinition(false), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true})
	err := svc.ProxyAudit(ctx, Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}, "t-px4", true, "x")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied))
	require.Contains(t, err.Error(), "该流程已关闭管理员代审")
}

// 存量流程 DSL 无 proxyAudit 键（缺省）按开处理，代审放行。
func TestProxyAudit_DefaultOnWithoutKey(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-7")
	proxySeedTask(t, q, "t-px7", "px-inst-7", "a", "yi", "")

	svc, _ := newRecallSvc(q, proxyDefinitionNoKey(), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true})
	require.NoError(t, svc.ProxyAudit(ctx, Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}, "t-px7", true, "缺省开"))

	row, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-px7")).First()
	require.NoError(t, err)
	require.Equal(t, string(enums.TaskStatusCompleted), row.Status)
}

// 候选池任务（assignee 空）不可代审。
func TestProxyAudit_RejectsUnassigned(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-5")
	proxySeedTask(t, q, "t-px5", "px-inst-5", "a", "", "")

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true})
	err := svc.ProxyAudit(ctx, Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}, "t-px5", true, "x")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrValidation))
}

// 委派中任务（Owner 在位）入口拒绝；锁内 complete 通道对"入口校验后才发生委派"兜底。
func TestProxyAudit_RejectsDelegated(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-6")
	proxySeedTask(t, q, "t-px6", "px-inst-6", "a", "delegatee", "")
	// Owner 在位 = 委派中
	_, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-px6")).
		UpdateSimple(q.WfTask.Owner.Value("yi"))
	require.NoError(t, err)

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true})
	err = svc.ProxyAudit(ctx, Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}, "t-px6", true, "x")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrValidation))

	// 锁内兜底：ctx 双信号（OnBehalfOf + proxy 来源）齐全直接打
	// completeWithApprovalInternal（模拟入口校验与行锁之间任务被委派的竞态），
	// 必须拒绝且不触归还分支
	ctx2 := WithEventSource(WithOnBehalfOf(SetUserToCtx(context.Background(),
		&Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}), "delegatee"), EventSourceProxy)
	err = WithInstanceTx(ctx2, q, "px-inst-6", func(scope *InstanceScope) error {
		return svc.completeWithApprovalInternal(ctx2, scope, &ApprovalRequest{
			TaskID: "t-px6", ApprovalResult: enums.ApprovalResultApproved, Comment: "x",
		})
	})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "已委派"), "锁内兜底要用代审专属文案")
}

// 会签（all）代审一人一票：子任务带标记完成，未达阈值父任务继续等待，同伴不受影响。
func TestProxyAudit_CountersignVote(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-7")
	base := time.Now().Add(-time.Hour)
	parent := &model.WfTask{
		ID: "t-parent7", ProcessInstanceID: secFixStrPtr("px-inst-7"), ProcessID: "proc-px",
		TaskDefKey: "a", Name: "a", TaskType: constants.TaskTypeUserTask,
		Status: string(enums.TaskStatusActive), ApprovalRule: secFixStrPtr(`{"type":"all"}`),
		TenantID: "t1", CreatedBy: "system", CreatedAt: base,
	}
	require.NoError(t, q.WfTask.Create(parent))
	for i, who := range []string{"jia", "yi"} {
		sub := &model.WfTask{
			ID: "t-sub7-" + who, ProcessInstanceID: secFixStrPtr("px-inst-7"), ProcessID: "proc-px",
			ParentID: secFixStrPtr("t-parent7"), TaskDefKey: "a", TaskType: constants.TaskTypeUserTask,
			Status: string(enums.TaskStatusActive), Assignee: secFixStrPtr(who),
			ApprovalRule: secFixStrPtr(`{"type":"all"}`), SequenceOrder: int32(i + 1),
			TenantID: "t1", CreatedBy: "system", CreatedAt: base,
		}
		require.NoError(t, q.WfTask.Create(sub))
	}

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true})
	require.NoError(t, svc.ProxyAudit(ctx, Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}, "t-sub7-jia", true, "先批一步"))

	sub1, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-sub7-jia")).First()
	require.NoError(t, err)
	require.Equal(t, string(enums.TaskStatusCompleted), sub1.Status)
	require.True(t, hasProxyMark(sub1), "标记只落在被代审的子任务上")

	sub2, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-sub7-yi")).First()
	require.NoError(t, err)
	require.Equal(t, string(enums.TaskStatusActive), sub2.Status, "同伴待办不受影响")
	require.False(t, hasProxyMark(sub2))

	prow, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-parent7")).First()
	require.NoError(t, err)
	require.Equal(t, string(enums.TaskStatusActive), prow.Status, "all 会签一票未达阈值，父任务继续等待")
}

// 详情装配：Task2ExecutionInfo 从任务变量读出 ProxyBy；无标记时为空。
func TestExecutionInfo_ProxyBy(t *testing.T) {
	marked := &model.WfTask{
		ID: "t-x", TaskDefKey: "a", Name: "a", Status: string(enums.TaskStatusCompleted),
		Variables: secFixStrPtr(`{"proxy_operator":"admin","approved":true}`),
	}
	info := Task2ExecutionInfo(marked, nil)
	require.Equal(t, "admin", info.ProxyBy)

	plain := &model.WfTask{ID: "t-y", Status: string(enums.TaskStatusCompleted)}
	require.Empty(t, Task2ExecutionInfo(plain, nil).ProxyBy)
}

// 代审标记只留在被代审的任务行上：流转下传变量（ExecuteNext 入参）不得携带
// proxy 键——下游任务的变量快照来自这份 vars，沾上会被误标代审、误拦收回。
func TestProxyAudit_MarkNotLeakedDownstream(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-8")
	proxySeedTask(t, q, "t-px8", "px-inst-8", "a", "yi", `{"amount":100}`)

	svc, eng := newRecallSvc(q, proxyDefinition(true), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true})
	require.NoError(t, svc.ProxyAudit(ctx, Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}, "t-px8", true, "同意"))

	row, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-px8")).First()
	require.NoError(t, err)
	require.True(t, hasProxyMark(row), "任务行本身要带标记（归档与收回拦截依据）")

	require.NotNil(t, eng.internal.execNextVars)
	require.NotContains(t, eng.internal.execNextVars, constants.VarsProxyOperator)
	require.NotContains(t, eng.internal.execNextVars, constants.VarsProxyTime)
	require.Equal(t, float64(100), eng.internal.execNextVars["amount"], "业务变量照常下传")
}

// internal 调用模式不打代审标记：代审后的 ExecuteNext 沿用原 ctx，下游
// autoApprove 等内部自动完成路径不得被误标（标记分支限定 API 模式）。
func TestProxyAudit_InternalCallingModeNotMarked(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-9")
	proxySeedTask(t, q, "t-px9", "px-inst-9", "a", "yi", `{"amount":1}`)

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	// 双信号齐全但调用模式是 internal（模拟下游自动完成链路）
	ctx := WithOnBehalfOf(WithEventSource(SetUserToCtx(context.Background(),
		&Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}), EventSourceProxy), "yi")
	ctx = WithInternalCallingMode(ctx)
	require.NoError(t, WithInstanceTx(ctx, q, "px-inst-9", func(scope *InstanceScope) error {
		return svc.completeWithApprovalInternal(ctx, scope, &ApprovalRequest{
			TaskID: "t-px9", ApprovalResult: enums.ApprovalResultApproved,
		})
	}))

	row, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-px9")).First()
	require.NoError(t, err)
	require.Equal(t, string(enums.TaskStatusCompleted), row.Status)
	require.False(t, hasProxyMark(row), "internal 自动完成不得打代审标记")
	require.Empty(t, proxyCommentsOf(t, q, "t-px9"), "internal 路径不落代审系统评论")
}

// 会签父任务变量不沾子任务的代审标记（mergeCountersignSubTaskVariables 剥离）。
func TestProxyAudit_CountersignParentNotMarked(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-10")
	base := time.Now().Add(-time.Hour)
	parent := &model.WfTask{
		ID: "t-parent10", ProcessInstanceID: secFixStrPtr("px-inst-10"), ProcessID: "proc-px",
		TaskDefKey: "a", Name: "a", TaskType: constants.TaskTypeUserTask,
		Status: string(enums.TaskStatusActive), ApprovalRule: secFixStrPtr(`{"type":"any"}`),
		TenantID: "t1", CreatedBy: "system", CreatedAt: base,
	}
	require.NoError(t, q.WfTask.Create(parent))
	for i, who := range []string{"jia", "yi"} {
		sub := &model.WfTask{
			ID: "t-sub10-" + who, ProcessInstanceID: secFixStrPtr("px-inst-10"), ProcessID: "proc-px",
			ParentID: secFixStrPtr("t-parent10"), TaskDefKey: "a", TaskType: constants.TaskTypeUserTask,
			Status: string(enums.TaskStatusActive), Assignee: secFixStrPtr(who),
			ApprovalRule: secFixStrPtr(`{"type":"any"}`), SequenceOrder: int32(i + 1),
			TenantID: "t1", CreatedBy: "system", CreatedAt: base,
		}
		require.NoError(t, q.WfTask.Create(sub))
	}

	svc, eng := newRecallSvc(q, proxyDefinition(true), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true})
	// any 规则：代审一票即达阈值，父任务完成并流转
	require.NoError(t, svc.ProxyAudit(ctx, Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}, "t-sub10-jia", true, "一票过"))

	prow, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-parent10")).First()
	require.NoError(t, err)
	require.Equal(t, string(enums.TaskStatusCompleted), prow.Status)
	require.False(t, hasProxyMark(prow), "父任务票面未被代审，变量不得沾子任务标记")

	sub, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-sub10-jia")).First()
	require.NoError(t, err)
	require.True(t, hasProxyMark(sub), "被代审子任务保留标记")

	require.NotNil(t, eng.internal.execNextVars)
	require.NotContains(t, eng.internal.execNextVars, constants.VarsProxyOperator)
}
