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

// 详情装配：转办过的任务从变量透出转办轨迹（转办前处理人/理由/时间），
// transfer_time 的 RFC3339 序列化文本规整为时间线时间格式；未转办任务为空。
func TestExecutionInfo_TransferTrace(t *testing.T) {
	transferred := &model.WfTask{
		ID: "t-tz", TaskDefKey: "a", Name: "a", Status: string(enums.TaskStatusActive),
		Variables: secFixStrPtr(`{"transfer_from":"1001","transfer_reason":"出差在外","transfer_time":"2026-10-20T10:14:00+08:00","amount":3}`),
	}
	info := Task2ExecutionInfo(transferred, nil)
	require.Equal(t, "1001", info.TransferredFrom)
	require.Equal(t, "出差在外", info.TransferredReason)
	require.Equal(t, "2026-10-20 10:14:00", info.TransferredAt)

	legacy := &model.WfTask{
		ID: "t-tz3", TaskDefKey: "a", Name: "a", Status: string(enums.TaskStatusActive),
		Variables: secFixStrPtr(`{"transfer_from":"1002","transfer_time":"2026-10-20 09:00:00"}`),
	}
	formatted := Task2ExecutionInfo(legacy, nil)
	require.Equal(t, "2026-10-20 09:00:00", formatted.TransferredAt)

	plain := &model.WfTask{ID: "t-tz2", Status: string(enums.TaskStatusActive)}
	empty := Task2ExecutionInfo(plain, nil)
	require.Empty(t, empty.TransferredFrom)
	require.Empty(t, empty.TransferredReason)
	require.Empty(t, empty.TransferredAt)
}

// 转办落轨迹行：归档表新增 task_type=forward 记录，无办理人（防已办/统计口径
// 虚增），转办人/接手人进变量、原因进意见；多次转办按发生顺序排列（真实时刻锚）。
func TestTransferWritesForwardTraceRow(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-fwd")
	proxySeedTask(t, q, "t-fwd", "px-inst-fwd", "a", "yi", `{}`)

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "yi", TenantID: "t1"})
	require.NoError(t, svc.Transfer(ctx, Actor{UserID: "yi", TenantID: "t1"}, "t-fwd", "er", "出差在外"))

	live, err := svc.taskDAO.Get(ctx, "t-fwd")
	require.NoError(t, err)
	require.Equal(t, "er", *live.Assignee, "任务本体原地换受理人")

	fwdRows := func() []*model.WfHiTask {
		rows, ferr := q.WfHiTask.WithContext(context.Background()).
			Where(q.WfHiTask.ProcessInstanceID.Eq("px-inst-fwd"),
				q.WfHiTask.TaskType.Eq(string(constants.TaskTypeForward))).
			Order(q.WfHiTask.CreatedAt).
			Find()
		require.NoError(t, ferr)
		return rows
	}
	rows := fwdRows()
	require.Len(t, rows, 1)
	require.Nil(t, rows[0].Assignee, "轨迹行不设办理人：已办/统计按 assignee+completed 计数")
	require.Nil(t, rows[0].TaskDefKey, "轨迹行不认领节点身份：按 taskDefKey 聚合节点状态的时间线会把在途节点误判为已完成")
	require.Equal(t, string(enums.EndReasonForwarded), *rows[0].EndReason)
	require.Equal(t, "出差在外", *rows[0].Comment)
	require.Contains(t, *rows[0].Variables, `"forward_by":"yi"`)
	require.Contains(t, *rows[0].Variables, `"forward_to":"er"`)
	require.False(t, rows[0].CreatedAt.Before(live.CreatedAt), "轨迹行锚在转办真实时刻，不早于活任务创建时间")

	// 第二次转办：轨迹链按转办发生顺序排列（真实时刻锚，同毫秒时插入序兜底）
	require.NoError(t, svc.Transfer(ctx, Actor{UserID: "er", TenantID: "t1"}, "t-fwd", "san", "接力处理"))
	rows = fwdRows()
	require.Len(t, rows, 2)
	require.Equal(t, "出差在外", *rows[0].Comment, "先发生的转办排在前")
	require.Equal(t, "接力处理", *rows[1].Comment, "后发生的转办排在后")
	live2, err := svc.taskDAO.Get(ctx, "t-fwd")
	require.NoError(t, err)
	require.Equal(t, "san", *live2.Assignee)
}

// 会签子任务的转办：轨迹行挂父任务（ParentID）且标题取节点名——时间线嵌在
// 会签节点的子票列表里渲染；计票/减签只查活任务表，归档行挂 ParentID 无副作用。
func TestTransferSubTaskTraceRowNestsUnderParent(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-fwd2")
	parent := &model.WfTask{
		ID: "t-fwd2-parent", ProcessInstanceID: secFixStrPtr("px-inst-fwd2"), ProcessID: "proc-px",
		TaskDefKey: "a", Name: "会签审批", TaskType: constants.TaskTypeUserTask,
		Status: string(enums.TaskStatusActive), ApprovalType: string(enums.ApprovalTypeCountersign),
		TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now().Add(-time.Hour),
	}
	require.NoError(t, q.WfTask.Create(parent))
	sub := &model.WfTask{
		ID: "t-fwd2-sub", ProcessInstanceID: secFixStrPtr("px-inst-fwd2"), ProcessID: "proc-px",
		ParentID: secFixStrPtr("t-fwd2-parent"), TaskDefKey: "a", Name: "会签审批_48001",
		TaskType: constants.TaskTypeUserTask, Status: string(enums.TaskStatusActive),
		Assignee: secFixStrPtr("yi"), SequenceOrder: 1, ApprovalType: string(enums.ApprovalTypeCountersign),
		TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now().Add(-time.Hour),
	}
	require.NoError(t, q.WfTask.Create(sub))

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "yi", TenantID: "t1"})
	require.NoError(t, svc.Transfer(ctx, Actor{UserID: "yi", TenantID: "t1"}, "t-fwd2-sub", "er", "代审"))

	rows, err := q.WfHiTask.WithContext(context.Background()).
		Where(q.WfHiTask.ProcessInstanceID.Eq("px-inst-fwd2"),
			q.WfHiTask.TaskType.Eq(string(constants.TaskTypeForward))).
		Find()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.NotNil(t, rows[0].ParentID)
	require.Equal(t, "t-fwd2-parent", *rows[0].ParentID, "轨迹行挂会签父任务，时间线嵌进子票列表")
	require.Equal(t, "会签审批", rows[0].Name, "标题取节点名，子任务名含受理人ID不可读")

	// 计票不受影响：子任务原地换人，未投票仍是未完成（all 规则 0/1 不定局）
	isCompleted, _, err := svc.checkCountersignSubTaskCompletionInternal(ctx, bareScope(q), "t-fwd2-parent", `{"type":"all"}`)
	require.NoError(t, err)
	require.False(t, isCompleted)
	live, err := svc.taskDAO.Get(ctx, "t-fwd2-sub")
	require.NoError(t, err)
	require.Equal(t, "er", *live.Assignee)
}

// 会签合并剥离保留键：子票上的转办/兜底/加签/改派标记不合并进父任务变量，
// 否则父行被时间线误标、流转出口漏剥时会污染下游；父任务自身的留痕键
// （如兜底 fallback_*）是合法归档数据，剥离后回填。
func TestCountersignMergeStripsReservedVars(t *testing.T) {
	q := proxyAuditDB(t)
	parent := &model.WfTask{
		ID: "t-merge-parent", ProcessInstanceID: secFixStrPtr("px-merge"), ProcessID: "proc-px",
		TaskDefKey: "a", Name: "会签审批", TaskType: constants.TaskTypeUserTask,
		Status: string(enums.TaskStatusActive), TenantID: "t1", CreatedBy: "system",
		Variables: secFixStrPtr(`{"days":1,"fallback_policy":"tenant_admin","fallback_reason":"无审批人"}`),
		CreatedAt: time.Now(),
	}
	require.NoError(t, q.WfTask.Create(parent))
	approved := "approved"
	sub := &model.WfTask{
		ID: "t-merge-sub", ProcessInstanceID: secFixStrPtr("px-merge"), ProcessID: "proc-px",
		ParentID: secFixStrPtr("t-merge-parent"), TaskDefKey: "a", Name: "会签审批_48001",
		TaskType: constants.TaskTypeUserTask, Status: string(enums.TaskStatusCompleted),
		EndReason: &approved, TenantID: "t1", CreatedBy: "system",
		Variables: secFixStrPtr(`{"approved":true,"transfer_from":"yi","transfer_reason":"出差","sign_added_by":"yi","fallback_policy":"admin","reassign_from":"yi","reassign_operator":"admin","days":2}`),
		CreatedAt: time.Now(),
	}
	require.NoError(t, q.WfTask.Create(sub))

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	svc.mergeCountersignSubTaskVariables(context.Background(), bareScope(q), parent)

	merged, err := ParseVariablesJSON(parent.Variables)
	require.NoError(t, err)
	require.Equal(t, float64(2), merged["days"], "业务变量照常合并（子任务覆盖同名键为既有语义）")
	require.Equal(t, true, merged["approved"])
	require.NotContains(t, merged, "transfer_from")
	require.NotContains(t, merged, "transfer_reason")
	require.NotContains(t, merged, "sign_added_by")
	require.NotContains(t, merged, "reassign_from", "改派留痕键族随前缀剥离")
	require.NotContains(t, merged, "reassign_operator")
	// 父任务自身的兜底留痕保留：子票的 fallback_policy=admin 不得覆盖父自身的值
	require.Equal(t, "tenant_admin", merged["fallback_policy"], "父任务自身的留痕键剥离后回填")
	require.Equal(t, "无审批人", merged["fallback_reason"])
}

// 委派归还是 CompleteWithApproval 的提前 return 旁路：提交变量与审批主路径
// 同口径剥离保留键——被委派人借 variables 伪造 transfer_from/proxy_operator
// 会污染任务留痕（时间线据此渲染假转办、recall 守卫据此误拦）。
func TestDelegatedResolveStripsReservedVars(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-dlg")
	proxySeedTask(t, q, "t-dlg", "px-inst-dlg", "a", "userB", `{}`)
	// 委派态：Assignee=被委派人，Owner=原审批人
	_, err := q.WfTask.WithContext(context.Background()).
		Where(q.WfTask.ID.Eq("t-dlg")).
		UpdateSimple(q.WfTask.Owner.Value("userA"))
	require.NoError(t, err)

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	err = svc.CompleteWithApproval(context.Background(), Actor{UserID: "userB", TenantID: "t1"}, &ApprovalRequest{
		TaskID:         "t-dlg",
		ApprovalResult: enums.ApprovalResultApproved,
		Comment:        "同意",
		Variables: map[string]interface{}{
			"amount":         100,
			"transfer_from":  "someone",
			"proxy_operator": "admin",
			"reassign_from":  "someone",
		},
	})
	require.NoError(t, err)

	row, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-dlg")).First()
	require.NoError(t, err)
	require.Equal(t, "userA", *row.Assignee, "委派归还：任务回到原审批人名下")
	vars, err := ParseVariablesJSON(row.Variables)
	require.NoError(t, err)
	require.Equal(t, float64(100), vars["amount"], "业务变量照常合并")
	require.NotContains(t, vars, "transfer_from")
	require.NotContains(t, vars, "proxy_operator")
	require.NotContains(t, vars, "reassign_from")
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

// 委派与归还成对落轨迹行：委派行记委派人与被委派人，归还行记归还人（被委
// 派人审结）；活任务的 delegate_* 列只留最近一次，过程链靠归档轨迹行。
func TestDelegateWritesTraceRows(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-dlg2")
	proxySeedTask(t, q, "t-dlg2", "px-inst-dlg2", "a", "userA", `{}`)

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "userA", TenantID: "t1"})
	require.NoError(t, svc.Delegate(ctx, Actor{UserID: "userA", TenantID: "t1"}, "t-dlg2", "userB", "出差代审"))

	dlgRows := func() []*model.WfHiTask {
		rows, ferr := q.WfHiTask.WithContext(context.Background()).
			Where(q.WfHiTask.ProcessInstanceID.Eq("px-inst-dlg2"),
				q.WfHiTask.TaskType.Eq(string(constants.TaskTypeDelegate))).
			Order(q.WfHiTask.CreatedAt).
			Find()
		require.NoError(t, ferr)
		return rows
	}
	rows := dlgRows()
	require.Len(t, rows, 1)
	require.Nil(t, rows[0].Assignee, "轨迹行不设办理人：统计按 assignee+completed 计数")
	require.Nil(t, rows[0].TaskDefKey, "轨迹行不认领节点身份")
	require.Equal(t, string(enums.EndReasonDelegated), *rows[0].EndReason)
	require.Equal(t, "userA", rows[0].CreatedBy, "委派行的 created_by 是委派操作人")
	require.Equal(t, "出差代审", *rows[0].Comment)
	require.Contains(t, *rows[0].Variables, `"trace_operator":"userA"`)
	require.Contains(t, *rows[0].Variables, `"trace_target":"userB"`)

	// 被委派人审结 → 任务归还原办理人，落归还轨迹行
	bCtx := SetUserToCtx(context.Background(), &Actor{UserID: "userB", TenantID: "t1"})
	require.NoError(t, svc.CompleteWithApproval(bCtx, Actor{UserID: "userB", TenantID: "t1"}, &ApprovalRequest{
		TaskID: "t-dlg2", ApprovalResult: enums.ApprovalResultApproved, Comment: "同意",
	}))
	rows = dlgRows()
	require.Len(t, rows, 2)
	require.Equal(t, string(enums.EndReasonResolved), *rows[1].EndReason)
	require.Equal(t, "userB", rows[1].CreatedBy, "归还行的 created_by 是归还人")
	live, err := svc.taskDAO.Get(ctx, "t-dlg2")
	require.NoError(t, err)
	require.Equal(t, "userA", *live.Assignee, "审结归还原办理人")
}

// 改派落轨迹行：操作人进 created_by，原受理人/新受理人进变量；活任务变量里的
// reassign_* 只留最近一次，连续改投（交接/代理）过程靠轨迹行。
func TestReassignWritesTraceRow(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "px-inst-rag")
	proxySeedTask(t, q, "t-rag", "px-inst-rag", "a", "userA", `{}`)

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "admin1", TenantID: "t1", WorkflowAdmin: true})
	_, err := svc.Reassign(ctx, Actor{UserID: "admin1", TenantID: "t1", WorkflowAdmin: true}, "t-rag", "userB", "负载调整")
	require.NoError(t, err)

	rows, rerr := q.WfHiTask.WithContext(context.Background()).
		Where(q.WfHiTask.ProcessInstanceID.Eq("px-inst-rag"),
			q.WfHiTask.TaskType.Eq(string(constants.TaskTypeReassign))).
		Find()
	require.NoError(t, rerr)
	require.Len(t, rows, 1)
	require.Nil(t, rows[0].Assignee)
	require.Nil(t, rows[0].TaskDefKey)
	require.Equal(t, string(enums.EndReasonReassigned), *rows[0].EndReason)
	require.Equal(t, "admin1", rows[0].CreatedBy, "改派行的 created_by 是改派操作人")
	require.Contains(t, *rows[0].Variables, `"trace_operator":"admin1"`)
	require.Contains(t, *rows[0].Variables, `"trace_target":"userB"`)
	require.Contains(t, *rows[0].Variables, `"reassign_from":"userA"`)
}
