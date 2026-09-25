package service

// Tests for task_service_recall.go: 守卫矩阵 + 重建任务字段 + 前沿任务终止。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
)

// ---- 夹具 ----

// recallEngineDouble 在 testEngineDouble 之上注入流程定义与实例读取，
// 供流程级开关与路径守卫解析（Recall 在锁外经 runtime 读实例拿 processID）。
type recallEngineDouble struct {
	testEngineDouble
	proc     ProcessService
	q        *query.Query
	internal *recallInternalDouble
}

func (e *recallEngineDouble) GetProcessService() ProcessService { return e.proc }

func (e *recallEngineDouble) GetRuntimeService() RuntimeService {
	return &recallRuntimeDouble{testRuntimeDouble: testRuntimeDouble{}, q: e.q}
}

func (e *recallEngineDouble) GetRuntimeServiceInternal() RuntimeServiceInternal { return e.internal }

// recallInternalDouble 补齐内部接口，记录 ExecuteNext 重入节点与下传变量供断言
type recallInternalDouble struct {
	testRuntimeDouble
	execNextNode string
	execNextVars map[string]interface{}
}

func (d *recallInternalDouble) GetExecution(context.Context, string) (types.RuleEngine, error) {
	return nil, nil
}
func (d *recallInternalDouble) PreloadChain(string, string, string) error { return nil }
func (d *recallInternalDouble) EvictStaleChain(context.Context, string, string) {
}
func (d *recallInternalDouble) ResolveSubProcessTarget(string, string) (string, bool) {
	return "", false
}
func (d *recallInternalDouble) StartSubProcessInstance(context.Context, string, string, string, map[string]interface{}) (string, error) {
	return "", nil
}
func (d *recallInternalDouble) SubProcessChildState(context.Context, string) (bool, bool, error) {
	return false, false, nil
}
func (d *recallInternalDouble) SubProcessChildTerminated(context.Context, string) (bool, error) {
	return false, nil
}
func (d *recallInternalDouble) ExecuteNext(_ context.Context, _, node string, vars map[string]interface{}) error {
	d.execNextNode = node
	d.execNextVars = vars
	return nil
}

type recallRuntimeDouble struct {
	testRuntimeDouble
	q *query.Query
}

func (r *recallRuntimeDouble) GetProcessInstance(_ context.Context, _ Actor, id string) (*model.WfInstance, error) {
	// 与生产 GetProcessInstance 同口径:活表优先,历史表回退
	if inst, err := r.q.WfInstance.WithContext(context.Background()).Where(r.q.WfInstance.ID.Eq(id)).First(); err == nil {
		return inst, nil
	}
	return dao.NewHiInstanceDAOWithQuery(r.q).Get(context.Background(), id)
}

type recallProcessFake struct {
	noopSecFixProcessService
	def string
}

func (f recallProcessFake) Get(context.Context, string) (*model.WfProcess, error) {
	return &model.WfProcess{DefinitionJSON: f.def}, nil
}

func recallNode(id, typ string) string { return fmt.Sprintf(`{"id":%q,"type":%q}`, id, typ) }
func recallConn(from, to string) string {
	return fmt.Sprintf(`{"fromId":%q,"toId":%q,"type":"Success"}`, from, to)
}

func recallDefinition(recallOn bool, nodes, conns []string) string {
	on := "false"
	if recallOn {
		on = "true"
	}
	join := func(items []string) string {
		out := ""
		for i, s := range items {
			if i > 0 {
				out += ","
			}
			out += s
		}
		return out
	}
	return fmt.Sprintf(`{"ruleChain":{"additionalInfo":{"actionPermissions":{"recall":%s}}},"metadata":{"nodes":[%s],"connections":[%s]}}`,
		on, join(nodes), join(conns))
}

func newRecallSvc(q *query.Query, def string, listener TaskEventListener) (*TaskServiceImpl, *recallEngineDouble) {
	eng := &recallEngineDouble{
		testEngineDouble: testEngineDouble{listener: listener},
		proc:             recallProcessFake{def: def},
		q:                q,
		internal:         &recallInternalDouble{},
	}
	svc := &TaskServiceImpl{
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:      dao.NewHiTaskDAOWithQuery(q),
		workflowEngine: eng,
		idGenerator:    DefaultIDGenerator,
	}
	return svc, eng
}

func recallSeedInstance(t *testing.T, q *query.Query, id, status string) {
	t.Helper()
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID:          id,
		ProcessID:   "proc-1",
		Name:        "recall-test",
		Status:      status,
		TenantID:    "t1",
		CreatedBy:   "system",
		StartUserID: "starter",
		CreatedAt:   time.Now(),
	}))
}

func recallSeedTask(t *testing.T, q *query.Query, id, instID, defKey, typ, status, assignee string, createdAt, endedAt time.Time, vars string) {
	t.Helper()
	task := &model.WfTask{
		ID:                id,
		ProcessInstanceID: secFixStrPtr(instID),
		ProcessID:         "proc-1",
		TaskDefKey:        defKey,
		Name:              defKey,
		TaskType:          typ,
		Status:            status,
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         createdAt,
	}
	if assignee != "" {
		task.Assignee = secFixStrPtr(assignee)
	}
	if !endedAt.IsZero() {
		task.EndedAt = &endedAt
		task.EndReason = secFixStrPtr(string(enums.ApprovalResultApproved))
	}
	if vars != "" {
		task.Variables = secFixStrPtr(vars)
	}
	require.NoError(t, q.WfTask.Create(task))
}

func recallTaskByID(t *testing.T, q *query.Query, id string) *model.WfTask {
	t.Helper()
	row, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq(id)).First()
	if err != nil {
		return nil
	}
	return row
}

// ---- 守卫矩阵 ----

// 单人审批：A(甲) 通过 → B(乙) 待审，甲收回后乙待办作废、甲重建待审、旧投票变量剔除。
func TestRecall_SingleApproval_RecreatesTask(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-1", string(enums.InstanceStatusActive))

	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-a", "inst-1", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), `{"_sequentialAssignees":["jia","yi"],"approved":true,"comment":"ok"}`)
	recallSeedTask(t, q, "t-b", "inst-1", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"yi", base.Add(2*time.Minute), time.Time{}, "")

	evtCh := make(chan TaskEvent, 8)
	svc, _ := newRecallSvc(q, def, func(_ context.Context, evt TaskEvent) { evtCh <- evt })

	require.NoError(t, svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-1", "填错了"))

	// 前沿任务终止并归档
	require.Nil(t, recallTaskByID(t, q, "t-b"), "B 待办应从运行表删除")
	require.EqualValues(t, 1, countHi(t, q, "t-b"), "B 待办应归档 hi 表")
	// 原投票归档为 recalled
	require.Nil(t, recallTaskByID(t, q, "t-a"), "原任务应从运行表删除")
	require.EqualValues(t, 1, countHi(t, q, "t-a"), "原任务应归档 hi 表")
	// 重建任务
	recreated := q.WfTask.WithContext(ctx)
	rows, err := recreated.Where(q.WfTask.ProcessInstanceID.Eq("inst-1")).Find()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	nt := rows[0]
	require.Equal(t, "a", nt.TaskDefKey)
	require.NotNil(t, nt.Assignee)
	require.Equal(t, "jia", *nt.Assignee)
	require.Equal(t, string(enums.TaskStatusActive), nt.Status)
	require.Nil(t, nt.EndedAt)
	// 变量：保留顺序缓存，剔除旧投票结果
	require.NotNil(t, nt.Variables)
	var vars map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*nt.Variables), &vars))
	require.Contains(t, vars, constants.KeySequentialAssignees)
	require.NotContains(t, vars, constants.VarsApproved)
	require.NotContains(t, vars, constants.VarsComment)

	// 事件：recalled（ToUsers=乙）+ assigned（重建任务）
	done := time.After(3 * time.Second)
	var gotRecalled, gotAssigned bool
	for !gotRecalled || !gotAssigned {
		select {
		case evt := <-evtCh:
			switch evt.Type {
			case TaskEventRecalled:
				gotRecalled = true
				require.Equal(t, nt.ID, evt.TaskID)
				require.Contains(t, evt.ToUsers, "yi")
			case TaskEventAssigned:
				gotAssigned = true
				require.Equal(t, []string{"jia"}, evt.ToUsers)
			}
		case <-done:
			t.Fatalf("事件未派发齐: recalled=%v assigned=%v", gotRecalled, gotAssigned)
		}
	}
}

// 收回重建任务剥离上一轮的引擎保留标记，顺序缓存等业务变量保留。
func TestRecall_RebuildStripsReservedMarks(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-marks", string(enums.InstanceStatusActive))

	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "tm-a", "inst-marks", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute),
		`{"_sequentialAssignees":["jia","yi"],"approved":true,"comment":"ok","fallback_policy":"tenant_admin","fallback_from":"role:r1","fallback_reason":"审批人为空，已转交租户管理员","fallback_time":"2026-09-24 10:00:00"}`)
	recallSeedTask(t, q, "tm-b", "inst-marks", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"yi", base.Add(2*time.Minute), time.Time{}, "")

	svc, _ := newRecallSvc(q, def, nil)
	require.NoError(t, svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-marks", "填错了"))

	rows, err := q.WfTask.WithContext(ctx).Where(q.WfTask.ProcessInstanceID.Eq("inst-marks")).Find()
	require.NoError(t, err)
	require.Len(t, rows, 1, "只应剩重建任务")
	require.NotNil(t, rows[0].Variables)
	var vars map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*rows[0].Variables), &vars))
	require.NotContains(t, vars, constants.VarsFallbackPolicy)
	require.NotContains(t, vars, constants.VarsFallbackFrom)
	require.NotContains(t, vars, constants.VarsFallbackReason)
	require.NotContains(t, vars, constants.VarsFallbackTime)
	require.Contains(t, vars, constants.KeySequentialAssignees, "顺序缓存保留")
	require.NotContains(t, vars, constants.VarsApproved)
}

func countHi(t *testing.T, q *query.Query, id string) int64 {
	t.Helper()
	n, err := q.WfHiTask.WithContext(context.Background()).Where(q.WfHiTask.ID.Eq(id)).Count()
	require.NoError(t, err)
	return n
}

// 顺序会签：只有最后一位已签署者能收回；中间签署者被更晚办理记录挡住。
func TestRecall_Sequential_OnlyLastSigner(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-2", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-jia", "inst-2", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	recallSeedTask(t, q, "t-yi", "inst-2", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"yi", base.Add(2*time.Minute), base.Add(3*time.Minute), "")
	recallSeedTask(t, q, "t-b", "inst-2", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"bing", base.Add(4*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	err := svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-2", "")
	require.Error(t, err, "中间签署者收回必须拒绝")
	require.True(t, errors.Is(err, ErrValidation))

	require.NoError(t, svc.Recall(
		SetUserToCtx(context.Background(), &Actor{UserID: "yi", TenantID: "t1", UserName: "乙"}),
		Actor{UserID: "yi", TenantID: "t1"}, "inst-2", ""), "最后一位签署者可收回")
	require.Nil(t, recallTaskByID(t, q, "t-b"), "B 待办应随收回终止")
}

// 顺序会签在途：最后一位未签时，最后一位已签署者收回会终止在途子任务。
func TestRecall_Sequential_MidFlightRecyclesInFlight(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true, []string{recallNode("a", "userTask")}, nil)
	recallSeedInstance(t, q, "inst-3", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-jia", "inst-3", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	recallSeedTask(t, q, "t-yi", "inst-3", "a", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"yi", base.Add(2*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	require.NoError(t, svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-3", ""))
	require.Nil(t, recallTaskByID(t, q, "t-yi"), "在途后续子任务应随收回终止")
	rows, _ := q.WfTask.WithContext(ctx).Where(q.WfTask.ProcessInstanceID.Eq("inst-3")).Find()
	require.Len(t, rows, 1)
	require.Equal(t, "jia", *rows[0].Assignee)
}

// 会签：同父同侪的更晚投票不阻断收回，父任务行从 completed 回置 active。
func TestRecall_Countersign_PeerExemptAndParentRevert(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-4", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-parent", "inst-4", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"", base, base.Add(3*time.Minute), "")
	recallSeedTask(t, q, "t-c1", "inst-4", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	if _, err := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq("t-c1")).
		UpdateSimple(q.WfTask.ParentID.Value("t-parent")); err != nil {
		t.Fatalf("set parent: %v", err)
	}
	recallSeedTask(t, q, "t-c2", "inst-4", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"yi", base, base.Add(2*time.Minute), "")
	if _, err := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq("t-c2")).
		UpdateSimple(q.WfTask.ParentID.Value("t-parent")); err != nil {
		t.Fatalf("set parent: %v", err)
	}
	recallSeedTask(t, q, "t-b", "inst-4", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"bing", base.Add(4*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	require.NoError(t, svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-4", ""))
	require.Nil(t, recallTaskByID(t, q, "t-c1"))
	parent := recallTaskByID(t, q, "t-parent")
	require.NotNil(t, parent)
	require.Equal(t, string(enums.TaskStatusActive), parent.Status, "父任务应回置 active")
	require.Nil(t, parent.EndedAt)
	rows, _ := q.WfTask.WithContext(ctx).Where(q.WfTask.ParentID.Eq("t-parent")).Find()
	require.Len(t, rows, 2, "父任务下应剩同侪乙的票 + 重建的甲的子任务")
	for _, r := range rows {
		if r.Status == string(enums.TaskStatusActive) {
			require.Equal(t, "jia", *r.Assignee, "重建的应是自己那票的子任务")
		}
	}
	// 同侪乙的票保留
	require.NotNil(t, recallTaskByID(t, q, "t-c2"))
}

// 下一节点已办理（存在更晚 completed 记录）→ 拒绝。
func TestRecall_Blocked_LaterCompletion(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-5", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-a", "inst-5", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	recallSeedTask(t, q, "t-b", "inst-5", "b", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"yi", base.Add(2*time.Minute), base.Add(3*time.Minute), "")
	svc, _ := newRecallSvc(q, def, nil)

	err := svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-5", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrValidation))
}

// 推进路径含自动化节点（A→serviceTask→B）→ 拒绝。
func TestRecall_Blocked_AutomationPath(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("s", "serviceTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "s"), recallConn("s", "b")})
	recallSeedInstance(t, q, "inst-6", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-a", "inst-6", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	recallSeedTask(t, q, "t-b", "inst-6", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"yi", base.Add(2*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	err := svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-6", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrValidation))
	require.Contains(t, err.Error(), "自动化")
}

// 无停泊任务（自动化在途执行窗口）→ 拒绝。
func TestRecall_Blocked_NoParked(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("s", "serviceTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "s"), recallConn("s", "b")})
	recallSeedInstance(t, q, "inst-7", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-a", "inst-7", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	svc, _ := newRecallSvc(q, def, nil)

	err := svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-7", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrValidation))
}

// 流程未开启收回开关 → 403 口径拒绝。
func TestRecall_Blocked_SwitchOff(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(false,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-8", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-a", "inst-8", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	recallSeedTask(t, q, "t-b", "inst-8", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"yi", base.Add(2*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	err := svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-8", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied))
}

// 非办理人 / 重复收回 → 无可收回记录；实例终态 → 拒绝。
func TestRecall_Blocked_NonAssignee_Duplicate_Terminal(t *testing.T) {
	q := secFixDB(t)
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-9", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-a", "inst-9", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	recallSeedTask(t, q, "t-b", "inst-9", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"yi", base.Add(2*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	// 乙没有自己已通过的记录 → E1
	err := svc.Recall(
		SetUserToCtx(context.Background(), &Actor{UserID: "yi", TenantID: "t1", UserName: "乙"}),
		Actor{UserID: "yi", TenantID: "t1"}, "inst-9", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrValidation))

	// 甲收回成功后再次收回 → E1（幂等拒绝）
	jiaCtx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	require.NoError(t, svc.Recall(jiaCtx, Actor{UserID: "jia", TenantID: "t1"}, "inst-9", ""))
	err = svc.Recall(jiaCtx, Actor{UserID: "jia", TenantID: "t1"}, "inst-9", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrValidation))

	// 非完成终态（已终止）→ 已结束，无法收回
	recallSeedInstance(t, q, "inst-10", string(enums.InstanceStatusTerminated))
	recallSeedTask(t, q, "t-a2", "inst-10", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	err = svc.Recall(jiaCtx, Actor{UserID: "jia", TenantID: "t1"}, "inst-10", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrValidation))
	require.Contains(t, err.Error(), "已结束")
}

// 路由网关与抄送节点可穿越：A→switch→cc→B 不阻断收回。
func TestRecall_PathTransparentThroughRoutersAndCC(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("sw", "switch"), recallNode("cc", "ccTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "sw"), recallConn("sw", "cc"), recallConn("cc", "b")})
	recallSeedInstance(t, q, "inst-cc", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-a", "inst-cc", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	recallSeedTask(t, q, "t-b", "inst-cc", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"yi", base.Add(2*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	require.NoError(t, svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-cc", ""))
}

// 未知节点类型 fail-closed 拒绝。
func TestRecall_Blocked_UnknownNodeType(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("x", "mysteryNode"), recallNode("b", "userTask")},
		[]string{recallConn("a", "x"), recallConn("x", "b")})
	recallSeedInstance(t, q, "inst-unk", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-a", "inst-unk", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	recallSeedTask(t, q, "t-b", "inst-unk", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"yi", base.Add(2*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	err := svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-unk", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrValidation))
}

// 重建任务沿用原任务属性字段；归档口径：本人原票 end_reason=recalled，前沿任务带审批人收回前缀。
func TestRecall_RebuildCarriesFields_AndArchiveReasons(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-fld", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	due := base.Add(48 * time.Hour)
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:                "t-a",
		ProcessInstanceID: secFixStrPtr("inst-fld"),
		ProcessID:         "proc-1",
		TaskDefKey:        "a",
		Name:              "甲节点",
		TaskType:          constants.TaskTypeUserTask,
		Status:            string(enums.TaskStatusCompleted),
		Assignee:          secFixStrPtr("jia"),
		DueDate:           &due,
		SequenceOrder:     7,
		ApprovalType:      string(enums.ApprovalTypeSingle),
		FormKey:           secFixStrPtr("frm-1"),
		Description:       secFixStrPtr("节点说明"),
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         base,
		EndedAt:           func() *time.Time { t2 := base.Add(time.Minute); return &t2 }(),
		EndReason:         secFixStrPtr(string(enums.ApprovalResultApproved)),
	}))
	recallSeedTask(t, q, "t-b", "inst-fld", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"yi", base.Add(2*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	require.NoError(t, svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-fld", ""))

	rows, _ := q.WfTask.WithContext(ctx).Where(q.WfTask.ProcessInstanceID.Eq("inst-fld")).Find()
	require.Len(t, rows, 1)
	nt := rows[0]
	require.NotNil(t, nt.FormKey)
	require.Equal(t, "frm-1", *nt.FormKey)
	require.NotNil(t, nt.DueDate)
	require.True(t, nt.DueDate.Equal(due), "到期时间应沿用原任务")
	require.EqualValues(t, 7, nt.SequenceOrder)
	require.Equal(t, string(enums.ApprovalTypeSingle), nt.ApprovalType)
	require.NotNil(t, nt.Description)
	require.Equal(t, "节点说明", *nt.Description)

	hiA, err := q.WfHiTask.WithContext(ctx).Where(q.WfHiTask.ID.Eq("t-a")).First()
	require.NoError(t, err)
	require.Equal(t, "recalled", *hiA.EndReason)
	hiB, err := q.WfHiTask.WithContext(ctx).Where(q.WfHiTask.ID.Eq("t-b")).First()
	require.NoError(t, err)
	require.Contains(t, *hiB.EndReason, "审批人收回")
}

// 候选池待认领也是合法停泊：下一节点 pending 任务不阻断收回。
func TestRecall_ParkedByCandidatePool(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-pool", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-a", "inst-pool", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	// 候选池任务：pending、无 assignee
	recallSeedTask(t, q, "t-b", "inst-pool", "b", constants.TaskTypeUserTask, string(enums.TaskStatusPending),
		"", base.Add(2*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	require.NoError(t, svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-pool", ""))
	require.Nil(t, recallTaskByID(t, q, "t-b"), "候选池待办应随收回作废")
}

// 同节点存活同伴的加签子任务不随收回作废：并行会签中途，甲收回自己的票，
// 乙的在途任务与其加签人丙的子任务都保留。
func TestRecall_SweepSparesPeerAddSignChildren(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true, []string{recallNode("a", "userTask")}, nil)
	recallSeedInstance(t, q, "inst-sign", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-parent", "inst-sign", "a", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"", base, time.Time{}, "")
	recallSeedTask(t, q, "t-c1", "inst-sign", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	if _, err := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq("t-c1")).
		UpdateSimple(q.WfTask.ParentID.Value("t-parent")); err != nil {
		t.Fatalf("set parent: %v", err)
	}
	recallSeedTask(t, q, "t-c2", "inst-sign", "a", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"yi", base, time.Time{}, "")
	if _, err := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq("t-c2")).
		UpdateSimple(q.WfTask.ParentID.Value("t-parent")); err != nil {
		t.Fatalf("set parent: %v", err)
	}
	// 乙的加签人丙：任务创建于甲完成之后（晚于 T.ended_at）
	recallSeedTask(t, q, "t-s1", "inst-sign", "a", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"bing", base.Add(5*time.Minute), time.Time{}, "")
	if _, err := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq("t-s1")).
		UpdateSimple(q.WfTask.ParentID.Value("t-c2")); err != nil {
		t.Fatalf("set parent: %v", err)
	}
	svc, _ := newRecallSvc(q, def, nil)

	require.NoError(t, svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-sign", ""))

	// 同伴乙与其加签人丙都保留
	require.NotNil(t, recallTaskByID(t, q, "t-c2"), "同伴在途任务应保留")
	require.NotNil(t, recallTaskByID(t, q, "t-s1"), "存活同伴的加签子任务不应被清扫")
	// 甲的票归档重建
	require.Nil(t, recallTaskByID(t, q, "t-c1"))
	rows, _ := q.WfTask.WithContext(ctx).Where(q.WfTask.ParentID.Eq("t-parent")).Find()
	require.Len(t, rows, 2, "父任务下应为 重建的甲票与乙票两行")
}

// 全节点类型分类表驱动：阻断集（自动化/子流程/延时，机器动作已发生）必须拒，
// 穿越集（路由/fork/join/抄送/起点，无业务副作用）必须放行。
// 未归类的未来新增节点类型落入 default 被 fail-closed 拒绝——新增类型时须
// 显式归入穿越集并在此表补行。
func TestRecall_NodeTypeClassification(t *testing.T) {
	blocking := []string{"serviceTask", "httpCall", "automation", "aiAgent", "delay", "subProcess", "startProcess"}
	transparent := []string{"switch", "jsSwitch", "msgTypeSwitch", "inclusive", "condition", "fork", "join", "ccTask", "start", "startTask"}

	newFixture := func(t *testing.T, midType string) (*query.Query, context.Context, *TaskServiceImpl, string) {
		t.Helper()
		q := secFixDB(t)
		ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
		def := recallDefinition(true,
			[]string{recallNode("a", "userTask"), recallNode("m", midType), recallNode("b", "userTask")},
			[]string{recallConn("a", "m"), recallConn("m", "b")})
		recallSeedInstance(t, q, "inst-cls", string(enums.InstanceStatusActive))
		base := time.Now().Add(-time.Hour)
		recallSeedTask(t, q, "t-a", "inst-cls", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
			"jia", base, base.Add(time.Minute), "")
		recallSeedTask(t, q, "t-b", "inst-cls", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
			"yi", base.Add(2*time.Minute), time.Time{}, "")
		svc, _ := newRecallSvc(q, def, nil)
		return q, ctx, svc, "inst-cls"
	}

	for _, typ := range blocking {
		t.Run("阻断/"+typ, func(t *testing.T) {
			q, ctx, svc, inst := newFixture(t, typ)
			err := svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, inst, "")
			require.Error(t, err, "%s 应阻断收回", typ)
			require.True(t, errors.Is(err, ErrValidation))
			require.NotNil(t, recallTaskByID(t, q, "t-a"), "阻断时不得动数据")
		})
	}
	for _, typ := range transparent {
		t.Run("穿越/"+typ, func(t *testing.T) {
			_, ctx, svc, inst := newFixture(t, typ)
			require.NoError(t, svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, inst, ""), "%s 应穿越", typ)
		})
	}
}

// ---- 终态收回（已完成实例窗口期内发起人/管理员整单重开） ----

// recallSeedHiInstance 造已完成归档实例（终端收回的前置形态：活表无行）。
func recallSeedHiInstance(t *testing.T, q *query.Query, id, status string, endedAt time.Time) {
	t.Helper()
	require.NoError(t, q.WfHiInstance.Create(&model.WfHiInstance{
		ID:          id,
		ProcessID:   "proc-1",
		Name:        "recall-term",
		Status:      status,
		TenantID:    "t1",
		CreatedBy:   "system",
		StartUserID: "starter",
		CreatedAt:   endedAt.Add(-time.Hour),
		EndedAt:     &endedAt,
	}))
}

// recallSeedHiTask 造归档任务行（终态收回的末节点判定与投票人来源）。
func recallSeedHiTask(t *testing.T, q *query.Query, id, instID, defKey, assignee string, endedAt time.Time) {
	t.Helper()
	row := &model.WfHiTask{
		ID:                id,
		ProcessInstanceID: secFixStrPtr(instID),
		ProcessID:         "proc-1",
		TaskDefKey:        &defKey,
		TaskType:          constants.TaskTypeUserTask,
		Status:            string(enums.TaskStatusCompleted),
		TenantID:          "t1",
		CreatedAt:         endedAt.Add(-time.Minute),
		EndedAt:           &endedAt,
	}
	if assignee != "" {
		row.Assignee = secFixStrPtr(assignee)
	}
	require.NoError(t, q.WfHiTask.Create(row))
}

// 发起人在窗口期内收回已完成实例：实例复活为运行中、归档行移除。
func TestRecallCompleted_ByStarterReopensInstance(t *testing.T) {
	q := secFixDB(t)
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedHiInstance(t, q, "inst-term", string(enums.InstanceStatusCompleted), time.Now().Add(-24*time.Hour))
	recallSeedHiTask(t, q, "ht-a", "inst-term", "a", "jia", time.Now().Add(-23*time.Hour))
	recallSeedHiTask(t, q, "ht-b", "inst-term", "b", "yi", time.Now().Add(-22*time.Hour))
	svc, eng := newRecallSvc(q, def, nil)

	require.NoError(t, svc.Recall(
		SetUserToCtx(context.Background(), &Actor{UserID: "starter", TenantID: "t1", UserName: "发起人"}),
		Actor{UserID: "starter", TenantID: "t1"}, "inst-term", "批错了，整单重开"))

	revived, err := q.WfInstance.WithContext(context.Background()).Where(q.WfInstance.ID.Eq("inst-term")).First()
	require.NoError(t, err, "实例应回插运行表")
	require.Equal(t, string(enums.InstanceStatusActive), revived.Status)
	require.Nil(t, revived.EndedAt)
	hiGone, err := q.WfHiInstance.WithContext(context.Background()).Where(q.WfHiInstance.ID.Eq("inst-term")).Count()
	require.NoError(t, err)
	require.EqualValues(t, 0, hiGone, "归档行应移除")
	require.Equal(t, "b", eng.internal.execNextNode, "应重入末节点 b")
}

// 终态收回复活实例剥离上一轮的引擎保留标记，收回次数照常累加。
func TestRecallCompleted_InstanceVarsStripReservedMarks(t *testing.T) {
	q := secFixDB(t)
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	endedAt := time.Now().Add(-24 * time.Hour)
	require.NoError(t, q.WfHiInstance.Create(&model.WfHiInstance{
		ID:          "inst-term-marks",
		ProcessID:   "proc-1",
		Name:        "recall-term-marks",
		Status:      string(enums.InstanceStatusCompleted),
		TenantID:    "t1",
		CreatedBy:   "system",
		StartUserID: "starter",
		CreatedAt:   endedAt.Add(-time.Hour),
		EndedAt:     &endedAt,
		Variables:   secFixStrPtr(`{"amount":100,"proxy_operator":"admin","fallback_policy":"auto_approve","fallback_from":"role:r1","fallback_reason":"审批人为空，自动通过","fallback_time":"2026-09-24 10:00:00"}`),
	}))
	recallSeedHiTask(t, q, "htm-a", "inst-term-marks", "a", "jia", time.Now().Add(-23*time.Hour))
	recallSeedHiTask(t, q, "htm-b", "inst-term-marks", "b", "yi", time.Now().Add(-22*time.Hour))
	svc, _ := newRecallSvc(q, def, nil)

	require.NoError(t, svc.Recall(
		SetUserToCtx(context.Background(), &Actor{UserID: "starter", TenantID: "t1", UserName: "发起人"}),
		Actor{UserID: "starter", TenantID: "t1"}, "inst-term-marks", "批错了，整单重开"))

	revived, err := q.WfInstance.WithContext(context.Background()).Where(q.WfInstance.ID.Eq("inst-term-marks")).First()
	require.NoError(t, err, "实例应回插运行表")
	require.NotNil(t, revived.Variables)
	var vars map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*revived.Variables), &vars))
	require.NotContains(t, vars, constants.VarsProxyOperator)
	require.NotContains(t, vars, constants.VarsProxyTime)
	require.NotContains(t, vars, constants.VarsFallbackPolicy)
	require.NotContains(t, vars, constants.VarsFallbackFrom)
	require.NotContains(t, vars, constants.VarsFallbackReason)
	require.NotContains(t, vars, constants.VarsFallbackTime)
	require.Equal(t, float64(100), vars["amount"], "业务变量保留")
	require.Equal(t, float64(1), vars[constants.VarsRecallCount], "收回次数照常累加")
}

func TestRecallCompleted_Guards(t *testing.T) {
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	defOff := recallDefinition(false,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})

	t.Run("非发起人且非管理员", func(t *testing.T) {
		q := secFixDB(t)
		recallSeedHiInstance(t, q, "inst-g1", string(enums.InstanceStatusCompleted), time.Now().Add(-time.Hour))
		recallSeedHiTask(t, q, "ht-g1", "inst-g1", "b", "yi", time.Now().Add(-30*time.Minute))
		svc, _ := newRecallSvc(q, def, nil)
		err := svc.Recall(
			SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"}),
			Actor{UserID: "jia", TenantID: "t1"}, "inst-g1", "")
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPermissionDenied))
		require.Contains(t, err.Error(), "发起人或管理员")
	})

	t.Run("超过窗口", func(t *testing.T) {
		q := secFixDB(t)
		recallSeedHiInstance(t, q, "inst-g2", string(enums.InstanceStatusCompleted), time.Now().Add(-8*24*time.Hour))
		recallSeedHiTask(t, q, "ht-g2", "inst-g2", "b", "yi", time.Now().Add(-8*24*time.Hour))
		svc, _ := newRecallSvc(q, def, nil)
		err := svc.Recall(
			SetUserToCtx(context.Background(), &Actor{UserID: "starter", TenantID: "t1", UserName: "发起人"}),
			Actor{UserID: "starter", TenantID: "t1"}, "inst-g2", "")
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrValidation))
		require.Contains(t, err.Error(), "窗口")
	})

	t.Run("流程未开启", func(t *testing.T) {
		q := secFixDB(t)
		recallSeedHiInstance(t, q, "inst-g3", string(enums.InstanceStatusCompleted), time.Now().Add(-time.Hour))
		recallSeedHiTask(t, q, "ht-g3", "inst-g3", "b", "yi", time.Now().Add(-30*time.Minute))
		svc, _ := newRecallSvc(q, defOff, nil)
		err := svc.Recall(
			SetUserToCtx(context.Background(), &Actor{UserID: "starter", TenantID: "t1", UserName: "发起人"}),
			Actor{UserID: "starter", TenantID: "t1"}, "inst-g3", "")
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPermissionDenied))
	})

	t.Run("管理员可收", func(t *testing.T) {
		q := secFixDB(t)
		recallSeedHiInstance(t, q, "inst-g4", string(enums.InstanceStatusCompleted), time.Now().Add(-time.Hour))
		recallSeedHiTask(t, q, "ht-g4", "inst-g4", "b", "yi", time.Now().Add(-30*time.Minute))
		svc, eng := newRecallSvc(q, def, nil)
		require.NoError(t, svc.Recall(
			SetUserToCtx(context.Background(), &Actor{UserID: "admin", TenantID: "t1", UserName: "管理员", WorkflowAdmin: true}),
			Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}, "inst-g4", ""))
		revived, err := q.WfInstance.WithContext(context.Background()).Where(q.WfInstance.ID.Eq("inst-g4")).First()
		require.NoError(t, err)
		require.Equal(t, string(enums.InstanceStatusActive), revived.Status)
		require.Equal(t, "b", eng.internal.execNextNode, "管理员收回同样重入末节点")
	})
}

// recallWindowDays 配置解析：流程级覆盖生效，非法值回落默认 7 天。
func TestRecallWindowDays(t *testing.T) {
	cases := []struct {
		ap   map[string]interface{}
		want int
	}{
		{nil, 7},
		{map[string]interface{}{"recallWindowDays": float64(30)}, 30},
		{map[string]interface{}{"recallWindowDays": float64(1)}, 1},
		{map[string]interface{}{"recallWindowDays": float64(0)}, 7},
		{map[string]interface{}{"recallWindowDays": float64(-1)}, 7},
		{map[string]interface{}{"recallWindowDays": float64(400)}, 7},
		{map[string]interface{}{"recallWindowDays": "7"}, 7},
	}
	for _, c := range cases {
		require.Equal(t, c.want, recallWindowDays(c.ap), "ap=%v", c.ap)
	}
}

// 开关缺省=开启（opt-out 语义）：流程未配置 actionPermissions 时收回可用。
func TestRecall_DefaultOnWithoutSwitch(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	// 无 actionPermissions 键的流程定义
	def := fmt.Sprintf(`{"ruleChain":{"additionalInfo":{"processType":"main"}},"metadata":{"nodes":[%s,%s],"connections":[%s]}}`,
		recallNode("a", "userTask"), recallNode("b", "userTask"), recallConn("a", "b"))
	recallSeedInstance(t, q, "inst-dft", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-a", "inst-dft", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	recallSeedTask(t, q, "t-b", "inst-dft", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"yi", base.Add(2*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	require.NoError(t, svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-dft", ""))
}

// 代审出的票不可收回：错误文案点名管理员代审；同会签轮次同伴的未代审票不受影响。
func TestRecall_BlockedByProxyMark(t *testing.T) {
	q := secFixDB(t)
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-px9", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	// 单人票带代审标记
	recallSeedTask(t, q, "t-px9", "inst-px9", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), `{"proxy_operator":"admin","approved":true}`)
	recallSeedTask(t, q, "t-px9b", "inst-px9", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"yi", base.Add(2*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	err := svc.Recall(
		SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"}),
		Actor{UserID: "jia", TenantID: "t1"}, "inst-px9", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrValidation))
	require.Contains(t, err.Error(), "管理员代审")

	// 会签同伴场景：自己的票无代审标记，同轮次他人带标记的更晚投票不阻断自己收回
	recallSeedInstance(t, q, "inst-px10", string(enums.InstanceStatusActive))
	recallSeedTask(t, q, "t-p10-parent", "inst-px10", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"", base, base.Add(3*time.Minute), "")
	recallSeedTask(t, q, "t-p10-jia", "inst-px10", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"jia", base, base.Add(time.Minute), "")
	if _, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-p10-jia")).
		UpdateSimple(q.WfTask.ParentID.Value("t-p10-parent")); err != nil {
		t.Fatalf("set parent: %v", err)
	}
	recallSeedTask(t, q, "t-p10-yi", "inst-px10", "a", constants.TaskTypeUserTask, string(enums.TaskStatusCompleted),
		"yi", base.Add(2*time.Minute), base.Add(2*time.Minute), `{"proxy_operator":"admin","approved":true}`)
	if _, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-p10-yi")).
		UpdateSimple(q.WfTask.ParentID.Value("t-p10-parent")); err != nil {
		t.Fatalf("set parent: %v", err)
	}
	recallSeedTask(t, q, "t-p10-b", "inst-px10", "b", constants.TaskTypeUserTask, string(enums.TaskStatusActive),
		"bing", base.Add(4*time.Minute), time.Time{}, "")

	require.NoError(t, svc.Recall(
		SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"}),
		Actor{UserID: "jia", TenantID: "t1"}, "inst-px10", ""), "同伴的代审票不应阻断自己收回")
	require.Nil(t, recallTaskByID(t, q, "t-p10-b"), "收回后前沿待办终止")
}

// 回归：任务清单曾按默认 pageSize=10 截断，本人票落在第 11 行起时收回被误拒、
// 守卫看不到页外的更晚记录而误放行。翻页取全量后两种失真都应消失。
func TestRecall_GuardSeesTasksBeyondDefaultPage(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-page", string(enums.InstanceStatusActive))
	base := time.Now().Add(-2 * time.Hour)
	// 11 条更早的他人完成记录占满默认第一页（created_at 升序），本人票与前沿
	// 待办被排到页外
	for i := 0; i < 11; i++ {
		at := base.Add(time.Duration(i) * time.Minute)
		recallSeedTask(t, q, fmt.Sprintf("t-fill-%d", i), "inst-page", "a", constants.TaskTypeUserTask,
			string(enums.TaskStatusCompleted), fmt.Sprintf("u%d", i), at, at, "")
	}
	recallSeedTask(t, q, "t-jia", "inst-page", "a", constants.TaskTypeUserTask,
		string(enums.TaskStatusCompleted), "jia", base.Add(40*time.Minute), base.Add(41*time.Minute), "")
	recallSeedTask(t, q, "t-front", "inst-page", "b", constants.TaskTypeUserTask,
		string(enums.TaskStatusActive), "bing", base.Add(42*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	require.NoError(t, svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-page", ""),
		"票在默认分页之外也应可收回")
	require.Nil(t, recallTaskByID(t, q, "t-front"), "页外的前沿待办必须被终止，不能残留幽灵待办")
	rebuilt, _ := q.WfTask.WithContext(ctx).Where(q.WfTask.ProcessInstanceID.Eq("inst-page")).
		Where(q.WfTask.TaskDefKey.Eq("a")).Where(q.WfTask.Status.Eq(string(enums.TaskStatusActive))).
		Where(q.WfTask.Assignee.Eq("jia")).First()
	require.NotNil(t, rebuilt, "应重建收回人的待审任务")
}

// 回归：系统自动完成的票（emptyApproverPolicy/selfApproval 兜底自动通过，
// UpdatedBy=system）不是人的决定，收回会把无人参与的通过变成收回人的待办。
func TestRecall_Blocked_SystemAutoCompleted(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-sys", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	recallSeedTask(t, q, "t-sys", "inst-sys", "a", constants.TaskTypeUserTask,
		string(enums.TaskStatusCompleted), "jia", base, base.Add(time.Minute), "")
	if _, err := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq("t-sys")).
		UpdateSimple(q.WfTask.UpdatedBy.Value("system")); err != nil {
		t.Fatalf("set updated_by: %v", err)
	}
	recallSeedTask(t, q, "t-sys-b", "inst-sys", "b", constants.TaskTypeUserTask,
		string(enums.TaskStatusActive), "bing", base.Add(2*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	err := svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-sys", "")
	require.ErrorContains(t, err, "系统自动完成")
}

// 回归：阈值达成后被终止的会签同侪要随收回复活，父任务里第一轮合并的
// approved 也要剥掉，否则第二轮判定在旧结果之上覆盖合并。
func TestRecall_RevivesThresholdTerminatedSiblings(t *testing.T) {
	q := secFixDB(t)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "jia", TenantID: "t1", UserName: "甲"})
	def := recallDefinition(true,
		[]string{recallNode("a", "userTask"), recallNode("b", "userTask")},
		[]string{recallConn("a", "b")})
	recallSeedInstance(t, q, "inst-revive", string(enums.InstanceStatusActive))
	base := time.Now().Add(-time.Hour)
	parentVars := `{"approved":true,"biz":"keep"}`
	recallSeedTask(t, q, "t-r-parent", "inst-revive", "a", constants.TaskTypeUserTask,
		string(enums.TaskStatusCompleted), "", base, base.Add(3*time.Minute), parentVars)
	recallSeedTask(t, q, "t-r-jia", "inst-revive", "a", constants.TaskTypeUserTask,
		string(enums.TaskStatusCompleted), "jia", base, base.Add(time.Minute), "")
	recallSeedTask(t, q, "t-r-yi", "inst-revive", "a", constants.TaskTypeUserTask,
		string(enums.TaskStatusCompleted), "yi", base, base.Add(2*time.Minute), "")
	threshold := countersignThresholdEndReason
	for _, id := range []string{"t-r-jia", "t-r-yi"} {
		if _, err := q.WfTask.WithContext(ctx).Where(q.WfTask.ID.Eq(id)).
			UpdateSimple(q.WfTask.ParentID.Value("t-r-parent")); err != nil {
			t.Fatalf("set parent: %v", err)
		}
	}
	// 丙的票被阈值达成终止：挂在 wf_task 里等待复活
	terminated := &model.WfTask{
		ID: "t-r-bing", ProcessInstanceID: secFixStrPtr("inst-revive"), ProcessID: "proc-1",
		TaskDefKey: "a", Name: "a", TaskType: constants.TaskTypeUserTask,
		Status: string(enums.TaskStatusTerminated), Assignee: secFixStrPtr("bing"),
		TenantID: "t1", CreatedBy: "system", CreatedAt: base,
		EndedAt: secFixTimePtr(base.Add(2 * time.Minute)), EndReason: &threshold,
		ParentID: secFixStrPtr("t-r-parent"),
	}
	require.NoError(t, q.WfTask.Create(terminated))
	recallSeedTask(t, q, "t-r-b", "inst-revive", "b", constants.TaskTypeUserTask,
		string(enums.TaskStatusActive), "bing", base.Add(4*time.Minute), time.Time{}, "")
	svc, _ := newRecallSvc(q, def, nil)

	require.NoError(t, svc.Recall(ctx, Actor{UserID: "jia", TenantID: "t1"}, "inst-revive", ""))
	parent := recallTaskByID(t, q, "t-r-parent")
	require.NotNil(t, parent)
	require.Equal(t, string(enums.TaskStatusActive), parent.Status)
	require.NotNil(t, parent.Variables)
	require.NotContains(t, *parent.Variables, "approved", "第一轮合并的审批结果要剥掉")
	require.Contains(t, *parent.Variables, "biz", "业务变量保留")
	revived := recallTaskByID(t, q, "t-r-bing")
	require.NotNil(t, revived, "被阈值终止的同侪要复活")
	require.Equal(t, string(enums.TaskStatusActive), revived.Status)
	require.Nil(t, revived.EndedAt)
	require.Nil(t, revived.EndReason)
}

// 单元：收回计数与父任务变量剥离的边界行为。
func TestRecallVarHelpers(t *testing.T) {
	vars := `{"a":1}`
	bumped := bumpRecallCount(secFixStrPtr(vars))
	require.Contains(t, *bumped, fmt.Sprintf(`"%s":1`, constants.VarsRecallCount))
	for i := 0; i < 25; i++ {
		bumped = bumpRecallCount(bumped)
	}
	require.True(t, recallCountExhausted(bumped))
	require.False(t, recallCountExhausted(secFixStrPtr(vars)))
	require.True(t, recallCountExhausted(secFixStrPtr("not-json")), "损坏变量按已达上限处理")

	stripped := stripCountersignMergedKeys(secFixStrPtr(`{"approved":false,"k":"v"}`))
	require.Contains(t, *stripped, `"k":"v"`)
	require.NotContains(t, *stripped, "approved")
	require.Equal(t, "not-json", *stripCountersignMergedKeys(secFixStrPtr("not-json")),
		"解析失败原样返回，不清空父任务变量")
	require.Nil(t, stripCountersignMergedKeys(nil))
}

func secFixTimePtr(t time.Time) *time.Time { return &t }

// 收回守卫穿越 switch 分支出边的回归。
func branchChain(nodes [][2]string, conns [][3]string) *types.RuleChain {
	chain := &types.RuleChain{Metadata: types.RuleMetadata{}}
	for _, n := range nodes {
		chain.Metadata.Nodes = append(chain.Metadata.Nodes, &types.RuleNode{Id: n[0], Type: n[1]})
	}
	for _, c := range conns {
		chain.Metadata.Connections = append(chain.Metadata.Connections, types.NodeConnection{FromId: c[0], ToId: c[2], Type: c[1]})
	}
	return chain
}

// T → switch → serviceTask → userTask：分支后藏自动化节点，拒绝。
func TestCheckRecallPath_AutomationBehindSwitchRejected(t *testing.T) {
	chain := branchChain(
		[][2]string{{"t", constants.NodeTypeUserTask}, {"sw", constants.NodeTypeSwitch}, {"svc", "httpCall"}, {"b", constants.NodeTypeUserTask}},
		[][3]string{{"t", types.Success, "sw"}, {"sw", "node_svc", "svc"}, {"svc", types.Success, "b"}},
	)
	err := checkRecallPath(chain, "t")
	require.Error(t, err, "分支后的自动化节点必须被看见并拒绝收回")
}

// 两条分支都是人工节点，放行。
func TestCheckRecallPath_UserTasksBehindSwitchAllowed(t *testing.T) {
	chain := branchChain(
		[][2]string{{"t", constants.NodeTypeUserTask}, {"sw", constants.NodeTypeSwitch}, {"b", constants.NodeTypeUserTask}, {"c", constants.NodeTypeUserTask}},
		[][3]string{{"t", types.Success, "sw"}, {"sw", "node_b", "b"}, {"sw", "Default", "c"}},
	)
	require.NoError(t, checkRecallPath(chain, "t"))
}

// 任一分支上有自动化节点即拒绝。
func TestCheckRecallPath_AutomationOnAnyBranchRejected(t *testing.T) {
	chain := branchChain(
		[][2]string{{"t", constants.NodeTypeUserTask}, {"sw", constants.NodeTypeSwitch}, {"b", constants.NodeTypeUserTask}, {"svc", "serviceTask"}},
		[][3]string{{"t", types.Success, "sw"}, {"sw", "node_b", "b"}, {"sw", "Default", "svc"}},
	)
	err := checkRecallPath(chain, "t")
	require.Error(t, err, "任一分支走向上有自动化节点都应拒绝")
}

// ---- 保留键解析 fail-closed ----
// 变量损坏时无法证明「未代审/未达上限」，按带标记/已达上限处理；空变量不受影响。
func TestRecallReservedVarGuardsFailClosed(t *testing.T) {
	corrupt := "{not-json"
	marked := `{"proxy_operator":"admin"}`
	unmarked := `{"amount":100}`
	counted := `{"_recallCount":1}`
	require.True(t, hasProxyMark(&model.WfTask{Variables: &corrupt}), "损坏变量按带代审标记处理，收回被拦")
	require.True(t, hasProxyMark(&model.WfTask{Variables: &marked}))
	require.False(t, hasProxyMark(&model.WfTask{Variables: &unmarked}))
	require.False(t, hasProxyMark(&model.WfTask{}))
	require.False(t, hasProxyMark(nil))

	require.True(t, recallCountExhausted(&corrupt), "损坏变量按已达上限处理，收回被拦")
	require.False(t, recallCountExhausted(&counted))
	require.False(t, recallCountExhausted(nil))
	require.False(t, recallCountExhausted(secFixStrPtr("{}")))
}
