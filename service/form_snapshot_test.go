package service

// 表单历史快照：历史任务查询透出变量快照（剥引擎内部键）、活表回落与
// 实例详情时间轴的行级 formModified 标记。

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

func newSnapshotHistoryService(t *testing.T) *HistoryServiceImpl {
	t.Helper()
	q := secFixDB(t)
	return &HistoryServiceImpl{
		hiTaskDAO: dao.NewHiTaskDAOWithQuery(q),
		taskDAO:   dao.NewTaskDAOWithQuery(q),
	}
}

func seedHiTask(t *testing.T, s *HistoryServiceImpl, id string, variables string) {
	t.Helper()
	instID := "inst-snap-" + id
	require.NoError(t, s.hiTaskDAO.Create(context.Background(), &model.WfHiTask{
		ID:                id,
		ProcessInstanceID: secFixStrPtr(instID),
		TaskDefKey:        secFixStrPtr("node1"),
		Name:              "部门经理审批",
		TaskType:          constants.TaskTypeUserTask,
		Status:            string(enums.TaskStatusCompleted),
		Assignee:          secFixStrPtr("u1"),
		Variables:         secFixStrPtr(variables),
		TenantID:          "t1",
		CreatedBy:         "u1",
		CreatedAt:         time.Now(),
	}))
}

// 历史任务查询带出变量快照，且引擎保留标记键与下划线内部键不出现在快照里。
func TestGetHistoricTaskInstance_VariablesStripsInternalKeys(t *testing.T) {
	s := newSnapshotHistoryService(t)
	seedHiTask(t, s, "task-hi-1", `{"amount":3000,"reason":"出差报销","approved":true,"comment":"ok","proxy_operator":"admin","reassign_from":"u2","_sequentialAssignees":["a","b"],"_ai":"x"}`)

	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "u1", TenantID: "t1"})
	hi, err := s.GetHistoricTaskInstance(ctx, Actor{UserID: "u1", TenantID: "t1"}, "task-hi-1")
	require.NoError(t, err)
	require.NotNil(t, hi.Variables)
	require.Equal(t, float64(3000), hi.Variables["amount"])
	require.Equal(t, "出差报销", hi.Variables["reason"])
	for _, k := range []string{"approved", "comment", "proxy_operator", "reassign_from", "_sequentialAssignees", "_ai"} {
		_, ok := hi.Variables[k]
		require.False(t, ok, "内部键 %s 不应透出", k)
	}
}

// 归档发生在实例终态：运行中实例的已完成任务只在活表，历史查询须回落；
// 在途任务不属于历史语义，回落后仍 404。
func TestGetHistoricTaskInstance_ActiveTableFallback(t *testing.T) {
	s := newSnapshotHistoryService(t)
	q := s.taskDAO.Underlying()
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:                "task-live-done",
		ProcessInstanceID: secFixStrPtr("inst-live"),
		TaskDefKey:        "node1",
		Name:              "部门经理审批",
		TaskType:          constants.TaskTypeUserTask,
		Status:            string(enums.TaskStatusCompleted),
		Assignee:          secFixStrPtr("u1"),
		Variables:         secFixStrPtr(`{"amount":500}`),
		TenantID:          "t1",
		CreatedBy:         "u1",
		CreatedAt:         time.Now(),
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:                "task-live-active",
		ProcessInstanceID: secFixStrPtr("inst-live"),
		TaskDefKey:        "node2",
		Name:              "总监审批",
		TaskType:          constants.TaskTypeUserTask,
		Status:            string(enums.TaskStatusActive),
		Assignee:          secFixStrPtr("u2"),
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         time.Now(),
	}))

	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "u1", TenantID: "t1"})
	hi, err := s.GetHistoricTaskInstance(ctx, Actor{UserID: "u1", TenantID: "t1"}, "task-live-done")
	require.NoError(t, err)
	require.Equal(t, float64(500), hi.Variables["amount"])

	_, err = s.GetHistoricTaskInstance(ctx, Actor{UserID: "u1", TenantID: "t1"}, "task-live-active")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrNotFound))
}

// 跨租户读取按权限拒绝（fail-closed），不得因回落路径绕过租户校验。
func TestGetHistoricTaskInstance_TenantMismatch(t *testing.T) {
	s := newSnapshotHistoryService(t)
	seedHiTask(t, s, "task-hi-tenant", `{"amount":1}`)

	other := SetUserToCtx(context.Background(), &Actor{UserID: "u9", TenantID: "t2"})
	_, err := s.GetHistoricTaskInstance(other, Actor{UserID: "u9", TenantID: "t2"}, "task-hi-tenant")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied))
}

// 列表缺省不带变量；includeTaskLocalVariables=true 才填充。
func TestGetHistoricTaskInstances_VariableFlag(t *testing.T) {
	s := newSnapshotHistoryService(t)
	seedHiTask(t, s, "task-hi-list", `{"amount":1}`)

	actor := Actor{UserID: "u1", TenantID: "t1"}
	off := false
	list, _, err := s.GetHistoricTaskInstances(context.Background(), actor, &HistoricTaskInstanceQuery{
		IncludeTaskLocalVariables: &off, PageSize: 10,
	})
	require.NoError(t, err)
	require.NotEmpty(t, list)
	require.Nil(t, list[0].Variables)

	on := true
	list, _, err = s.GetHistoricTaskInstances(context.Background(), actor, &HistoricTaskInstanceQuery{
		IncludeTaskLocalVariables: &on, PageSize: 10,
	})
	require.NoError(t, err)
	require.NotEmpty(t, list)
	require.NotNil(t, list[0].Variables)
	require.Equal(t, float64(1), list[0].Variables["amount"])
}

func snapTask(id string, endedAt time.Time, variables string, created time.Time) *model.WfTask {
	return &model.WfTask{
		ID:                id,
		ProcessInstanceID: secFixStrPtr("inst-fm"),
		TaskType:          constants.TaskTypeUserTask,
		Status:            string(enums.TaskStatusCompleted),
		Variables:         secFixStrPtr(variables),
		EndedAt:           &endedAt,
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         created,
	}
}

// 行级标记边界：链首不标、无差异不标、有差异标数量、内部键变化不算表单修改、
// 超限快照退出比对链、轨迹行不进链、标记递归下发给加签子行。
// 发起行（startTask）快照冻结发起初版表单，作为链首参与比对——第一个审批
// 节点的修改以它为前份。
func TestMarkFormModified_Boundaries(t *testing.T) {
	base := time.Date(2026, 10, 23, 9, 0, 0, 0, time.Local)
	startRow := snapTask("t-start", base.Add(5*time.Minute), `{"amount":2000,"reason":"报销"}`, base)
	startRow.TaskType = constants.TaskTypeStartTask
	tasks := []*model.WfTask{
		startRow,
		snapTask("t1st", base.Add(10*time.Minute), `{"amount":2000,"reason":"报销"}`, base),
		snapTask("t2nd", base.Add(20*time.Minute), `{"amount":2000,"reason":"报销"}`, base.Add(time.Minute)),
		snapTask("t3rd", base.Add(30*time.Minute), `{"amount":3000,"reason":"报销","proxy_operator":"admin"}`, base.Add(2*time.Minute)),
		snapTask("t4th", base.Add(40*time.Minute), `{"amount":3000,"reason":"报销","city":"北京"}`, base.Add(3*time.Minute)),
		// 转办轨迹行：变量只有轨迹键，不进比对链
		{
			ID: "t-trace", ProcessInstanceID: secFixStrPtr("inst-fm"),
			TaskType: constants.TaskTypeForward, Status: string(enums.TaskStatusCompleted),
			Variables: secFixStrPtr(`{"forward_by":"u1","forward_to":"u2"}`),
			EndedAt:   secFixTimePtr(base.Add(35 * time.Minute)), TenantID: "t1", CreatedBy: "u1", CreatedAt: base,
		},
		// 超限快照（>256KB）退出比对链，不标记也不参与 diff
		snapTask("t-huge", base.Add(50*time.Minute), `{"blob":"`+strings.Repeat("x", 300*1024)+`"}`, base.Add(4*time.Minute)),
	}
	approvals := []dto.ExecutionInfo{{TaskID: "t-start"}, {TaskID: "t1st"}, {TaskID: "t2nd"}, {TaskID: "t3rd"}, {TaskID: "t4th"},
		{TaskID: "t-trace"}, {TaskID: "t-huge", SubExecutions: []dto.ExecutionInfo{{TaskID: "t-sub"}}}}

	markFormModified(tasks, approvals)

	find := func(id string) *dto.ExecutionInfo {
		for i := range approvals {
			if approvals[i].TaskID == id {
				return &approvals[i]
			}
		}
		return nil
	}
	require.False(t, find("t-start").FormModified, "发起行是链首，无前份可比，不标记")
	require.Empty(t, find("t-start").FormPrevTaskID, "链首无前份")
	require.False(t, find("t1st").FormModified, "与发起初版快照无差异，不标记")
	require.Equal(t, "t-start", find("t1st").FormPrevTaskID)
	require.False(t, find("t2nd").FormModified, "与上一份快照无差异，不标记")
	require.Equal(t, "t1st", find("t2nd").FormPrevTaskID)
	require.True(t, find("t3rd").FormModified, "amount 变化应标记")
	require.Equal(t, 1, find("t3rd").FormChangedCount, "proxy_operator 是内部键不算差异")
	require.Equal(t, "t2nd", find("t3rd").FormPrevTaskID)
	require.True(t, find("t4th").FormModified, "新增 city 字段应标记")
	require.Equal(t, 1, find("t4th").FormChangedCount)
	require.Equal(t, "t3rd", find("t4th").FormPrevTaskID)
	require.False(t, find("t-trace").FormModified, "轨迹行不进比对链")
	require.Empty(t, find("t-trace").FormPrevTaskID)
	require.False(t, find("t-huge").FormModified, "超限快照跳过标记")
	require.Empty(t, find("t-huge").FormPrevTaskID)
}

// 第一审批节点直接改了发起初版的值：以发起行快照为前份，可检测。
func TestMarkFormModified_FirstApproverEditDetected(t *testing.T) {
	base := time.Date(2026, 10, 23, 9, 0, 0, 0, time.Local)
	startRow := snapTask("t-start", base, `{"amount":1000}`, base)
	startRow.TaskType = constants.TaskTypeStartTask
	tasks := []*model.WfTask{startRow, snapTask("t-approve", base.Add(time.Minute), `{"amount":3000}`, base)}
	approvals := []dto.ExecutionInfo{{TaskID: "t-start"}, {TaskID: "t-approve"}}
	markFormModified(tasks, approvals)
	require.False(t, approvals[0].FormModified)
	require.True(t, approvals[1].FormModified)
	require.Equal(t, 1, approvals[1].FormChangedCount)
	require.Equal(t, "t-start", approvals[1].FormPrevTaskID)
}

// 子任务的标记经 SubExecutions 子树下发（加签行在子树里，不在顶层列表）。
func TestMarkFormModified_ReachesSubExecutions(t *testing.T) {
	base := time.Date(2026, 10, 23, 9, 0, 0, 0, time.Local)
	tasks := []*model.WfTask{
		snapTask("parent", base.Add(10*time.Minute), `{"amount":100}`, base),
		snapTask("childA", base.Add(20*time.Minute), `{"amount":999}`, base.Add(time.Minute)),
	}
	approvals := []dto.ExecutionInfo{{
		TaskID: "parent",
		SubExecutions: []dto.ExecutionInfo{
			{TaskID: "childA"},
		},
	}}
	markFormModified(tasks, approvals)
	require.False(t, approvals[0].FormModified)
	require.True(t, approvals[0].SubExecutions[0].FormModified)
	require.Equal(t, 1, approvals[0].SubExecutions[0].FormChangedCount)
}
