// This file covers the re-entry variable snapshot fixes: countersign merge
// three-way semantics, the re-entry snapshot strip, and detail-exit form
// permission normalization (approver + CC viewer).

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/stretchr/testify/require"
)

// 会签合并三方口径：子票未修改的键不参与覆盖——先投子票改过的键不被
// 后投子票的创建快照旧值盖回；同键多票修改时按投票时间后投者赢。
func TestCountersignMergeThreeWayKeepsPeerEdits(t *testing.T) {
	q := proxyAuditDB(t)
	parent := &model.WfTask{
		ID: "t-3w-parent", ProcessInstanceID: secFixStrPtr("px-3w"), ProcessID: "proc-px",
		TaskDefKey: "a", Name: "会签审批", TaskType: constants.TaskTypeUserTask,
		Status: string(enums.TaskStatusActive), TenantID: "t1", CreatedBy: "system",
		Variables: secFixStrPtr(`{"days":1,"memo":"x"}`),
		CreatedAt: time.Now(),
	}
	require.NoError(t, q.WfTask.Create(parent))
	approved := "approved"
	vote := func(id string, endedAt time.Time, vars string) {
		sub := &model.WfTask{
			ID: id, ProcessInstanceID: secFixStrPtr("px-3w"), ProcessID: "proc-px",
			ParentID: secFixStrPtr("t-3w-parent"), TaskDefKey: "a", Name: "会签审批_" + id,
			TaskType: constants.TaskTypeUserTask, Status: string(enums.TaskStatusCompleted),
			EndReason: &approved, TenantID: "t1", CreatedBy: "system",
			Variables: secFixStrPtr(vars), CreatedAt: endedAt.Add(-time.Hour), EndedAt: &endedAt,
		}
		require.NoError(t, q.WfTask.Create(sub))
	}
	// 甲先投：改 days=5
	vote("t-3w-a", time.Now().Add(-2*time.Hour), `{"days":5}`)
	// 乙后投：days 未改（快照值仍=创建基线 1），新增 amount
	vote("t-3w-b", time.Now().Add(-time.Hour), `{"days":1,"amount":9}`)
	// 丙最后投：改 days=7
	vote("t-3w-c", time.Now(), `{"days":7}`)

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	svc.mergeCountersignSubTaskVariables(context.Background(), bareScope(q), parent)

	merged, err := ParseVariablesJSON(parent.Variables)
	require.NoError(t, err)
	require.Equal(t, float64(7), merged["days"], "同键多票修改按投票时间后投者赢")
	require.Equal(t, float64(9), merged["amount"], "新增键照常并入")
	require.Equal(t, "x", merged["memo"], "基线键不受子票影响")
}

// StripReentrySnapshot：重入快照剥审批结果键与引擎保留标记，业务键原样保留。
func TestStripReentrySnapshot(t *testing.T) {
	vars := map[string]interface{}{
		"days":            float64(3),
		"approved":        true,
		"comment":         "ok",
		"transfer_from":   "u1",
		"reassign_from":   "u1",
		"fallback_policy": "admin",
	}
	out := StripReentrySnapshot(vars)
	require.Equal(t, float64(3), out["days"])
	require.NotContains(t, out, "approved")
	require.NotContains(t, out, "comment")
	require.NotContains(t, out, "transfer_from")
	require.NotContains(t, out, "reassign_from")
	require.NotContains(t, out, "fallback_policy")

	require.Nil(t, StripReentrySnapshot(nil))

	clean := map[string]interface{}{"memo": "keep"}
	got := StripReentrySnapshot(clean)
	require.Len(t, got, 1, "无保留键时原样返回不复制")
}

// 详情出口权限表归一：id 条目优先，数据键条目换算到 id，无主陈旧键剔除；
// 映射为空（无 schema）时原样透出不丢权限位。
func TestNormalizeDetailFormPermissions(t *testing.T) {
	idToField := map[string]string{"f_1": "days", "f_2": "reason"}
	fp := map[string]interface{}{
		"f_1":   "h", // id 条目
		"days":  "w", // 存量数据键条目，同字段：id 赢
		"f_2":   "r",
		"ghost": "h", // 已删字段的陈旧键，剔除
	}
	out := normalizeDetailFormPermissions(fp, idToField)
	require.Equal(t, "h", out["f_1"])
	require.Equal(t, "r", out["f_2"])
	require.NotContains(t, out, "days", "数据键条目换算到 id，不再以数据键下发")
	require.NotContains(t, out, "ghost")

	raw := normalizeDetailFormPermissions(map[string]interface{}{"days": "h"}, nil)
	require.Equal(t, "h", raw["days"], "无 schema 时原样透出")
}

// 抄送人视角字段权限：按其 ccTask 行所在节点取配置，w 归一为 r，
// 多节点同字段取最严；无抄送行返回 nil。
func TestResolveCCFormPermissions(t *testing.T) {
	dsl := `{"ruleChain":{"additionalInfo":{"form":{"fields":[{"id":"f_1","field":"days"},{"id":"f_2","field":"reason"}]}}},` +
		`"metadata":{"nodes":[` +
		`{"id":"cc-a","type":"ccTask","additionalInfo":{"formPermissions":{"f_1":"w","f_2":"h"}}},` +
		`{"id":"cc-b","type":"ccTask","additionalInfo":{"formPermissions":{"days":"h"}}}` +
		`]}}`
	chain, err := (&model.WfProcess{DefinitionJSON: dsl}).ToRuleChain()
	require.NoError(t, err)

	ccUser := "cc1"
	other := "cc2"
	now := time.Now()
	rows := []*model.WfTask{
		{
			ID: "t-cc-1", ProcessInstanceID: secFixStrPtr("px-cc"), ProcessID: "proc-px",
			TaskDefKey: "cc-a", Name: "抄送A", TaskType: constants.TaskTypeCCTask,
			Status: string(enums.TaskStatusCompleted), Assignee: &ccUser,
			TenantID: "t1", CreatedBy: "system", CreatedAt: now, EndedAt: &now,
		},
		{
			ID: "t-cc-2", ProcessInstanceID: secFixStrPtr("px-cc"), ProcessID: "proc-px",
			TaskDefKey: "cc-b", Name: "抄送B", TaskType: constants.TaskTypeCCTask,
			Status: string(enums.TaskStatusCompleted), Assignee: &ccUser,
			TenantID: "t1", CreatedBy: "system", CreatedAt: now, EndedAt: &now,
		},
		{
			ID: "t-cc-3", ProcessInstanceID: secFixStrPtr("px-cc"), ProcessID: "proc-px",
			TaskDefKey: "cc-a", Name: "抄送A", TaskType: constants.TaskTypeCCTask,
			Status: string(enums.TaskStatusCompleted), Assignee: &other,
			TenantID: "t1", CreatedBy: "system", CreatedAt: now, EndedAt: &now,
		},
	}

	out := resolveCCFormPermissions(chain, rows, "cc1")
	require.Equal(t, "h", out["f_2"], "h 配置透出")
	require.Equal(t, "h", out["f_1"], "w 归一为 r，另一节点 h 取最严后为 h")
	require.NotContains(t, out, "days", "数据键条目换算到字段 id")

	cc2 := resolveCCFormPermissions(chain, rows, "cc2")
	require.Equal(t, "r", cc2["f_1"], "单节点 w 归一为 r")

	require.Nil(t, resolveCCFormPermissions(chain, rows, "nobody"), "无抄送行返回 nil")
	require.Nil(t, resolveCCFormPermissions(chain, nil, "cc1"))
}

// 终态任务不再开放变量写：会签聚合窗口现读子票变量，投完票再改会污染下游
// 路由数据；已完成任务改快照会让归档留痕失真。
func TestTaskVariablesRejectTerminalTask(t *testing.T) {
	q := proxyAuditDB(t)
	assignee := "u1"
	completed := "approved"
	task := &model.WfTask{
		ID: "t-var-done", ProcessInstanceID: nil, ProcessID: "proc-px",
		TaskDefKey: "a", Name: "审批", TaskType: constants.TaskTypeUserTask,
		Status: string(enums.TaskStatusCompleted), EndReason: &completed,
		Assignee: &assignee, TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}
	require.NoError(t, q.WfTask.Create(task))

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "u1", TenantID: "t1"})

	err := svc.SetTaskVariables(ctx, Actor{UserID: "u1", TenantID: "t1"}, "t-var-done", map[string]interface{}{"days": 9})
	require.True(t, errors.Is(err, ErrConflict), "completed 任务变量写应拒绝，got %v", err)

	err = svc.SetTaskVariable(ctx, Actor{UserID: "u1", TenantID: "t1"}, "t-var-done", "days", 9)
	require.True(t, errors.Is(err, ErrConflict))

	err = svc.RemoveTaskVariable(ctx, Actor{UserID: "u1", TenantID: "t1"}, "t-var-done", "days")
	require.True(t, errors.Is(err, ErrConflict))
}
