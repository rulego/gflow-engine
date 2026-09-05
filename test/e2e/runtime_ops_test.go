// Package e2e — 运行时运维接口（Restart/Restore/ForceResume/ReDrive）的端到端测试。
//
// 覆盖场景：
//   - RestartProcessInstance 正常路径（新实例 ID / businessKey -restart 后缀 /
//     变量继承 / 从头推进）与跨租户拒绝
//   - CompleteProcessInstance 幂等重试（重复调用返回 nil，归档仅一条）
//   - 顺序审批 dueDate 逐任务重算（timeoutPolicy 相对各自创建时刻）
//   - subProcess 子实例终止 → 父流程经 Failure 边恢复
//   - RestoreAllProcessInstances / RestoreProcessInstance 越权拦截
//   - ForceResumeInstance（活动分支拒绝 / 非 fork 拓扑拒绝 / 卡死实例救回）
//   - ReDriveProcessInstance（健康实例幂等 / 跨租户拒绝）
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/enums"
)

// adminActor 运维接口测试用的管理员身份（与 e2e 租户一致）。
func adminActor() service.Actor {
	return service.Actor{UserID: "admin", UserName: "admin", TenantID: e2eTenantID}
}

// ---------------------------------------------------------------------------
// RestartProcessInstance 正常路径
// ---------------------------------------------------------------------------

func TestE2E_RestartProcessInstance_HappyPath(t *testing.T) {
	env := newE2EEnv(t)
	env.deployLinearTwoStepProcess("restart_two_step", "重启回归", "approver_1", "approver_2", "")

	vars := map[string]interface{}{"reason": "重启回归测试", "amount": 88}
	instID := env.startInstanceWithVars("restart_two_step", "starter", vars)

	// 推进到第二步后重启（原实例保持非终态也可以重启：原实例不动，派生新实例）
	var task1 string
	require.Eventually(t, func() bool {
		task1 = env.firstActiveTaskIDByDefKey(instID, "first_task")
		return task1 != ""
	}, 3*time.Second, 50*time.Millisecond, "first approval task should exist")
	env.approveAs(task1, "approver_1", "first ok")

	// 原实例 business_key 为空启动时由引擎自动生成（BIZ_ 前缀），读取作为对照
	var origBizKey string
	require.NoError(t, env.db.Raw("SELECT business_key FROM wf_instance WHERE id = ?", instID).
		Scan(&origBizKey).Error, "read original business key")
	require.NotEmpty(t, origBizKey, "engine should auto-generate business key")

	newID, err := env.engine.GetRuntimeService().RestartProcessInstance(
		env.userCtx("admin"), adminActor(), instID, "")
	require.NoError(t, err, "restart should succeed and return new instance id")
	require.NotEmpty(t, newID, "new instance id must be returned")
	require.NotEqual(t, instID, newID, "new instance id must differ from original")

	// 1) 原实例不受影响
	origStatus := env.instanceStatus(instID)
	assert.Equal(t, string(enums.InstanceStatusActive), origStatus,
		"original instance must stay untouched")

	// 2) 新实例 businessKey = 原值 + "-restart"
	var newBizKey string
	require.NoError(t, env.db.Raw("SELECT business_key FROM wf_instance WHERE id = ?", newID).
		Scan(&newBizKey).Error)
	assert.Equal(t, origBizKey+"-restart", newBizKey, "new instance business key must carry -restart suffix")

	// 3) 新实例 active 且从第一步重新推进（approver_1 有新任务）
	assert.Equal(t, string(enums.InstanceStatusActive), env.instanceStatus(newID))
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(newID, "approver_1")) == 1
	}, 3*time.Second, 50*time.Millisecond, "restarted instance should create a fresh first-step task")

	// 4) 变量继承自原实例
	gotVars, err := env.engine.GetRuntimeService().GetProcessInstanceVariables(
		env.userCtx("admin"), adminActor(), newID)
	require.NoError(t, err)
	assert.Equal(t, "重启回归测试", gotVars["reason"], "variables must be inherited from original instance")

	// 5) 新实例可独立走完
	var taskNew string
	require.Eventually(t, func() bool {
		taskNew = env.firstActiveTaskIDByDefKey(newID, "first_task")
		return taskNew != ""
	}, 3*time.Second, 50*time.Millisecond, "restarted instance should have first-step task")
	env.approveAs(taskNew, "approver_1", "restarted first ok")
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(newID, "approver_2")) == 1
	}, 3*time.Second, 50*time.Millisecond, "restarted instance should advance to second approver")
}

func TestE2E_RestartProcessInstance_CrossTenantRejected(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("restart_cross_tenant", "跨租户重启", "single", []string{"approver_x"}, nil)
	instID := env.startInstance("restart_cross_tenant", "starter")

	_, err := env.engine.GetRuntimeService().RestartProcessInstance(
		context.Background(), service.Actor{UserID: "outsider", TenantID: "other-tenant"}, instID, "")
	require.Error(t, err, "cross-tenant restart must be rejected")
	assert.ErrorIs(t, err, service.ErrPermissionDenied)
}

// ---------------------------------------------------------------------------
// CompleteProcessInstance 幂等重试
// ---------------------------------------------------------------------------

func TestE2E_CompleteProcessInstance_IdempotentRetry(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("complete_idempotent", "幂等完成", "single", []string{"approver_i"}, nil)
	instID := env.startInstance("complete_idempotent", "starter")

	// 走到自然完成
	var task string
	require.Eventually(t, func() bool {
		task = env.firstActiveTaskIDByDefKey(instID, "approval_node")
		return task != ""
	}, 3*time.Second, 50*time.Millisecond)
	env.approveAs(task, "approver_i", "done")
	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 3*time.Second, 50*time.Millisecond, "instance should complete naturally")

	// 幂等重试：对已完成实例重复调用必须返回 nil（重试安全），且不产生第二条归档
	for i := 0; i < 3; i++ {
		require.NoError(t, env.engine.GetRuntimeService().CompleteProcessInstance(
			env.userCtx("admin"), adminActor(), instID, "retry"),
			"retry #%d on completed instance must be a no-op success", i+1)
	}

	var n int64
	require.NoError(t, env.db.Raw("SELECT COUNT(*) FROM wf_hi_instance WHERE id = ?", instID).Scan(&n).Error)
	assert.Equal(t, int64(1), n, "repeat completion must not create duplicate archive rows")

	// 不存在的实例：必须显式报错（ErrInstanceNotFound），而不是静默成功
	err := env.engine.GetRuntimeService().CompleteProcessInstance(
		env.userCtx("admin"), adminActor(), "inst-not-exist", "x")
	require.Error(t, err, "completing a missing instance must fail")
	assert.True(t, errors.Is(err, service.ErrInstanceNotFound),
		"expected ErrInstanceNotFound, got %v", err)
}

// ---------------------------------------------------------------------------
// 顺序审批 dueDate 逐任务重算
// ---------------------------------------------------------------------------

func TestE2E_SequentialApproval_DueDateRecomputedPerTask(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("seq_due_e2e", "顺序dueDate", "sequential", []string{"seq_a", "seq_b"},
		map[string]interface{}{
			"timeoutPolicy": map[string]interface{}{"dueInMinutes": 60},
		})

	instID := env.startInstance("seq_due_e2e", "starter")

	var task1 model.WfTask
	require.Eventually(t, func() bool {
		rows := env.activeTasksFor(instID, "seq_a")
		if len(rows) == 0 {
			return false
		}
		task1 = *rows[0]
		return true
	}, 3*time.Second, 50*time.Millisecond, "first sequential task should be created")

	require.NotNil(t, task1.DueDate, "task1 must carry a dueDate from timeoutPolicy")
	d1 := task1.DueDate.Sub(task1.CreatedAt)
	assert.InDelta(t, float64(60*time.Minute), float64(d1), float64(3*time.Minute),
		"task1 dueDate should be createdAt + dueInMinutes")

	// 第一人通过 → 第二个子任务按【自己的创建时刻】重算 dueDate
	env.approveAs(task1.ID, "seq_a", "first ok")

	var task2 model.WfTask
	require.Eventually(t, func() bool {
		rows := env.activeTasksFor(instID, "seq_b")
		if len(rows) == 0 {
			return false
		}
		task2 = *rows[0]
		return true
	}, 3*time.Second, 50*time.Millisecond, "second sequential task should be created after first approval")

	require.NotNil(t, task2.DueDate, "second sequential task must carry a dueDate")
	d2 := task2.DueDate.Sub(task2.CreatedAt)
	assert.InDelta(t, float64(60*time.Minute), float64(d2), float64(3*time.Minute),
		"task2 dueDate should be its own createdAt + dueInMinutes")
	assert.True(t, task2.CreatedAt.After(task1.CreatedAt), "task2 must be created after task1")
	assert.True(t, task2.DueDate.After(*task1.DueDate),
		"task2 dueDate must be later than task1's (proves per-creation recomputation, not a shared static date)")
}

// ---------------------------------------------------------------------------
// subProcess 子实例终止 → 父流程经 Failure 边恢复
// ---------------------------------------------------------------------------

func TestE2E_SubProcess_ChildTerminated_ResumesParentViaFailureEdge(t *testing.T) {
	env := newE2EEnv(t)

	// 子流程：单步审批（先部署，父流程的 subProcess.targetId 才能解析到它）
	env.deploySimpleProcess("e2e_sub_child", "子流程", "single", []string{"child_approver"}, nil)

	// 父流程：subProcess（Success/Failure 都连到 after_child）→ after_child → end
	parentDef := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id": "e2e_sub_parent_chain", "name": "父流程", "root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{
					"id":   "sub_node",
					"type": "subProcess",
					"name": "子流程节点",
					"configuration": map[string]interface{}{
						"targetId": "e2e_sub_child",
					},
				},
				{
					"id":   "after_child",
					"type": "userTask",
					"name": "子流程后的审批",
					"configuration": map[string]interface{}{
						"candidateType":   "user",
						"candidateConfig": map[string]interface{}{"userIds": []string{"parent_approver"}},
						"approvalType":    "single",
					},
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "sub_node", "toId": "after_child", "type": "Success"},
				{"fromId": "sub_node", "toId": "after_child", "type": "Failure"},
				{"fromId": "after_child", "toId": "end", "type": "Success"},
				{"fromId": "after_child", "toId": "end", "type": "Failure"},
			},
		},
	}
	env.deployRawProcess("e2e_sub_parent", "e2e_sub_parent_chain", "父流程", parentDef)

	parentID := env.startInstance("e2e_sub_parent", "starter")

	// 子实例异步启动：parent_id = parentID
	var childID string
	require.Eventually(t, func() bool {
		return env.db.Raw("SELECT id FROM wf_instance WHERE parent_id = ? LIMIT 1", parentID).
			Scan(&childID).Error == nil && childID != ""
	}, 5*time.Second, 50*time.Millisecond, "child instance should be started with parent_id set")

	// 子实例的审批任务出现后，直接终止子实例
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(childID, "child_approver")) == 1
	}, 5*time.Second, 50*time.Millisecond, "child approval task should be created")

	require.NoError(t, env.engine.GetRuntimeService().TerminateProcessInstance(
		env.userCtx("admin"), adminActor(), childID, "子流程被运营终止"),
		"terminating the child instance should succeed")

	// 子实例归档为 terminated
	require.Eventually(t, func() bool {
		var status string
		if err := env.db.Raw("SELECT status FROM wf_hi_instance WHERE id = ?", childID).Scan(&status).Error; err != nil {
			return false
		}
		return status == string(enums.InstanceStatusTerminated)
	}, 3*time.Second, 50*time.Millisecond, "child should be archived as terminated")

	// 核心断言：子终止传播 → 父 subProcess 节点重入走 Failure 边 → 父继续推进到 after_child
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(parentID, "parent_approver")) == 1
	}, 5*time.Second, 50*time.Millisecond,
		"parent must resume via the subProcess Failure edge after child termination")
	assert.Equal(t, string(enums.InstanceStatusActive), env.instanceStatus(parentID),
		"parent should still be active and waiting on after_child")
}

// deployRawProcess 部署一份调用方自组的 DSL（ruleChain.id 必须与 chainAlias 一致）。
func (e *e2eTestEnv) deployRawProcess(processKey, chainAlias, name string, def map[string]interface{}) {
	e.t.Helper()
	def["ruleChain"].(map[string]interface{})["id"] = chainAlias
	raw, err := json.Marshal(def)
	require.NoError(e.t, err, "marshal def")

	ctx := e.userCtx("admin")
	_, err = e.engine.GetProcessService().Deploy(ctx, service.Actor{UserID: "admin", TenantID: e2eTenantID}, &model.WfProcess{
		ProcessKey:     processKey,
		Name:           name,
		DefinitionJSON: string(raw),
		Status:         string(enums.ProcessStatusActive),
		TenantID:       e2eTenantID,
		CreatedBy:      "admin",
	}, true)
	require.NoError(e.t, err, "deploy process %s", processKey)
}

// ---------------------------------------------------------------------------
// Restore 越权拦截
// ---------------------------------------------------------------------------

func TestE2E_RestoreProcessInstance_CrossTenantRejected(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForkJoinProcess("restore_cross_tenant", "user_a", "user_b")
	instID := env.startInstance("restore_cross_tenant", "starter")
	env.waitForBranchesActive(instID)

	err := env.engine.GetRuntimeService().RestoreProcessInstance(
		context.Background(), service.Actor{UserID: "outsider", TenantID: "other-tenant"}, instID)
	require.Error(t, err, "cross-tenant restore must be rejected")
	assert.ErrorIs(t, err, service.ErrPermissionDenied)

	// 同租户合法恢复不产生重复任务（与 parallel_restore 的断言口径一致）
	beforeA := env.allTaskCountByDefKey(instID, "task_a")
	require.NoError(t, env.engine.GetRuntimeService().RestoreProcessInstance(
		env.userCtx("admin"), adminActor(), instID))
	assert.Equal(t, beforeA, env.allTaskCountByDefKey(instID, "task_a"),
		"authorized restore must not duplicate tasks")
}

func TestE2E_RestoreAllProcessInstances_NonAdminRejected(t *testing.T) {
	env := newE2EEnv(t) // 只为初始化引擎；本用例走权限门，不触库
	err := env.engine.GetRuntimeService().RestoreAllProcessInstances(
		context.Background(), service.Actor{UserID: "operator", TenantID: e2eTenantID})
	require.Error(t, err, "non-system, non-superadmin actor must be rejected")
	assert.ErrorIs(t, err, service.ErrPermissionDenied)
}

// ---------------------------------------------------------------------------
// ForceResumeInstance / ReDriveProcessInstance
// ---------------------------------------------------------------------------

func TestE2E_ForceResumeInstance_ErrorPaths(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("fr_linear", "非fork流程", "single", []string{"fr_approver"}, nil)
	instID := env.startInstance("fr_linear", "starter")
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "fr_approver")) == 1
	}, 3*time.Second, 50*time.Millisecond)

	// 线性拓扑（非 fork-join）→ ErrUnsupportedForkTopology
	err := env.engine.GetRuntimeService().ForceResumeInstance(env.userCtx("admin"), adminActor(), instID)
	require.Error(t, err, "non-fork topology must be rejected")
	assert.ErrorIs(t, err, service.ErrUnsupportedForkTopology)

	// 跨租户拒绝（租户校验先于拓扑判定）
	err = env.engine.GetRuntimeService().ForceResumeInstance(
		context.Background(), service.Actor{UserID: "outsider", TenantID: "other-tenant"}, instID)
	require.Error(t, err)
	assert.ErrorIs(t, err, service.ErrPermissionDenied)
}

func TestE2E_ForceResumeInstance_ActiveBranchesBlocked(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForkJoinProcess("fr_active_branches", "user_a", "user_b")
	instID := env.startInstance("fr_active_branches", "starter")
	env.waitForBranchesActive(instID)

	// 仅 approve A：B 分支仍 active → 强制恢复必须被拒绝（数据未丢，不该救）
	taskA := env.firstActiveTaskIDByDefKey(instID, "task_a")
	env.approveAs(taskA, "user_a", "a ok")

	err := env.engine.GetRuntimeService().ForceResumeInstance(env.userCtx("admin"), adminActor(), instID)
	require.Error(t, err, "force resume must be blocked while branches are still active")
	assert.ErrorIs(t, err, service.ErrForceResumeActiveBranches)
}

func TestE2E_ForceResumeInstance_RecoversStuckInstance(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForkJoinProcess("fr_stuck", "user_a", "user_b")
	instID := env.startInstance("fr_stuck", "starter")
	env.waitForBranchesActive(instID)

	// 构造"卡死"态：A 正常 approve；B 的任务被人为置为 completed+approved（模拟最后一次
	// approve 已落库但引擎推进失败的残留态）——所有任务已终态但实例仍 active，
	// join 未放行。这正是 ForceResumeInstance 文档声明的救援场景。
	// end_reason='approved' 必须带上：重驱后 userTask 按 EndReason 评估审批结果。
	taskA := env.firstActiveTaskIDByDefKey(instID, "task_a")
	env.approveAs(taskA, "user_a", "a ok")
	require.NoError(t, env.db.Exec(
		"UPDATE wf_task SET status = ?, end_reason = ?, ended_at = ? WHERE process_instance_id = ? AND task_def_key = ?",
		string(enums.TaskStatusCompleted), string(enums.ApprovalResultApproved), time.Now(), instID, "task_b").Error,
		"simulate stuck state: branch B task completed without engine advance")

	// 救援：所有任务已终态、实例仍 active → ForceResume 应放行并补跑 join/end
	require.NoError(t, env.engine.GetRuntimeService().ForceResumeInstance(
		env.userCtx("admin"), adminActor(), instID),
		"force resume should recover the stuck fork-join instance")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 5*time.Second, 50*time.Millisecond,
		"stuck instance should complete after force resume; status=%q", env.instanceStatus(instID))
}

func TestE2E_ReDriveProcessInstance_HealthyAndCrossTenant(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("rd_linear", "重驱动流程", "single", []string{"rd_approver"}, nil)
	instID := env.startInstance("rd_linear", "starter")
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "rd_approver")) == 1
	}, 3*time.Second, 50*time.Millisecond)

	// 健康实例重驱动：幂等自愈（userTask OnMsg 见既有任务则继续等待），不得重复建任务
	require.NoError(t, env.engine.GetRuntimeService().ReDriveProcessInstance(
		env.userCtx("admin"), adminActor(), instID))
	assert.Equal(t, 1, env.activeTaskCountByDefKey(instID, "approval_node"),
		"re-driving a healthy instance must not duplicate tasks")
	assert.Equal(t, string(enums.InstanceStatusActive), env.instanceStatus(instID))

	// 跨租户拒绝
	err := env.engine.GetRuntimeService().ReDriveProcessInstance(
		context.Background(), service.Actor{UserID: "outsider", TenantID: "other-tenant"}, instID)
	require.Error(t, err, "cross-tenant re-drive must be rejected")
	assert.ErrorIs(t, err, service.ErrPermissionDenied)
}

// subProcess 节点没有 Failure 出边时：子实例终止 → 父实例被终止留痕
// （意外失败兜底语义，reason 携带 "subProcess child terminated" 供排障）。
func TestE2E_SubProcess_ChildTerminated_NoFailureEdge_TerminatesParent(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("e2e_sub_child_nf", "子流程", "single", []string{"child_approver_nf"}, nil)

	// 父流程：subProcess 仅连 Success → end（无 Failure 分支）
	parentDef := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id": "e2e_sub_parent_nf_chain", "name": "父流程无Failure边", "root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{
					"id":   "sub_node",
					"type": "subProcess",
					"name": "子流程节点",
					"configuration": map[string]interface{}{
						"targetId": "e2e_sub_child_nf",
					},
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "sub_node", "toId": "end", "type": "Success"},
			},
		},
	}
	env.deployRawProcess("e2e_sub_parent_nf", "e2e_sub_parent_nf_chain", "父流程无Failure边", parentDef)

	parentID := env.startInstance("e2e_sub_parent_nf", "starter")

	var childID string
	require.Eventually(t, func() bool {
		return env.db.Raw("SELECT id FROM wf_instance WHERE parent_id = ? LIMIT 1", parentID).
			Scan(&childID).Error == nil && childID != ""
	}, 5*time.Second, 50*time.Millisecond, "child instance should be started")
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(childID, "child_approver_nf")) == 1
	}, 5*time.Second, 50*time.Millisecond, "child approval task should be created")

	require.NoError(t, env.engine.GetRuntimeService().TerminateProcessInstance(
		env.userCtx("admin"), adminActor(), childID, "子流程被运营终止"))

	// 无 Failure 分支兜底：父实例走完整终止链路（终止活跃任务+归档），运行时表
	// 不再保留该行，end_reason 携带子流程终止原因供排障。
	var hiStatus, hiReason string
	require.Eventually(t, func() bool {
		var row struct {
			Status string
			Reason string
		}
		if err := env.db.Raw(
			"SELECT status AS status, IFNULL(end_reason,'') AS reason FROM wf_hi_instance WHERE id = ?",
			parentID).Scan(&row).Error; err != nil {
			return false
		}
		hiStatus, hiReason = row.Status, row.Reason
		return row.Status == string(enums.InstanceStatusTerminated) && row.Reason != ""
	}, 5*time.Second, 50*time.Millisecond,
		"parent without Failure edge should be terminated with explanatory reason; status=%q reason=%q",
		hiStatus, hiReason)
}
