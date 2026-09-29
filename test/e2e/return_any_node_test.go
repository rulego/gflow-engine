package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/enums"
)

// 回退目标放宽为"任意已完成上游节点"的回归用例集：
//   - 多跳回退后中间节点旧票必须清理，重跑不得被旧 Completed 任务静默跳过；
//   - 目标校验（非上游/不存在/自身/非办理人）与并行网关守卫；
//   - 详情接口 returnableNodes 列表与写路径同口径。

// deployLinearReturnProcess 部署 A → B → C 三节点线性流，所有节点开启回退。
func (e *e2eTestEnv) deployLinearReturnProcess(processKey, name string, approvers [3]string) {
	e.t.Helper()
	nodeIDs := [3]string{"node_a", "node_b", "node_c"}
	names := [3]string{"A审批", "B审批", "C审批"}
	nodes := make([]map[string]interface{}, 0, 4)
	for i := 0; i < 3; i++ {
		nodes = append(nodes, map[string]interface{}{
			"id": nodeIDs[i], "type": "userTask", "name": names[i],
			"configuration": map[string]interface{}{
				"approver":    map[string]interface{}{"type": "user", "userIds": []string{approvers[i]}},
				"approveMode": "single",
			},
			"additionalInfo": map[string]interface{}{"actionPermissions": map[string]interface{}{"return": true}},
		})
	}
	nodes = append(nodes, map[string]interface{}{"id": "end", "type": "end", "name": "End"})
	e.deployReturnDef(processKey, name, nodes, []map[string]interface{}{
		{"fromId": "node_a", "toId": "node_b", "type": "Success"},
		{"fromId": "node_a", "toId": "end", "type": "Failure"},
		{"fromId": "node_b", "toId": "node_c", "type": "Success"},
		{"fromId": "node_b", "toId": "end", "type": "Failure"},
		{"fromId": "node_c", "toId": "end", "type": "Success"},
		{"fromId": "node_c", "toId": "end", "type": "Failure"},
	})
}

// deployForkReturnProcess 部署 fork → [a1(→a2), b] → join → c → end，所有审批节点开启回退。
func (e *e2eTestEnv) deployForkReturnProcess(processKey string, a1, a2, b, c string) {
	e.t.Helper()
	userTask := func(id, name, assignee string) map[string]interface{} {
		return map[string]interface{}{
			"id": id, "type": "userTask", "name": name,
			"configuration": map[string]interface{}{
				"approver":    map[string]interface{}{"type": "user", "userIds": []string{assignee}},
				"approveMode": "single",
			},
			"additionalInfo": map[string]interface{}{"actionPermissions": map[string]interface{}{"return": true}},
		}
	}
	nodes := []map[string]interface{}{
		{"id": "fork1", "type": "fork", "name": "Parallel Fork"},
		userTask("task_a1", "Branch A Step 1", a1),
		userTask("task_b", "Branch B", b),
		{
			"id": "join1", "type": "join", "name": "Parallel Join",
			"configuration": map[string]interface{}{"timeout": 5},
		},
		userTask("task_c", "Post Join", c),
		{"id": "end", "type": "end", "name": "End"},
	}
	conns := []map[string]interface{}{
		{"fromId": "fork1", "toId": "task_a1", "type": "Success"},
		{"fromId": "fork1", "toId": "task_b", "type": "Success"},
		{"fromId": "task_a1", "toId": "join1", "type": "Success"},
		{"fromId": "task_a1", "toId": "join1", "type": "Failure"},
		{"fromId": "task_b", "toId": "join1", "type": "Success"},
		{"fromId": "task_b", "toId": "join1", "type": "Failure"},
		{"fromId": "join1", "toId": "task_c", "type": "Success"},
		{"fromId": "task_c", "toId": "end", "type": "Success"},
		{"fromId": "task_c", "toId": "end", "type": "Failure"},
	}
	if a2 != "" {
		nodes = append([]map[string]interface{}{nodes[0], nodes[1],
			userTask("task_a2", "Branch A Step 2", a2)}, nodes[2:]...)
		conns = []map[string]interface{}{
			{"fromId": "fork1", "toId": "task_a1", "type": "Success"},
			{"fromId": "fork1", "toId": "task_b", "type": "Success"},
			{"fromId": "task_a1", "toId": "task_a2", "type": "Success"},
			{"fromId": "task_a1", "toId": "task_a2", "type": "Failure"},
			{"fromId": "task_a2", "toId": "join1", "type": "Success"},
			{"fromId": "task_a2", "toId": "join1", "type": "Failure"},
			{"fromId": "task_b", "toId": "join1", "type": "Success"},
			{"fromId": "task_b", "toId": "join1", "type": "Failure"},
			{"fromId": "join1", "toId": "task_c", "type": "Success"},
			{"fromId": "task_c", "toId": "end", "type": "Success"},
			{"fromId": "task_c", "toId": "end", "type": "Failure"},
		}
	}
	e.deployReturnDef(processKey, processKey, nodes, conns)
}

// deploySequentialReturnProcess 部署 A(单人) → B(顺序审批 b1→b2) → end，开启回退。
func (e *e2eTestEnv) deploySequentialReturnProcess(processKey string, a, b1, b2 string) {
	e.t.Helper()
	nodes := []map[string]interface{}{
		{
			"id": "node_a", "type": "userTask", "name": "A审批",
			"configuration": map[string]interface{}{
				"approver":    map[string]interface{}{"type": "user", "userIds": []string{a}},
				"approveMode": "single",
			},
			"additionalInfo": map[string]interface{}{"actionPermissions": map[string]interface{}{"return": true}},
		},
		{
			"id": "node_b", "type": "userTask", "name": "B顺序审批",
			"configuration": map[string]interface{}{
				"approver":    map[string]interface{}{"type": "user", "userIds": []string{b1, b2}},
				"approveMode": "sequential",
			},
			"additionalInfo": map[string]interface{}{"actionPermissions": map[string]interface{}{"return": true}},
		},
		{"id": "end", "type": "end", "name": "End"},
	}
	e.deployReturnDef(processKey, processKey, nodes, []map[string]interface{}{
		{"fromId": "node_a", "toId": "node_b", "type": "Success"},
		{"fromId": "node_a", "toId": "end", "type": "Failure"},
		{"fromId": "node_b", "toId": "end", "type": "Success"},
		{"fromId": "node_b", "toId": "end", "type": "Failure"},
	})
}

// deployReturnDef 通用的回退测试部署入口。
func (e *e2eTestEnv) deployReturnDef(processKey, name string, nodes []map[string]interface{}, conns []map[string]interface{}) {
	e.t.Helper()
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": processKey, "name": name, "root": true},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes":          nodes,
			"connections":    conns,
		},
	}
	raw, err := json.Marshal(def)
	require.NoError(e.t, err, "marshal def")
	_, err = e.engine.GetProcessService().Deploy(e.userCtx("admin"), service.Actor{UserID: "admin", TenantID: e2eTenantID, WorkflowAdmin: true}, &model.WfProcess{
		ProcessKey:     processKey,
		Name:           name,
		DefinitionJSON: string(raw),
		Status:         string(enums.ProcessStatusActive),
		TenantID:       e2eTenantID,
		CreatedBy:      "admin",
	}, true)
	require.NoError(e.t, err, "deploy process")
}

// taskRowsByDefKey 统计运行表中某节点的任务行数（含终态；活跃实例的已完成任务留在运行表）。
func (e *e2eTestEnv) taskRowsByDefKey(instanceID, defKey string) int {
	e.t.Helper()
	var count int64
	require.NoError(e.t, e.db.Raw("SELECT count(*) FROM wf_task WHERE process_instance_id = ? AND task_def_key = ?",
		instanceID, defKey).Scan(&count).Error)
	return int(count)
}

// hiTaskRowsByDefKey 统计历史表中某节点的归档行数。
func (e *e2eTestEnv) hiTaskRowsByDefKey(instanceID, defKey string) int {
	e.t.Helper()
	var count int64
	require.NoError(e.t, e.db.Raw("SELECT count(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_def_key = ?",
		instanceID, defKey).Scan(&count).Error)
	return int(count)
}

// 回退可直接退到任意已完成的上游节点（跨过中间节点）：中间节点旧票必须清理，
// 流程重跑经过时不得被旧 Completed 任务静默跳过。
func TestE2E_Return_MultiHopToFirstNode(t *testing.T) {
	env := newE2EEnv(t)
	env.deployLinearReturnProcess("ret_multihop_e2e", "Return MultiHop",
		[3]string{"user_a", "user_b", "user_c"})

	instanceID := env.startInstance("ret_multihop_e2e", "starter_001")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a")) > 0 },
		2*time.Second, 50*time.Millisecond, "node_a task should be created")
	env.approveAs(env.activeTasksFor(instanceID, "user_a")[0].ID, "user_a", "a 同意")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_b")) > 0 },
		2*time.Second, 50*time.Millisecond, "node_b task should be created")
	env.approveAs(env.activeTasksFor(instanceID, "user_b")[0].ID, "user_b", "b 同意")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_c")) > 0 },
		2*time.Second, 50*time.Millisecond, "node_c task should be created")
	tasksC := env.activeTasksFor(instanceID, "user_c")
	require.Len(t, tasksC, 1)

	// user_c 跨过 B 直接退回 node_a
	err := env.engine.GetTaskService().Return(env.userCtxAs("user_c"), service.Actor{UserID: "user_c", TenantID: e2eTenantID}, tasksC[0].ID, "node_a", "材料不齐退回发起环节")
	require.NoError(t, err, "multi-hop return should succeed")

	// B 的旧票已从运行表清理并归档
	require.Eventually(t, func() bool { return env.taskRowsByDefKey(instanceID, "node_b") == 0 },
		2*time.Second, 50*time.Millisecond, "node_b stale tasks must be superseded out of runtime table")
	assert.GreaterOrEqual(t, env.hiTaskRowsByDefKey(instanceID, "node_b"), 1, "node_b old task must be archived")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a")) > 0 },
		3*time.Second, 50*time.Millisecond, "node_a task must be recreated after multi-hop return")

	// 关键回归：A 重新通过后，B 必须重新出任务（不得被旧票判定为已完成而跳过）
	tasksA2 := env.activeTasksFor(instanceID, "user_a")
	require.Len(t, tasksA2, 1)
	env.approveAs(tasksA2[0].ID, "user_a", "补充后同意")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_b")) > 0 },
		3*time.Second, 50*time.Millisecond, "node_b must re-run (no silent pass by stale completed task)")
	tasksB2 := env.activeTasksFor(instanceID, "user_b")
	env.approveAs(tasksB2[0].ID, "user_b", "b 重新同意")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_c")) > 0 },
		3*time.Second, 50*time.Millisecond, "node_c must re-run after re-approval chain")
	tasksC2 := env.activeTasksFor(instanceID, "user_c")
	env.approveAs(tasksC2[0].ID, "user_c", "c 同意")
	require.Eventually(t, func() bool { return env.instanceStatus(instanceID) == "completed" },
		3*time.Second, 50*time.Millisecond, "instance should complete after full re-run")
}

// 非法回退目标一律拒绝：自身节点/不存在节点/下游节点/非办理人。
func TestE2E_Return_InvalidTargetsRejected(t *testing.T) {
	env := newE2EEnv(t)
	env.deployLinearReturnProcess("ret_invalid_e2e", "Return Invalid Targets",
		[3]string{"user_a", "user_b", "user_c"})

	// 推进到 C
	instanceID := env.startInstance("ret_invalid_e2e", "starter_001")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a")) > 0 }, 2*time.Second, 50*time.Millisecond)
	env.approveAs(env.activeTasksFor(instanceID, "user_a")[0].ID, "user_a", "a")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_b")) > 0 }, 2*time.Second, 50*time.Millisecond)
	env.approveAs(env.activeTasksFor(instanceID, "user_b")[0].ID, "user_b", "b")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_c")) > 0 }, 2*time.Second, 50*time.Millisecond)
	tasksC := env.activeTasksFor(instanceID, "user_c")
	require.Len(t, tasksC, 1)

	returnAs := func(actorUserID, target string) error {
		return env.engine.GetTaskService().Return(env.userCtxAs(actorUserID),
			service.Actor{UserID: actorUserID, TenantID: e2eTenantID}, tasksC[0].ID, target, "测试")
	}

	assert.Error(t, returnAs("user_c", "node_c"), "回退到当前节点自身必须拒绝")
	assert.Error(t, returnAs("user_c", "node_x"), "回退到不存在的节点必须拒绝")
	assert.Error(t, returnAs("user_b", "node_a"), "非当前任务办理人必须拒绝")

	// 实例不受失败回退影响
	assert.Len(t, env.activeTasksFor(instanceID, "user_c"), 1, "失败的回退不应影响在途任务")
}

// join 之后的回退（重执行区域穿过 join）必须拒绝，实例不受影响。
func TestE2E_Return_CrossesParallelGatewayRejected(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForkReturnProcess("ret_fork_reject_e2e", "user_a1", "", "user_b", "user_c")

	instanceID := env.startInstance("ret_fork_reject_e2e", "starter_001")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a1")) > 0 },
		2*time.Second, 50*time.Millisecond, "branch A task should be created")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_b")) > 0 },
		2*time.Second, 50*time.Millisecond, "branch B task should be created")
	env.approveAs(env.activeTasksFor(instanceID, "user_a1")[0].ID, "user_a1", "a1")
	env.approveAs(env.activeTasksFor(instanceID, "user_b")[0].ID, "user_b", "b")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_c")) > 0 },
		3*time.Second, 50*time.Millisecond, "post-join task should be created after both branches approve")
	tasksC := env.activeTasksFor(instanceID, "user_c")
	require.Len(t, tasksC, 1)

	// 回退到分支尾节点：重执行区域穿过 join，必须拒绝
	err := env.engine.GetTaskService().Return(env.userCtxAs("user_c"), service.Actor{UserID: "user_c", TenantID: e2eTenantID}, tasksC[0].ID, "task_b", "跨分支退回")
	assert.Error(t, err, "return crossing a parallel gateway must be rejected")
	assert.Len(t, env.activeTasksFor(instanceID, "user_c"), 1, "被拒绝的回退不应影响在途任务")
}

// 同一分支内的回退不受网关守卫误伤：a2 退回 a1 时兄弟分支照常办理，全程可走完。
func TestE2E_Return_WithinBranchAllowed(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForkReturnProcess("ret_fork_same_branch_e2e", "user_a1", "user_a2", "user_b", "user_c")

	instanceID := env.startInstance("ret_fork_same_branch_e2e", "starter_001")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a1")) > 0 },
		2*time.Second, 50*time.Millisecond, "branch A step1 task should be created")
	env.approveAs(env.activeTasksFor(instanceID, "user_a1")[0].ID, "user_a1", "a1")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a2")) > 0 },
		2*time.Second, 50*time.Millisecond, "branch A step2 task should be created")
	tasksA2 := env.activeTasksFor(instanceID, "user_a2")
	require.Len(t, tasksA2, 1)

	// a2 退回同支的 a1：区域不跨网关，必须放行
	err := env.engine.GetTaskService().Return(env.userCtxAs("user_a2"), service.Actor{UserID: "user_a2", TenantID: e2eTenantID}, tasksA2[0].ID, "task_a1", "分支内退回")
	require.NoError(t, err, "within-branch return should be allowed")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a1")) > 0 },
		3*time.Second, 50*time.Millisecond, "branch A step1 must be recreated after return")

	// 兄弟分支不受影响，重走后全程完成
	env.approveAs(env.activeTasksFor(instanceID, "user_a1")[0].ID, "user_a1", "a1 重新同意")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a2")) > 0 },
		3*time.Second, 50*time.Millisecond, "branch A step2 recreated")
	env.approveAs(env.activeTasksFor(instanceID, "user_a2")[0].ID, "user_a2", "a2")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_b")) > 0 },
		2*time.Second, 50*time.Millisecond, "branch B task still pending")
	env.approveAs(env.activeTasksFor(instanceID, "user_b")[0].ID, "user_b", "b")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_c")) > 0 },
		3*time.Second, 50*time.Millisecond, "post-join task created after both branches")
	env.approveAs(env.activeTasksFor(instanceID, "user_c")[0].ID, "user_c", "c")
	require.Eventually(t, func() bool { return env.instanceStatus(instanceID) == "completed" },
		3*time.Second, 50*time.Millisecond, "instance should complete")
}

// 顺序审批节点第 2 环审批人回退：退到上一外部节点，本节点必须从第 1 环整节点重跑，
// 而不是按旧进度续跑。
func TestE2E_Return_SequentialNodeRestartsFromFirstStep(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySequentialReturnProcess("ret_seq_e2e", "user_a", "user_b1", "user_b2")

	instanceID := env.startInstance("ret_seq_e2e", "starter_001")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a")) > 0 },
		2*time.Second, 50*time.Millisecond, "node_a task should be created")
	env.approveAs(env.activeTasksFor(instanceID, "user_a")[0].ID, "user_a", "a")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_b1")) > 0 },
		2*time.Second, 50*time.Millisecond, "sequential step1 (b1) task should be created")
	env.approveAs(env.activeTasksFor(instanceID, "user_b1")[0].ID, "user_b1", "b1")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_b2")) > 0 },
		2*time.Second, 50*time.Millisecond, "sequential step2 (b2) task should be created")
	tasksB2 := env.activeTasksFor(instanceID, "user_b2")
	require.Len(t, tasksB2, 1)

	// b2 退回 node_a：此前只能"退回本节点"，现在必须能退到上一外部节点
	err := env.engine.GetTaskService().Return(env.userCtxAs("user_b2"), service.Actor{UserID: "user_b2", TenantID: e2eTenantID}, tasksB2[0].ID, "node_a", "前置环节材料需修正")
	require.NoError(t, err, "sequential step2 return to previous external node should succeed")
	require.Eventually(t, func() bool { return env.taskRowsByDefKey(instanceID, "node_b") == 0 },
		2*time.Second, 50*time.Millisecond, "sequential node stale rows (b1 completed vote) must be cleaned")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a")) > 0 },
		3*time.Second, 50*time.Millisecond, "node_a task recreated")

	// 关键断言：A 重新通过后顺序节点从第 1 环（b1）重跑，而非按旧进度直接轮到 b2
	tasksA2 := env.activeTasksFor(instanceID, "user_a")
	require.Len(t, tasksA2, 1)
	env.approveAs(tasksA2[0].ID, "user_a", "a 修正后同意")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_b1")) > 0 },
		3*time.Second, 50*time.Millisecond, "sequential node must restart from step1 (b1), not resume at b2")
	assert.Empty(t, env.activeTasksFor(instanceID, "user_b2"), "b2 不应先于 b1 收到任务")
	env.approveAs(env.activeTasksFor(instanceID, "user_b1")[0].ID, "user_b1", "b1 重新同意")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_b2")) > 0 },
		3*time.Second, 50*time.Millisecond, "sequential step2 recreated")
	env.approveAs(env.activeTasksFor(instanceID, "user_b2")[0].ID, "user_b2", "b2")
	require.Eventually(t, func() bool { return env.instanceStatus(instanceID) == "completed" },
		3*time.Second, 50*time.Millisecond, "instance should complete")
}

// 详情接口 returnableNodes 与写路径同口径：最近完成在前、剔除自身、剔除不合法目标。
func TestE2E_ReturnableNodesInDetail(t *testing.T) {
	env := newE2EEnv(t)
	env.deployLinearReturnProcess("ret_detail_e2e", "Return Detail",
		[3]string{"user_a", "user_b", "user_c"})

	instanceID := env.startInstance("ret_detail_e2e", "starter_001")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a")) > 0 }, 2*time.Second, 50*time.Millisecond)

	// B 在途：A 办理人的可退列表只有 node_a... A 自己就是 node_a 办理人，排除自身后列表为空
	detailA, err := env.engine.GetRuntimeService().GetProcessInstanceDetail(env.userCtxAs("user_a"),
		service.Actor{UserID: "user_a", TenantID: e2eTenantID}, instanceID)
	require.NoError(t, err, "detail as user_a")
	assert.Empty(t, detailA.ReturnableNodes, "首个审批节点无可退目标（自身被剔除）")

	// 发起人视角（无可办任务）：不计算该字段
	detailStarter, err := env.engine.GetRuntimeService().GetProcessInstanceDetail(env.userCtxAs("starter_001"),
		service.Actor{UserID: "starter_001", TenantID: e2eTenantID}, instanceID)
	require.NoError(t, err, "detail as starter")
	assert.Empty(t, detailStarter.ReturnableNodes, "无可办任务的查看者不装配可退列表")

	env.approveAs(env.activeTasksFor(instanceID, "user_a")[0].ID, "user_a", "a")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_b")) > 0 }, 2*time.Second, 50*time.Millisecond)
	detailB, err := env.engine.GetRuntimeService().GetProcessInstanceDetail(env.userCtxAs("user_b"),
		service.Actor{UserID: "user_b", TenantID: e2eTenantID}, instanceID)
	require.NoError(t, err, "detail as user_b")
	require.Len(t, detailB.ReturnableNodes, 1, "B 在途时只有 node_a 可退")
	assert.Equal(t, "node_a", detailB.ReturnableNodes[0].Key)

	env.approveAs(env.activeTasksFor(instanceID, "user_b")[0].ID, "user_b", "b")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_c")) > 0 }, 2*time.Second, 50*time.Millisecond)
	detailC, err := env.engine.GetRuntimeService().GetProcessInstanceDetail(env.userCtxAs("user_c"),
		service.Actor{UserID: "user_c", TenantID: e2eTenantID}, instanceID)
	require.NoError(t, err, "detail as user_c")
	require.Len(t, detailC.ReturnableNodes, 2, "C 在途时 node_b 与 node_a 均可退")
	assert.Equal(t, "node_b", detailC.ReturnableNodes[0].Key, "最近完成的节点排在前")
	assert.Equal(t, "B审批", detailC.ReturnableNodes[0].Name)
	assert.Equal(t, "node_a", detailC.ReturnableNodes[1].Key)
	assert.Equal(t, "A审批", detailC.ReturnableNodes[1].Name)
}
