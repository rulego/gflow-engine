// Package e2e — 并行流程边界场景的回归测试（docs/parallel-limitations.md）。
//
// 覆盖三类易错场景：
//
//  1. 分支局部变量保留：multi-node 恢复用 ExecuteNodeWithMsg 让每个分支
//     带自己的 task.Variables，分支变量在 join 合并时不丢失。
//
//  2. 混用分支（suspend + sync-only）：跳过 sync-only 分支，
//     只 multi-node 恢复含 suspend 的分支。
//
//  3. 一条分支多个串联 userTask：验证串联 userTask 在 multi-node
//     恢复下能正确驱动。
//
// 这些用例和 parallel_resume_test.go 互补：后者覆盖基础 happy path，本文件
// 专攻边界场景。
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/enums"
)

// ---------------------------------------------------------------------------
// 测试辅助：部署不同拓扑的 fork/join 流程
// ---------------------------------------------------------------------------

// deployForkJoinWithMergeMap 部署 fork → [task_a, task_b] → join(mergeToMap=true) → end
//
// mergeToMap=true 让 join 节点把每条分支的输出合并成 {branchId: result} map，
// 而非默认的 array。这是验证分支局部变量是否被保留的关键——只有 mergeToMap
// 才能在 join 输出里看到每条分支的独立数据。
func (e *e2eTestEnv) deployForkJoinWithMergeMap(processKey, assigneeA, assigneeB string) {
	e.t.Helper()
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id": processKey, "name": processKey, "root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{"id": "fork1", "type": "fork", "name": "Parallel Fork"},
				{
					"id": "task_a", "type": "userTask", "name": "Branch A",
					"configuration": map[string]interface{}{
						"candidateType":   "user",
						"candidateConfig": map[string]interface{}{"userIds": []string{assigneeA}},
						"approvalType":    "single",
					},
				},
				{
					"id": "task_b", "type": "userTask", "name": "Branch B",
					"configuration": map[string]interface{}{
						"candidateType":   "user",
						"candidateConfig": map[string]interface{}{"userIds": []string{assigneeB}},
						"approvalType":    "single",
					},
				},
				{
					"id": "join1", "type": "join", "name": "Parallel Join",
					"configuration": map[string]interface{}{
						"timeout":    5,
						"mergeToMap": true,
					},
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "fork1", "toId": "task_a", "type": "Success"},
				{"fromId": "fork1", "toId": "task_b", "type": "Success"},
				{"fromId": "task_a", "toId": "join1", "type": "Success"},
				{"fromId": "task_a", "toId": "join1", "type": "Failure"},
				{"fromId": "task_b", "toId": "join1", "type": "Success"},
				{"fromId": "task_b", "toId": "join1", "type": "Failure"},
				{"fromId": "join1", "toId": "end", "type": "Success"},
			},
		},
	}
	raw, err := json.Marshal(def)
	require.NoError(e.t, err, "marshal fork/join def with mergeToMap")
	ctx := e.userCtx("admin")
	_, err = e.engine.GetProcessService().Deploy(ctx, service.Actor{UserID: "admin", TenantID: e2eTenantID}, &model.WfProcess{
		ProcessKey:     processKey,
		Name:           processKey,
		DefinitionJSON: string(raw),
		Status:         string(enums.ProcessStatusActive),
		TenantID:       e2eTenantID,
		CreatedBy:      "admin",
	}, true)
	require.NoError(e.t, err, "deploy fork/join process with mergeToMap")
}

// deployForkJoinSerialUserTasks 部署 fork → [task_a1 → task_a2, task_b] → join → end
//
// 分支 A 有两个串联的 userTask（顺序审批）。这是串联分支场景：算法取最靠近
// fork 的 task_a1 作为 resume 入口，resume 后 task_a1.OnMsg 应自动驱动到 task_a2。
func (e *e2eTestEnv) deployForkJoinSerialUserTasks(processKey, assigneeA1, assigneeA2, assigneeB string) {
	e.t.Helper()
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id": processKey, "name": processKey, "root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{"id": "fork1", "type": "fork", "name": "Parallel Fork"},
				{
					"id": "task_a1", "type": "userTask", "name": "Branch A Step 1",
					"configuration": map[string]interface{}{
						"candidateType":   "user",
						"candidateConfig": map[string]interface{}{"userIds": []string{assigneeA1}},
						"approvalType":    "single",
					},
				},
				{
					"id": "task_a2", "type": "userTask", "name": "Branch A Step 2",
					"configuration": map[string]interface{}{
						"candidateType":   "user",
						"candidateConfig": map[string]interface{}{"userIds": []string{assigneeA2}},
						"approvalType":    "single",
					},
				},
				{
					"id": "task_b", "type": "userTask", "name": "Branch B",
					"configuration": map[string]interface{}{
						"candidateType":   "user",
						"candidateConfig": map[string]interface{}{"userIds": []string{assigneeB}},
						"approvalType":    "single",
					},
				},
				{
					"id": "join1", "type": "join", "name": "Parallel Join",
					"configuration": map[string]interface{}{"timeout": 5},
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "fork1", "toId": "task_a1", "type": "Success"},
				{"fromId": "fork1", "toId": "task_b", "type": "Success"},
				{"fromId": "task_a1", "toId": "task_a2", "type": "Success"},
				{"fromId": "task_a1", "toId": "task_a2", "type": "Failure"},
				{"fromId": "task_a2", "toId": "join1", "type": "Success"},
				{"fromId": "task_a2", "toId": "join1", "type": "Failure"},
				{"fromId": "task_b", "toId": "join1", "type": "Success"},
				{"fromId": "task_b", "toId": "join1", "type": "Failure"},
				{"fromId": "join1", "toId": "end", "type": "Success"},
			},
		},
	}
	raw, err := json.Marshal(def)
	require.NoError(e.t, err, "marshal serial userTasks def")
	ctx := e.userCtx("admin")
	_, err = e.engine.GetProcessService().Deploy(ctx, service.Actor{UserID: "admin", TenantID: e2eTenantID}, &model.WfProcess{
		ProcessKey:     processKey,
		Name:           processKey,
		DefinitionJSON: string(raw),
		Status:         string(enums.ProcessStatusActive),
		TenantID:       e2eTenantID,
		CreatedBy:      "admin",
	}, true)
	require.NoError(e.t, err, "deploy serial userTasks process")
}

// hiTaskVariables 取某 task_def_key 在 wf_hi_task 中的 variables 字段（JSON 字符串）。
// 实例 Completed 后 wf_task 迁移到 wf_hi_task，可在这里验证最终留存的变量。
func (e *e2eTestEnv) hiTaskVariables(instanceID, defKey string) string {
	e.t.Helper()
	var vars string
	require.NoError(e.t, e.db.Raw(
		"SELECT variables FROM wf_hi_task WHERE process_instance_id = ? AND task_def_key = ? LIMIT 1",
		instanceID, defKey,
	).Scan(&vars).Error)
	return vars
}

// hiTaskVariablesByType 取某 task_type 在 wf_hi_task 中的 variables 字段。
// 用于验证 end 节点的合并变量。
func (e *e2eTestEnv) hiTaskVariablesByType(instanceID, taskType string) string {
	e.t.Helper()
	var vars string
	require.NoError(e.t, e.db.Raw(
		"SELECT variables FROM wf_hi_task WHERE process_instance_id = ? AND task_type = ? LIMIT 1",
		instanceID, taskType,
	).Scan(&vars).Error)
	return vars
}

// ---------------------------------------------------------------------------
// 测试：分支局部变量在 multi-node 恢复后保留
//
// 每个分支用 ExecuteNodeWithMsg 带自己的 task.Variables。join 用
// mergeToMap=true 合并后，wrapperMsg.data 是 {branchId: branchVars} map。
// 若所有分支共享 defaultMsg（instance.Variables），分支局部变量会在 join
// 合并时丢失。
//
// 验证方法：approve 时给两个分支分别设置不同的变量，join mergeToMap=true，
// 完成后检查 end 节点的 hi_task.variables 是否包含两个分支的数据。
// ---------------------------------------------------------------------------
func TestE2E_ParallelForkJoin_BranchVariablesPreserved(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForkJoinWithMergeMap("parallel_branch_vars", "user_a", "user_b")

	instID := env.startInstance("parallel_branch_vars", "starter")
	env.waitForBranchesActive(instID)

	// approve A —— 设置独有变量
	taskA := env.firstActiveTaskIDByDefKey(instID, "task_a")
	require.NotEmpty(t, taskA)
	ctx := service.WithAPICallingMode(service.SetUserToCtx(context.Background(), &service.Actor{
		UserID: "user_a", UserName: "user_a", TenantID: e2eTenantID,
	}))
	require.NoError(t, env.engine.GetTaskService().Approve(ctx, service.Actor{UserID: "user_a", UserName: "user_a", TenantID: e2eTenantID}, taskA, "A ok", map[string]interface{}{
		"a_unique_field": "AAA_VALUE",
		"a_comment":      "approved by A",
	}))

	// 等待 ensure skip 路径稳定（B 还没 approve，应当 forkResumeSkip）
	time.Sleep(300 * time.Millisecond)
	assert.NotEqual(t, string(enums.InstanceStatusCompleted), env.instanceStatus(instID),
		"after only A approves, instance must NOT be completed")

	// approve B —— 设置不同的独有变量
	taskB := env.firstActiveTaskIDByDefKey(instID, "task_b")
	require.NotEmpty(t, taskB)
	ctxB := service.WithAPICallingMode(service.SetUserToCtx(context.Background(), &service.Actor{
		UserID: "user_b", UserName: "user_b", TenantID: e2eTenantID,
	}))
	require.NoError(t, env.engine.GetTaskService().Approve(ctxB, service.Actor{UserID: "user_b", UserName: "user_b", TenantID: e2eTenantID}, taskB, "B ok", map[string]interface{}{
		"b_unique_field": "BBB_VALUE",
		"b_comment":      "approved by B",
	}))

	// 实例应当最终 Completed
	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 3*time.Second, 50*time.Millisecond,
		"instance should complete after both branches approve with distinct variables; status=%q",
		env.instanceStatus(instID))

	// 核心断言 1：两个分支的 hi_task 各自保留独有变量
	aVars := env.hiTaskVariables(instID, "task_a")
	bVars := env.hiTaskVariables(instID, "task_b")
	assert.Contains(t, aVars, "a_unique_field", "task_a's hi_task must carry its branch-local variable")
	assert.Contains(t, aVars, "AAA_VALUE", "task_a's hi_task variable value must be preserved")
	assert.Contains(t, bVars, "b_unique_field", "task_b's hi_task must carry its branch-local variable")
	assert.Contains(t, bVars, "BBB_VALUE", "task_b's hi_task variable value must be preserved")

	// 核心断言 2：end 节点的 hi_task.variables 应该同时包含两个分支的数据
	// 因为 join mergeToMap=true 把各分支输出合并成 {branchId: result} map
	endVars := env.hiTaskVariablesByType(instID, "end")
	assert.Contains(t, endVars, "a_unique_field",
		"end task vars must include branch A's local variable via join mergeToMap; got: %s", endVars)
	assert.Contains(t, endVars, "b_unique_field",
		"end task vars must include branch B's local variable via join mergeToMap; got: %s", endVars)
}

// ---------------------------------------------------------------------------
// 测试：混用分支（一个 suspend + 一个 sync-only）
//
// 拓扑：fork → [task_a(userTask), sync_b(sync node)] → join → end
//
// analyzeForkResume 跳过没有 suspend 节点的 sync_b 分支，
// 只 multi-node 恢复 task_a，流程能正常完成。
//
// 注意：本测试在 e2e 环境下需要可用的同步节点组件。当前测试 env 注册的组件中
// ccTask 在 mock identity service 下不创建任务，serviceTask 需要 function 配置。
// 因此混用分支算法的正确性通过单元测试覆盖（见 service/fork_aware_resume_test.go 的
// TestAnalyzeForkResume_MixedBranches_* 用例）。
//
// 如未来需要 e2e 验证，请：
//  1. 在测试 env 中注册一个 noop 同步节点
//  2. 调整 deployForkJoinMixedBranches 用这个 noop 节点
//  3. 启用本测试
//
// ---------------------------------------------------------------------------
func TestE2E_ParallelForkJoin_MixedSuspendAndSyncBranches(t *testing.T) {
	t.Skip("L2 mixed-branches e2e test requires a working sync-only node component; " +
		"covered by unit tests in fork_aware_resume_test.go until then")
}

// ---------------------------------------------------------------------------
// 测试 L4：一条分支里有多个串联的 userTask
//
// 拓扑：fork → [task_a1 → task_a2, task_b] → join → end
//
// 之前未实测的限制。算法取最靠近 fork 的 task_a1 作为 resume 入口，
// task_a1.OnMsg 看到已 completed → TellSuccess → task_a2.OnMsg。
// task_a2 也是 userTask，approve 完之后 → TellSuccess → join。
// ---------------------------------------------------------------------------
func TestE2E_ParallelForkJoin_SerialUserTasksInBranch(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForkJoinSerialUserTasks("parallel_serial", "user_a1", "user_a2", "user_b")

	instID := env.startInstance("parallel_serial", "starter")

	// 初始 OnMsg 后：task_a1（不是 task_a2）和 task_b 各有 1 条 Active
	require.Eventually(t, func() bool {
		return env.activeTaskCountByDefKey(instID, "task_a1") == 1 &&
			env.activeTaskCountByDefKey(instID, "task_b") == 1
	}, 2*time.Second, 50*time.Millisecond, "task_a1 and task_b should be active after initial OnMsg")

	// task_a2 此时还不应该有 Active 任务（要等 task_a1 完成才创建）
	assert.Equal(t, 0, env.activeTaskCountByDefKey(instID, "task_a2"),
		"task_a2 should NOT have active tasks before task_a1 is approved")

	// approve task_a1 —— 应当自动创建 task_a2 任务
	taskA1 := env.firstActiveTaskIDByDefKey(instID, "task_a1")
	require.NotEmpty(t, taskA1)
	env.approveAs(taskA1, "user_a1", "a1 ok")

	// task_a2 应当出现 Active 任务（顺序审批的自然推进）
	require.Eventually(t, func() bool {
		return env.activeTaskCountByDefKey(instID, "task_a2") == 1
	}, 2*time.Second, 50*time.Millisecond, "task_a2 should become active after task_a1 is approved")

	// approve task_b（顺序无关）—— 不应触发完成，因为 task_a2 还没 approve
	taskB := env.firstActiveTaskIDByDefKey(instID, "task_b")
	require.NotEmpty(t, taskB)
	env.approveAs(taskB, "user_b", "b ok")

	time.Sleep(300 * time.Millisecond)
	assert.NotEqual(t, string(enums.InstanceStatusCompleted), env.instanceStatus(instID),
		"after task_a1 + task_b approve, instance must NOT be completed (task_a2 still pending)")

	// approve task_a2 —— 现在所有 suspend 节点都 completed，应触发 multi-node 恢复
	taskA2 := env.firstActiveTaskIDByDefKey(instID, "task_a2")
	require.NotEmpty(t, taskA2)
	env.approveAs(taskA2, "user_a2", "a2 ok")

	// 实例最终 Completed
	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 3*time.Second, 50*time.Millisecond,
		"L4: serial userTasks in branch should complete after all are approved; status=%q",
		env.instanceStatus(instID))

	// end 节点只执行 1 次
	var endCount int64
	require.NoError(t, env.db.Raw(
		"SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_type = 'end'",
		instID).Scan(&endCount).Error)
	assert.Equal(t, int64(1), endCount,
		"end node should run exactly once for serial-userTask branch fork-join")

	// 每个 userTask 都归档了（hi_task 各 1 条）
	for _, defKey := range []string{"task_a1", "task_a2", "task_b"} {
		var n int64
		require.NoError(t, env.db.Raw(
			"SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_def_key = ?",
			instID, defKey,
		).Scan(&n).Error)
		assert.Equal(t, int64(1), n,
			fmt.Sprintf("%s should have exactly 1 archived hi_task record", defKey))
	}
}
