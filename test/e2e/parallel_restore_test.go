// Package e2e 同包下的并行恢复测试。
//
// 这些用例覆盖 Fork → [userTask_A, userTask_B] → Join → End 流程在
// RestoreProcessInstance 下的恢复行为，重点回归以下场景：
//   - 恢复后 wf_task 表不出现重复 TaskDefKey 记录
//     （RestoreProcessInstance 注入 task_id + TaskCreator 检测到已存在时跳过 CreateTask）
//   - 恢复后所有并行分支的 userTask 仍处于 Active，可继续审批
//
// 已知限制：gflow-engine 当前架构下，approve 后用 ExecuteNext(startNode=userTask)
// 重启节点会丢失 fork 父上下文，导致 join 节点的 TellCollect 永远收不齐，
// 实例无法 completed。这个问题不是恢复路径引入的（baseline 不调 restore 也复现），
// 影响 "fork + 任何暂停型节点（userTask/AIAgent 同步/Delay）" 组合。
// 详见 docs/parallel-limitations.md。
//
// 因此本文件不验证"恢复后实例能 completed"，只验证恢复本身不破坏 wf_task 数据
// 与 userTask 的 Active 状态。
package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/enums"
)

// deployForkJoinProcess 部署一个 Fork → [userTask_A, userTask_B] → Join → End
// 的并发流程。两条分支各有一个 single 审批的 userTask。
func (e *e2eTestEnv) deployForkJoinProcess(processKey, assigneeA, assigneeB string) {
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
					"configuration": map[string]interface{}{"timeout": 5},
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
	require.NoError(e.t, err, "marshal fork/join def")
	ctx := e.userCtx("admin")
	_, err = e.engine.GetProcessService().Deploy(ctx, service.Actor{UserID: "admin", TenantID: e2eTenantID}, &model.WfProcess{
		ProcessKey:     processKey,
		Name:           processKey,
		DefinitionJSON: string(raw),
		Status:         string(enums.ProcessStatusActive),
		TenantID:       e2eTenantID,
		CreatedBy:      "admin",
	}, true)
	require.NoError(e.t, err, "deploy fork/join process")
}

// activeTaskCountByDefKey 统计某 TaskDefKey 的 Active 任务数。
func (e *e2eTestEnv) activeTaskCountByDefKey(instanceID, defKey string) int {
	e.t.Helper()
	var n int64
	require.NoError(e.t, e.db.Raw(
		"SELECT COUNT(*) FROM wf_task WHERE process_instance_id = ? AND task_def_key = ? AND status = ?",
		instanceID, defKey, string(enums.TaskStatusActive),
	).Scan(&n).Error)
	return int(n)
}

// allTaskCountByDefKey 统计某 TaskDefKey 的所有状态任务数（含 Completed、Pending 等）。
func (e *e2eTestEnv) allTaskCountByDefKey(instanceID, defKey string) int {
	e.t.Helper()
	var n int64
	require.NoError(e.t, e.db.Raw(
		"SELECT COUNT(*) FROM wf_task WHERE process_instance_id = ? AND task_def_key = ?",
		instanceID, defKey,
	).Scan(&n).Error)
	return int(n)
}

// firstActiveTaskIDByDefKey 取某 TaskDefKey 的 Active task 的 ID。
func (e *e2eTestEnv) firstActiveTaskIDByDefKey(instanceID, defKey string) string {
	e.t.Helper()
	var id string
	require.NoError(e.t, e.db.Raw(
		"SELECT id FROM wf_task WHERE process_instance_id = ? AND task_def_key = ? AND status = ? LIMIT 1",
		instanceID, defKey, string(enums.TaskStatusActive),
	).Scan(&id).Error)
	return id
}

// restoreInstance 调用 RuntimeService.RestoreProcessInstance 模拟重启恢复。
// 注意：直接调单实例恢复接口，绕过 RestoreAllProcessInstances 的 tenant 过滤。
func (e *e2eTestEnv) restoreInstance(instanceID string) {
	e.t.Helper()
	require.NoError(e.t, e.engine.GetRuntimeService().RestoreProcessInstance(
		context.Background(), service.SystemActor(), instanceID,
	), "restore process instance")
}

// waitForBranchesActive 等到 task_a 和 task_b 各有 1 条 Active 任务。
func (e *e2eTestEnv) waitForBranchesActive(instanceID string) {
	e.t.Helper()
	require.Eventually(e.t, func() bool {
		return e.activeTaskCountByDefKey(instanceID, "task_a") == 1 &&
			e.activeTaskCountByDefKey(instanceID, "task_b") == 1
	}, 2*time.Second, 50*time.Millisecond, "branches A/B should both become active")
}

// ---------------------------------------------------------------------------
// 测试 1：恢复后 wf_task 不出现重复 TaskDefKey 记录。
// 若不注入 task_id 让 TaskCreator 跳过已存在任务，恢复会让每个分支多出一条
// Pending/Active 记录，旧的 wf_task 永远停在那成僵尸数据。
// ---------------------------------------------------------------------------
func TestE2E_ParallelRestore_NoDuplicateTasks(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForkJoinProcess("parallel_dup", "user_a", "user_b")

	instID := env.startInstance("parallel_dup", "starter")
	env.waitForBranchesActive(instID)

	// 记录恢复前的 task_id
	idABefore := env.firstActiveTaskIDByDefKey(instID, "task_a")
	idBBefore := env.firstActiveTaskIDByDefKey(instID, "task_b")
	require.NotEmpty(t, idABefore)
	require.NotEmpty(t, idBBefore)

	// 模拟重启
	env.restoreInstance(instID)
	time.Sleep(300 * time.Millisecond)

	// 核心断言：A、B 各只有 1 条 Active，全表也只有那 1 条（无重复）
	assert.Equal(t, 1, env.activeTaskCountByDefKey(instID, "task_a"),
		"task_a should have exactly 1 Active record after restore")
	assert.Equal(t, 1, env.activeTaskCountByDefKey(instID, "task_b"),
		"task_b should have exactly 1 Active record after restore")
	assert.Equal(t, 1, env.allTaskCountByDefKey(instID, "task_a"),
		"task_a should have exactly 1 record in total (no duplicate)")
	assert.Equal(t, 1, env.allTaskCountByDefKey(instID, "task_b"),
		"task_b should have exactly 1 record in total (no duplicate)")

	// 恢复前后 task_id 必须一致（旧 task 复用，不是新建）
	assert.Equal(t, idABefore, env.firstActiveTaskIDByDefKey(instID, "task_a"),
		"task_a id must be unchanged across restore")
	assert.Equal(t, idBBefore, env.firstActiveTaskIDByDefKey(instID, "task_b"),
		"task_b id must be unchanged across restore")
}

// ---------------------------------------------------------------------------
// 测试 2：恢复后所有并行分支的 userTask 仍处于 Active 等待状态。
// 验证恢复不会"丢"任何分支——重启后用户还能在 UI 上看到自己的待办。
// ---------------------------------------------------------------------------
func TestE2E_ParallelRestore_AllBranchesActive(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForkJoinProcess("parallel_active", "user_a", "user_b")
	instID := env.startInstance("parallel_active", "starter")
	env.waitForBranchesActive(instID)

	env.restoreInstance(instID)
	time.Sleep(300 * time.Millisecond)

	// 两个分支都还能查到 Active 任务（没丢）
	assert.Len(t, env.activeTasksFor(instID, "user_a"), 1,
		"branch A still has 1 active task after restore")
	assert.Len(t, env.activeTasksFor(instID, "user_b"), 1,
		"branch B still has 1 active task after restore")

	// 实例状态仍是 active（恢复不应误触发完成或终止）
	assert.Equal(t, string(enums.InstanceStatusActive), env.instanceStatus(instID),
		"instance should still be active after restore")
}
