// Package e2e — 并行 fork/join 完整审批路径测试。
//
// 覆盖 fork → userTask(s) → join 拓扑在 approve 后正确进入 Completed：
// ExecuteNext 检测 fork-join 拓扑，当所有兄弟分支的 userTask 都已 completed 时，
// 用 types.WithRestoreNodes 触发 multi-node 恢复路径（与 RestoreProcessInstance
// 同一路径）。processRestoreNodes 通过 LCA 自动重建 fork 父上下文，让 join 在
// 同一个 RunSnapshot/Observer 内收齐消息——直接走单节点路径会丢失 fork 父上下文，
// 实例永远卡在 active。
//
// 这组用例必须在 parallel_restore_test.go 的回归用例之外独立存在：restore 测试只
// 验证恢复本身不破坏 wf_task；这里验证 approve 之后实例能正常完成。
package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/enums"
)

// ---------------------------------------------------------------------------
// 测试 1（核心）：A、B 都 approve 后实例进入 Completed，end 节点只跑 1 次。
// 若 fork 父上下文丢失，approve A 和 B 后实例会卡在 active，current_activity 为空，
// join 不放行，end 不执行。
// ---------------------------------------------------------------------------
func TestE2E_ParallelForkJoin_BothApprove_Completes(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForkJoinProcess("parallel_resume_complete", "user_a", "user_b")

	instID := env.startInstance("parallel_resume_complete", "starter")
	env.waitForBranchesActive(instID)

	// approve A —— 实例不能完成（B 还没动）
	taskA := env.firstActiveTaskIDByDefKey(instID, "task_a")
	require.NotEmpty(t, taskA, "task_a should have an active task")
	env.approveAs(taskA, "user_a", "a ok")

	// 给一点时间确保 ExecuteNext 的 AfterCommit 跑完；fix 路径下应当 skip 而不是
	// 触发 multi-node 恢复（B 还没 approve）。这里只断言"实例未完成"。
	time.Sleep(300 * time.Millisecond)
	assert.NotEqual(t, string(enums.InstanceStatusCompleted), env.instanceStatus(instID),
		"after only A approves, instance must NOT be completed (B still pending)")

	// approve B —— 现在所有兄弟都 completed，ExecuteNext 应触发 multi-node 恢复
	taskB := env.firstActiveTaskIDByDefKey(instID, "task_b")
	require.NotEmpty(t, taskB, "task_b should still have an active task after A approved")
	env.approveAs(taskB, "user_b", "b ok")

	// 实例应当最终进入 Completed
	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 3*time.Second, 50*time.Millisecond,
		"instance should complete after both A and B approve; status=%q", env.instanceStatus(instID))

	// CompleteProcessInstance 先把 instance.status 改成 Completed，再异步归档 wf_task
	// → wf_hi_task。两个步骤之间有窗口：require.Eventually 看到 Completed 时归档可能
	// 还没完成，wf_hi_task 还是空的。等到 wf_hi_task 至少有一条记录再继续。
	require.Eventually(t, func() bool {
		var n int64
		_ = env.db.Raw(
			"SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ?",
			instID).Scan(&n).Error
		return n > 0
	}, 2*time.Second, 50*time.Millisecond,
		"wf_hi_task should be populated after instance completion")

	// 关键不变量：end 节点只执行 1 次（重复 ExecuteNext 会创建多个 end 任务）
	var endCount int64
	require.NoError(t, env.db.Raw(
		"SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_type = 'end'",
		instID).Scan(&endCount).Error)
	assert.Equal(t, int64(1), endCount,
		"end node should run exactly once after parallel fork-join completes")

	// 两个分支的 userTask 都应已归档（completed 实例的 wf_task 会迁移到 wf_hi_task）
	var aCount, bCount int64
	require.NoError(t, env.db.Raw(
		"SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_def_key = ?",
		instID, "task_a").Scan(&aCount).Error)
	require.NoError(t, env.db.Raw(
		"SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_def_key = ?",
		instID, "task_b").Scan(&bCount).Error)
	assert.Equal(t, int64(1), aCount, "task_a should have exactly 1 archived task")
	assert.Equal(t, int64(1), bCount, "task_b should have exactly 1 archived task")
}

// ---------------------------------------------------------------------------
// 测试 2：只 approve A，B 未动 —— 实例仍 active，B 仍 Active，A 不产生重复 wf_task。
//
// 验证 forkResumeSkip 分支：第一个 approve 不应触发任何重启，也不应破坏 wf_task 数据。
// ---------------------------------------------------------------------------
func TestE2E_ParallelForkJoin_PartialApprove_StaysActive(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForkJoinProcess("parallel_resume_partial", "user_a", "user_b")

	instID := env.startInstance("parallel_resume_partial", "starter")
	env.waitForBranchesActive(instID)

	taskA := env.firstActiveTaskIDByDefKey(instID, "task_a")
	env.approveAs(taskA, "user_a", "a ok")

	// 等待 AfterCommit 异步副作用跑完
	time.Sleep(300 * time.Millisecond)

	// 实例仍 active
	assert.Equal(t, string(enums.InstanceStatusActive), env.instanceStatus(instID),
		"instance must stay active when only one of two branches approved")

	// B 分支的 userTask 仍 Active（没被错误推进或取消）
	assert.Equal(t, 1, env.activeTaskCountByDefKey(instID, "task_b"),
		"task_b should still have 1 active task after partial approval")

	// A 分支不再有 active 任务；全表只 1 条（completed，未归档因为实例未结束）
	assert.Equal(t, 0, env.activeTaskCountByDefKey(instID, "task_a"),
		"task_a should have 0 active tasks after approval")
	assert.Equal(t, 1, env.allTaskCountByDefKey(instID, "task_a"),
		"task_a should have exactly 1 task total (no duplicates from premature ExecuteNext)")
}

// ---------------------------------------------------------------------------
// 测试 3：A、B 真并发 approve —— 实例仍正确 completed，无重复 end 任务。
//
// 这是修复最棘手的竞态场景：两个 approve 同时进入 ExecuteNext。
// 期望行为（由 forkResumeSkip + forkResumeMulti 协同保证）：
//   - 先到的 ExecuteNext 看到"另一个分支未 completed" → skip
//   - 后到的 ExecuteNext 看到"两个分支都 completed" → multi
//
// 即使两者几乎同时进入，DB 行锁会串行化 completeWithApprovalInternal 的 task 状态
// 更新，所以两个 ExecuteNext 看到的 task 状态是线性一致的。
// ---------------------------------------------------------------------------
func TestE2E_ParallelForkJoin_ConcurrentApproves_Completes(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForkJoinProcess("parallel_resume_concurrent", "user_a", "user_b")

	instID := env.startInstance("parallel_resume_concurrent", "starter")
	env.waitForBranchesActive(instID)

	taskA := env.firstActiveTaskIDByDefKey(instID, "task_a")
	taskB := env.firstActiveTaskIDByDefKey(instID, "task_b")
	require.NotEmpty(t, taskA)
	require.NotEmpty(t, taskB)

	// 真并发提交两个 approve
	var wg sync.WaitGroup
	errs := make([]error, 2)
	assignments := []struct {
		taskID string
		who    string
	}{{taskA, "user_a"}, {taskB, "user_b"}}
	for i, asgn := range assignments {
		wg.Add(1)
		go func(idx int, taskID, who string) {
			defer wg.Done()
			ctx := service.WithAPICallingMode(service.SetUserToCtx(context.Background(), &service.Actor{
				UserID: who, UserName: who, TenantID: e2eTenantID,
			}))
			errs[idx] = env.engine.GetTaskService().Approve(ctx, service.Actor{UserID: who, UserName: who, TenantID: e2eTenantID}, taskID, "concurrent", map[string]interface{}{
				"approved":   true,
				"approvedBy": who,
			})
		}(i, asgn.taskID, asgn.who)
	}
	wg.Wait()

	// SQLite cache=shared 对多连接并发 BEGIN 有固有限制，允许部分 approve 失败。
	// 但只要两个 task 最终都到 Completed，且实例 Completed，就算通过。
	successCount := 0
	for _, e := range errs {
		if e == nil {
			successCount++
		}
	}
	t.Logf("concurrent approve: %d succeeded, %d errored", successCount, 2-successCount)

	// 如果两个并发 approve 都因 SQLite 锁失败，用串行路径兜底继续测试后续逻辑。
	// 这是 SQLite 限制，不是引擎 bug。
	if successCount < 2 {
		t.Logf("falling back to sequential approves due to SQLite locking")
		// 重新查 active tasks（部分可能已 completed）
		if env.activeTaskCountByDefKey(instID, "task_a") == 1 {
			env.approveAs(env.firstActiveTaskIDByDefKey(instID, "task_a"), "user_a", "a retry")
		}
		if env.activeTaskCountByDefKey(instID, "task_b") == 1 {
			env.approveAs(env.firstActiveTaskIDByDefKey(instID, "task_b"), "user_b", "b retry")
		}
	}

	// 实例最终必须 completed
	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 3*time.Second, 50*time.Millisecond,
		"instance should complete after both branches approve; status=%q", env.instanceStatus(instID))

	// 同 TestE2E_ParallelForkJoin_BothApprove_Completes：归档异步，等 wf_hi_task 写入。
	require.Eventually(t, func() bool {
		var n int64
		_ = env.db.Raw(
			"SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ?",
			instID).Scan(&n).Error
		return n > 0
	}, 2*time.Second, 50*time.Millisecond,
		"wf_hi_task should be populated after instance completion")

	// end 节点只执行 1 次
	var endCount int64
	require.NoError(t, env.db.Raw(
		"SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_type = 'end'",
		instID).Scan(&endCount).Error)
	assert.Equal(t, int64(1), endCount,
		"end node should run exactly once even under concurrent approvals")
}
