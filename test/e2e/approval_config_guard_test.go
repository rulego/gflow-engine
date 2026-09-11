// Package e2e — 审批配置防护的端到端测试：部署期校验拦截、驳回兜底路由、会签超时口径。
//
// 覆盖场景：
//   - userTask 缺 configuration：部署期拦截（空配置运行期 no assignees 才炸）
//   - reject.strategy=toNode 目标节点不存在：部署期拦截（防静默吞票/卡死）
//   - 驳回跳转失败兜底：节点只有 Failure 出边时必须沿 Failure 边路由（不得静默丢消息卡 active）
//   - 驳回跳转失败且无 Reject/Failure 出边：兜底终止实例
//   - all 会签子任务继承 due_date（timeout 配置必须作用到实际办理的子任务）
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

// deployRawExpectError 部署 DSL 并返回错误（用于部署期校验拦截用例）。
func (e *e2eTestEnv) deployRawExpectError(processKey, name string, def map[string]interface{}) error {
	e.t.Helper()
	raw, err := json.Marshal(def)
	require.NoError(e.t, err, "marshal def")
	_, err = e.engine.GetProcessService().Deploy(e.userCtx("admin"), service.Actor{
		UserID: "admin", TenantID: e2eTenantID, WorkflowAdmin: true,
	}, &model.WfProcess{
		ProcessKey:     processKey,
		Name:           name,
		DefinitionJSON: string(raw),
		Status:         string(enums.ProcessStatusActive),
		TenantID:       e2eTenantID,
		CreatedBy:      "admin",
	}, true)
	return err
}

// userTask 完全不写 configuration：空配置是最需要拦截的形态，运行期才炸是
// no assignees 的误导性错误。
func TestE2E_DeployValidation_UserTaskWithoutConfiguration(t *testing.T) {
	env := newE2EEnv(t)
	err := env.deployRawExpectError("cfg_nil_e2e", "空配置拦截", map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": "cfg_nil_e2e", "name": "空配置拦截", "root": true},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{"id": "approval_node", "type": "userTask", "name": "审批"},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "approval_node", "toId": "end", "type": "Success"},
			},
		},
	})
	require.Error(t, err, "userTask without configuration must fail deployment")
	assert.ErrorIs(t, err, service.ErrValidation)
	assert.Contains(t, err.Error(), "approver.type", "error should point at the approver config")
}

// reject.strategy=toNode 指向不存在的节点：部署期拦截（运行期兜底是降级路径，
// 配错应该在写入前被发现）。
func TestE2E_DeployValidation_RejectToNodeGhostTarget(t *testing.T) {
	env := newE2EEnv(t)
	err := env.deployRawExpectError("cfg_ghost_e2e", "悬空驳回目标拦截", map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": "cfg_ghost_e2e", "name": "悬空驳回目标拦截", "root": true},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{
					"id": "approval_node", "type": "userTask", "name": "审批",
					"configuration": map[string]interface{}{
						"approver":    map[string]interface{}{"type": "user", "userIds": []string{"u1"}},
						"approveMode": "single",
						"reject":      map[string]interface{}{"strategy": "toNode", "target": "ghost_node"},
					},
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "approval_node", "toId": "end", "type": "Success"},
			},
		},
	})
	require.Error(t, err, "toNode with ghost target must fail deployment")
	assert.ErrorIs(t, err, service.ErrValidation)
	assert.Contains(t, err.Error(), "reject.target", "error should point at the reject target")
}

// 部署一个带 Failure 出边到兜底审批节点的流程，用于驳回跳转失败的路由测试。
//
//	first_task(u1, reject=toPrev 首节点必失败) --Success/Failure--> rescue_task(u2) --> end
func (e *e2eTestEnv) deployRejectFallbackProcess(processKey string, withFailureEdge bool) {
	e.t.Helper()
	conns := []map[string]interface{}{
		{"fromId": "first_task", "toId": "rescue_task", "type": "Success"},
		{"fromId": "rescue_task", "toId": "end", "type": "Success"},
	}
	if withFailureEdge {
		conns = append(conns, map[string]interface{}{"fromId": "first_task", "toId": "rescue_task", "type": "Failure"})
	}
	e.deployRawProcess(processKey, processKey, "驳回兜底路由", map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": processKey, "name": "驳回兜底路由", "root": true},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{
					"id": "first_task", "type": "userTask", "name": "首节点审批",
					"configuration": map[string]interface{}{
						"approver":    map[string]interface{}{"type": "user", "userIds": []string{"u1"}},
						"approveMode": "single",
						// 首节点没有上一个 userTask，跳转必失败，进入兜底路径
						"reject": map[string]interface{}{"strategy": "toPrev"},
					},
				},
				{
					"id": "rescue_task", "type": "userTask", "name": "兜底审批",
					"configuration": map[string]interface{}{
						"approver":    map[string]interface{}{"type": "user", "userIds": []string{"u2"}},
						"approveMode": "single",
					},
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": conns,
		},
	})
}

// 驳回跳转失败 + 节点只有 Failure 出边：消息必须沿 Failure 边路由到下游节点，
// 不能被静默丢弃让实例永久卡 active。
func TestE2E_RejectFallback_RoutesAlongFailureEdge(t *testing.T) {
	env := newE2EEnv(t)
	env.deployRejectFallbackProcess("reject_fb_edge_e2e", true)

	instID := env.startInstance("reject_fb_edge_e2e", "starter")
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "u1")) == 1
	}, 3*time.Second, 50*time.Millisecond, "first task should be created")

	env.rejectAs(env.activeTasksFor(instID, "u1")[0].ID, "u1", "toPrev 在首节点必失败，走兜底")

	// 核心断言：Failure 出边被真正路由——兜底审批节点收到任务，实例保持可推进
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "u2")) == 1
	}, 3*time.Second, 50*time.Millisecond,
		"reject fallback must route along the Failure edge instead of dropping the message")

	rescue := env.activeTasksFor(instID, "u2")[0]
	env.approveAs(rescue.ID, "u2", "兜底通过")
	require.Eventually(t, func() bool {
		st := env.instanceStatus(instID)
		return st == string(enums.InstanceStatusCompleted)
	}, 3*time.Second, 50*time.Millisecond, "instance should complete after rescue approval")
}

// 驳回跳转失败 + 无 Reject/Failure 出边：兜底终止实例，不得卡 active。
func TestE2E_RejectFallback_NoEdge_Terminates(t *testing.T) {
	env := newE2EEnv(t)
	env.deployRejectFallbackProcess("reject_fb_term_e2e", false)

	instID := env.startInstance("reject_fb_term_e2e", "starter")
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "u1")) == 1
	}, 3*time.Second, 50*time.Millisecond, "first task should be created")

	env.rejectAs(env.activeTasksFor(instID, "u1")[0].ID, "u1", "无出边兜底应终止")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusTerminated)
	}, 3*time.Second, 50*time.Millisecond, "no-edge fallback must terminate the instance")
}

// all 会签子任务必须继承 due_date：逾期扫描与超时动作作用在子任务上，
// 子任务缺 due_date 等于 timeout 配置整体落空。
func TestE2E_CountersignSubTasks_InheritDueDate(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("cnt_due_e2e", "会签超时", "all",
		[]string{"a_user", "b_user", "c_user"},
		map[string]interface{}{
			"timeout": map[string]interface{}{"dueInMinutes": 30, "action": "remind"},
		})

	instID := env.startInstance("cnt_due_e2e", "starter")
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "")) >= 3
	}, 3*time.Second, 50*time.Millisecond, "countersign sub tasks should be created")

	var nullDue int64
	require.NoError(t, env.db.Raw(
		"SELECT COUNT(*) FROM wf_task WHERE process_instance_id = ? AND due_date IS NULL", instID).
		Scan(&nullDue).Error)
	assert.Zero(t, nullDue, "all countersign tasks (parent and sub tasks) must carry due_date")

	for _, task := range env.activeTasksFor(instID, "") {
		require.NotNil(t, task.DueDate, "sub task %s must inherit due_date", task.ID)
		assert.WithinDuration(t, time.Now().Add(30*time.Minute), *task.DueDate, 2*time.Minute,
			"due_date should be ~30min from creation")
	}
}
