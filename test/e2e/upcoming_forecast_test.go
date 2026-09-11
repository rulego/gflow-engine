package e2e

// 后续审批人预测 e2e：审批详情的 upcoming 字段应沿活跃节点向前解析出
// 依次待执行的审批人；发起人自选节点不展开具体人员。

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/dto"
)

// deployForecastProcess 部署 A → B(initiatorSelect) → C(dept) 的顺序链。
func (e *e2eTestEnv) deployForecastProcess(processKey string) {
	e.t.Helper()
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": processKey, "name": "forecast", "root": true},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{"id": "node_a", "type": "userTask", "name": "A审批", "configuration": map[string]interface{}{
					"approver":    map[string]interface{}{"type": "user", "userIds": []string{"fc_a"}},
					"approveMode": "sequential",
				}},
				{"id": "node_b", "type": "userTask", "name": "B自选", "configuration": map[string]interface{}{
					"approver":    map[string]interface{}{"type": "initiatorSelect", "expression": "${msg.picked}"},
					"approveMode": "single",
				}},
				{"id": "node_c", "type": "userTask", "name": "C部门", "configuration": map[string]interface{}{
					"approver":    map[string]interface{}{"type": "dept", "deptIds": []string{"dept-fc"}},
					"approveMode": "single",
				}},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "node_a", "toId": "node_b", "type": "Success"},
				{"fromId": "node_b", "toId": "node_c", "type": "Success"},
				{"fromId": "node_c", "toId": "end", "type": "Success"},
			},
		},
	}
	e.deployRawProcess(processKey, processKey, "forecast", def)
}

func (e *e2eTestEnv) upcomingFor(instanceID string) []dto.UpcomingNode {
	detail, err := e.engine.GetRuntimeService().GetProcessInstanceDetail(
		e.userCtx("starter"), service.Actor{UserID: "starter", TenantID: e2eTenantID}, instanceID)
	require.NoError(e.t, err, "instance detail")
	return detail.Upcoming
}

func TestE2E_UpcomingForecast(t *testing.T) {
	env := newE2EEnv(t)
	env.deployForecastProcess("fc_e2e")

	// 预置部门成员：dept-fc → fc_c1/fc_c2
	identity, ok := env.engine.GetIdentityService().(*service.IdentityServiceImpl)
	require.True(t, ok, "identity service should be the mock impl")
	identity.AddMockUser(&service.User{ID: "fc_c1", TenantID: e2eTenantID})
	identity.AddMockUser(&service.User{ID: "fc_c2", TenantID: e2eTenantID})
	identity.AddMockUserDepartment("fc_c1", "dept-fc")
	identity.AddMockUserDepartment("fc_c2", "dept-fc")

	instanceID := env.startInstanceWithVars("fc_e2e", "starter", map[string]interface{}{"picked": []string{"fc_b"}})
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "fc_a")) > 0 },
		2*time.Second, 50*time.Millisecond, "node_a task should be created")

	// A 活跃时：upcoming = B（发起人自选按流程变量解析）+ C（部门展开成员）
	upcoming := env.upcomingFor(instanceID)
	require.Len(t, upcoming, 2, "A 之后应预测到 B、C 两个节点")
	assert.Equal(t, "node_b", upcoming[0].NodeID)
	assert.Equal(t, []string{"fc_b"}, upcoming[0].Assignees, "发起人自选按变量解析")
	assert.Empty(t, upcoming[0].Unresolved)
	assert.Equal(t, "node_c", upcoming[1].NodeID)
	assert.ElementsMatch(t, []string{"fc_c1", "fc_c2"}, upcoming[1].Assignees, "部门节点应展开成员")

	// A 通过 → B 活跃：upcoming 只剩 C
	env.approveAs(env.activeTasksFor(instanceID, "fc_a")[0].ID, "fc_a", "a 通过")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "")) > 0 },
		2*time.Second, 50*time.Millisecond, "B 应产生任务（发起人自选落定后建任务）")
	upcoming = env.upcomingFor(instanceID)
	require.Len(t, upcoming, 1)
	assert.Equal(t, "node_c", upcoming[0].NodeID)
	assert.ElementsMatch(t, []string{"fc_c1", "fc_c2"}, upcoming[1-1].Assignees)
}
