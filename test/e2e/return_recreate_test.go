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

// 回退后目标节点任务必须重建，退回人出局。
func TestE2E_Return_RecreatesTargetTask(t *testing.T) {
	env := newE2EEnv(t)

	permissions := map[string]interface{}{
		"return": true,
	}
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id": "ret_recreate_e2e", "name": "Return Recreate", "root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{"id": "node_a", "type": "userTask", "name": "A审批", "configuration": map[string]interface{}{
					"candidateType": "user", "candidateConfig": map[string]interface{}{"userIds": []string{"user_a"}},
					"approvalType": "single",
				}, "additionalInfo": map[string]interface{}{"actionPermissions": permissions}},
				{"id": "node_b", "type": "userTask", "name": "B审批", "configuration": map[string]interface{}{
					"candidateType": "user", "candidateConfig": map[string]interface{}{"userIds": []string{"user_b"}},
					"approvalType": "single",
				}, "additionalInfo": map[string]interface{}{"actionPermissions": permissions}},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "node_a", "toId": "node_b", "type": "Success"},
				{"fromId": "node_b", "toId": "end", "type": "Success"},
			},
		},
	}
	raw, err := json.Marshal(def)
	require.NoError(t, err, "marshal def")

	_, err = env.engine.GetProcessService().Deploy(env.userCtx("admin"), service.Actor{UserID: "admin", TenantID: e2eTenantID, WorkflowAdmin: true}, &model.WfProcess{
		ProcessKey:     "ret_recreate_e2e",
		Name:           "Return Recreate",
		DefinitionJSON: string(raw),
		Status:         string(enums.ProcessStatusActive),
		TenantID:       e2eTenantID,
		CreatedBy:      "admin",
	}, true)
	require.NoError(t, err, "deploy process")

	instanceID := env.startInstance("ret_recreate_e2e", "starter_001")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a")) > 0 },
		2*time.Second, 50*time.Millisecond, "node_a task should be created")

	tasksA := env.activeTasksFor(instanceID, "user_a")
	require.Len(t, tasksA, 1)
	env.approveAs(tasksA[0].ID, "user_a", "a 同意")

	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_b")) > 0 },
		2*time.Second, 50*time.Millisecond, "node_b task should be created after A approves")
	tasksB := env.activeTasksFor(instanceID, "user_b")
	require.Len(t, tasksB, 1)

	// user_b 退回 node_a
	err = env.engine.GetTaskService().Return(env.userCtxAs("user_b"), service.Actor{UserID: "user_b", TenantID: e2eTenantID}, tasksB[0].ID, "node_a", "材料不齐退回")
	require.NoError(t, err, "return should succeed")

	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_a")) > 0 },
		3*time.Second, 50*time.Millisecond, "node_a task must be recreated after return")
	assert.Empty(t, env.activeTasksFor(instanceID, "user_b"), "return 后 B 不应再有活跃任务")

	// 重新办理后流程走完
	tasksA2 := env.activeTasksFor(instanceID, "user_a")
	require.Len(t, tasksA2, 1)
	env.approveAs(tasksA2[0].ID, "user_a", "补充后同意")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "user_b")) > 0 },
		2*time.Second, 50*time.Millisecond, "B task recreated after A re-approves")
	tasksB2 := env.activeTasksFor(instanceID, "user_b")
	env.approveAs(tasksB2[0].ID, "user_b", "b 同意")
	require.Eventually(t, func() bool { return env.instanceStatus(instanceID) == "completed" },
		2*time.Second, 50*time.Millisecond, "instance should complete after re-approval")
}
