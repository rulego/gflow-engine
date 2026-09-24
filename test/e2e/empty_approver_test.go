package e2e

// 空审批人兜底（emptyApproverPolicy）e2e：审批节点解析不到审批人时不再失败终止
// 实例，按节点级策略分流——转租户管理员（单人直派/多人代认领）、自动通过（锁外
// 系统审批管道推进）、挂起待指派（候选池非空不变式）。另覆盖强制改派的系统评论留痕。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/enums"
)

// deployEmptyApproverProcess 部署单节点角色审批流程（角色固定无成员），
// policy 为空串时不写 emptyApproverPolicy（验证缺省行为）。
func (e *e2eTestEnv) deployEmptyApproverProcess(processKey, policy string) {
	e.t.Helper()
	configuration := map[string]interface{}{
		"approver": map[string]interface{}{
			"type":    "role",
			"roleIds": []string{"role-no-members"},
		},
		"approveMode": "single",
	}
	if policy != "" {
		configuration["emptyApproverPolicy"] = policy
	}
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id":   processKey,
			"name": processKey,
			"root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{"id": "approval_node", "type": "userTask", "name": "审批", "configuration": configuration},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "approval_node", "toId": "end", "type": "Success"},
				{"fromId": "approval_node", "toId": "end", "type": "Failure"},
			},
		},
	}
	e.deployRawProcess(processKey, processKey, processKey, def)
}

// candidatesOf 取任务的候选池行（entity_type/entity_id）。
func (e *e2eTestEnv) candidatesOf(taskID string) []model.WfTaskAssignee {
	e.t.Helper()
	var rows []model.WfTaskAssignee
	require.NoError(e.t, e.db.Raw("SELECT * FROM wf_task_assignee WHERE task_id = ?", taskID).Scan(&rows).Error)
	return rows
}

// candidateEntityIDs 返回指定实体类型的候选 ID 集合。
func candidateEntityIDs(rows []model.WfTaskAssignee, entityType string) []string {
	var out []string
	for _, r := range rows {
		if r.EntityType == entityType {
			out = append(out, r.EntityID)
		}
	}
	return out
}

// clearSideTables 清理候选池与评论表（resetE2ETables 未覆盖，用例间防串扰）。
func (e *e2eTestEnv) clearSideTables() {
	e.t.Helper()
	require.NoError(e.t, e.db.Exec("DELETE FROM wf_task_assignee").Error)
	require.NoError(e.t, e.db.Exec("DELETE FROM wf_task_comment").Error)
}

// mockIdentity 取引擎内置 Mock 身份服务（空审批人用例配置管理员名单用）。
func (e *e2eTestEnv) mockIdentity() *service.IdentityServiceImpl {
	e.t.Helper()
	identity, ok := e.engine.GetIdentityService().(*service.IdentityServiceImpl)
	require.True(e.t, ok, "identity service should be the mock impl")
	return identity
}

// taskVariables 解析任务变量 JSON。
func taskVariables(t *testing.T, task *model.WfTask) map[string]interface{} {
	t.Helper()
	require.NotNil(t, task.Variables)
	vars := map[string]interface{}{}
	require.NoError(t, json.Unmarshal([]byte(*task.Variables), &vars))
	return vars
}

// nodeTasksFor 取实例上指定节点（task_def_key）的任务行，兼容归档前后两个表。
func nodeTasksFor(t *testing.T, env *e2eTestEnv, instanceID, taskDefKey string) []*model.WfTask {
	t.Helper()
	var out []*model.WfTask
	for _, task := range env.allTasksFor(instanceID) {
		if task.TaskDefKey == taskDefKey {
			out = append(out, task)
		}
	}
	return out
}

func TestE2E_EmptyApprover_DefaultPolicy_RoutesToSingleTenantAdmin(t *testing.T) {
	env := newE2EEnv(t)
	resetE2ETables(t)
	env.clearSideTables()
	env.mockIdentity().SetMockTenantAdminIDs([]string{"adm1"})
	env.deployEmptyApproverProcess("ea_admin_single", "")

	instID := env.startInstance("ea_admin_single", "starter1")
	waitFor(t, "fallback task", func() bool {
		return len(env.allTasksFor(instID)) > 0
	})

	tasks := env.allTasksFor(instID)
	require.Len(t, tasks, 1)
	require.Equal(t, string(enums.TaskStatusActive), tasks[0].Status, "管理员唯一时直接指派为 active")
	require.Equal(t, "adm1", *tasks[0].Assignee)
	vars := taskVariables(t, tasks[0])
	require.Equal(t, "tenant_admin", vars["fallback_policy"])
	require.Equal(t, "审批人为空，已转交租户管理员", vars["fallback_reason"])
	require.Equal(t, "role:role-no-members", vars["fallback_from"])
	require.Equal(t, string(enums.InstanceStatusActive), env.instanceStatus(instID), "实例不再失败终止")
}

func TestE2E_EmptyApprover_MultiAdmins_BuildsClaimPool(t *testing.T) {
	env := newE2EEnv(t)
	resetE2ETables(t)
	env.clearSideTables()
	env.mockIdentity().SetMockTenantAdminIDs([]string{"adm1", "adm2"})
	env.deployEmptyApproverProcess("ea_admin_multi", "tenant_admin")

	instID := env.startInstance("ea_admin_multi", "starter1")
	waitFor(t, "fallback claim task", func() bool {
		return len(env.allTasksFor(instID)) > 0
	})

	tasks := env.allTasksFor(instID)
	require.Len(t, tasks, 1)
	require.Equal(t, string(enums.TaskStatusPending), tasks[0].Status, "多管理员建代认领任务")
	require.Nil(t, tasks[0].Assignee)

	ids := candidateEntityIDs(env.candidatesOf(tasks[0].ID), "person")
	require.ElementsMatch(t, []string{"adm1", "adm2"}, ids)
	require.Equal(t, string(enums.InstanceStatusActive), env.instanceStatus(instID))
}

func TestE2E_EmptyApprover_NoAdmin_ParksWithRoleCandidates(t *testing.T) {
	env := newE2EEnv(t)
	resetE2ETables(t)
	env.clearSideTables()
	env.mockIdentity().SetMockTenantAdminIDs(nil)
	env.deployEmptyApproverProcess("ea_park_role", "tenant_admin")

	instID := env.startInstance("ea_park_role", "starter1")
	waitFor(t, "parked task", func() bool {
		return len(env.allTasksFor(instID)) > 0
	})

	tasks := env.allTasksFor(instID)
	require.Len(t, tasks, 1)
	require.Equal(t, string(enums.TaskStatusPending), tasks[0].Status)
	// 角色候选实体照写：成员展开为空即无人可认领，天然挂起
	ids := candidateEntityIDs(env.candidatesOf(tasks[0].ID), "role")
	require.ElementsMatch(t, []string{"role-no-members"}, ids)
	require.Equal(t, "park", taskVariables(t, tasks[0])["fallback_policy"])
	require.Equal(t, string(enums.InstanceStatusActive), env.instanceStatus(instID))
}

func TestE2E_EmptyApprover_AutoApprove_CompletesAndAdvances(t *testing.T) {
	env := newE2EEnv(t)
	resetE2ETables(t)
	env.clearSideTables()
	env.mockIdentity().SetMockTenantAdminIDs([]string{"adm1"})
	env.deployEmptyApproverProcess("ea_auto", "auto_approve")

	instID := env.startInstance("ea_auto", "starter1")
	env.waitForInstanceStatus(instID, string(enums.InstanceStatusCompleted))

	// 实例完成后运行表任务归档，allTasksFor 可能同时含 end 节点的系统任务，
	// 只断言审批节点的任务
	tasks := nodeTasksFor(t, env, instID, "approval_node")
	require.Len(t, tasks, 1)
	require.Equal(t, string(enums.TaskStatusCompleted), tasks[0].Status)
	require.NotNil(t, tasks[0].EndReason)
	require.Equal(t, string(enums.ApprovalResultApproved), *tasks[0].EndReason)
	require.Equal(t, "auto_approve", taskVariables(t, tasks[0])["fallback_policy"])

	// 审批意见注明自动通过缘由（系统评论随任务留痕）
	var content string
	require.NoError(t, env.db.Raw("SELECT content FROM wf_task_comment WHERE process_instance_id = ? LIMIT 1", instID).Scan(&content).Error)
	require.True(t, strings.Contains(content, "审批人为空"), "auto approve comment should explain why, got: %s", content)
}

func TestE2E_EmptyApprover_AdminIsStarter_AutoApproves(t *testing.T) {
	env := newE2EEnv(t)
	resetE2ETables(t)
	env.clearSideTables()
	env.mockIdentity().SetMockTenantAdminIDs([]string{"starter1"})
	env.deployEmptyApproverProcess("ea_admin_is_starter", "tenant_admin")

	instID := env.startInstance("ea_admin_is_starter", "starter1")
	env.waitForInstanceStatus(instID, string(enums.InstanceStatusCompleted))

	tasks := nodeTasksFor(t, env, instID, "approval_node")
	require.Len(t, tasks, 1)
	require.Equal(t, string(enums.TaskStatusCompleted), tasks[0].Status)
	// 生效动作是自动通过，留痕按实际行为记录
	require.Equal(t, "auto_approve", taskVariables(t, tasks[0])["fallback_policy"])
}

func TestE2E_EmptyApprover_VoteCountThreshold_AutoApproves(t *testing.T) {
	// 票签 count 阈值（3 票）下兜底自动通过：合成单票结构必须以 1 票即过的规则
	// 收尾，否则阈值凑不齐会被判成自动拒绝（回归审查发现的语义反转）
	env := newE2EEnv(t)
	resetE2ETables(t)
	env.clearSideTables()
	env.mockIdentity().SetMockTenantAdminIDs(nil)

	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id":   "ea_vote_count",
			"name": "ea_vote_count",
			"root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{
					"id":   "approval_node",
					"type": "userTask",
					"name": "审批",
					"configuration": map[string]interface{}{
						"approver": map[string]interface{}{
							"type":    "role",
							"roleIds": []string{"role-no-members"},
						},
						"approveMode":         "vote",
						"emptyApproverPolicy": "auto_approve",
						"voteRule": map[string]interface{}{
							"type":  "count",
							"value": float64(3),
						},
					},
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "approval_node", "toId": "end", "type": "Success"},
				{"fromId": "approval_node", "toId": "end", "type": "Failure"},
			},
		},
	}
	env.deployRawProcess("ea_vote_count", "ea_vote_count", "ea_vote_count", def)

	instID := env.startInstance("ea_vote_count", "starter1")
	env.waitForInstanceStatus(instID, string(enums.InstanceStatusCompleted))

	tasks := nodeTasksFor(t, env, instID, "approval_node")
	require.NotEmpty(t, tasks, "vote node tasks should exist")
	for _, task := range tasks {
		if task.Status == string(enums.TaskStatusCompleted) && task.EndReason != nil {
			require.Equal(t, string(enums.ApprovalResultApproved), *task.EndReason,
				"auto approve must complete as approved, not rejected by unmet vote threshold")
		}
	}
}

func TestE2E_EmptyApprover_NoGroupEntity_ParksWithPlaceholderCandidates(t *testing.T) {
	env := newE2EEnv(t)
	resetE2ETables(t)
	env.clearSideTables()
	env.mockIdentity().SetMockTenantAdminIDs(nil)

	// 发起人自选：表达式引用运行期不存在的变量，解析为空且无组实体可写
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id":   "ea_park_placeholder",
			"name": "ea_park_placeholder",
			"root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{
					"id":   "approval_node",
					"type": "userTask",
					"name": "审批",
					"configuration": map[string]interface{}{
						"approver": map[string]interface{}{
							"type":       "initiatorSelect",
							"expression": "${selectApprover}",
						},
						"approveMode":         "single",
						"emptyApproverPolicy": "park",
					},
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "approval_node", "toId": "end", "type": "Success"},
				{"fromId": "approval_node", "toId": "end", "type": "Failure"},
			},
		},
	}
	env.deployRawProcess("ea_park_placeholder", "ea_park_placeholder", "ea_park_placeholder", def)

	instID := env.startInstance("ea_park_placeholder", "starter1")
	waitFor(t, "parked task", func() bool {
		return len(env.allTasksFor(instID)) > 0
	})

	tasks := env.allTasksFor(instID)
	require.Len(t, tasks, 1)
	require.Equal(t, string(enums.TaskStatusPending), tasks[0].Status)
	// 无组实体且无管理员：占位候选封住空池（防止任意同租户用户认领）
	ids := candidateEntityIDs(env.candidatesOf(tasks[0].ID), "person")
	require.ElementsMatch(t, []string{"__parked__"}, ids)
}

func TestE2E_Reassign_WritesSystemComment(t *testing.T) {
	env := newE2EEnv(t)
	resetE2ETables(t)
	env.clearSideTables()
	env.mockIdentity().SetMockTenantAdminIDs(nil)

	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id":   "ea_reassign",
			"name": "ea_reassign",
			"root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{
					"id":   "approval_node",
					"type": "userTask",
					"name": "审批",
					"configuration": map[string]interface{}{
						"approver": map[string]interface{}{
							"type":    "user",
							"userIds": []string{"approver1"},
						},
						"approveMode": "single",
					},
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "approval_node", "toId": "end", "type": "Success"},
				{"fromId": "approval_node", "toId": "end", "type": "Failure"},
			},
		},
	}
	env.deployRawProcess("ea_reassign", "ea_reassign", "ea_reassign", def)

	instID := env.startInstance("ea_reassign", "starter1")
	waitFor(t, "assigned task", func() bool {
		tasks := env.activeTasksFor(instID, "approver1")
		return len(tasks) == 1
	})
	taskID := env.activeTasksFor(instID, "approver1")[0].ID

	admin := service.Actor{UserID: "boss", UserName: "boss", TenantID: e2eTenantID, WorkflowAdmin: true}
	newAssignee, err := env.engine.GetTaskServiceInternal().Reassign(env.userCtx("boss"), admin, taskID, "approver2", "出差交接")
	require.NoError(t, err)
	require.Equal(t, "approver1", newAssignee)

	// 系统评论随审批记录可见：含原/新办理人与原因
	var comment struct {
		Content string
		UserID  string
	}
	require.NoError(t, env.db.Raw(
		"SELECT content, user_id FROM wf_task_comment WHERE task_id = ? ORDER BY created_at DESC LIMIT 1", taskID).
		Scan(&comment).Error)
	require.True(t, strings.Contains(comment.Content, "强制改派"), "comment: %s", comment.Content)
	require.True(t, strings.Contains(comment.Content, "approver1"), "comment: %s", comment.Content)
	require.True(t, strings.Contains(comment.Content, "approver2"), "comment: %s", comment.Content)
	require.True(t, strings.Contains(comment.Content, "出差交接"), "comment: %s", comment.Content)
	require.Equal(t, "boss", comment.UserID, "评论操作人=改派管理员")
	// 任务保持 active 且已易主
	task := env.taskByID(taskID)
	require.Equal(t, string(enums.TaskStatusActive), task.Status)
	require.Equal(t, "approver2", *task.Assignee)
}
