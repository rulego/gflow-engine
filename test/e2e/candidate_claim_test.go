package e2e

// 签收（认领）链路 e2e：部署带候选池的流程，验证 role/department/person 三类
// 候选实体在待办可见性、详情放行、签收/取消签收、办理与统计口径上行为一致。

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/enums"
)

// seedClaimIdentity 注册签收场景的用户与组织关系：
// alice/bob/dave 属于 dept-tech，carol 属于 role-finance，eve 无组织归属。
func seedClaimIdentity(t *testing.T, e *e2eTestEnv) {
	t.Helper()
	identity, ok := e.engine.GetIdentityService().(*service.IdentityServiceImpl)
	require.True(t, ok, "identity service should be the mock impl")
	for _, id := range []string{"alice", "bob", "dave", "carol", "eve"} {
		identity.AddMockUser(&service.User{ID: id, TenantID: e2eTenantID})
	}
	identity.AddMockUserDepartment("alice", "dept-tech")
	identity.AddMockUserDepartment("bob", "dept-tech")
	identity.AddMockUserDepartment("dave", "dept-tech")
	identity.AddMockRoleUsers("role-finance", []string{"carol"})
}

// deployCandidateProcess 部署单节点候选审批流程，candidateType 为 dept/role，
// candidateConfig 键分别为 departmentIds/roleIds。
func (e *e2eTestEnv) deployCandidateProcess(processKey, name, candidateType, configKey string, entityIDs []string) {
	e.t.Helper()
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id":   processKey,
			"name": name,
			"root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{
					"id":   "approval_node",
					"type": "userTask",
					"name": name,
					"configuration": map[string]interface{}{
						"candidateType": candidateType,
						"candidateConfig": map[string]interface{}{
							configKey: entityIDs,
						},
						"approvalType": "single",
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
	e.deployRawProcess(processKey, processKey, name, def)
}

// startClaimInstance 发起实例并等待首个节点任务落库（链路异步驱动）。
func (e *e2eTestEnv) startClaimInstance(processKey, starter string) string {
	e.t.Helper()
	instID := e.startInstance(processKey, starter)
	waitFor(e.t, "instance task", func() bool {
		return len(e.allTasksFor(instID)) > 0
	})
	return instID
}

// pendingClaimTaskID 取实例当前的待签收任务 ID。
func (e *e2eTestEnv) pendingClaimTaskID(instanceID string) string {
	e.t.Helper()
	for _, task := range e.allTasksFor(instanceID) {
		if task.Status == string(enums.TaskStatusPending) {
			return task.ID
		}
	}
	e.t.Fatalf("instance %s has no pending claim task", instanceID)
	return ""
}

// waitForInstanceStatus 轮询等待实例到达目标状态（完成后归档迁移也是异步）。
func (e *e2eTestEnv) waitForInstanceStatus(instanceID, status string) {
	e.t.Helper()
	waitFor(e.t, "instance status "+status, func() bool {
		return e.instanceStatus(instanceID) == status
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func (e *e2eTestEnv) taskByID(taskID string) *model.WfTask {
	e.t.Helper()
	var row model.WfTask
	require.NoError(e.t, e.db.Raw("SELECT * FROM wf_task WHERE id = ?", taskID).First(&row).Error, "load task "+taskID)
	return &row
}

// todoContains 判断用户待办列表是否包含实例。
func (e *e2eTestEnv) todoContains(userID, instanceID string) bool {
	e.t.Helper()
	instances, _, err := e.engine.GetRuntimeService().GetTodoProcessInstanceList(
		e.userCtx(userID), service.Actor{UserID: userID, TenantID: e2eTenantID},
		1, 20, "", nil, "", false)
	require.NoError(e.t, err, "todo list for "+userID)
	for _, inst := range instances {
		if inst.ID == instanceID {
			return true
		}
	}
	return false
}

// claimableContains 判断实例是否在用户可签收集合中。
func (e *e2eTestEnv) claimableContains(userID, instanceID string) bool {
	e.t.Helper()
	ids, err := e.engine.GetTaskService().GetClaimableInstanceIDs(
		e.userCtx(userID), service.Actor{UserID: userID, TenantID: e2eTenantID}, []string{instanceID})
	require.NoError(e.t, err, "claimable check for "+userID)
	for _, id := range ids {
		if id == instanceID {
			return true
		}
	}
	return false
}

func (e *e2eTestEnv) claimTask(userID, taskID string) error {
	return e.engine.GetTaskService().Claim(e.userCtx(userID), service.Actor{UserID: userID, TenantID: e2eTenantID}, taskID)
}

func (e *e2eTestEnv) unclaimTask(userID, taskID string) error {
	return e.engine.GetTaskService().Unclaim(e.userCtx(userID), service.Actor{UserID: userID, TenantID: e2eTenantID}, taskID)
}

// detailErr 返回用户读取实例详情的错误（nil 表示可读）。
func (e *e2eTestEnv) detailErr(userID, instanceID string) error {
	_, err := e.engine.GetRuntimeService().GetProcessInstanceDetail(
		e.userCtx(userID), service.Actor{UserID: userID, TenantID: e2eTenantID}, instanceID)
	return err
}

// todoCount 取用户待办统计的 todoCount。
func (e *e2eTestEnv) todoCount(userID string) int64 {
	e.t.Helper()
	stats, err := e.engine.GetTaskService().GetApprovalStatistics(
		e.userCtx(userID), service.Actor{UserID: userID, TenantID: e2eTenantID})
	require.NoError(e.t, err, "statistics for "+userID)
	v, ok := stats["todoCount"].(int64)
	require.True(e.t, ok, "todoCount should be int64, got %T", stats["todoCount"])
	return v
}

func TestClaimThread_DeptCandidate(t *testing.T) {
	e := newE2EEnv(t)
	seedClaimIdentity(t, e)

	e.deployCandidateProcess("claim-dept-flow", "部门候选审批", "dept", "departmentIds", []string{"dept-tech"})
	instID := e.startClaimInstance("claim-dept-flow", "eve")

	// 签收前：部门成员待办可见，池外用户不可见
	require.True(t, e.todoContains("alice", instID), "部门成员应看到待签收任务")
	require.True(t, e.todoContains("bob", instID))
	require.False(t, e.todoContains("carol", instID), "池外用户不应看到待签收任务")

	// 可签收集合：成员命中，池外不命中
	require.True(t, e.claimableContains("alice", instID))
	require.False(t, e.claimableContains("carol", instID))

	// 详情：成员可读，池外拒绝
	require.NoError(t, e.detailErr("alice", instID))
	require.ErrorIs(t, e.detailErr("carol", instID), service.ErrPermissionDenied)

	taskID := e.pendingClaimTaskID(instID)

	// 未签收不能办理
	err := e.engine.GetTaskService().Approve(e.userCtxAs("alice"), service.Actor{UserID: "alice", TenantID: e2eTenantID}, taskID, "提前通过", nil)
	require.Error(t, err, "未签收的任务不能直接通过")

	// 池外不能签收
	require.ErrorIs(t, e.claimTask("carol", taskID), service.ErrPermissionDenied)

	// 签收：assignee 与 claimed_at 落库，任务转 active
	require.NoError(t, e.claimTask("alice", taskID))
	task := e.taskByID(taskID)
	require.NotNil(t, task.Assignee)
	require.Equal(t, "alice", *task.Assignee)
	require.NotNil(t, task.ClaimedAt)
	require.Equal(t, string(enums.TaskStatusActive), task.Status)

	// 签收后：其他成员待办消失，不能再签收，详情也不再可见
	require.False(t, e.todoContains("bob", instID), "他人签收后剩余成员待办应移除")
	require.Error(t, e.claimTask("bob", taskID), "已签收任务不能被重复签收")
	require.ErrorIs(t, e.detailErr("bob", instID), service.ErrPermissionDenied)

	// 签收人办理，实例完成
	e.approveAs(taskID, "alice", "同意")
	e.waitForInstanceStatus(instID, string(enums.InstanceStatusCompleted))
}

func TestClaimThread_RoleCandidate(t *testing.T) {
	e := newE2EEnv(t)
	seedClaimIdentity(t, e)

	e.deployCandidateProcess("claim-role-flow", "角色候选审批", "role", "roleIds", []string{"role-finance"})
	instID := e.startClaimInstance("claim-role-flow", "eve")

	// 角色成员与部门成员同规则：可见、可签收、池外不可见
	require.True(t, e.todoContains("carol", instID))
	require.False(t, e.todoContains("dave", instID))
	require.True(t, e.claimableContains("carol", instID))
	require.NoError(t, e.detailErr("carol", instID))
	require.ErrorIs(t, e.detailErr("dave", instID), service.ErrPermissionDenied)

	taskID := e.pendingClaimTaskID(instID)
	require.NoError(t, e.claimTask("carol", taskID))
	require.False(t, e.todoContains("dave", instID), "他人签收后剩余成员待办应移除")

	e.approveAs(taskID, "carol", "同意")
	e.waitForInstanceStatus(instID, string(enums.InstanceStatusCompleted))
}

// 混合候选池：部门候选任务手动追加 person 候选后，两类成员同权可见可签收，
// 任一人签收后其余成员（含另一实体的成员）失去可见性。
func TestClaimMixedPool_PersonAndDept(t *testing.T) {
	e := newE2EEnv(t)
	seedClaimIdentity(t, e)

	e.deployCandidateProcess("claim-mixed-flow", "混合候选审批", "dept", "departmentIds", []string{"dept-tech"})
	instID := e.startClaimInstance("claim-mixed-flow", "eve")
	taskID := e.pendingClaimTaskID(instID)

	require.NoError(t, e.engine.GetTaskService().AddCandidates(
		e.userCtx("eve"), service.Actor{UserID: "eve", TenantID: e2eTenantID},
		taskID, string(enums.EntityTypePerson), []string{"carol"}),
		"追加 person 候选")

	require.True(t, e.todoContains("alice", instID))
	require.True(t, e.todoContains("carol", instID), "追加的 person 候选应可见")

	require.NoError(t, e.claimTask("carol", taskID))
	require.False(t, e.todoContains("alice", instID), "任一成员签收后全池失去待签收可见")
	require.ErrorIs(t, e.detailErr("alice", instID), service.ErrPermissionDenied)

	e.approveAs(taskID, "carol", "同意")
	e.waitForInstanceStatus(instID, string(enums.InstanceStatusCompleted))
}

func TestClaimUnclaim_ReturnToPool(t *testing.T) {
	e := newE2EEnv(t)
	seedClaimIdentity(t, e)

	e.deployCandidateProcess("claim-unclaim-flow", "签收回退审批", "dept", "departmentIds", []string{"dept-tech"})
	instID := e.startClaimInstance("claim-unclaim-flow", "eve")
	taskID := e.pendingClaimTaskID(instID)

	require.NoError(t, e.claimTask("alice", taskID))
	require.NoError(t, e.unclaimTask("alice", taskID))

	// 取消签收后任务回到未签收态：assignee 清空、claimed_at 清空
	task := e.taskByID(taskID)
	require.Equal(t, string(enums.TaskStatusPending), task.Status)
	require.NotNil(t, task.Assignee)
	require.Empty(t, *task.Assignee, "取消签收后 assignee 应清空")
	require.Nil(t, task.ClaimedAt, "取消签收后 claimed_at 应清空，否则时间线与详情仍按已签收展示")

	// 任务回池：其他成员可见可签收，原签收人失去办理权
	require.True(t, e.todoContains("bob", instID))
	require.NoError(t, e.claimTask("bob", taskID))
	err := e.engine.GetTaskService().Approve(e.userCtxAs("alice"), service.Actor{UserID: "alice", TenantID: e2eTenantID}, taskID, "越权通过", nil)
	require.Error(t, err, "取消签收后原签收人不能再办理")

	e.approveAs(taskID, "bob", "同意")
	e.waitForInstanceStatus(instID, string(enums.InstanceStatusCompleted))
}

// 未签收候选任务计入成员待办统计；签收转已指派口径；办理后出待办。
func TestClaimStatistics_TodoCount(t *testing.T) {
	e := newE2EEnv(t)
	seedClaimIdentity(t, e)

	require.Equal(t, int64(0), e.todoCount("alice"))

	e.deployCandidateProcess("claim-stat-flow", "统计口径审批", "dept", "departmentIds", []string{"dept-tech"})
	instID := e.startClaimInstance("claim-stat-flow", "eve")

	require.Equal(t, int64(1), e.todoCount("alice"), "未签收候选任务应计入部门成员待办数")
	require.Equal(t, int64(0), e.todoCount("carol"), "池外用户不应计数")

	taskID := e.pendingClaimTaskID(instID)
	require.NoError(t, e.claimTask("alice", taskID))
	require.Equal(t, int64(1), e.todoCount("alice"), "签收后转为已指派待办，计数不变")
	require.Equal(t, int64(0), e.todoCount("bob"))

	e.approveAs(taskID, "alice", "同意")
	e.waitForInstanceStatus(instID, string(enums.InstanceStatusCompleted))
	require.Equal(t, int64(0), e.todoCount("alice"), "办理后应移出待办")
}
