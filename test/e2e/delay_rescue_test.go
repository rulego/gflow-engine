// Package e2e — delay 期限落库 / 超期检测 / 救援重入，以及 businessKey 唯一约束的端到端测试。
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

// deployDelayProcess 部署 delay → end 的两节点流程（delayMs 为节点原始配置）。
func (e *e2eTestEnv) deployDelayProcess(processKey, delayMs string) {
	e.t.Helper()
	// delayMs 为节点原始配置（支持纯数字或 ${msg.field} 模板）
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id": processKey, "name": processKey, "root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{
					"id":   "delay_node",
					"type": "delay",
					"name": "延时",
					"configuration": map[string]interface{}{
						"delayMs": delayMs,
					},
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "delay_node", "toId": "end", "type": "Success"},
				{"fromId": "delay_node", "toId": "end", "type": "Failure"},
			},
		},
	}
	raw, err := json.Marshal(def)
	require.NoError(e.t, err, "marshal def")
	ctx := e.userCtx("admin")
	_, err = e.engine.GetProcessService().Deploy(ctx, service.Actor{UserID: "admin", TenantID: e2eTenantID}, &model.WfProcess{
		ProcessKey:     processKey,
		Name:           processKey,
		DefinitionJSON: string(raw),
		Status:         string(enums.ProcessStatusActive),
		TenantID:       e2eTenantID,
		CreatedBy:      "admin",
	}, true)
	require.NoError(e.t, err, "deploy process")
}

// startWithBizKey 以显式 businessKey 启动实例（businessKey 冲突用例需要固定键）。
func (e *e2eTestEnv) startWithBizKey(processKey, starter, bizKey string, vars map[string]interface{}) (string, error) {
	e.t.Helper()
	ctx := e.userCtx(starter)
	return e.engine.GetRuntimeService().StartProcessInstanceByKey(ctx, service.Actor{
		UserID: starter, UserName: starter, TenantID: e2eTenantID,
	}, processKey, bizKey, vars)
}

// waitingDelayTask 读取实例的 delay 任务行（未归档）。
func (e *e2eTestEnv) waitingDelayTask(instanceID string) *model.WfTask {
	e.t.Helper()
	var rows []*model.WfTask
	require.NoError(e.t, e.db.Raw(
		"SELECT * FROM wf_task WHERE process_instance_id = ? AND task_type = ?", instanceID, "delay").Scan(&rows).Error)
	for _, r := range rows {
		return r
	}
	return nil
}

// ---------------------------------------------------------------------------
// delay 期限落库
// ---------------------------------------------------------------------------

// 静态 delayMs：任务行带 DueDate ≈ CreatedAt + delayMs。
func TestE2E_DelayTask_DueDateWritten(t *testing.T) {
	env := newE2EEnv(t)
	env.deployDelayProcess("delay_due_static", "3600000") // 1 小时，测试期内不会自然到期

	instID, err := env.startWithBizKey("delay_due_static", "starter", "", nil)
	require.NoError(t, err)

	var task *model.WfTask
	require.Eventually(t, func() bool {
		task = env.waitingDelayTask(instID)
		return task != nil
	}, 3*time.Second, 50*time.Millisecond, "delay task row should exist")
	require.NotNil(t, task.DueDate, "delay task must carry due_date")
	assert.WithinDuration(t, task.CreatedAt.Add(time.Hour), *task.DueDate, 10*time.Second,
		"due_date should be created_at + 1h")
}

// 模板 delayMs（${msg.field}）：建行那一刻按消息上下文求值出绝对期限。
func TestE2E_DelayTask_TemplateDelayMsEvaluated(t *testing.T) {
	env := newE2EEnv(t)
	env.deployDelayProcess("delay_due_tmpl", "${msg.waitMs}")

	instID, err := env.startWithBizKey("delay_due_tmpl", "starter", "",
		map[string]interface{}{"waitMs": 7000})
	require.NoError(t, err)

	var task *model.WfTask
	require.Eventually(t, func() bool {
		task = env.waitingDelayTask(instID)
		return task != nil
	}, 3*time.Second, 50*time.Millisecond, "delay task row should exist")
	require.NotNil(t, task.DueDate, "template delayMs must still produce due_date")
	assert.WithinDuration(t, task.CreatedAt.Add(7*time.Second), *task.DueDate, 10*time.Second,
		"due_date should be created_at + 7s (msg.waitMs)")
}

// ---------------------------------------------------------------------------
// 超期检测 + 救援
// ---------------------------------------------------------------------------

// 计时器丢失（DueDate 早已过线 + 已等待时长超过延时）→ 检测命中，救援立即放行
// 完成既有任务行，实例继续推进到完成；不产生第二条 delay 任务行。
func TestE2E_DelayRescue_ExpiredTaskAdvancesWithoutDuplicate(t *testing.T) {
	env := newE2EEnv(t)
	env.deployDelayProcess("delay_rescue_exp", "3600000")

	instID, err := env.startWithBizKey("delay_rescue_exp", "starter", "", nil)
	require.NoError(t, err)

	var task *model.WfTask
	require.Eventually(t, func() bool {
		task = env.waitingDelayTask(instID)
		return task != nil
	}, 3*time.Second, 50*time.Millisecond)
	delayTaskID := task.ID

	// 模拟计时器丢失后时间流逝：到期时间早已过线、创建时刻远早于延时窗口
	require.NoError(t, env.db.Exec(
		"UPDATE wf_task SET due_date = ?, created_at = ? WHERE id = ?",
		time.Now().Add(-2*time.Hour), time.Now().Add(-3*time.Hour), delayTaskID).Error)

	// 检测：超期 delay 命中（宽限 60s，2h 前已到期）
	expired, err := env.engine.GetRuntimeService().GetExpiredDelayTasks(env.userCtx("admin"), e2eTenantID)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	assert.Equal(t, delayTaskID, expired[0].ID)

	// 救援：偏移 3h ≥ 延时 1h → 立即放行，完成既有任务行并推进到 end
	require.NoError(t, env.engine.GetRuntimeService().RescueExpiredDelayTask(
		env.userCtx("admin"), adminActor(), delayTaskID))
	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 3*time.Second, 50*time.Millisecond, "instance should complete after rescue")

	// 唯一不变量：delay 节点只有一条任务行（完成归档后落 wf_hi_task）
	var total int64
	require.NoError(t, env.db.Raw(
		"SELECT (SELECT COUNT(*) FROM wf_task WHERE process_instance_id = ? AND task_type = 'delay') + "+
			"(SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_type = 'delay')",
		instID, instID).Scan(&total).Error)
	assert.Equal(t, int64(1), total, "rescue must not create a second delay task row")
}

// 未到期（DueDate 在未来）→ 检测不命中，救援被拒绝，任务行原样保留。
func TestE2E_DelayRescue_NotExpiredRejected(t *testing.T) {
	env := newE2EEnv(t)
	env.deployDelayProcess("delay_rescue_future", "3600000")

	instID, err := env.startWithBizKey("delay_rescue_future", "starter", "", nil)
	require.NoError(t, err)

	var task *model.WfTask
	require.Eventually(t, func() bool {
		task = env.waitingDelayTask(instID)
		return task != nil
	}, 3*time.Second, 50*time.Millisecond)

	expired, err := env.engine.GetRuntimeService().GetExpiredDelayTasks(env.userCtx("admin"), e2eTenantID)
	require.NoError(t, err)
	assert.Empty(t, expired, "future delay task must not be detected as expired")

	err = env.engine.GetRuntimeService().RescueExpiredDelayTask(env.userCtx("admin"), adminActor(), task.ID)
	require.Error(t, err, "rescuing a not-yet-expired delay task must be rejected")
	assert.Contains(t, err.Error(), "not expired")

	assert.NotNil(t, env.waitingDelayTask(instID), "delay task row must stay untouched")
	assert.Equal(t, string(enums.InstanceStatusActive), env.instanceStatus(instID))
}

// DueDate 被推进到过去但已等待时长仍小于延时（并发推进边缘）→ 救援放行重挂
// 剩余计时，任务行不完成、不重复，实例保持等待。
func TestE2E_DelayRescue_EarlyDueDateRearmsRemainingTimer(t *testing.T) {
	env := newE2EEnv(t)
	env.deployDelayProcess("delay_rescue_rearm", "3600000")

	instID, err := env.startWithBizKey("delay_rescue_rearm", "starter", "", nil)
	require.NoError(t, err)

	var task *model.WfTask
	require.Eventually(t, func() bool {
		task = env.waitingDelayTask(instID)
		return task != nil
	}, 3*time.Second, 50*time.Millisecond)

	// 只有 due_date 被改到过去，created_at 保持当前 → 偏移远小于延时
	require.NoError(t, env.db.Exec(
		"UPDATE wf_task SET due_date = ? WHERE id = ?", time.Now().Add(-2*time.Hour), task.ID).Error)

	require.NoError(t, env.engine.GetRuntimeService().RescueExpiredDelayTask(
		env.userCtx("admin"), adminActor(), task.ID))

	time.Sleep(300 * time.Millisecond)
	still := env.waitingDelayTask(instID)
	require.NotNil(t, still, "delay task should stay waiting after re-arm")
	assert.Equal(t, task.ID, still.ID, "re-arm must reuse the existing task row")
	assert.Equal(t, string(enums.InstanceStatusActive), env.instanceStatus(instID),
		"instance should keep waiting for the remaining delay")
}

// 跨租户救援被拒绝。
func TestE2E_DelayRescue_CrossTenantRejected(t *testing.T) {
	env := newE2EEnv(t)
	env.deployDelayProcess("delay_rescue_cross", "3600000")

	instID, err := env.startWithBizKey("delay_rescue_cross", "starter", "", nil)
	require.NoError(t, err)

	var task *model.WfTask
	require.Eventually(t, func() bool {
		task = env.waitingDelayTask(instID)
		return task != nil
	}, 3*time.Second, 50*time.Millisecond)
	require.NoError(t, env.db.Exec(
		"UPDATE wf_task SET due_date = ? WHERE id = ?", time.Now().Add(-2*time.Hour), task.ID).Error)

	err = env.engine.GetRuntimeService().RescueExpiredDelayTask(
		env.userCtx("admin"), service.Actor{UserID: "outsider", TenantID: "other-tenant"}, task.ID)
	require.Error(t, err, "cross-tenant rescue must be rejected")
	assert.ErrorIs(t, err, service.ErrPermissionDenied)
}

// ---------------------------------------------------------------------------
// businessKey 唯一（S8）
// ---------------------------------------------------------------------------

// 先查路径：同 businessKey 的 active 实例存在时，第二次发起返回 ErrConflict。
func TestE2E_BusinessKey_PrecheckRejectsDuplicateActive(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("bizkey_precheck", "业务键先查", "single", []string{"approver_b"}, nil)

	inst1, err := env.startWithBizKey("bizkey_precheck", "starter", "BK-PRE", nil)
	require.NoError(t, err)
	require.NotEmpty(t, inst1)

	_, err = env.startWithBizKey("bizkey_precheck", "starter", "BK-PRE", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, service.ErrConflict)
}

// 唯一约束路径：先查只看 active 实例，挂起实例躲过先查时由数据库唯一索引
// 兜底，插入冲突映射为同一友好冲突错误。
func TestE2E_BusinessKey_UniqueConstraintMappedToConflict(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("bizkey_unique", "业务键唯一", "single", []string{"approver_b"}, nil)

	inst1, err := env.startWithBizKey("bizkey_unique", "starter", "BK-SUS", nil)
	require.NoError(t, err)
	require.NotEmpty(t, inst1)

	// 挂起后躲过先查（先查只匹配 active 实例），实例行仍在表中占住唯一键
	require.NoError(t, env.engine.GetRuntimeService().SuspendProcessInstance(
		env.userCtx("admin"), adminActor(), inst1))

	_, err = env.startWithBizKey("bizkey_unique", "starter", "BK-SUS", nil)
	require.Error(t, err, "unique constraint must reject the second insert")
	assert.ErrorIs(t, err, service.ErrConflict,
		"unique violation should map to the existing businessKey conflict error")
	assert.Contains(t, err.Error(), "BK-SUS")
}

// 不同 businessKey / 空 businessKey（自动生成）不受唯一约束影响。
func TestE2E_BusinessKey_DistinctKeysCoexist(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("bizkey_coexist", "业务键共存", "single", []string{"approver_b"}, nil)

	_, err := env.startWithBizKey("bizkey_coexist", "starter", "BK-A", nil)
	require.NoError(t, err)
	_, err = env.startWithBizKey("bizkey_coexist", "starter", "BK-B", nil)
	require.NoError(t, err)
	// 空 businessKey：引擎自动生成（BIZ_ 前缀），两实例可共存
	_, err = env.startWithBizKey("bizkey_coexist", "starter", "", nil)
	require.NoError(t, err)
	_, err = env.startWithBizKey("bizkey_coexist", "starter", "", nil)
	require.NoError(t, err)
}
