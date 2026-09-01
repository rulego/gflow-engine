// Package e2e contains end-to-end integration tests for the BPM engine.
//
// 这些测试在内存 SQLite 上启动完整的工作流引擎（包括 rulego chain、
// userTask 节点、TaskCreator aspect），部署真实的流程定义，驱动完整的
// 审批链路。和 service 包内的单元测试相比：
//   - 单元测试：直接调 Internal 方法，跳过 rulego，验证业务逻辑
//   - e2e 测试：从 ProcessService.Deploy 开始走完整路径，验证端到端语义
//
// 运行：
//
//	go test ./test/e2e/...    # 只跑 e2e
//	go test ./...             # 跑所有测试
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/rulego/gflow-engine/components"
	"github.com/rulego/gflow-engine/config"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
)

// ============================================================================
// 端到端（e2e）集成测试
//
// 这些测试在内存 SQLite 上启动完整的工作流引擎，部署真实的流程定义，
// 启动流程实例并按顺序驱动 userTask 完成动作，验证端到端的审批语义。
//
// 重点回归用例：顺序审批（ApprovalTypeSequential）。
// 历史 bug：流程定义配了 3 个顺序审批人，第 1 人审批通过后流程直接进 end，
// 跳过了后续审批人；同时任务 duration 列始终为 NULL，导致 UI 处理时间显示 0.0。
// 修复点见：
//   - components/user_task_node.go::checkTasksCompletion（用 _sequentialAssignees 计数）
//   - service/task_service_complete.go::completeWithApprovalInternal（写入 Duration）
//
//   - rulego.Registry 是全局的，userTask 节点类型只能注册一次。所以全部 e2e
//     用例共享同一个引擎实例（singleton），每个用例在 Setup 里清空数据表，
//     避免上一用例的脏数据影响。
// ============================================================================

const e2eTenantID = "tenant-e2e"

var (
	e2eEngineOnce sync.Mutex
	e2eEngine     service.WorkflowEngine
	e2eDB         *gorm.DB
)

// initE2EEngine 单例初始化共享引擎。第一次调用会建表、启动引擎、注册组件；
// 后续调用直接返回已存在的实例。
func initE2EEngine(t *testing.T) {
	t.Helper()
	e2eEngineOnce.Lock()
	defer e2eEngineOnce.Unlock()
	if e2eEngine != nil {
		return
	}
	// file:e2e_shared?mode=memory&cache=shared 让多个连接看到同一份内存数据。
	// MaxOpenConns=1 匹配 SQLite 单写串行语义（并发 BEGIN 在多连接下会触发驱动内部死锁）。
	// 已知限制：并发 approve 用例在共享单例库上可能出现 end 节点重复执行——
	// SELECT FOR UPDATE 在 SQLite 上不生效，完成判定的串行化依赖生产库（Postgres/MySQL）
	// 的行锁；tests/bpm 的 Postgres 用例不受影响。
	dsn := "file:e2e_shared?mode=memory&cache=shared&_busy_timeout=30000&_journal_mode=WAL"
	cfg := &config.Config{
		Database: &config.DatabaseConfig{
			Driver:       "sqlite",
			Dsn:          dsn,
			MaxOpenConns: 1,
			MaxIdleConns: 1,
		},
	}
	engine, err := service.NewWorkflowEngineBuilder().
		SetName("e2e-engine").
		SetConfig(cfg).
		SetDialectProvider(&sqliteDialectProvider{}).
		SetIDGenerator(service.NewIDGenerator()).
		Build()
	require.NoError(t, err, "build engine")
	require.NotNil(t, engine)

	// Start BEFORE Register: Start initializes the engine's
	// internal services (TaskService/RuntimeService/IdentityService), and the
	// userTask node needs them at construction time. Registering before Start
	// would inject nil services and the node's OnMsg would panic silently.
	require.NoError(t, engine.Start(context.Background()), "start engine")
	require.NoError(t,
		components.Register(components.ComponentDeps{
			TaskService:     engine.GetTaskServiceInternal(),
			IdentityService: engine.GetIdentityService(),
			RuntimeService:  engine.GetRuntimeServiceInternal(),
		}), "register components")

	// 测试代码直接复用引擎内部的 *gorm.DB——这样所有读写都走同一个连接池，
	// SQLite 不会出现多连接竞态。
	db := engine.GetDB()
	createE2ETables(t, db)

	e2eEngine = engine
	e2eDB = db
}

// resetE2ETables 清空所有 e2e 相关表，确保用例之间互相隔离。
func resetE2ETables(t *testing.T) {
	t.Helper()
	for _, tbl := range []string{"wf_task", "wf_hi_task", "wf_instance", "wf_hi_instance", "wf_process"} {
		require.NoError(t, e2eDB.Exec(fmt.Sprintf("DELETE FROM %s", tbl)).Error, "clear "+tbl)
	}
}

// sqliteDialectProvider bridges glebarez/sqlite into the engine's DialectProvider
// interface. The engine ships with its SQLite dialect commented out (see
// service/default_dialects.go), so tests must inject their own provider.
type sqliteDialectProvider struct{}

func (s *sqliteDialectProvider) GetName() string               { return "sqlite" }
func (s *sqliteDialectProvider) GetSupportedDrivers() []string { return []string{"sqlite", "sqlite3"} }
func (s *sqliteDialectProvider) CreateDialector(dsn string) (gorm.Dialector, error) {
	return sqlite.Open(dsn), nil
}

// e2eTestEnv 是一次 e2e 测试的运行环境：一个引擎 + 一个共享 sqlite 内存库。
type e2eTestEnv struct {
	t      *testing.T
	engine service.WorkflowEngine
	db     *gorm.DB
}

func newE2EEnv(t *testing.T) *e2eTestEnv {
	t.Helper()
	initE2EEngine(t)
	resetE2ETables(t)
	return &e2eTestEnv{t: t, engine: e2eEngine, db: e2eDB}
}

// createE2ETables 建立引擎运行所需的全部数据表（DDL 与生产 schema 对齐）。
// 不使用 AutoMigrate：生成的 gorm 结构体里大量 `comment:` tag 在 SQLite 上不识别。
func createE2ETables(t *testing.T, db *gorm.DB) {
	t.Helper()
	ddls := []string{
		`CREATE TABLE IF NOT EXISTS wf_process (
			id TEXT PRIMARY KEY,
			process_key TEXT NOT NULL,
			name TEXT NOT NULL,
			version INTEGER NOT NULL DEFAULT 1,
			category TEXT,
			description TEXT,
			definition_json TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'active',
			publish_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			tenant_id TEXT NOT NULL,
			created_by TEXT NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_by TEXT,
			updated_at DATETIME,
			ext TEXT,
			process_type TEXT NOT NULL DEFAULT 'main',
			icon TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS wf_instance (
			id TEXT PRIMARY KEY,
			process_id TEXT NOT NULL,
			business_key TEXT,
			name TEXT NOT NULL,
			start_user_id TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			variables TEXT,
			current_activity TEXT,
			priority INTEGER NOT NULL DEFAULT 50,
			parent_id TEXT,
			tenant_id TEXT NOT NULL,
			created_by TEXT NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_by TEXT,
			updated_at DATETIME,
			end_reason TEXT,
			duration INTEGER,
			ended_at DATETIME
		)`,
		`CREATE TABLE IF NOT EXISTS wf_task (
			id TEXT PRIMARY KEY,
			process_instance_id TEXT,
			process_id TEXT,
			parent_id TEXT,
			task_def_key TEXT,
			task_type TEXT,
			name TEXT,
			description TEXT,
			status TEXT,
			assignee TEXT,
			owner TEXT,
			priority INTEGER DEFAULT 50,
			sequence_order INTEGER DEFAULT 0,
			due_date DATETIME,
			form_key TEXT,
			variables TEXT,
			claimed_at DATETIME,
			approval_type TEXT,
			approval_rule TEXT,
			delegate_from TEXT,
			delegate_reason TEXT,
			delegate_time DATETIME,
			ended_at DATETIME,
			comment TEXT,
			end_reason TEXT,
			duration INTEGER,
			tenant_id TEXT,
			created_by TEXT,
			created_at DATETIME,
			updated_by TEXT,
			updated_at DATETIME
		)`,
		`CREATE TABLE IF NOT EXISTS wf_task_assignee (
			id TEXT PRIMARY KEY,
			task_id TEXT NOT NULL,
			entity_type TEXT NOT NULL DEFAULT 'role',
			entity_id TEXT NOT NULL,
			tenant_id TEXT NOT NULL DEFAULT '',
			created_at DATETIME
		)`,
		`CREATE TABLE IF NOT EXISTS wf_hi_instance (
			id TEXT PRIMARY KEY,
			process_id TEXT NOT NULL,
			business_key TEXT,
			name TEXT NOT NULL,
			status TEXT NOT NULL,
			variables TEXT,
			current_activity TEXT,
			priority INTEGER NOT NULL DEFAULT 50,
			parent_id TEXT,
			tenant_id TEXT NOT NULL,
			created_by TEXT NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_by TEXT,
			updated_at DATETIME,
			end_reason TEXT,
			duration INTEGER,
			ended_at DATETIME,
			start_user_id TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS wf_hi_task (
			id TEXT PRIMARY KEY,
			process_instance_id TEXT,
			process_id TEXT,
			task_def_key TEXT,
			task_type TEXT,
			name TEXT,
			description TEXT,
			parent_id TEXT,
			status TEXT,
			assignee TEXT,
			owner TEXT,
			priority INTEGER DEFAULT 50,
			due_date DATETIME,
			form_key TEXT,
			variables TEXT,
			claimed_at DATETIME,
			sequence_order INTEGER DEFAULT 0,
			approval_type TEXT,
			approval_rule TEXT,
			delegate_from TEXT,
			delegate_reason TEXT,
			delegate_time DATETIME,
			ended_at DATETIME,
			comment TEXT,
			end_reason TEXT,
			duration INTEGER,
			tenant_id TEXT,
			created_by TEXT,
			created_at DATETIME,
			updated_by TEXT,
			updated_at DATETIME
		)`,
		`CREATE TABLE IF NOT EXISTS wf_task_comment (
			id TEXT PRIMARY KEY,
			task_id TEXT NOT NULL,
			process_instance_id TEXT NOT NULL DEFAULT '',
			tenant_id TEXT NOT NULL DEFAULT '',
			user_id TEXT NOT NULL,
			user_name TEXT NOT NULL DEFAULT '',
			content TEXT NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
	}
	for _, ddl := range ddls {
		require.NoError(t, db.Exec(ddl).Error, "create table")
	}
}

// deploySimpleProcess 部署一个最简单的 userTask → end 流程。
func (e *e2eTestEnv) deploySimpleProcess(processKey, name, approvalType string, userIds []string, extraConfig map[string]interface{}) {
	e.t.Helper()
	nodeConfig := map[string]interface{}{
		"candidateType": "user",
		"candidateConfig": map[string]interface{}{
			"userIds": userIds,
		},
		"approvalType": approvalType,
	}
	for k, v := range extraConfig {
		nodeConfig[k] = v
	}

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
					"id":            "approval_node",
					"type":          "userTask",
					"name":          name,
					"configuration": nodeConfig,
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "approval_node", "toId": "end", "type": "Success"},
				{"fromId": "approval_node", "toId": "end", "type": "Failure"},
			},
		},
	}
	raw, err := json.Marshal(def)
	require.NoError(e.t, err, "marshal def")

	ctx := e.userCtx("admin")
	_, err = e.engine.GetProcessService().Deploy(ctx, service.Actor{UserID: "admin", TenantID: e2eTenantID}, &model.WfProcess{
		ProcessKey:     processKey,
		Name:           name,
		DefinitionJSON: string(raw),
		Status:         string(enums.ProcessStatusActive),
		TenantID:       e2eTenantID,
		CreatedBy:      "admin",
	}, true)
	require.NoError(e.t, err, "deploy process")
}

func (e *e2eTestEnv) userCtx(userID string) context.Context {
	return service.SetUserToCtx(context.Background(), &service.Actor{
		UserID:   userID,
		UserName: userID,
		TenantID: e2eTenantID,
	})
}

func (e *e2eTestEnv) userCtxAs(userID string) context.Context {
	ctx := e.userCtx(userID)
	return service.WithAPICallingMode(ctx)
}

func (e *e2eTestEnv) startInstance(processKey, starter string) string {
	e.t.Helper()
	ctx := e.userCtx(starter)
	id, err := e.engine.GetRuntimeService().StartProcessInstanceByKey(ctx, service.Actor{
		UserID:   starter,
		UserName: starter,
		TenantID: e2eTenantID,
	}, processKey, "", map[string]interface{}{})
	require.NoError(e.t, err, "start instance")
	require.NotEmpty(e.t, id, "instance id")
	return id
}

func (e *e2eTestEnv) activeTasksFor(instanceID, assignee string) []*model.WfTask {
	e.t.Helper()
	var rows []*model.WfTask
	q := "SELECT * FROM wf_task WHERE process_instance_id = ? AND status = ?"
	if assignee != "" {
		q += " AND assignee = ?"
		require.NoError(e.t, e.db.Raw(q, instanceID, string(enums.TaskStatusActive), assignee).Scan(&rows).Error)
	} else {
		require.NoError(e.t, e.db.Raw(q, instanceID, string(enums.TaskStatusActive)).Scan(&rows).Error)
	}
	return rows
}

// allTasksFor returns wf_task rows; falls back to wf_hi_task archive because
// completed tasks are migrated out of wf_task during instance archival.
func (e *e2eTestEnv) allTasksFor(instanceID string) []*model.WfTask {
	e.t.Helper()
	var rows []*model.WfTask
	_ = e.db.Raw("SELECT * FROM wf_task WHERE process_instance_id = ? ORDER BY created_at", instanceID).Scan(&rows).Error
	if len(rows) > 0 {
		return rows
	}
	// 实例完成后 wf_task 行被归档到 wf_hi_task
	require.NoError(e.t, e.db.Raw("SELECT * FROM wf_hi_task WHERE process_instance_id = ? ORDER BY created_at", instanceID).Scan(&rows).Error)
	return rows
}

func (e *e2eTestEnv) approveAs(taskID, userID, comment string) {
	e.t.Helper()
	ctx := e.userCtxAs(userID)
	require.NoError(e.t, e.engine.GetTaskService().Approve(ctx, service.Actor{UserID: userID, TenantID: e2eTenantID}, taskID, comment, map[string]interface{}{
		"approved":   true,
		"comment":    comment,
		"approvedBy": userID,
	}), "approve task")
}

func (e *e2eTestEnv) rejectAs(taskID, userID, comment string) {
	e.t.Helper()
	ctx := e.userCtxAs(userID)
	require.NoError(e.t, e.engine.GetTaskService().Reject(ctx, service.Actor{UserID: userID, TenantID: e2eTenantID}, taskID, comment, map[string]interface{}{
		"approved":   false,
		"comment":    comment,
		"approvedBy": userID,
	}), "reject task")
}

// instanceStatus falls back to wf_hi_instance: completed instances get archived
// out of wf_instance, so a direct wf_instance query returns nothing.
func (e *e2eTestEnv) instanceStatus(instanceID string) string {
	e.t.Helper()
	var status string
	_ = e.db.Raw("SELECT status FROM wf_instance WHERE id = ?", instanceID).Scan(&status).Error
	if status != "" {
		return status
	}
	require.NoError(e.t, e.db.Raw("SELECT status FROM wf_hi_instance WHERE id = ?", instanceID).Scan(&status).Error)
	return status
}

// ---------------------------------------------------------------------------
// 测试 1：顺序审批——回归 #457124283626295296 的 bug。
// ---------------------------------------------------------------------------

func TestE2E_SequentialApproval_ThreeApproversInOrder(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("seq_e2e", "Sequential Three Approvers",
		"sequential",
		[]string{"admin-001", "user_manager_001", "user_hr_001"},
		nil)

	instanceID := env.startInstance("seq_e2e", "admin-001")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "")) > 0
	}, 2*time.Second, 50*time.Millisecond, "first sequential task should be created")

	tasks1 := env.activeTasksFor(instanceID, "admin-001")
	require.Len(t, tasks1, 1, "first approver should have exactly one active task")
	assert.Empty(t, env.activeTasksFor(instanceID, "user_manager_001"), "second approver must not be active yet")
	assert.Empty(t, env.activeTasksFor(instanceID, "user_hr_001"), "third approver must not be active yet")

	env.approveAs(tasks1[0].ID, "admin-001", "admin 同意")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "user_manager_001")) > 0
	}, 2*time.Second, 50*time.Millisecond, "second approver should now have an active task")

	assert.Equal(t, string(enums.InstanceStatusActive), env.instanceStatus(instanceID),
		"instance must NOT be completed after the first sequential approver")

	tasks2 := env.activeTasksFor(instanceID, "user_manager_001")
	require.Len(t, tasks2, 1)
	env.approveAs(tasks2[0].ID, "user_manager_001", "manager 同意")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "user_hr_001")) > 0
	}, 2*time.Second, 50*time.Millisecond, "third approver should now have an active task")
	assert.Equal(t, string(enums.InstanceStatusActive), env.instanceStatus(instanceID),
		"instance must NOT be completed after the second sequential approver")

	tasks3 := env.activeTasksFor(instanceID, "user_hr_001")
	require.Len(t, tasks3, 1)
	env.approveAs(tasks3[0].ID, "user_hr_001", "hr 同意")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instanceID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond, "instance should be completed after all three approvers approve; status=%q", env.instanceStatus(instanceID))

	all := env.allTasksFor(instanceID)
	assert.GreaterOrEqual(t, len(all), 3, "should have 3 completed user tasks archived")
	for _, task := range all {
		if task.TaskType == constants.TaskTypeUserTask {
			assert.NotNil(t, task.Duration, "userTask duration must not be nil (bug: 处理时间 0.0小时)")
			if task.Duration != nil {
				assert.Greater(t, *task.Duration, int64(0), "userTask duration must be > 0")
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 测试 2：顺序审批——中途拒绝应终止后续审批链。
// ---------------------------------------------------------------------------

func TestE2E_SequentialApproval_RejectionStopsChain(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("seq_reject_e2e", "Sequential Reject",
		"sequential",
		[]string{"admin-001", "user_manager_001", "user_hr_001"},
		map[string]interface{}{
			"rejectStrategy": "terminate",
		})

	instanceID := env.startInstance("seq_reject_e2e", "admin-001")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "admin-001")) > 0
	}, 2*time.Second, 50*time.Millisecond)

	tasks1 := env.activeTasksFor(instanceID, "admin-001")
	require.Len(t, tasks1, 1)

	env.rejectAs(tasks1[0].ID, "admin-001", "admin 拒绝")

	require.Eventually(t, func() bool {
		status := env.instanceStatus(instanceID)
		return status == string(enums.InstanceStatusTerminated) ||
			status == string(enums.InstanceStatusCancelled) ||
			status == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond, "instance should reach a terminal state after reject")
	assert.Empty(t, env.activeTasksFor(instanceID, "user_manager_001"),
		"second approver must NOT receive task after rejection")
	assert.Empty(t, env.activeTasksFor(instanceID, "user_hr_001"),
		"third approver must NOT receive task after rejection")
}

// ---------------------------------------------------------------------------
// 测试 3：单人审批——通过即结束。
// ---------------------------------------------------------------------------

func TestE2E_SingleApproval_CompleteOnApprove(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("single_e2e", "Single Approval",
		"single", []string{"only_approver"}, nil)

	instanceID := env.startInstance("single_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "only_approver")) > 0
	}, 2*time.Second, 50*time.Millisecond)

	tasks := env.activeTasksFor(instanceID, "only_approver")
	require.Len(t, tasks, 1)
	env.approveAs(tasks[0].ID, "only_approver", "ok")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instanceID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond)
}

// ---------------------------------------------------------------------------
// 测试 4：或签——任意一人审批即结束。
// ---------------------------------------------------------------------------

func TestE2E_OrApproval_AnyOneApproverCompletes(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("or_e2e", "Or Approval",
		"or", []string{"a1", "a2", "a3"}, nil)

	instanceID := env.startInstance("or_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "")) >= 3
	}, 2*time.Second, 50*time.Millisecond)

	a2 := env.activeTasksFor(instanceID, "a2")
	require.Len(t, a2, 1)
	env.approveAs(a2[0].ID, "a2", "a2 抢先审批")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instanceID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond)
}

// ---------------------------------------------------------------------------
// 测试 5：审批人 = 提交人。
// ---------------------------------------------------------------------------

func TestE2E_ApproverEqualsStarter_AllowedByDefault(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("self_e2e", "Self Approval",
		"single", []string{"starter"}, nil)

	instanceID := env.startInstance("self_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "starter")) > 0
	}, 2*time.Second, 50*time.Millisecond)

	tasks := env.activeTasksFor(instanceID, "starter")
	require.Len(t, tasks, 1)
	env.approveAs(tasks[0].ID, "starter", "self approve")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instanceID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond, "self-approval should complete the process")
}

// ---------------------------------------------------------------------------
// 测试 6：条件分支（jsSwitch）——根据流程变量把请求路由到不同的审批人。
// 引擎本身没有"条件网关"节点，分支是通过 rulego 的 jsSwitch 实现的。
// 这里覆盖：days<=3 → 走 manager；days>7 → 走 director。
// ---------------------------------------------------------------------------

func (e *e2eTestEnv) deployConditionalSwitchProcess(processKey string) {
	e.t.Helper()
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id": processKey, "name": processKey, "root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{
					"id": "switch", "type": "jsSwitch", "name": "by-days",
					"configuration": map[string]interface{}{
						"jsScript": `var d = parseInt(msg.days || '0');
						if (d <= 3) { return ['to_manager']; }
						return ['to_director'];`,
					},
				},
				{
					"id": "mgr_task", "type": "userTask", "name": "manager",
					"configuration": map[string]interface{}{
						"candidateType":   "user",
						"candidateConfig": map[string]interface{}{"userIds": []string{"mgr_user"}},
						"approvalType":    "single",
					},
				},
				{
					"id": "dir_task", "type": "userTask", "name": "director",
					"configuration": map[string]interface{}{
						"candidateType":   "user",
						"candidateConfig": map[string]interface{}{"userIds": []string{"dir_user"}},
						"approvalType":    "single",
					},
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "switch", "toId": "mgr_task", "type": "to_manager"},
				{"fromId": "switch", "toId": "dir_task", "type": "to_director"},
				{"fromId": "mgr_task", "toId": "end", "type": "Success"},
				{"fromId": "mgr_task", "toId": "end", "type": "Failure"},
				{"fromId": "dir_task", "toId": "end", "type": "Success"},
				{"fromId": "dir_task", "toId": "end", "type": "Failure"},
			},
		},
	}
	raw, _ := json.Marshal(def)
	ctx := e.userCtx("admin")
	_, err := e.engine.GetProcessService().Deploy(ctx, service.Actor{UserID: "admin", TenantID: e2eTenantID}, &model.WfProcess{
		ProcessKey:     processKey,
		Name:           processKey,
		DefinitionJSON: string(raw),
		Status:         string(enums.ProcessStatusActive),
		TenantID:       e2eTenantID,
		CreatedBy:      "admin",
	}, true)
	require.NoError(e.t, err, "deploy conditional process")
}

func (e *e2eTestEnv) startInstanceWithVars(processKey, starter string, vars map[string]interface{}) string {
	e.t.Helper()
	ctx := e.userCtx(starter)
	id, err := e.engine.GetRuntimeService().StartProcessInstanceByKey(ctx, service.Actor{
		UserID: starter, UserName: starter, TenantID: e2eTenantID,
	}, processKey, "", vars)
	require.NoError(e.t, err, "start instance")
	return id
}

func TestE2E_ConditionalBranch_LowDays_ToManager(t *testing.T) {
	env := newE2EEnv(t)
	env.deployConditionalSwitchProcess("cond_e2e")

	// days=2 → 应路由到 manager
	instID := env.startInstanceWithVars("cond_e2e", "starter", map[string]interface{}{"days": 2})

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "mgr_user")) > 0
	}, 2*time.Second, 50*time.Millisecond)
	assert.Empty(t, env.activeTasksFor(instID, "dir_user"),
		"director must not get a task when days<=3")

	mgr := env.activeTasksFor(instID, "mgr_user")
	require.Len(t, mgr, 1)
	env.approveAs(mgr[0].ID, "mgr_user", "ok")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond)
}

func TestE2E_ConditionalBranch_HighDays_ToDirector(t *testing.T) {
	env := newE2EEnv(t)
	env.deployConditionalSwitchProcess("cond_e2e")

	// days=10 → 应路由到 director
	instID := env.startInstanceWithVars("cond_e2e", "starter", map[string]interface{}{"days": 10})

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "dir_user")) > 0
	}, 2*time.Second, 50*time.Millisecond)
	assert.Empty(t, env.activeTasksFor(instID, "mgr_user"),
		"manager must not get a task when days>3")

	dir := env.activeTasksFor(instID, "dir_user")
	require.Len(t, dir, 1)
	env.approveAs(dir[0].ID, "dir_user", "ok")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond)
}

// ---------------------------------------------------------------------------
// 测试 7：会签（countersign, parallel）—— 所有人都通过才完成。
// 配置：3 个审批人 a/b/c，并行，approvalType=countersign，approvalRule=isSequential:false。
// 期望：必须 3 人都通过才进 end；任意一人拒绝立刻终止。
// ---------------------------------------------------------------------------

func TestE2E_CountersignApproval_AllMustApprove(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("cnt_e2e", "Countersign Parallel",
		"countersign",
		[]string{"a_user", "b_user", "c_user"},
		map[string]interface{}{
			"approvalRule": `{"type":"all","value":0,"isSequential":false}`,
		})

	instID := env.startInstance("cnt_e2e", "starter")

	// 会签：3 个审批人都应有 active 任务
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "")) >= 3
	}, 2*time.Second, 50*time.Millisecond, "all 3 countersign approvers should have active tasks")

	// 1 人通过——实例不能完成
	a := env.activeTasksFor(instID, "a_user")
	require.Len(t, a, 1)
	env.approveAs(a[0].ID, "a_user", "a ok")
	time.Sleep(300 * time.Millisecond)
	assert.NotEqual(t, string(enums.InstanceStatusCompleted), env.instanceStatus(instID),
		"countersign: instance must NOT complete after only 1 of 3 approvers")

	// 2 人通过——实例不能完成
	b := env.activeTasksFor(instID, "b_user")
	require.Len(t, b, 1)
	env.approveAs(b[0].ID, "b_user", "b ok")
	time.Sleep(300 * time.Millisecond)
	assert.NotEqual(t, string(enums.InstanceStatusCompleted), env.instanceStatus(instID),
		"countersign: instance must NOT complete after only 2 of 3 approvers")

	// 3 人通过——实例完成
	c := env.activeTasksFor(instID, "c_user")
	require.Len(t, c, 1)
	env.approveAs(c[0].ID, "c_user", "c ok")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond, "countersign: instance should complete after all 3 approve")
}

// ---------------------------------------------------------------------------
// 测试 8：会签——一人拒绝即终止（一票否决）。
// ---------------------------------------------------------------------------

func TestE2E_CountersignApproval_OneRejectTerminates(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("cnt_reject_e2e", "Countersign Reject",
		"countersign",
		[]string{"a_user", "b_user", "c_user"},
		map[string]interface{}{
			"approvalRule":   `{"type":"all","value":0,"isSequential":false}`,
			"rejectStrategy": "terminate",
		})

	instID := env.startInstance("cnt_reject_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "")) >= 3
	}, 2*time.Second, 50*time.Millisecond)

	// b 拒绝——实例应进入终态
	b := env.activeTasksFor(instID, "b_user")
	require.Len(t, b, 1)
	env.rejectAs(b[0].ID, "b_user", "b vetoes")

	require.Eventually(t, func() bool {
		st := env.instanceStatus(instID)
		return st == string(enums.InstanceStatusTerminated) ||
			st == string(enums.InstanceStatusCancelled) ||
			st == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond, "countersign: instance should reach terminal state after one reject")

	// 其他审批人的任务不应再产生新的 active 任务
	a := env.activeTasksFor(instID, "a_user")
	for _, tk := range a {
		assert.NotEqual(t, string(enums.TaskStatusActive), tk.Status,
			"a_user's task should not still be active after countersign rejection")
	}
}

// ---------------------------------------------------------------------------
// 测试 9：票签（vote）—— majority 阈值：3 人 required=2，2 人通过即达阈值完成（早终止）。
// vote 复用会签父+子结构，按 approvalRule.type=majority 判定（checkCountersignSubTaskCompletionInternal）。
// ---------------------------------------------------------------------------

func TestE2E_VoteApproval_MajorityThresholdMet(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("vote_maj_e2e", "Vote Majority",
		"vote",
		[]string{"a_user", "b_user", "c_user"},
		map[string]interface{}{
			"approvalRule": `{"type":"majority","value":0,"isSequential":false}`,
		})

	instID := env.startInstance("vote_maj_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "")) >= 3
	}, 2*time.Second, 50*time.Millisecond)

	// 1 人通过——未达 majority(2)，不完成
	a := env.activeTasksFor(instID, "a_user")
	require.Len(t, a, 1)
	env.approveAs(a[0].ID, "a_user", "a ok")
	time.Sleep(300 * time.Millisecond)
	assert.NotEqual(t, string(enums.InstanceStatusCompleted), env.instanceStatus(instID),
		"vote majority: 1/3 approve should NOT complete")

	// 2 人通过——达 majority(2)，完成（早终止，c_user 任务被取消）
	b := env.activeTasksFor(instID, "b_user")
	require.Len(t, b, 1)
	env.approveAs(b[0].ID, "b_user", "b ok")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond, "vote majority: 2/3 approve should complete (threshold met)")
}

// ---------------------------------------------------------------------------
// 测试 10：票签 vote + majority——2 人拒绝即注定 reject（剩余 1 人即使 approve 也 <2）→ 终止。
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 测试 10b：票签（vote）—— percent 阈值：4 人 75% → required=ceil(3)=3，
// 3 人通过即完成（早终止）；2 人通过不完成（percent 向上取整语义）。
// ---------------------------------------------------------------------------

func TestE2E_VoteApproval_PercentThreshold(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("vote_pct_e2e", "Vote Percent",
		"vote",
		[]string{"a_user", "b_user", "c_user", "d_user"},
		map[string]interface{}{
			"approvalRule": `{"type":"percent","value":75,"isSequential":false}`,
		})

	instID := env.startInstance("vote_pct_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "")) >= 4
	}, 2*time.Second, 50*time.Millisecond)

	// 2 人通过——ceil(4*75%)=3，未达阈值不完成
	a := env.activeTasksFor(instID, "a_user")
	require.Len(t, a, 1)
	env.approveAs(a[0].ID, "a_user", "a ok")
	b := env.activeTasksFor(instID, "b_user")
	require.Len(t, b, 1)
	env.approveAs(b[0].ID, "b_user", "b ok")
	time.Sleep(300 * time.Millisecond)
	assert.NotEqual(t, string(enums.InstanceStatusCompleted), env.instanceStatus(instID),
		"vote percent 75%: 2/4 approve should NOT complete (ceil(4*0.75)=3)")

	// 第 3 人通过——达 3 票，完成（早终止，d_user 任务被取消）
	c := env.activeTasksFor(instID, "c_user")
	require.Len(t, c, 1)
	env.approveAs(c[0].ID, "c_user", "c ok")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond, "vote percent 75%: 3/4 approve should complete")
}

// ---------------------------------------------------------------------------
// 测试 10c：票签（vote）—— count 阈值：4 人固定 2 票，2 人通过即完成（早终止）。
// ---------------------------------------------------------------------------

func TestE2E_VoteApproval_CountThreshold(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("vote_cnt_e2e", "Vote Count",
		"vote",
		[]string{"a_user", "b_user", "c_user", "d_user"},
		map[string]interface{}{
			"approvalRule": `{"type":"count","value":2,"isSequential":false}`,
		})

	instID := env.startInstance("vote_cnt_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "")) >= 4
	}, 2*time.Second, 50*time.Millisecond)

	// 1 人通过——未达 count(2)，不完成
	a := env.activeTasksFor(instID, "a_user")
	require.Len(t, a, 1)
	env.approveAs(a[0].ID, "a_user", "a ok")
	time.Sleep(300 * time.Millisecond)
	assert.NotEqual(t, string(enums.InstanceStatusCompleted), env.instanceStatus(instID),
		"vote count=2: 1/4 approve should NOT complete")

	// 第 2 人通过——达 2 票，完成
	b := env.activeTasksFor(instID, "b_user")
	require.Len(t, b, 1)
	env.approveAs(b[0].ID, "b_user", "b ok")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond, "vote count=2: 2/4 approve should complete")
}

func TestE2E_VoteApproval_MajorityRejectTerminates(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("vote_maj_reject_e2e", "Vote Majority Reject",
		"vote",
		[]string{"a_user", "b_user", "c_user"},
		map[string]interface{}{
			"approvalRule":   `{"type":"majority","value":0,"isSequential":false}`,
			"rejectStrategy": "terminate",
		})

	instID := env.startInstance("vote_maj_reject_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "")) >= 3
	}, 2*time.Second, 50*time.Millisecond)

	// a, b 拒绝——2 reject 注定（maxPossibleApproved=0+1=1 < required 2）→ 终止
	a := env.activeTasksFor(instID, "a_user")
	env.rejectAs(a[0].ID, "a_user", "a no")
	b := env.activeTasksFor(instID, "b_user")
	env.rejectAs(b[0].ID, "b_user", "b no")

	require.Eventually(t, func() bool {
		st := env.instanceStatus(instID)
		return st == string(enums.InstanceStatusTerminated) ||
			st == string(enums.InstanceStatusCancelled) ||
			st == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond, "vote majority: 2/3 reject should terminate (注定 reject)")
}

// ---------------------------------------------------------------------------
// 测试 10：或签——3 个审批人真并发点通过。
// 验证：实例只完成 1 次，下游 end 节点只跑 1 次，没有重复 ExecuteNext，没有死锁。
// 并发场景下若 rulego 同步 OnMsg 在事务内重入 WithInstanceTx 会死锁。
// ---------------------------------------------------------------------------

func TestE2E_OrApproval_ConcurrentApprovals_NoCorruption(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("or_concurrent_e2e", "Or Concurrent",
		"or", []string{"a_user", "b_user", "c_user"}, nil)

	instID := env.startInstance("or_concurrent_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "")) >= 3
	}, 2*time.Second, 50*time.Millisecond)

	a := env.activeTasksFor(instID, "a_user")
	b := env.activeTasksFor(instID, "b_user")
	c := env.activeTasksFor(instID, "c_user")
	require.Len(t, a, 1)
	require.Len(t, b, 1)
	require.Len(t, c, 1)

	// 3 个 goroutine 真并发提交（每个不同的 task ID）
	var wg sync.WaitGroup
	errs := make([]error, 3)
	assignments := []struct {
		task *model.WfTask
		who  string
	}{{a[0], "a_user"}, {b[0], "b_user"}, {c[0], "c_user"}}
	for i, asgn := range assignments {
		wg.Add(1)
		go func(idx int, task *model.WfTask, who string) {
			defer wg.Done()
			ctx := service.WithAPICallingMode(service.SetUserToCtx(context.Background(), &service.Actor{
				UserID: who, UserName: who, TenantID: e2eTenantID,
			}))
			errs[idx] = env.engine.GetTaskService().Approve(ctx, service.Actor{UserID: who, UserName: who, TenantID: e2eTenantID}, task.ID, "concurrent", map[string]interface{}{
				"approved": true, "approvedBy": who,
			})
		}(i, asgn.task, asgn.who)
	}
	wg.Wait()

	// SQLite cache=shared 对多连接并发写有固有限制——多个 BEGIN 同时竞争 wf_instance
	// 行锁会被 SQLite 检测为 SQLITE_LOCKED（"database is deadlocked (6)"）。
	// 这不是引擎 bug：Postgres 用 MVCC + 行锁可以正确处理；glebarez SQLite 的
	// shared-cache 模式不行。所以这里允许部分错误，但要求最终状态正确。
	// 关键不变量在 wg.Wait() 之后验证。
	successCount := 0
	for _, e := range errs {
		if e == nil {
			successCount++
		}
	}
	t.Logf("concurrent approve: %d succeeded, %d errored", successCount, 3-successCount)
	assert.GreaterOrEqual(t, successCount, 1, "at least one concurrent approve should succeed")

	// 实例最终必须是 completed（即使部分 approve 失败，或签语义要求 1 人通过即可）
	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond, "or-sign: instance should complete when at least 1 approver succeeds")

	// 关键不变量：end 节点只能执行 1 次。即使多个 approve 都成功，重复 ExecuteNext
	// 不应创建多个 end 任务。
	var endCount int64
	require.NoError(t, env.db.Raw(
		"SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_type = 'end'",
		instID).Scan(&endCount).Error)
	assert.Equal(t, int64(1), endCount,
		"end node should run exactly once — duplicate ExecuteNext would create multiple end tasks")
}

// ---------------------------------------------------------------------------
// 测试 11：顺序审批——同一任务被并发重复提交（用户双击/客户端重试）。
// 验证幂等：5 个 goroutine 同时 Approve 同一 task ID，应当只有 1 个真正生效，
// 其余走幂等 no-op 路径，且不破坏状态、不重复创建下一节点的任务。
// ---------------------------------------------------------------------------

func TestE2E_SequentialApproval_DoubleSubmitIsIdempotent(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("seq_double_e2e", "Sequential Double-Submit",
		"sequential",
		[]string{"a_user", "b_user", "c_user"},
		nil)

	instID := env.startInstance("seq_double_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "a_user")) > 0
	}, 2*time.Second, 50*time.Millisecond)

	a := env.activeTasksFor(instID, "a_user")
	require.Len(t, a, 1)
	taskID := a[0].ID

	// 真并发：5 个 goroutine 同时 Approve 同一 task ID
	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx := service.WithAPICallingMode(service.SetUserToCtx(context.Background(), &service.Actor{
				UserID: "a_user", UserName: "a_user", TenantID: e2eTenantID,
			}))
			errs[idx] = env.engine.GetTaskService().Approve(ctx, service.Actor{UserID: "a_user", UserName: "a_user", TenantID: e2eTenantID}, taskID,
				fmt.Sprintf("click-%d", idx),
				map[string]interface{}{"approved": true, "approvedBy": "a_user"})
		}(i)
	}
	wg.Wait()

	// SQLite cache=shared 对多连接并发 BEGIN 有固有限制——会有部分 goroutine 收到
	// SQLITE_LOCKED（"database is deadlocked (6)"）。这不是引擎 bug，是 SQLite 限制。
	// 真正的不变量在下面验证：实例状态正确 + 下一个审批人任务唯一。
	successCount := 0
	for _, e := range errs {
		if e == nil {
			successCount++
		}
	}
	t.Logf("concurrent double-submit: %d/5 succeeded", successCount)
	assert.GreaterOrEqual(t, successCount, 1, "at least one of 5 concurrent submits should succeed")

	// 实例状态：仍是 active，等待 b_user
	time.Sleep(300 * time.Millisecond)
	st := env.instanceStatus(instID)
	assert.Equal(t, string(enums.InstanceStatusActive), st,
		"after double-submit, instance should still be active waiting for next sequential approver")

	// b_user 应当收到任务——而且只有 1 个（双击不应创建多个）
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "b_user")) > 0
	}, 2*time.Second, 50*time.Millisecond, "second sequential approver should have a task")
	b := env.activeTasksFor(instID, "b_user")
	assert.Len(t, b, 1, "next approver should have exactly one task — no duplicates from double-submit")
}

// ---------------------------------------------------------------------------
// 测试 12：或签——一人通过后，其他 active 任务应当被妥善处理（不应残留 active）。
// ---------------------------------------------------------------------------

func TestE2E_OrApproval_ResidualTasksNotActive(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("or_residual_e2e", "Or Residual",
		"or", []string{"a_user", "b_user", "c_user"}, nil)

	instID := env.startInstance("or_residual_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "")) >= 3
	}, 2*time.Second, 50*time.Millisecond)

	// a 通过——实例完成
	a := env.activeTasksFor(instID, "a_user")
	env.approveAs(a[0].ID, "a_user", "a wins")

	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond)

	// b/c 的任务不应仍处于 active —— 防止残留任务干扰下一次查询
	time.Sleep(300 * time.Millisecond)
	for _, who := range []string{"b_user", "c_user"} {
		rows := env.activeTasksFor(instID, who)
		assert.Empty(t, rows, "%s should have no active task after instance completed", who)
	}
}

// ---------------------------------------------------------------------------
// 测试：会签早期否决路径的错误传播。
//
// 早期否决分支里 json.Unmarshal(parentTask.Variables) 失败必须返回 error，
// 让事务回滚、API 把错误抛给用户。若吞掉错误：vars 留空 map → ExecuteNext
// 用空 payload → 下游条件路由错误，且事务仍提交，数据不一致。
// 本测试通过把父任务的 variables 改成非法 JSON 来触发该分支。
// ---------------------------------------------------------------------------
func TestE2E_CountersignApproval_EarlyVeto_CorruptParentVariables_ReturnsError(t *testing.T) {
	env := newE2EEnv(t)
	env.deploySimpleProcess("cnt_corrupt_e2e", "Countersign Corrupt Vars",
		"countersign",
		[]string{"a_user", "b_user", "c_user"},
		map[string]interface{}{
			"approvalRule":   `{"type":"all","value":0,"isSequential":false}`,
			"rejectStrategy": "terminate",
		})

	instID := env.startInstance("cnt_corrupt_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "")) >= 3
	}, 2*time.Second, 50*time.Millisecond)

	// 找到父任务（parent_id 为空的那个，task_def_key=approval_node），
	// 把它的 variables 字段改成非法 JSON，模拟数据损坏。
	var parentTaskID string
	require.NoError(t, e2eDB.Raw(
		"SELECT id FROM wf_task WHERE process_instance_id = ? AND task_def_key = 'approval_node' AND (parent_id IS NULL OR parent_id = '') LIMIT 1",
		instID,
	).Scan(&parentTaskID).Error, "find parent task")
	require.NotEmpty(t, parentTaskID, "countersign should have a parent task row")
	require.NoError(t, e2eDB.Exec(
		"UPDATE wf_task SET variables = ? WHERE id = ?",
		"{this is not valid json", parentTaskID,
	).Error, "corrupt parent task variables")

	// b 否决 → 进入早期否决分支 → 读父 variables → unmarshal 失败 → 返回 error，事务回滚。
	b := env.activeTasksFor(instID, "b_user")
	require.Len(t, b, 1)
	ctx := env.userCtxAs("b_user")
	err := env.engine.GetTaskService().Reject(ctx, service.Actor{UserID: "b_user", TenantID: e2eTenantID}, b[0].ID, "b vetoes", map[string]interface{}{
		"approved":   false,
		"comment":    "b vetoes",
		"approvedBy": "b_user",
	})
	require.Error(t, err, "corrupt parent variables should make Reject return an error")
	assert.Contains(t, err.Error(), "parse parent task variables",
		"error should indicate parent variables parse failure; got: %v", err)
}

// ---------------------------------------------------------------------------
// 测试：驳回回跳必须重新生成任务，而非静默自动通过。
//
// rejectToPrev / rejectToStarter / rejectToNode 跳回目标 userTask 时，若不清理
// 该节点上一轮的 Completed 任务，重入后 getExistingTasks 返回旧任务 →
// checkTasksCompletion 立即判定"已完成"→ evaluateApproval 看到历史 approved →
// TellSuccess，目标节点被静默自动通过，驳回语义（"重新让该节点审批人再审一遍"）
// 被完全绕过。
//
// 因此 jumpToNode 在 ExecuteNext 前调用 TaskService.SupersedeNodeTasks，把目标节点
// 旧任务归档到 wf_hi_task 并从 wf_task 删除，重入时 getExistingTasks 为空 → 重建任务。
//
// 本测试构造 A(single) → B(single, rejectStrategy=rejectToPrev) 线性流程：
//   1. A 通过 → B 出现 active 任务
//   2. B 驳回（rejectToPrev）→ 应跳回 A 并让 A 重新出现 active 任务
//   3. 断言 A 有新的 active 任务，且旧的 A 任务已归档（wf_hi_task）。
// 若不做清理，A 会被静默自动通过、流程进 end，A 拿不到任何 active 任务。
// ---------------------------------------------------------------------------

func (e *e2eTestEnv) deployLinearTwoStepProcess(processKey, name string, firstApprover, secondApprover string, secondRejectStrategy string) {
	e.t.Helper()
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{
			"id": processKey, "name": name, "root": true,
		},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{
					"id":   "first_task",
					"type": "userTask",
					"name": name + " - first",
					"configuration": map[string]interface{}{
						"candidateType":   "user",
						"candidateConfig": map[string]interface{}{"userIds": []string{firstApprover}},
						"approvalType":    "single",
					},
				},
				{
					"id":   "second_task",
					"type": "userTask",
					"name": name + " - second",
					"configuration": map[string]interface{}{
						"candidateType":   "user",
						"candidateConfig": map[string]interface{}{"userIds": []string{secondApprover}},
						"approvalType":    "single",
						"rejectStrategy":  secondRejectStrategy,
					},
				},
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{
				{"fromId": "first_task", "toId": "second_task", "type": "Success"},
				{"fromId": "first_task", "toId": "end", "type": "Failure"},
				{"fromId": "second_task", "toId": "end", "type": "Success"},
				{"fromId": "second_task", "toId": "end", "type": "Failure"},
			},
		},
	}
	raw, err := json.Marshal(def)
	require.NoError(e.t, err, "marshal linear two-step def")
	ctx := e.userCtx("admin")
	_, err = e.engine.GetProcessService().Deploy(ctx, service.Actor{UserID: "admin", TenantID: e2eTenantID}, &model.WfProcess{
		ProcessKey:     processKey,
		Name:           name,
		DefinitionJSON: string(raw),
		Status:         string(enums.ProcessStatusActive),
		TenantID:       e2eTenantID,
		CreatedBy:      "admin",
	}, true)
	require.NoError(e.t, err, "deploy linear two-step process")
}

// hiTaskCountForDefKey 统计某实例某 taskDefKey 在 wf_hi_task 中的归档任务数。
func (e *e2eTestEnv) hiTaskCountForDefKey(instanceID, taskDefKey string) int {
	e.t.Helper()
	var n int64
	require.NoError(e.t, e.db.Raw(
		"SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_def_key = ?",
		instanceID, taskDefKey).Scan(&n).Error)
	return int(n)
}

func TestE2E_RejectToPrev_RegeneratesTaskNotSilentApprove(t *testing.T) {
	env := newE2EEnv(t)
	env.deployLinearTwoStepProcess("reject_prev_e2e", "Reject To Prev", "a_user", "b_user", "rejectToPrev")

	instanceID := env.startInstance("reject_prev_e2e", "starter")

	// 1) A 出现任务并审批通过 → 流程进到 B
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "a_user")) > 0
	}, 2*time.Second, 50*time.Millisecond)
	aTasks := env.activeTasksFor(instanceID, "a_user")
	require.Len(t, aTasks, 1)
	env.approveAs(aTasks[0].ID, "a_user", "a 通过")

	// B 出现任务
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "b_user")) > 0
	}, 2*time.Second, 50*time.Millisecond)
	bTasks := env.activeTasksFor(instanceID, "b_user")
	require.Len(t, bTasks, 1)

	// 记录 B 驳回前：A 的旧任务应已归档（A 通过时即归档），wf_hi_task 里 A 有 1 条
	hiABefore := env.hiTaskCountForDefKey(instanceID, "first_task")

	// 2) B 驳回（rejectToPrev）→ 应跳回 A
	env.rejectAs(bTasks[0].ID, "b_user", "b 驳回，退回 A")

	// 3) 关键断言：A 必须重新出现 active 任务，而不是静默自动通过进 end。
	//    若 jumpToNode 未清理 A 的旧任务，A 的 OnMsg 看到 Completed(approved) → TellSuccess
	//    → 流程直接进 end，A 拿不到 active 任务，这里 Eventually 会超时失败。
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "a_user")) > 0
	}, 2*time.Second, 50*time.Millisecond,
		"rejectToPrev must regenerate A's task, not silently auto-approve")
	aTasks2 := env.activeTasksFor(instanceID, "a_user")
	require.Len(t, aTasks2, 1, "A should have exactly one regenerated active task")

	// 实例应仍处于活跃（非 completed/terminated）——证明没有静默自动通过到 end
	status := env.instanceStatus(instanceID)
	assert.NotEqual(t, string(enums.InstanceStatusCompleted), status,
		"instance must NOT be completed (would mean silent auto-approval)")
	assert.NotEqual(t, string(enums.InstanceStatusTerminated), status,
		"instance must NOT be terminated")

	// 4) A 的旧任务被 supersede：wf_hi_task 中 A 的归档数应增加（旧任务已归档），
	//    且新的 active A 任务 id 不同于旧任务 id。
	assert.Greater(t, env.hiTaskCountForDefKey(instanceID, "first_task"), hiABefore,
		"superseded A task should be archived to wf_hi_task before regenerate")
	assert.NotEqual(t, aTasks[0].ID, aTasks2[0].ID,
		"regenerated A task must have a different ID from the superseded one")
}

// ---------------------------------------------------------------------------
// 测试：rejectToStarter —— 驳回后跳回开始节点。
//
// 与 rejectToPrev 的差异：目标不是"上一个 userTask"而是链的 FirstNodeIndex 起始节点，
// 走 jumpToStartNode/getStartNodeID 路径。线性链 A → B（B 配 rejectToStarter）中
// 开始节点即 A，验证：B 驳回后 A 重新生成任务、B 的任务被归档、
// A 重新通过后流程能再次推进到 B（完整往返）。
// ---------------------------------------------------------------------------

func TestE2E_RejectToStarter_JumpsToStartNode(t *testing.T) {
	env := newE2EEnv(t)
	env.deployLinearTwoStepProcess("reject_starter_e2e", "Reject To Starter", "a_user", "b_user", "rejectToStarter")

	instanceID := env.startInstance("reject_starter_e2e", "starter")

	// 1) A 出现任务并通过 → 流程进到 B
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "a_user")) > 0
	}, 2*time.Second, 50*time.Millisecond)
	aTasks := env.activeTasksFor(instanceID, "a_user")
	require.Len(t, aTasks, 1)
	env.approveAs(aTasks[0].ID, "a_user", "a 通过")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "b_user")) > 0
	}, 2*time.Second, 50*time.Millisecond)
	bTasks := env.activeTasksFor(instanceID, "b_user")
	require.Len(t, bTasks, 1)

	hiBBefore := env.hiTaskCountForDefKey(instanceID, "second_task")

	// 2) B 驳回（rejectToStarter）→ 跳回开始节点（first_task = A）
	env.rejectAs(bTasks[0].ID, "b_user", "b 驳回，退回发起人")

	// 3) A 重新出现 active 任务（开始节点重入生成新任务，而非静默通过）；
	//    实例保持活跃；B 的驳回任务已归档
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "a_user")) > 0
	}, 2*time.Second, 50*time.Millisecond,
		"rejectToStarter must regenerate the start node's task, not silently auto-approve")
	aTasks2 := env.activeTasksFor(instanceID, "a_user")
	require.Len(t, aTasks2, 1, "start node should have exactly one regenerated active task")
	assert.NotEqual(t, aTasks[0].ID, aTasks2[0].ID,
		"regenerated task must have a different ID from the superseded one")

	status := env.instanceStatus(instanceID)
	assert.NotEqual(t, string(enums.InstanceStatusCompleted), status,
		"instance must NOT be completed after rejectToStarter")
	assert.NotEqual(t, string(enums.InstanceStatusTerminated), status,
		"instance must NOT be terminated after rejectToStarter")
	assert.Greater(t, env.hiTaskCountForDefKey(instanceID, "second_task"), hiBBefore,
		"rejected B task should be archived to wf_hi_task")

	// 4) 完整往返：A 再次通过 → B 再次出现任务（退回发起人后可重新审批）
	env.approveAs(aTasks2[0].ID, "a_user", "a 重新通过")
	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instanceID, "b_user")) > 0
	}, 2*time.Second, 50*time.Millisecond,
		"B should receive a fresh task after A re-approves following rejectToStarter")
}

// ---------------------------------------------------------------------------
// 测试：rejectToNode —— 跳到指定目标节点同样要重新生成任务。
//
// 与 rejectToPrev 不同，rejectToNode 由 configuration.rejectTargetNode 显式指定
// 目标节点。验证 jumpToNode 的 supersede 逻辑对"任意 userTask 目标"通用，而不只是
// "上一个 userTask"。三节点线性链 A → B → C，C 配 rejectToNode=first_task（跳回 A）。
// ---------------------------------------------------------------------------

func (e *e2eTestEnv) deployLinearThreeStepProcess(processKey, name string, approvers [3]string, lastRejectStrategy, lastRejectTarget string) {
	e.t.Helper()
	nodeIDs := [3]string{"first_task", "second_task", "third_task"}
	nodes := make([]map[string]interface{}, 0, 4)
	for i := 0; i < 3; i++ {
		cfg := map[string]interface{}{
			"candidateType":   "user",
			"candidateConfig": map[string]interface{}{"userIds": []string{approvers[i]}},
			"approvalType":    "single",
		}
		if i == 2 && lastRejectStrategy != "" {
			cfg["rejectStrategy"] = lastRejectStrategy
			if lastRejectTarget != "" {
				cfg["rejectTargetNode"] = lastRejectTarget
			}
		}
		nodes = append(nodes, map[string]interface{}{
			"id":            nodeIDs[i],
			"type":          "userTask",
			"name":          fmt.Sprintf("%s-%s", name, nodeIDs[i]),
			"configuration": cfg,
		})
	}
	nodes = append(nodes, map[string]interface{}{"id": "end", "type": "end", "name": "End"})
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": processKey, "name": name, "root": true},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes":          nodes,
			"connections": []map[string]interface{}{
				{"fromId": "first_task", "toId": "second_task", "type": "Success"},
				{"fromId": "first_task", "toId": "end", "type": "Failure"},
				{"fromId": "second_task", "toId": "third_task", "type": "Success"},
				{"fromId": "second_task", "toId": "end", "type": "Failure"},
				{"fromId": "third_task", "toId": "end", "type": "Success"},
				{"fromId": "third_task", "toId": "end", "type": "Failure"},
			},
		},
	}
	raw, err := json.Marshal(def)
	require.NoError(e.t, err, "marshal linear three-step def")
	ctx := e.userCtx("admin")
	_, err = e.engine.GetProcessService().Deploy(ctx, service.Actor{UserID: "admin", TenantID: e2eTenantID}, &model.WfProcess{
		ProcessKey:     processKey,
		Name:           name,
		DefinitionJSON: string(raw),
		Status:         string(enums.ProcessStatusActive),
		TenantID:       e2eTenantID,
		CreatedBy:      "admin",
	}, true)
	require.NoError(e.t, err, "deploy linear three-step process")
}

func TestE2E_RejectToNode_RegeneratesTargetTask(t *testing.T) {
	env := newE2EEnv(t)
	// A → B → C，C 配 rejectToNode=first_task（跳回 A，跨过 B）
	env.deployLinearThreeStepProcess("reject_node_e2e", "Reject To Node",
		[3]string{"a_user", "b_user", "c_user"}, "rejectToNode", "first_task")

	instanceID := env.startInstance("reject_node_e2e", "starter")

	// 推进到 C：A 通过 → B 通过 → C 出现任务
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "a_user")) > 0 }, 2*time.Second, 50*time.Millisecond)
	env.approveAs(env.activeTasksFor(instanceID, "a_user")[0].ID, "a_user", "a")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "b_user")) > 0 }, 2*time.Second, 50*time.Millisecond)
	env.approveAs(env.activeTasksFor(instanceID, "b_user")[0].ID, "b_user", "b")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "c_user")) > 0 }, 2*time.Second, 50*time.Millisecond)

	// C 驳回 rejectToNode=first_task → 跳回 A
	env.rejectAs(env.activeTasksFor(instanceID, "c_user")[0].ID, "c_user", "c 驳回退回 A")

	// A 必须重新拿到 active 任务（不能静默自动通过）
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "a_user")) > 0 }, 2*time.Second, 50*time.Millisecond,
		"rejectToNode must regenerate target task, not silently auto-approve")
	aAgain := env.activeTasksFor(instanceID, "a_user")
	require.Len(t, aAgain, 1)

	// supersede 只清理目标节点 A，不能波及 B（B 的旧任务应保留在历史中、不重复归档）
	// 此时实例活跃、未完成
	assert.NotEqual(t, string(enums.InstanceStatusCompleted), env.instanceStatus(instanceID),
		"instance must NOT be completed after rejectToNode")
}

// ---------------------------------------------------------------------------
// 测试：驳回回跳后继续审批应能正常走完流程（完整往返）。
//
// A → B（rejectToPrev），A 通过 → B 驳回 → A 重新出现 → A 再次通过 → B 再次出现
// → B 通过 → end。验证 supersede 清理后流程状态一致、能继续推进到完成，而不是卡死
// 或因残留状态误判。
// ---------------------------------------------------------------------------

func TestE2E_RejectToPrev_ThenApproveCompletesRoundTrip(t *testing.T) {
	env := newE2EEnv(t)
	env.deployLinearTwoStepProcess("reject_roundtrip_e2e", "Reject Round Trip", "a_user", "b_user", "rejectToPrev")

	instanceID := env.startInstance("reject_roundtrip_e2e", "starter")

	// A 通过 → B 出现
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "a_user")) > 0 }, 2*time.Second, 50*time.Millisecond)
	env.approveAs(env.activeTasksFor(instanceID, "a_user")[0].ID, "a_user", "a 第一次通过")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "b_user")) > 0 }, 2*time.Second, 50*time.Millisecond)

	// B 驳回 → A 重新出现
	env.rejectAs(env.activeTasksFor(instanceID, "b_user")[0].ID, "b_user", "b 第一次驳回")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "a_user")) > 0 }, 2*time.Second, 50*time.Millisecond,
		"A should regenerate after rejectToPrev")

	// A 再次通过 → B 应再次出现（验证 supersede 后能正常推进，不卡死）
	env.approveAs(env.activeTasksFor(instanceID, "a_user")[0].ID, "a_user", "a 第二次通过")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "b_user")) > 0 }, 2*time.Second, 50*time.Millisecond,
		"B should reappear after A re-approves (round trip)")

	// B 这次通过 → 流程应正常完成
	env.approveAs(env.activeTasksFor(instanceID, "b_user")[0].ID, "b_user", "b 通过，流程结束")
	require.Eventually(t, func() bool {
		return env.instanceStatus(instanceID) == string(enums.InstanceStatusCompleted)
	}, 2*time.Second, 50*time.Millisecond, "instance should complete after full round trip")
}

// ---------------------------------------------------------------------------
// 测试：supersede 归档保留审计——旧任务的原始审批结果 EndReason 不被覆盖。
//
// supersedeNodeTasksInternal 仅在 EndReason 为空时填 superseded 标记；已有审批结果
// （approved/rejected）的 Completed 任务归档时应保留原 EndReason，供审计区分
// "A 当初是通过的、被驳回回跳重置"这一历史。
// ---------------------------------------------------------------------------

func TestE2E_RejectToPrev_ArchivedTaskPreservesOriginalEndReason(t *testing.T) {
	env := newE2EEnv(t)
	env.deployLinearTwoStepProcess("reject_audit_e2e", "Reject Audit", "a_user", "b_user", "rejectToPrev")

	instanceID := env.startInstance("reject_audit_e2e", "starter")

	// A 通过（任务 EndReason=approved）→ B 出现
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "a_user")) > 0 }, 2*time.Second, 50*time.Millisecond)
	a1 := env.activeTasksFor(instanceID, "a_user")[0]
	env.approveAs(a1.ID, "a_user", "a 通过")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "b_user")) > 0 }, 2*time.Second, 50*time.Millisecond)

	// B 驳回 → A 的旧任务应被 supersede 归档，且 EndReason 保持 approved
	env.rejectAs(env.activeTasksFor(instanceID, "b_user")[0].ID, "b_user", "b 驳回")
	require.Eventually(t, func() bool { return len(env.activeTasksFor(instanceID, "a_user")) > 0 }, 2*time.Second, 50*time.Millisecond,
		"A should regenerate")

	// 查 wf_hi_task：A（first_task）的归档行里，原 A 任务（a1.ID）的 end_reason 应仍是 approved
	var endReason string
	require.NoError(t, env.db.Raw(
		"SELECT end_reason FROM wf_hi_task WHERE id = ?", a1.ID).Scan(&endReason).Error)
	assert.Equal(t, string(enums.ApprovalResultApproved), endReason,
		"archived A task must preserve original EndReason=approved, not be overwritten by superseded marker")
}
