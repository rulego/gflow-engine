// Package multiinstance — 双副本共享一库的并发安全集成测试。
//
// 两个引擎实例（各自的 execGate / enginePool / service 集）连接同一个 PostgreSQL
// 库，注入同一把共享键锁（对应多副本部署的 Redis 分布式锁），验证：
//   - fork-join 最后两条分支在两个副本上并发 approve，end 只执行一次、各分支任务
//     各只一条，实例恰好完成一次
//   - ProcessService.Update 后 InvalidateExecutionCache 对另一副本的已装载链生效
//
// 独立成包（不与 test/e2e 混用）：components.Register 是进程级全局注册表，同包
// 注册两个引擎的 services 会互相覆盖。PG 行锁（FOR UPDATE）真实生效，与生产多实例
// 行为一致；无 PG 环境自动跳过（GFLOW_MULTI_PG_DSN 可覆盖 DSN）。
package multiinstance

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/rulego/gflow-engine/components"
	"github.com/rulego/gflow-engine/config"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/gflow-engine/utils/lock"
)

const miTenantID = "default"

// miDDL PG 方言的引擎表 DDL（与生产 schema 对齐的最小集，不含业务扩展列）。
var miDDL = []string{
	`CREATE TABLE IF NOT EXISTS wf_process (
		id TEXT PRIMARY KEY, process_key TEXT NOT NULL, name TEXT NOT NULL,
		version INTEGER NOT NULL DEFAULT 1, category TEXT, description TEXT,
		definition_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
		publish_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, tenant_id TEXT NOT NULL,
		created_by TEXT NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_by TEXT, updated_at TIMESTAMP, ext TEXT,
		process_type TEXT NOT NULL DEFAULT 'main', icon TEXT NOT NULL DEFAULT '')`,
	`CREATE TABLE IF NOT EXISTS wf_instance (
		id TEXT PRIMARY KEY, process_id TEXT NOT NULL, business_key TEXT, name TEXT NOT NULL,
		start_user_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, variables TEXT,
		current_activity TEXT, priority INTEGER NOT NULL DEFAULT 50, parent_id TEXT,
		tenant_id TEXT NOT NULL, created_by TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_by TEXT, updated_at TIMESTAMP,
		end_reason TEXT, duration INTEGER, ended_at TIMESTAMP)`,
	`CREATE TABLE IF NOT EXISTS wf_task (
		id TEXT PRIMARY KEY, process_instance_id TEXT, process_id TEXT, parent_id TEXT,
		task_def_key TEXT, task_type TEXT, name TEXT, description TEXT, status TEXT,
		assignee TEXT, owner TEXT, priority INTEGER DEFAULT 50, sequence_order INTEGER DEFAULT 0,
		due_date TIMESTAMP, form_key TEXT, variables TEXT, claimed_at TIMESTAMP,
		approval_type TEXT, approval_rule TEXT, delegate_from TEXT, delegate_reason TEXT,
		delegate_time TIMESTAMP, ended_at TIMESTAMP, comment TEXT, end_reason TEXT,
		duration INTEGER, tenant_id TEXT, created_by TEXT, created_at TIMESTAMP,
		updated_by TEXT, updated_at TIMESTAMP)`,
	`CREATE TABLE IF NOT EXISTS wf_task_assignee (
		id TEXT PRIMARY KEY, task_id TEXT NOT NULL, entity_type TEXT NOT NULL DEFAULT 'role',
		entity_id TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '', created_at TIMESTAMP)`,
	`CREATE TABLE IF NOT EXISTS wf_hi_instance (
		id TEXT PRIMARY KEY, process_id TEXT NOT NULL, business_key TEXT, name TEXT NOT NULL,
		start_user_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, variables TEXT,
		current_activity TEXT, priority INTEGER NOT NULL DEFAULT 50, parent_id TEXT,
		tenant_id TEXT NOT NULL, created_by TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_by TEXT, updated_at TIMESTAMP,
		end_reason TEXT, duration INTEGER, ended_at TIMESTAMP)`,
	`CREATE TABLE IF NOT EXISTS wf_hi_task (
		id TEXT PRIMARY KEY, process_instance_id TEXT, process_id TEXT, parent_id TEXT,
		task_def_key TEXT, task_type TEXT, name TEXT, description TEXT, status TEXT,
		assignee TEXT, owner TEXT, priority INTEGER DEFAULT 50, sequence_order INTEGER DEFAULT 0,
		due_date TIMESTAMP, form_key TEXT, variables TEXT, claimed_at TIMESTAMP,
		approval_type TEXT, approval_rule TEXT, delegate_from TEXT, delegate_reason TEXT,
		delegate_time TIMESTAMP, ended_at TIMESTAMP, comment TEXT, end_reason TEXT,
		duration INTEGER, tenant_id TEXT, created_by TEXT, created_at TIMESTAMP,
		updated_by TEXT, updated_at TIMESTAMP)`,
	`CREATE TABLE IF NOT EXISTS wf_task_comment (
		id TEXT PRIMARY KEY, task_id TEXT NOT NULL, process_instance_id TEXT NOT NULL DEFAULT '',
		tenant_id TEXT NOT NULL DEFAULT '', user_id TEXT NOT NULL,
		user_name TEXT NOT NULL DEFAULT '', content TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)`,
}

// miEnv 两个副本引擎 + 共享 PG 库。
type miEnv struct {
	t        *testing.T
	replicaA service.WorkflowEngine // 首个副本：组件全局注册表指向它的 services
	replicaB service.WorkflowEngine
	db       *gorm.DB
}

func newMultiReplicaEnv(t *testing.T) *miEnv {
	t.Helper()
	dsn := os.Getenv("GFLOW_MULTI_PG_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/gflow_engine_pgtest?sslmode=disable"
	}

	// 建表用独立连接（引擎起来前把 schema 准备好）
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Skipf("PG 不可用，跳过双副本测试: %v", err)
	}
	for _, ddl := range miDDL {
		require.NoError(t, db.Exec(ddl).Error, "create table")
	}
	for _, tbl := range []string{"wf_task_comment", "wf_hi_task", "wf_hi_instance", "wf_task_assignee", "wf_task", "wf_instance", "wf_process"} {
		require.NoError(t, db.Exec("DELETE FROM "+tbl).Error, "clear "+tbl)
	}

	// 共享键锁：两副本注入同一实例，模拟多副本部署下的 Redis 分布式锁语义
	shared := lock.NewLocalLock()
	t.Cleanup(shared.Close)

	cfgA := &config.Config{Database: &config.DatabaseConfig{Driver: "postgres", Dsn: dsn, MaxOpenConns: 8, MaxIdleConns: 4}}
	replicaA, err := service.NewWorkflowEngineBuilder().
		SetName("replica-a").SetConfig(cfgA).
		SetIDGenerator(service.NewIDGenerator()).
		SetLocker(shared).
		Build()
	require.NoError(t, err, "build replica A")
	require.NoError(t, replicaA.Start(context.Background()), "start replica A")

	cfgB := &config.Config{Database: &config.DatabaseConfig{Driver: "postgres", Dsn: dsn, MaxOpenConns: 8, MaxIdleConns: 4}}
	replicaB, err := service.NewWorkflowEngineBuilder().
		SetName("replica-b").SetConfig(cfgB).
		SetIDGenerator(service.NewIDGenerator()).
		SetLocker(shared).
		Build()
	require.NoError(t, err, "build replica B")
	require.NoError(t, replicaB.Start(context.Background()), "start replica B")

	// components.Register 是进程级全局注册表（两副本同进程只注册一次）。
	// 节点服务经 A 注入；A/B 的 services 都绑定同一个 PG 库，语义等价。
	require.NoError(t, components.Register(components.ComponentDeps{
		TaskService:     replicaA.GetTaskServiceInternal(),
		IdentityService: replicaA.GetIdentityService(),
		RuntimeService:  replicaA.GetRuntimeServiceInternal(),
	}), "register components")

	t.Cleanup(func() {
		_ = replicaB.Stop(context.Background())
		_ = replicaA.Stop(context.Background())
		if sqlDB, e := db.DB(); e == nil {
			_ = sqlDB.Close()
		}
	})
	return &miEnv{t: t, replicaA: replicaA, replicaB: replicaB, db: db}
}

func (e *miEnv) deployForkJoin(processKey string) {
	e.t.Helper()
	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": processKey, "name": processKey, "root": true},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{"id": "fork1", "type": "fork", "name": "Parallel Fork"},
				{"id": "task_a", "type": "userTask", "name": "Branch A", "configuration": map[string]interface{}{
					"candidateType": "user", "candidateConfig": map[string]interface{}{"userIds": []string{"user_a"}}, "approvalType": "single"}},
				{"id": "task_b", "type": "userTask", "name": "Branch B", "configuration": map[string]interface{}{
					"candidateType": "user", "candidateConfig": map[string]interface{}{"userIds": []string{"user_b"}}, "approvalType": "single"}},
				{"id": "join1", "type": "join", "name": "Parallel Join", "configuration": map[string]interface{}{"timeout": 5}},
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
	require.NoError(e.t, err)
	_, err = e.replicaA.GetProcessService().Deploy(context.Background(),
		service.Actor{UserID: "admin", TenantID: miTenantID},
		&model.WfProcess{ProcessKey: processKey, Name: processKey, DefinitionJSON: string(raw),
			Status: string(enums.ProcessStatusActive), TenantID: miTenantID, CreatedBy: "admin"}, true)
	require.NoError(e.t, err, "deploy fork/join process")
}

func (e *miEnv) startInstance(processKey, via string) string {
	e.t.Helper()
	var eng service.WorkflowEngine
	if via == "B" {
		eng = e.replicaB
	} else {
		eng = e.replicaA
	}
	instID, err := eng.GetRuntimeService().StartProcessInstanceByKey(context.Background(),
		service.Actor{UserID: "starter", TenantID: miTenantID}, processKey, "", map[string]interface{}{"amount": 100})
	require.NoError(e.t, err, "start instance via replica "+via)
	return instID
}

func (e *miEnv) activeTaskID(instanceID, defKey string) string {
	e.t.Helper()
	require.Eventually(e.t, func() bool {
		var n int64
		_ = e.db.Raw("SELECT COUNT(*) FROM wf_task WHERE process_instance_id = ? AND task_def_key = ? AND status = ?",
			instanceID, defKey, string(enums.TaskStatusActive)).Scan(&n).Error
		return n == 1
	}, 3*time.Second, 50*time.Millisecond, defKey+" 应恰好有 1 条 Active 任务")
	var id string
	require.NoError(e.t, e.db.Raw("SELECT id FROM wf_task WHERE process_instance_id = ? AND task_def_key = ? AND status = ? LIMIT 1",
		instanceID, defKey, string(enums.TaskStatusActive)).Scan(&id).Error)
	return id
}

func (e *miEnv) approveVia(via, taskID, who string) error {
	eng := e.replicaA
	if via == "B" {
		eng = e.replicaB
	}
	ctx := service.WithAPICallingMode(service.SetUserToCtx(context.Background(),
		&service.Actor{UserID: who, UserName: who, TenantID: miTenantID}))
	return eng.GetTaskService().Approve(ctx, service.Actor{UserID: who, UserName: who, TenantID: miTenantID}, taskID, "multi-replica", nil)
}

func (e *miEnv) instanceStatus(instanceID string) string {
	e.t.Helper()
	var st string
	_ = e.db.Raw("SELECT status FROM wf_instance WHERE id = ?", instanceID).Scan(&st).Error
	if st != "" {
		return st
	}
	// 完成即归档删运行时行，读历史表（与 e2e 的 instanceStatus 同口径）
	require.NoError(e.t, e.db.Raw("SELECT status FROM wf_hi_instance WHERE id = ?", instanceID).Scan(&st).Error)
	return st
}

// ---------------------------------------------------------------------------
// fork 两分支的最后两张任务分别经 A、B 两副本并发 approve：两副本的 AfterCommit
// 驱动同时判定 fork-join 收齐时，跨副本门闩须保证先到者恢复、后到者幂等退出。
// ---------------------------------------------------------------------------
func TestMultiReplica_ForkJoinConcurrentApprove_SingleEndRun(t *testing.T) {
	env := newMultiReplicaEnv(t)
	env.deployForkJoin("mi_fork_concurrent")
	instID := env.startInstance("mi_fork_concurrent", "A")

	taskA := env.activeTaskID(instID, "task_a")
	taskB := env.activeTaskID(instID, "task_b")

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = env.approveVia("A", taskA, "user_a") }()
	go func() { defer wg.Done(); errs[1] = env.approveVia("B", taskB, "user_b") }()
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, fmt.Sprintf("replica %c approve", 'A'+rune(i)))
	}

	// 实例恰好完成一次
	require.Eventually(t, func() bool {
		return env.instanceStatus(instID) == string(enums.InstanceStatusCompleted)
	}, 5*time.Second, 50*time.Millisecond, "两副本并发 approve 后实例应完成，status=%q", env.instanceStatus(instID))

	// 归档落定后断言不变量
	require.Eventually(t, func() bool {
		var n int64
		_ = env.db.Raw("SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ?", instID).Scan(&n).Error
		return n > 0
	}, 3*time.Second, 50*time.Millisecond, "wf_hi_task 应已归档")

	count := func(sql string, args ...interface{}) int64 {
		var n int64
		require.NoError(t, env.db.Raw(sql, args...).Scan(&n).Error)
		return n
	}
	assert.Equal(t, int64(1), count("SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_type = 'end'", instID),
		"end 节点应恰好执行 1 次")
	assert.Equal(t, int64(1), count("SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_type = 'end' AND status = 'completed'", instID),
		"end 任务应 Completed（join 收集的数组变量不能让 complete 合并失败）")
	assert.Equal(t, int64(1), count("SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_def_key = 'task_a'", instID),
		"task_a 应恰好 1 条")
	assert.Equal(t, int64(1), count("SELECT COUNT(*) FROM wf_hi_task WHERE process_instance_id = ? AND task_def_key = 'task_b'", instID),
		"task_b 应恰好 1 条")
	// 运行时表应清空（归档迁移完成，无孤儿行）
	assert.Equal(t, int64(0), count("SELECT COUNT(*) FROM wf_task WHERE process_instance_id = ?", instID),
		"实例完成后 wf_task 不应残留")
	assert.Equal(t, int64(0), count("SELECT COUNT(*) FROM wf_instance WHERE id = ?", instID),
		"实例完成后 wf_instance 不应残留")
}

// ---------------------------------------------------------------------------
// 缓存失效：A 副本 Update 定义（就地改 definition_json）后，B 副本已装载的链应被
// 驱逐（跨副本广播），下次 GetExecution 装载新定义而非命中旧缓存。
// ---------------------------------------------------------------------------
func TestMultiReplica_ExecutionCacheInvalidation(t *testing.T) {
	env := newMultiReplicaEnv(t)

	def := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": "mi_cache_inval", "name": "旧名", "root": true},
		"metadata": map[string]interface{}{
			"firstNodeIndex": 0,
			"nodes": []map[string]interface{}{
				{"id": "end", "type": "end", "name": "End"},
			},
			"connections": []map[string]interface{}{},
		},
	}
	rawOld, _ := json.Marshal(def)
	proc, err := env.replicaA.GetProcessService().Deploy(context.Background(),
		service.Actor{UserID: "admin", TenantID: miTenantID},
		&model.WfProcess{ProcessKey: "mi_cache_inval", Name: "mi_cache_inval", DefinitionJSON: string(rawOld),
			Status: string(enums.ProcessStatusActive), TenantID: miTenantID, CreatedBy: "admin"}, true)
	require.NoError(t, err, "deploy")

	// B 副本装载旧链
	rsB, ok := env.replicaB.GetRuntimeService().(*service.RuntimeServiceImpl)
	require.True(t, ok, "RuntimeService 实现应为 RuntimeServiceImpl")
	eOld, err := rsB.GetExecution(context.Background(), proc.ID)
	require.NoError(t, err, "B 装载链")
	require.Equal(t, "旧名", eOld.Definition().RuleChain.Name, "装载的应是旧定义")

	// A 副本就地改名
	def["ruleChain"].(map[string]interface{})["name"] = "新名"
	rawNew, _ := json.Marshal(def)
	require.NoError(t, env.replicaA.GetProcessService().Update(context.Background(),
		service.Actor{UserID: "admin", TenantID: miTenantID},
		&model.WfProcess{ID: proc.ID, ProcessKey: proc.ProcessKey, Name: "mi_cache_inval",
			DefinitionJSON: string(rawNew), Status: string(enums.ProcessStatusActive),
			TenantID: miTenantID}), "update definition")

	// Update 内部已驱逐+广播；B 再取链应拿到新定义而非旧缓存
	eNew, err := rsB.GetExecution(context.Background(), proc.ID)
	require.NoError(t, err, "B 重新装载链")
	assert.Equal(t, "新名", eNew.Definition().RuleChain.Name, "失效广播后 B 应装载新定义")
}
