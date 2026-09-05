package service

// Tests for runtime_enginepool.go: per-tenant engine pool lifecycle —
// version eviction, tenant isolation of pools/aliases, and GetExecution
// self-healing. Engines are constructed directly against an in-memory
// SQLite DB (no full engine bootstrap).

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/action"
)

var poolTestCounter int32

const poolTestMarkerFunc = "pooltest_noop"

func init() {
	// functions 节点 + noop 函数注册（供可解析的最小链）
	action.Functions.Register(poolTestMarkerFunc, func(ctx types.RuleContext, msg types.RuleMsg) {
		ctx.TellSuccess(msg)
	})
}

// poolTestChainDef 构造 ruleChain.id=chainID 的最小链（单 functions 节点）。
func poolTestChainDef(chainID string) string {
	return fmt.Sprintf(`{"ruleChain":{"id":%q,"name":"t","root":true},"metadata":{"firstNodeIndex":0,"nodes":[{"id":"n1","type":"functions","name":"n","configuration":{"functionName":%q}}],"connections":[]}}`,
		chainID, poolTestMarkerFunc)
}

// newPoolTestRS 构造一个带独立 SQLite 内存库的 RuntimeServiceImpl（每测独立 DSN，互不污染）。
func newPoolTestRS(t *testing.T) (*RuntimeServiceImpl, *gorm.DB) {
	t.Helper()
	dsn := fmt.Sprintf("file:pooltest_%d?mode=memory&cache=shared", atomic.AddInt32(&poolTestCounter, 1))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, e := db.DB(); e == nil {
			_ = sqlDB.Close()
		}
	})
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS wf_process (
			id TEXT PRIMARY KEY, process_key TEXT NOT NULL, name TEXT NOT NULL,
			version INTEGER NOT NULL DEFAULT 1, category TEXT, description TEXT,
			definition_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
			publish_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, tenant_id TEXT NOT NULL,
			created_by TEXT NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_by TEXT, updated_at DATETIME, ext TEXT, process_type TEXT NOT NULL DEFAULT 'main', icon TEXT NOT NULL DEFAULT '')`,
		`CREATE TABLE IF NOT EXISTS wf_instance (
			id TEXT PRIMARY KEY, process_id TEXT NOT NULL, business_key TEXT, name TEXT NOT NULL,
			start_user_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, variables TEXT,
			current_activity TEXT, priority INTEGER NOT NULL DEFAULT 50, parent_id TEXT,
			tenant_id TEXT NOT NULL, created_by TEXT NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_by TEXT, updated_at DATETIME,
			end_reason TEXT, duration INTEGER, ended_at DATETIME)`,
	} {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	q := query.Use(db)
	rs := &RuntimeServiceImpl{
		enginePool:  rulego.NewRuleGo(),
		processDAO:  dao.NewProcessDAOWithQuery(q),
		instanceDAO: dao.NewInstanceDAOWithQuery(q),
	}
	return rs, db
}

// seedProcess 插入一条流程定义行（用 raw INSERT 避免 NOT NULL 字段零值问题）。
func seedProcess(db *gorm.DB, id, key string, version int32, tenant, chainID string) {
	def := poolTestChainDef(chainID)
	if err := db.Exec(`INSERT INTO wf_process (id, process_key, name, version, definition_json, status, tenant_id, created_by, process_type, icon) VALUES (?,?,?,?,?, 'active', ?, 'tester', 'main', '')`,
		id, key, key, version, def, tenant).Error; err != nil {
		panic(fmt.Sprintf("seed process %s: %v", id, err))
	}
}

// seedActiveInstance 插入一条 active 实例（归属 processID/tenant）。
func seedActiveInstance(db *gorm.DB, id, processID, tenant string) {
	if err := db.Exec(`INSERT INTO wf_instance (id, process_id, name, status, tenant_id, created_by, start_user_id) VALUES (?,?,?, ?, ?, 'tester', 'tester')`,
		id, processID, processID, string(enums.InstanceStatusActive), tenant).Error; err != nil {
		panic(fmt.Sprintf("seed instance %s: %v", id, err))
	}
}

// TestEnginePool_VersionActiveEviction: v2 部署后清 v1（v1 无活实例）。
func TestEnginePool_VersionActiveEviction(t *testing.T) {
	rs, db := newPoolTestRS(t)
	const tenant = "tenant-evict"
	const key = "evict-key"

	// v1 预加载进池
	seedProcess(db, "proc-v1", key, 1, tenant, "chain-evict")
	require := assert.New(t)
	require.NoError(rs.PreloadChain(tenant, "proc-v1", poolTestChainDef("chain-evict")))
	_, ok := rs.poolFor(tenant).Get("proc-v1")
	require.True(ok, "v1 预加载后应在池")

	// 部署 v2（同 key 最新版），v1 无活实例 → EvictStaleChain 应驱逐 v1
	seedProcess(db, "proc-v2", key, 2, tenant, "chain-evict")
	rs.EvictStaleChain(context.Background(), tenant, "proc-v1")
	_, ok = rs.poolFor(tenant).Get("proc-v1")
	require.False(ok, "v1 非最新版且无活实例 → 应被驱逐")
}

// TestEnginePool_VersionActiveEviction_KeptByActiveInstance: v1 有 active
// 实例时，即便 v2 是最新版，v1 链也应保留（在途实例续跑需要）。
func TestEnginePool_VersionActiveEviction_KeptByActiveInstance(t *testing.T) {
	rs, db := newPoolTestRS(t)
	const tenant = "tenant-evict2"
	const key = "evict-key2"
	require := assert.New(t)

	seedProcess(db, "proc-v1", key, 1, tenant, "chain-evict2")
	require.NoError(rs.PreloadChain(tenant, "proc-v1", poolTestChainDef("chain-evict2")))
	seedActiveInstance(db, "inst-1", "proc-v1", tenant)        // v1 有活实例
	seedProcess(db, "proc-v2", key, 2, tenant, "chain-evict2") // v2 最新

	rs.EvictStaleChain(context.Background(), tenant, "proc-v1")
	_, ok := rs.poolFor(tenant).Get("proc-v1")
	require.True(ok, "v1 虽非最新但有活实例 → 应保留")
}

// TestEnginePool_EvictionTenantIsolation: 驱逐 A 池的 v1 不影响 B 池。
func TestEnginePool_EvictionTenantIsolation(t *testing.T) {
	rs, db := newPoolTestRS(t)
	require := assert.New(t)

	// 两租户各部署同名 key（ruleChain.id 也同），各自预加载
	seedProcess(db, "proc-A1", "shared-key", 1, "tenant-A", "shared-chain")
	seedProcess(db, "proc-B1", "shared-key", 1, "tenant-B", "shared-chain")
	require.NoError(rs.PreloadChain("tenant-A", "proc-A1", poolTestChainDef("shared-chain")))
	require.NoError(rs.PreloadChain("tenant-B", "proc-B1", poolTestChainDef("shared-chain")))

	// tenant-A 部署 v2（最新），驱逐 A 的 v1
	seedProcess(db, "proc-A2", "shared-key", 2, "tenant-A", "shared-chain")
	rs.EvictStaleChain(context.Background(), "tenant-A", "proc-A1")

	_, ok := rs.poolFor("tenant-A").Get("proc-A1")
	require.False(ok, "A 的 v1 应被驱逐")
	// B 的 v1 不受影响（B 没有部署 v2，proc-B1 仍是 B 的最新版 → 即便驱逐逻辑跑也保留）
	_, ok = rs.poolFor("tenant-B").Get("proc-B1")
	require.True(ok, "B 的 v1 不应受 A 驱逐影响")
	// B 的别名也仍在
	_, ok = rs.poolFor("tenant-B").Get("shared-chain")
	require.True(ok, "B 池别名仍在")
}

// TestEnginePool_GetExecutionSelfHeal: 链被驱逐后，GetExecution 应从 DB
// 重注册（驱逐安全的保证）。
func TestEnginePool_GetExecutionSelfHeal(t *testing.T) {
	rs, db := newPoolTestRS(t)
	const tenant = "tenant-heal"
	require := assert.New(t)

	seedProcess(db, "proc-h", "heal-key", 1, tenant, "chain-heal")
	require.NoError(rs.PreloadChain(tenant, "proc-h", poolTestChainDef("chain-heal")))
	require.True(func() bool { _, ok := rs.poolFor(tenant).Get("proc-h"); return ok }())

	// 模拟驱逐（直接 Del）
	rs.poolFor(tenant).Del("proc-h")
	_, ok := rs.poolFor(tenant).Get("proc-h")
	require.False(ok, "驱逐后应不在池")

	// GetExecution 应自愈重注册
	_, err := rs.GetExecution(context.Background(), "proc-h")
	require.NoError(err)
	_, ok = rs.poolFor(tenant).Get("proc-h")
	require.True(ok, "GetExecution 应自愈重注册被驱逐的链")
}

// TestSubProcess_DeriveParentNode: 内存映射丢失（重启）后，
// CompleteProcessInstance 能从 DB 派生父的 subProcess 节点ID 恢复父流程。
// 直接测 deriveSubProcessParentNode：子 ruleChain.id → 父链里 targetId 匹配的 subProcess 节点。
func TestSubProcess_DeriveParentNode(t *testing.T) {
	rs, db := newPoolTestRS(t)
	const tenant = "tenant-derive"
	require := assert.New(t)

	// 子流程定义：ruleChain.id = child_alias
	seedProcess(db, "child-proc", "child-key", 1, tenant, "child_alias")
	// 父流程定义：含 subProcess 节点（id=p_sub, targetId=child_alias）
	parentDef := `{"ruleChain":{"id":"parent_chain","name":"父"},"metadata":{"nodes":[` +
		`{"id":"p_sub","type":"subProcess","name":"子流程","configuration":{"targetId":"child_alias"}},` +
		`{"id":"p_other","type":"subProcess","name":"另一子流程","configuration":{"targetId":"other_alias"}}]}}`
	require.NoError(db.Exec(`INSERT INTO wf_process (id, process_key, name, version, definition_json, status, tenant_id, created_by, process_type, icon) VALUES (?,?,?,?,?, 'active', ?, 'tester', 'main', '')`,
		"parent-proc", "parent-key", "parent-key", 1, parentDef, tenant).Error)
	// 父实例（active）
	require.NoError(db.Exec(`INSERT INTO wf_instance (id, process_id, name, status, tenant_id, created_by, start_user_id) VALUES (?,?,'父',?,?, 'tester', 'tester')`,
		"parent-inst", "parent-proc", string(enums.InstanceStatusActive), tenant).Error)

	// 重推导：parent-inst + child-proc → 父链里 targetId=child_alias 的 subProcess 节点
	node, ok := rs.deriveSubProcessParentNode(context.Background(), "parent-inst", "child-proc")
	require.True(ok, "应能从 DB 派生 subProcess 父节点")
	require.Equal("p_sub", node, "派生节点应为 targetId=child_alias 的 p_sub，而非 p_other")

	// 反例：不存在的子流程 → 派生失败
	_, ok = rs.deriveSubProcessParentNode(context.Background(), "parent-inst", "no-such-proc")
	require.False(ok, "子流程不存在时派生应失败")
}

// tenantIsolationDefTpl 一条可解析的最小链：ruleChain.id 由 %s 注入，单 functions 节点。
const tenantIsolationDefTpl = `{
  "ruleChain": {"id": "%s", "name": "isolation", "root": true, "debugMode": false},
  "metadata": {"firstNodeIndex": 0, "nodes": [
    {"id": "n1", "type": "functions", "name": "n", "configuration": {"functionName": "tp_isolation_noop"}}
  ], "connections": []}
}`

// TestRuntimeService_TenantPoolIsolation 验证引擎级租户隔离：
// 两租户用完全相同的 ruleChain.id 部署各自的流程定义（processDef.ID 不同）时
// 每租户独立池、同名别名各自解析到本租户引擎、主键互不可见。
func TestRuntimeService_TenantPoolIsolation(t *testing.T) {
	action.Functions.Register("tp_isolation_noop", func(ctx types.RuleContext, msg types.RuleMsg) {
		ctx.TellSuccess(msg)
	})
	defer action.Functions.UnRegister("tp_isolation_noop")

	// 仅需 enginePool 字段即可测 poolFor 逻辑（DAO 等字段本用例不涉及）
	rs := &RuntimeServiceImpl{enginePool: rulego.NewRuleGo()}

	const sharedChainID = "leave_approval" // 两租户共用此 ruleChain.id（别名）
	def := []byte(fmt.Sprintf(tenantIsolationDefTpl, sharedChainID))

	poolA := rs.poolFor("tenant-A")
	poolB := rs.poolFor("tenant-B")

	_, err := poolA.New("proc-A-uuid", def) // A 注册：主键 proc-A-uuid，别名 leave_approval
	assert.Nil(t, err)
	_, err = poolB.New("proc-B-uuid", def) // B 注册：主键 proc-B-uuid，别名 leave_approval
	assert.Nil(t, err)

	// T1: 不同租户拿到不同池实例
	assert.True(t, poolA != poolB, "两租户应为不同池")
	// 同租户重复取池稳定（LoadOrStore 幂等）
	assert.True(t, rs.poolFor("tenant-A") == poolA, "同租户取池应稳定")

	// T2: 同名别名各自解析到本租户引擎（核心：别名租户内隔离）
	eA, ok := poolA.Get(sharedChainID)
	assert.True(t, ok, "A 池应能按别名解析")
	assert.Equal(t, "proc-A-uuid", eA.Id(), "A 的别名应指向 A 引擎")
	eB, ok := poolB.Get(sharedChainID)
	assert.True(t, ok, "B 池应能按别名解析")
	assert.Equal(t, "proc-B-uuid", eB.Id(), "B 的别名应指向 B 引擎")
	assert.True(t, eA.Id() != eB.Id(), "两租户同名别名不应串")

	// T3: A 的主键在 B 池查不到（主键也隔离）
	_, ok = poolB.Get("proc-A-uuid")
	assert.False(t, ok, "A 的 processDef.ID 不应在 B 池")

	// 兜底：空租户回退默认池
	assert.True(t, rs.poolFor("") == rs.enginePool, "空租户应回退默认池")
}

// TestRuntimeService_TenantPool_SameTenantAliasOverwrite 验证同租户内重部署：
// 同租户同 ruleChain.id 重注册（新版部署），别名指向新引擎（旧的可被 Del 清理）。
func TestRuntimeService_TenantPool_SameTenantAliasOverwrite(t *testing.T) {
	action.Functions.Register("tp_isolation_noop", func(ctx types.RuleContext, msg types.RuleMsg) {
		ctx.TellSuccess(msg)
	})
	defer action.Functions.UnRegister("tp_isolation_noop")

	rs := &RuntimeServiceImpl{enginePool: rulego.NewRuleGo()}
	pool := rs.poolFor("tenant-X")
	def := []byte(fmt.Sprintf(tenantIsolationDefTpl, "shared"))

	_, _ = pool.New("proc-v1", def)
	e1, ok := pool.Get("shared")
	assert.True(t, ok)
	assert.Equal(t, "proc-v1", e1.Id())

	// 同租户新版（新 processDef.ID，同 ruleChain.id）
	_, _ = pool.New("proc-v2", def)
	e2, ok := pool.Get("shared")
	assert.True(t, ok)
	assert.Equal(t, "proc-v2", e2.Id(), "同租户别名应指向最新注册的引擎")

	// 旧版主键仍在（EvictStaleChain 才会清）
	_, ok = pool.Get("proc-v1")
	assert.True(t, ok, "旧版主键仍在池中（需显式驱逐）")
}

// initExecution 装载期迁移：遗留 routeGateway DSL（引擎未注册该类型，直接装载必失败）
// 经 MigrateRouteGateway 转为 switch 后可正常装载，且 Default 出边保持指向原 Success 后继。
func TestInitExecution_MigratesLegacyRouteGateway(t *testing.T) {
	rs, _ := newPoolTestRS(t)
	def := `{"ruleChain":{"id":"legacy_route","name":"t","root":true},"metadata":{
	  "nodes":[
	    {"id":"n1","type":"functions","name":"start marker","configuration":{"functionName":"pooltest_noop"}},
	    {"id":"route1","type":"routeGateway","name":"路由","configuration":{"routeList":[{"title":"A","routeKey":"r1","conditionList":[]}]}},
	    {"id":"n2","type":"functions","name":"end marker","configuration":{"functionName":"pooltest_noop"}}],
	  "connections":[
	    {"fromId":"n1","toId":"route1","type":"Success"},
	    {"fromId":"route1","toId":"n2","type":"Success"}]}}`
	engine, err := rs.initExecution("t1", "p_legacy_route", def)
	if err != nil {
		t.Fatalf("initExecution should load migrated routeGateway DSL, got: %v", err)
	}
	if engine == nil {
		t.Fatal("engine should not be nil")
	}
}

// TestEnginePool_InvalidateExecutionCache: Update/Delete 就地改 definition_json 后，
// InvalidateExecutionCache 必须驱逐注册表内所有服务实例的池条目（默认池+各租户池）
// 并触发跨副本广播钩子；ApplyRemoteExecutionInvalidate 只清本地、不再广播（防循环）；
// 驱逐后 GetExecution 按需自愈重装载。
func TestEnginePool_InvalidateExecutionCache(t *testing.T) {
	rs, db := newPoolTestRS(t)
	// 测试直构的 rs 不经 NewRuntimeService，手动登记进失效注册表
	runtimeServiceRegistry.Store(rs, struct{}{})
	t.Cleanup(func() { runtimeServiceRegistry.Delete(rs) })
	const tenant = "tenant-inval"
	require := assert.New(t)

	seedProcess(db, "proc-inval", "inval-key", 1, tenant, "chain-inval")
	require.NoError(rs.PreloadChain(tenant, "proc-inval", poolTestChainDef("chain-inval")))
	_, ok := rs.poolFor(tenant).Get("proc-inval")
	require.True(ok, "预加载后应在租户池")

	broadcast := make(chan string, 1)
	oldHook := getEnginePoolHook()
	SetEnginePoolInvalidateHook(func(processID string) { broadcast <- processID })
	t.Cleanup(func() { SetEnginePoolInvalidateHook(oldHook) })

	InvalidateExecutionCache("proc-inval")

	_, ok = rs.poolFor(tenant).Get("proc-inval")
	require.False(ok, "失效后应从租户池驱逐")
	select {
	case got := <-broadcast:
		require.Equal("proc-inval", got)
	default:
		t.Fatal("InvalidateExecutionCache 应触发跨副本广播钩子")
	}

	// 自愈：驱逐后 GetExecution 慢路径从 DB 重装载最新定义
	_, err := rs.GetExecution(context.Background(), "proc-inval")
	require.NoError(err)
	_, ok = rs.poolFor(tenant).Get("proc-inval")
	require.True(ok, "驱逐后 GetExecution 应自愈重装载")

	// 远程失效路径：只清本地、不触发钩子
	ApplyRemoteExecutionInvalidate("proc-inval")
	_, ok = rs.poolFor(tenant).Get("proc-inval")
	require.False(ok)
	select {
	case got := <-broadcast:
		t.Fatalf("ApplyRemoteExecutionInvalidate 不应再广播，却收到 %q", got)
	default:
	}
}
