package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
)

// runtimeServiceRegistry 已创建的 RuntimeServiceImpl 注册表。enginePool（已装载的
// 规则链）挂在每个服务实例上，失效时须逐个清理；池缓存按 processID 键且不回读 DB
// （见 GetExecution 快路径），Update 就地改 definition_json 后若不驱逐，本副本会
// 继续用旧链推进实例。
var runtimeServiceRegistry sync.Map // *RuntimeServiceImpl -> struct{}

// enginePoolInvalidateHook 跨副本失效广播钩子（多副本部署由 gflow 注入：PUBLISH 到
// redis），语义与 forkGraph 的同名机制一致（见 fork_aware_resume.go）。
var (
	enginePoolHookMu         sync.RWMutex
	enginePoolInvalidateHook func(processID string)
)

// SetEnginePoolInvalidateHook 注入跨副本 enginePool 失效广播钩子（gflow 启动时调）。
func SetEnginePoolInvalidateHook(h func(processID string)) {
	enginePoolHookMu.Lock()
	defer enginePoolHookMu.Unlock()
	enginePoolInvalidateHook = h
}

func getEnginePoolHook() func(processID string) {
	enginePoolHookMu.RLock()
	defer enginePoolHookMu.RUnlock()
	return enginePoolInvalidateHook
}

// InvalidateExecutionCache 驱逐所有引擎池（含各租户池）中 processID 的已装载链，
// 并触发跨副本失效广播。ProcessService.Update / Delete（就地改 definition_json、
// processID 不变）调用；Deploy 走新版本新 processID，不命中旧缓存。
func InvalidateExecutionCache(processID string) {
	if processID == "" {
		return
	}
	invalidateExecutionCacheLocal(processID)
	if hook := getEnginePoolHook(); hook != nil {
		hook(processID)
	}
}

// ApplyRemoteExecutionInvalidate 仅清本地（不广播），供跨副本订阅方收到远程失效
// 消息时调用，避免广播循环。
func ApplyRemoteExecutionInvalidate(processID string) {
	if processID == "" {
		return
	}
	invalidateExecutionCacheLocal(processID)
}

func invalidateExecutionCacheLocal(processID string) {
	runtimeServiceRegistry.Range(func(k, _ any) bool {
		if s, ok := k.(*RuntimeServiceImpl); ok && s != nil {
			s.enginePool.Del(processID)
			s.enginePools.Range(func(_, v any) bool {
				if p, ok := v.(types.RuleEnginePool); ok {
					p.Del(processID)
				}
				return true
			})
		}
		return true
	})
}

// GetExecution 根据ID获取执行实例（租户感知：先查 processID→tenant 缓存定位租户池）。
func (s *RuntimeServiceImpl) GetExecution(ctx context.Context, processID string) (types.RuleEngine, error) {
	// 快速路径：缓存命中（processID→tenant）+ 池命中 → 免 DB 查询
	if tenantID, ok := s.processTenants.Load(processID); ok {
		if engine, ok := s.poolFor(tenantID.(string)).Get(processID); ok {
			return engine, nil
		}
	}
	// 慢路径：DB 查 processDef（拿 tenant + def 用于自愈注册）
	processDef, err := s.processDAO.Get(ctx, processID)
	if err != nil {
		return nil, fmt.Errorf("failed to get process definition: %w", err)
	}
	if processDef == nil {
		return nil, fmt.Errorf("process definition not found: %s", processID)
	}
	s.processTenants.Store(processDef.ID, processDef.TenantID) // 回填缓存
	pool := s.poolFor(processDef.TenantID)
	if engine, ok := pool.Get(processID); ok {
		return engine, nil
	}
	return s.initExecution(processDef.TenantID, processDef.ID, processDef.DefinitionJSON)
}

// PreloadChain 预加载流程定义的规则链到【租户专属】enginePool，不启动实例。
// 用于让 subProcess 子链在 Deploy 时即进池（父流程用 ruleChain.id 别名 TellFlow 可命中）。
// 幂等：enginePool.New 对已存在的 id 直接返回，故重复调用安全。
func (s *RuntimeServiceImpl) PreloadChain(tenantID, processID, processDef string) error {
	if processID == "" || processDef == "" {
		return nil
	}
	_, err := s.initExecution(tenantID, processID, processDef)
	// 注册 subProcess 目标：(tenant|ruleChain.id) -> processID，供 subProcess 节点解析子流程定义。
	if tenantID != "" {
		if rcID := extractRuleChainID(processDef); rcID != "" {
			s.subProcessTargets.Store(tenantID+"|"+rcID, processID)
		}
	}
	return err
}

// extractRuleChainID 从流程定义 JSON 解析 ruleChain.id（subProcess targetId 寻址用）。
func extractRuleChainID(def string) string {
	var rc struct {
		RuleChain struct {
			ID string `json:"id"`
		} `json:"ruleChain"`
	}
	if json.Unmarshal([]byte(def), &rc) == nil {
		return rc.RuleChain.ID
	}
	return ""
}

// poolFor 取（或惰性创建）租户专属引擎池。tenantID 为空时回退到默认 enginePool。
// 每租户独立池 → 别名（ruleChain.id）天然租户内隔离，杜绝跨租户撞。
func (s *RuntimeServiceImpl) poolFor(tenantID string) types.RuleEnginePool {
	if tenantID == "" {
		return s.enginePool
	}
	if v, ok := s.enginePools.Load(tenantID); ok {
		return v.(types.RuleEnginePool)
	}
	actual, _ := s.enginePools.LoadOrStore(tenantID, rulego.NewRuleGo())
	return actual.(types.RuleEnginePool)
}

// EvictStaleChain 驱逐过期版本链：若 processID 版本【非最新版】且【无 active 实例】，
// 从租户池移除其链+别名。池中只保留「最新版」或「有活实例的旧版」。
// best-effort：查询失败/并发竞争都不影响正确性——GetExecution 自愈会按需重注册。
// 只能在事务外调用；事务内（如 TerminateInTx）须改用 evictStaleChain 并传入事务 q，
// 走全局连接会与当前事务互等（SQLite 等单写锁数据库）且读不到未提交的删除结果。
func (s *RuntimeServiceImpl) EvictStaleChain(ctx context.Context, tenantID, processID string) {
	s.evictStaleChain(ctx, s.processDAO.Query, tenantID, processID)
}

// evictStaleChain 统一驱逐实现；q 事务外传全局 Query，事务内传事务 tx。
func (s *RuntimeServiceImpl) evictStaleChain(ctx context.Context, q *query.Query, tenantID, processID string) {
	if tenantID == "" || processID == "" || q == nil {
		return
	}
	processDAO := dao.NewProcessDAOWithQuery(q)
	processDef, err := processDAO.Get(ctx, processID)
	if err != nil || processDef == nil {
		return
	}
	// 是最新版则保留（新版要随时可启动/subProcess 引用）
	if latest, err := processDAO.GetLatestByKey(ctx, tenantID, processDef.ProcessKey); err == nil && latest != nil && latest.ID == processID {
		return
	}
	// 仍有 active 实例则保留（在途实例续跑需要）
	if n, err := dao.NewInstanceDAOWithQuery(q).CountActiveByProcessID(ctx, tenantID, processID); err == nil && n > 0 {
		return
	}
	s.poolFor(tenantID).Del(processID)
}

func (s *RuntimeServiceImpl) initExecution(tenantID, processID, processDef string) (types.RuleEngine, error) {
	// 缓存 processID -> tenantID（不可变），供 GetExecution 快速路径免 DB 选池。
	if tenantID != "" && processID != "" {
		s.processTenants.Store(processID, tenantID)
	}
	// 引擎装载期兜底（内存态，不回写 DB）：部署链路（ProcessService.create）已做
	// MigrateRouteGateway/EnsureEndNode/EnsureSwitchDefaultEdges 归一，但已部署的定义
	// 不会重新走部署链——遗留 routeGateway / 缺 end 节点 / 缺 Default 兜底边的定义
	// 在此处对副本做同样处理（先迁移再补 end 再补 Default，Default 边才能指向新
	// end），保证在途流程实例也能正常装载/完结/不卡死。
	defCopy := &model.WfProcess{DefinitionJSON: processDef}
	defCopy.MigrateRouteGateway()
	defCopy.EnsureEndNode()
	defCopy.EnsureSwitchDefaultEdges()
	processDef = defCopy.DefinitionJSON
	c := rulego.NewConfig()
	// 注册到租户专属池：Pool.New 内部 WithRuleEnginePool(该租户池) 绑给引擎，
	// 引擎内 ctx.TellFlow（如 subProcess）自动在该租户池内解析别名，天然租户隔离。
	return s.poolFor(tenantID).New(processID, []byte(processDef), rulego.WithConfig(c),
		types.WithAspects(&TaskCreator{
			instanceDAO:    s.instanceDAO,
			workflowEngine: s.workflowEngine,
		}))
}
