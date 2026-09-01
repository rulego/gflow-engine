package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/config"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/utils/lock"
	"gorm.io/gorm"
)

// WorkflowEngineImpl 工作流引擎实现类
// 整合所有核心服务，提供统一的工作流引擎访问入口
type WorkflowEngineImpl struct {
	name    string
	version string
	running bool
	mutex   sync.RWMutex

	// 核心服务
	taskService     TaskService
	processService  ProcessService
	runtimeService  RuntimeService
	historyService  HistoryService
	identityService IdentityService

	// 配置和数据库
	config            *config.Config
	db                *gorm.DB
	query             *query.Query
	idGenerator       IDGenerator
	locker            lock.Locker
	ruleChainExecutor RuleChainExecutor

	// 事件监听器（支持多个，按注册顺序派发）
	ccTaskCreatedListeners []CCTaskCreatedListener
	taskEventListeners     []TaskEventListener
}

// NewWorkflowEngine 创建工作流引擎实例
func NewWorkflowEngine(name string, cfg *config.Config) WorkflowEngine {
	return &WorkflowEngineImpl{
		name:    name,
		version: Version,
		running: false,
		config:  cfg,
	}
}

// GetTaskService 获取任务服务
func (e *WorkflowEngineImpl) GetTaskService() TaskService {
	return e.taskService
}

// GetProcessService 获取流程服务
func (e *WorkflowEngineImpl) GetProcessService() ProcessService {
	return e.processService
}

// GetRuntimeService 获取运行时服务
func (e *WorkflowEngineImpl) GetRuntimeService() RuntimeService {
	return e.runtimeService
}

// GetTaskServiceInternal 获取引擎内部机制用的任务服务。
// 字段持有公共 TaskService 接口；具体实现（TaskServiceImpl）同时满足
// TaskServiceInternal，这里做一次断言。宿主注入自定义 TaskService 且未实现
// 内部接口时返回 nil（组件注册方据此在启动期报错）。
func (e *WorkflowEngineImpl) GetTaskServiceInternal() TaskServiceInternal {
	if ts, ok := e.taskService.(TaskServiceInternal); ok {
		return ts
	}
	return nil
}

// GetRuntimeServiceInternal 获取引擎内部机制用的运行时服务。语义同
// GetTaskServiceInternal。
func (e *WorkflowEngineImpl) GetRuntimeServiceInternal() RuntimeServiceInternal {
	if rs, ok := e.runtimeService.(RuntimeServiceInternal); ok {
		return rs
	}
	return nil
}

// CountTenantData 统计租户在引擎各表的行数（表名 -> count）。
// 扫全部 7 张引擎表（含归档历史表），任一 count>0 即代表该租户在引擎侧仍有数据。
// 引擎未启动（DB 未初始化）时返回错误。
func (e *WorkflowEngineImpl) CountTenantData(ctx context.Context, tenantID string) (map[string]int64, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty")
	}
	if e.db == nil {
		return nil, fmt.Errorf("workflow engine database is not initialized")
	}
	tables := []string{
		"wf_process",
		"wf_instance",
		"wf_task",
		"wf_task_assignee",
		"wf_task_comment",
		"wf_hi_instance",
		"wf_hi_task",
	}
	counts := make(map[string]int64, len(tables))
	for _, table := range tables {
		var count int64
		if err := e.db.WithContext(ctx).Table(table).
			Where("tenant_id = ?", tenantID).
			Count(&count).Error; err != nil {
			return nil, fmt.Errorf("failed to count tenant data in %s: %w", table, err)
		}
		counts[table] = count
	}
	return counts, nil
}

// GetHistoryService 获取历史服务
func (e *WorkflowEngineImpl) GetHistoryService() HistoryService {
	return e.historyService
}

// GetIdentityService 获取身份服务
func (e *WorkflowEngineImpl) GetIdentityService() IdentityService {
	return e.identityService
}

// GetLocker 获取分布式锁
func (e *WorkflowEngineImpl) GetLocker() lock.Locker {
	return e.locker
}

// GetDB 返回引擎使用的 *gorm.DB 实例，便于上层共享同一连接池。
// 在 CC 通知等"事务边界外但仍希望在引擎事务附近写入"的场景下，
// 上层可使用此 DB 而不是另开独立连接，缩小"任务已提交但伴生写入失败"的窗口。
// 引擎初始化前调用返回 nil。
func (e *WorkflowEngineImpl) GetDB() *gorm.DB {
	return e.db
}

// initDatabase 初始化数据库连接
func (e *WorkflowEngineImpl) initDatabase(cfg *config.Config) error {
	if cfg == nil || cfg.Database == nil {
		return fmt.Errorf("database config is required")
	}

	// 使用方言注册表创建数据库方言处理器
	dialector, err := CreateDialector(cfg.Database.Driver, cfg.Database.Dsn)
	if err != nil {
		return fmt.Errorf("failed to create dialector for driver '%s': %w", cfg.Database.Driver, err)
	}

	// 创建数据库连接
	db, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// 配置连接池
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying sql.DB: %w", err)
	}

	if cfg.Database.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(cfg.Database.MaxOpenConns)
	}
	if cfg.Database.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	}
	if cfg.Database.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(cfg.Database.ConnMaxLifetime)
	}

	// 测试连接
	if err := sqlDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	e.db = db

	// 初始化Query实例
	e.query = query.Use(db)
	query.SetDefault(db)

	return nil
}

// initServices 初始化所有服务
func (e *WorkflowEngineImpl) initServices() error {
	if e.query == nil {
		return fmt.Errorf("database query is not initialized")
	}

	// 初始化TaskService
	if e.taskService == nil {
		e.taskService = NewTaskServiceWithQuery(e.query, e)
	}

	// 初始化ProcessService
	if e.processService == nil {
		e.processService = NewProcessServiceWithQuery(e.query, e)
	}

	// 初始化RuntimeService
	if e.runtimeService == nil {
		e.runtimeService = NewRuntimeServiceWithQuery(e.query, e)
	}

	// 初始化HistoryService
	if e.historyService == nil {
		e.historyService = NewHistoryServiceWithQuery(e.query)
	}

	// 初始化IdentityService（兜底使用 Mock 实现）
	// 上层应用应通过 Builder.SetIdentityService() 注入真实实现，
	// 否则 UserTaskNode 中的审批人解析（角色/部门/主管等）将使用硬编码的测试数据。
	if e.identityService == nil {
		e.identityService = NewIdentityServiceWithQuery(e.query)
	}

	// 注入的自定义 task/runtime 服务实现须同时满足 Internal 接口：引擎内部级联
	// （审批完成后续跑、会签子任务、子流程推进）经 Internal 接口调用这两个服务，
	// 缺失会在运行期以 nil 接口 panic。默认实现（Task/RuntimeServiceImpl）天然满足。
	if _, ok := e.taskService.(TaskServiceInternal); !ok {
		return fmt.Errorf("custom TaskService must also implement TaskServiceInternal (embed TaskServiceImpl or implement its methods)")
	}
	if _, ok := e.runtimeService.(RuntimeServiceInternal); !ok {
		return fmt.Errorf("custom RuntimeService must also implement RuntimeServiceInternal (embed RuntimeServiceImpl or implement its methods)")
	}

	return nil
}

// Start 启动工作流引擎
func (e *WorkflowEngineImpl) Start(ctx context.Context) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.running {
		return fmt.Errorf("workflow engine '%s' is already running", e.name)
	}

	// 验证配置
	if e.config == nil {
		return fmt.Errorf("config is not set")
	}

	if err := e.config.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// 初始化数据库
	if e.db == nil {
		if err := e.initDatabase(e.config); err != nil {
			return fmt.Errorf("failed to initialize database: %w", err)
		}
	}

	// 初始化服务
	if err := e.initServices(); err != nil {
		return fmt.Errorf("failed to initialize services: %w", err)
	}

	// Loud warning when the mock IdentityService is wired in production paths.
	// The mock returns synthetic users for ANY input, so any approval routed
	// by role/department/manager silently resolves to user001-user005.
	//
	// We log at WARN (not error) so callers in dev/test aren't blocked, but
	// the IDENTITY_SERVICE_MOCK_IN_USE marker lets CI / log dashboards fail
	// builds or page on this. Callers that want a hard failure at startup
	// should use Builder.RequireIdentityService().
	if e.identityService != nil && isMockIdentity(e.identityService) {
		logrus.WithField("identity_warning", "mock_in_use").
			Warn("IDENTITY_SERVICE_MOCK_IN_USE: IdentityService is the engine mock. " +
				"Approvals routed by user/role will resolve to fake users. " +
				"Call Builder.SetIdentityService() with a real implementation.")
	}

	// 初始化Locker（如果未设置）
	if e.locker == nil {
		e.locker = lock.NewLocalLock()
	}

	// 验证必要的服务是否已配置
	if e.taskService == nil {
		return fmt.Errorf("task service is not configured")
	}
	if e.processService == nil {
		return fmt.Errorf("process service is not configured")
	}
	if e.runtimeService == nil {
		return fmt.Errorf("runtime service is not configured")
	}

	e.running = true
	return nil
}

// Stop 停止工作流引擎
func (e *WorkflowEngineImpl) Stop(ctx context.Context) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if !e.running {
		return fmt.Errorf("workflow engine '%s' is not running", e.name)
	}

	// 各服务共享 e.db 连接池，无独立停机资源需要释放，这里只翻转运行态。

	e.running = false
	return nil
}

// IsRunning 检查引擎是否正在运行
func (e *WorkflowEngineImpl) IsRunning() bool {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return e.running
}

// GetName 获取引擎名称
func (e *WorkflowEngineImpl) GetName() string {
	return e.name
}

// GetVersion 获取引擎版本
func (e *WorkflowEngineImpl) GetVersion() string {
	return e.version
}

// GetIDGenerator 获取ID生成器
func (e *WorkflowEngineImpl) GetIDGenerator() IDGenerator {
	return e.idGenerator
}

// setTaskService 设置任务服务（内部方法）
func (e *WorkflowEngineImpl) setTaskService(service TaskService) {
	e.taskService = service
}

// setProcessService 设置流程服务（内部方法）
func (e *WorkflowEngineImpl) setProcessService(service ProcessService) {
	e.processService = service
}

// setRuntimeService 设置运行时服务（内部方法）
func (e *WorkflowEngineImpl) setRuntimeService(service RuntimeService) {
	e.runtimeService = service
}

// setHistoryService 设置历史服务（内部方法）
func (e *WorkflowEngineImpl) setHistoryService(service HistoryService) {
	e.historyService = service
}

// setIdentityService 设置身份服务（内部方法）
func (e *WorkflowEngineImpl) setIdentityService(service IdentityService) {
	e.identityService = service
}

// setConfig 设置数据库配置（内部方法）
func (e *WorkflowEngineImpl) setConfig(c *config.Config) {
	e.config = c
}

// setName 设置引擎名称（内部方法）
func (e *WorkflowEngineImpl) setName(name string) {
	e.name = name
}

func (e *WorkflowEngineImpl) setIDGenerator(idGenerator IDGenerator) {
	e.idGenerator = idGenerator
}

func (e *WorkflowEngineImpl) setLocker(locker lock.Locker) {
	e.locker = locker
}

func (e *WorkflowEngineImpl) setRuleChainExecutor(executor RuleChainExecutor) {
	e.ruleChainExecutor = executor
}

func (e *WorkflowEngineImpl) GetRuleChainExecutor() RuleChainExecutor {
	return e.ruleChainExecutor
}

// SetRuleChainExecutor 运行时设置规则链执行器。生产一般在 Builder.SetRuleChainExecutor
// 构建期注入（router 启用 RuleGoSrv 时）；此 public setter 供集成测试动态注入
// （如 onCompleted 自动化链触发测试），不影响构建期注入路径。
func (e *WorkflowEngineImpl) SetRuleChainExecutor(executor RuleChainExecutor) {
	e.ruleChainExecutor = executor
}

// setCCTaskCreatedListener 内部 setter，由 Builder.Build 调用（替换已注册列表）。
func (e *WorkflowEngineImpl) setCCTaskCreatedListener(listener CCTaskCreatedListener) {
	if listener == nil {
		e.ccTaskCreatedListeners = nil
		return
	}
	e.ccTaskCreatedListeners = []CCTaskCreatedListener{listener}
}

// addCCTaskCreatedListener 追加 CC 事件监听器（由 Builder.Build 调用）。
func (e *WorkflowEngineImpl) addCCTaskCreatedListener(listener CCTaskCreatedListener) {
	if listener != nil {
		e.ccTaskCreatedListeners = append(e.ccTaskCreatedListeners, listener)
	}
}

// setTaskEventListener 设置任务事件监听器（由 Builder 在 Build 时调用，替换已注册列表）。
func (e *WorkflowEngineImpl) setTaskEventListener(listener TaskEventListener) {
	if listener == nil {
		e.taskEventListeners = nil
		return
	}
	e.taskEventListeners = []TaskEventListener{listener}
}

// addTaskEventListener 追加任务事件监听器（由 Builder.Build 调用）。
func (e *WorkflowEngineImpl) addTaskEventListener(listener TaskEventListener) {
	if listener != nil {
		e.taskEventListeners = append(e.taskEventListeners, listener)
	}
}

// GetCCTaskCreatedListener 获取 CC 抄送任务创建监听器（可能为 nil）。
// 注册了多个监听器时返回按注册顺序依次调用的组合监听器。
//
// 注册时机：Builder.SetCCTaskCreatedListener/AddCCTaskCreatedListener → engine
// 构造 → 调用方通过此 getter 拿到 listener 后，通常传给
// components.Register 注入到 CCTaskNode。
func (e *WorkflowEngineImpl) GetCCTaskCreatedListener() CCTaskCreatedListener {
	switch len(e.ccTaskCreatedListeners) {
	case 0:
		return nil
	case 1:
		return e.ccTaskCreatedListeners[0]
	default:
		listeners := append([]CCTaskCreatedListener(nil), e.ccTaskCreatedListeners...)
		return func(ctx context.Context, evt CCEvent) {
			for _, l := range listeners {
				l(ctx, evt)
			}
		}
	}
}

// GetTaskEventListener 获取任务事件监听器（可能为 nil）。
// 注册了多个监听器时返回按注册顺序依次调用的组合监听器；
// panic 恢复仍由 DispatchTaskEvent 统一兜底。
func (e *WorkflowEngineImpl) GetTaskEventListener() TaskEventListener {
	switch len(e.taskEventListeners) {
	case 0:
		return nil
	case 1:
		return e.taskEventListeners[0]
	default:
		listeners := append([]TaskEventListener(nil), e.taskEventListeners...)
		return func(ctx context.Context, evt TaskEvent) {
			for _, l := range listeners {
				l(ctx, evt)
			}
		}
	}
}
