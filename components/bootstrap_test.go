package components

import (
	"context"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/utils/lock"
)

// ---- 占位桩：仅用于让接口值非 nil；被测路径不调用其任何方法，调用即 panic ----

type unusedTaskInternal struct{ service.TaskServiceInternal }
type unusedRuntimeInternal struct{ service.RuntimeServiceInternal }
type unusedIdentity struct{ service.IdentityService }
type unusedExecutor struct{ service.RuleChainExecutor }
type unusedLocker struct{ lock.Locker }

func unusedCCTaskListener(context.Context, service.CCEvent) {}
func unusedTaskListener(context.Context, service.TaskEvent) {}

// fakeBootstrapEngine 可配置的 WorkflowEngine 测试替身。
type fakeBootstrapEngine struct {
	running  bool
	task     service.TaskServiceInternal
	runtime  service.RuntimeServiceInternal
	identity service.IdentityService
	executor service.RuleChainExecutor
}

func (e *fakeBootstrapEngine) GetDB() *gorm.DB                           { return nil }
func (e *fakeBootstrapEngine) GetTaskService() service.TaskService       { return nil }
func (e *fakeBootstrapEngine) GetProcessService() service.ProcessService { return nil }
func (e *fakeBootstrapEngine) GetRuntimeService() service.RuntimeService { return nil }
func (e *fakeBootstrapEngine) GetTaskServiceInternal() service.TaskServiceInternal {
	return e.task
}
func (e *fakeBootstrapEngine) GetRuntimeServiceInternal() service.RuntimeServiceInternal {
	return e.runtime
}
func (e *fakeBootstrapEngine) CountTenantData(ctx context.Context, tenantID string) (map[string]int64, error) {
	return nil, nil
}
func (e *fakeBootstrapEngine) GetHistoryService() service.HistoryService       { return nil }
func (e *fakeBootstrapEngine) GetIdentityService() service.IdentityService     { return e.identity }
func (e *fakeBootstrapEngine) GetLocker() lock.Locker                          { return unusedLocker{} }
func (e *fakeBootstrapEngine) Start(ctx context.Context) error                 { return nil }
func (e *fakeBootstrapEngine) Stop(ctx context.Context) error                  { return nil }
func (e *fakeBootstrapEngine) IsRunning() bool                                 { return e.running }
func (e *fakeBootstrapEngine) GetName() string                                 { return "fake" }
func (e *fakeBootstrapEngine) GetVersion() string                              { return "test" }
func (e *fakeBootstrapEngine) GetIDGenerator() service.IDGenerator             { return nil }
func (e *fakeBootstrapEngine) GetRuleChainExecutor() service.RuleChainExecutor { return e.executor }
func (e *fakeBootstrapEngine) GetCCTaskCreatedListener() service.CCTaskCreatedListener {
	return unusedCCTaskListener
}
func (e *fakeBootstrapEngine) GetTaskEventListener() service.TaskEventListener {
	return unusedTaskListener
}

// startedEngine 返回一个"已启动、内部服务齐全"的引擎替身。
func startedEngine() *fakeBootstrapEngine {
	return &fakeBootstrapEngine{
		running:  true,
		task:     unusedTaskInternal{},
		runtime:  unusedRuntimeInternal{},
		identity: unusedIdentity{},
		executor: &unusedExecutor{},
	}
}

// 注意：本文件所有测试都必须避免真实调用 Register（会把节点原型绑到 dummy 依赖
// 占坑全局 rulego.Registry，破坏 start_process_node_test 等真实执行节点的用例）。
// 全链路注册路径由 test/e2e 的真实引擎用例覆盖。

func TestRegisterFromEngineRejectsNilEngine(t *testing.T) {
	err := RegisterFromEngine(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "engine is nil")
}

func TestRegisterFromEngineRequiresStartedEngine(t *testing.T) {
	e := startedEngine()
	e.running = false
	err := RegisterFromEngine(e)
	require.Error(t, err)
	require.Contains(t, err.Error(), "engine.Start")
}

// 前置校验必须在触碰任何全局状态之前完成：即使引擎未启动，也不得产生部分注册。
func TestRegisterFromEnginePreflightBeforeAnySideEffect(t *testing.T) {
	e := startedEngine()
	e.running = false
	require.Error(t, RegisterFromEngine(e, WithServiceFuncs([]ServiceFunc{
		{Def: ServiceFuncDef{Name: "test:bootstrap:must-not-register"}, Fn: noOpFn},
	})))
	_, ok := Services.Get("test:bootstrap:must-not-register")
	require.False(t, ok, "failed preflight must not leave partial registration")
}

func TestAssembleDepsRejectsNilEngine(t *testing.T) {
	_, err := assembleDeps(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "engine is nil")
}

func TestAssembleDepsRequiresStartedEngine(t *testing.T) {
	e := startedEngine()
	e.running = false
	_, err := assembleDeps(e)
	require.Error(t, err)
	require.Contains(t, err.Error(), "engine.Start")
}

// 默认自动化执行器取自引擎（GetRuleChainExecutor），其余依赖逐项自取。
func TestAssembleDepsPullsServicesFromEngine(t *testing.T) {
	e := startedEngine()
	exec := &unusedExecutor{}
	e.executor = exec

	deps, err := assembleDeps(e)
	require.NoError(t, err)
	require.Equal(t, exec, deps.AutomationExecutor)
	require.Equal(t, e.task, deps.TaskService)
	require.Equal(t, e.runtime, deps.RuntimeService)
	require.Equal(t, e.identity, deps.IdentityService)
	require.NotNil(t, deps.CCTaskCreatedListener)
	require.NotNil(t, deps.TaskEventListener)
	require.Empty(t, deps.ServiceFuncs)
}

func TestAssembleDepsOverridesAutomationExecutor(t *testing.T) {
	override := &unusedExecutor{}
	deps, err := assembleDeps(startedEngine(), WithAutomationExecutor(override))
	require.NoError(t, err)
	require.Equal(t, override, deps.AutomationExecutor)
}

func TestAssembleDepsCarriesServiceFuncs(t *testing.T) {
	fn := ServiceFunc{Def: ServiceFuncDef{Name: "test:bootstrap:assemble"}, Fn: noOpFn}
	deps, err := assembleDeps(startedEngine(), WithServiceFuncs([]ServiceFunc{fn}))
	require.NoError(t, err)
	require.Len(t, deps.ServiceFuncs, 1)
	require.Equal(t, "test:bootstrap:assemble", deps.ServiceFuncs[0].Def.Name)
}

func TestRegisterRejectsNilRequiredDeps(t *testing.T) {
	valid := ComponentDeps{
		TaskService:     unusedTaskInternal{},
		IdentityService: unusedIdentity{},
		RuntimeService:  unusedRuntimeInternal{},
	}
	for _, field := range []string{"TaskService", "IdentityService", "RuntimeService"} {
		deps := valid
		switch field {
		case "TaskService":
			deps.TaskService = nil
		case "IdentityService":
			deps.IdentityService = nil
		case "RuntimeService":
			deps.RuntimeService = nil
		}
		err := Register(deps)
		require.Error(t, err, "%s must be required", field)
		require.Contains(t, err.Error(), field)
	}
}

// resetRecordedDeps 清除首次依赖记录，让告警测试不依赖其它测试的执行顺序。
func resetRecordedDeps() {
	recordedDepsMu.Lock()
	defer recordedDepsMu.Unlock()
	recordedDeps = ComponentDeps{}
	hasRecordedDeps = false
}

// 直接驱动记录器，不经过 Register（避免真实注册占坑全局注册表）。
func TestWarnIfDepsMismatchLogs(t *testing.T) {
	hook := test.NewLocal(logrus.StandardLogger())
	t.Cleanup(func() { logrus.StandardLogger().ReplaceHooks(logrus.LevelHooks{}) })

	resetRecordedDeps()

	base := ComponentDeps{
		TaskService:     unusedTaskInternal{},
		IdentityService: unusedIdentity{},
		RuntimeService:  unusedRuntimeInternal{},
	}
	warnIfDepsMismatch(base)

	differing := base
	differing.AutomationExecutor = &unusedExecutor{}
	warnIfDepsMismatch(differing)

	found := false
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, "differ from first registration") {
			found = true
		}
	}
	require.True(t, found, "deps mismatch must be logged loudly")
}

func TestDepsChanged(t *testing.T) {
	a := ComponentDeps{
		TaskService:           unusedTaskInternal{},
		IdentityService:       unusedIdentity{},
		RuntimeService:        unusedRuntimeInternal{},
		AutomationExecutor:    &unusedExecutor{},
		CCTaskCreatedListener: unusedCCTaskListener,
	}
	require.Empty(t, depsChanged(a, a), "identical deps must not report changes")

	// 指针实现（真实依赖的常态）：不同实例即不同身份
	b := a
	b.TaskService = &unusedTaskInternal{}
	require.Equal(t, []string{"TaskService"}, depsChanged(a, b))

	// 值类型实现：结构相等视为同一依赖（depKey 语义，避免误报）
	c := a
	c.TaskService = unusedTaskInternal{}
	require.Empty(t, depsChanged(a, c))

	d := a
	d.TaskEventListener = unusedTaskListener
	require.Equal(t, []string{"TaskEventListener"}, depsChanged(a, d))

	var nilDeps ComponentDeps
	got := depsChanged(nilDeps, a)
	require.ElementsMatch(t, []string{
		"TaskService", "IdentityService", "RuntimeService", "AutomationExecutor", "CCTaskCreatedListener",
	}, got)
}
