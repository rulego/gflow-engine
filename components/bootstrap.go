package components

import (
	"errors"

	"github.com/rulego/gflow-engine/service"
)

// bootstrapOptions RegisterFromEngine 的可选项。必填依赖（任务/身份/运行时服务）
// 一律从引擎自取，不进 Option——集成方没有传错或传 nil 的机会。
type bootstrapOptions struct {
	serviceFuncs []ServiceFunc
	// automationExecutor 非 nil 时覆盖引擎默认（GetRuleChainExecutor）。
	automationExecutor service.RuleChainExecutor
}

// Option 引擎组件装配可选项。
type Option func(*bootstrapOptions)

// WithServiceFuncs 随组件引导一并注册宿主服务任务函数，
// 等价于注册后逐个调 Services.Register。
func WithServiceFuncs(funcs []ServiceFunc) Option {
	return func(o *bootstrapOptions) {
		o.serviceFuncs = append(o.serviceFuncs, funcs...)
	}
}

// WithAutomationExecutor 覆盖自动化执行器。默认取引擎的 GetRuleChainExecutor
// （宿主经 Builder.SetRuleChainExecutor 注入的同一实例），仅在刻意与引擎
// 解耦的场景使用。
func WithAutomationExecutor(executor service.RuleChainExecutor) Option {
	return func(o *bootstrapOptions) {
		o.automationExecutor = executor
	}
}

// RegisterFromEngine 引擎统一装配入口：从已启动的引擎实例自取内部服务，
// 注册全部 BPM 节点类型并注入依赖，同时注册宿主服务任务函数（WithServiceFuncs）。
//
// 用法：
//
//	engine, err := builder.Build()
//	if err != nil { ... }
//	if err := engine.Start(ctx); err != nil { ... }
//	if err := components.RegisterFromEngine(engine,
//	    components.WithServiceFuncs(myFuncs)); err != nil { ... }
//
// 前置：engine.Start 已成功返回——内部服务在 Start 时才装配完成，未启动
// 引擎的依赖是 nil，本入口会在注册前拦截并报错，避免 nil 依赖占坑全局注册表。
// 幂等，重复调用安全。
func RegisterFromEngine(e service.WorkflowEngine, opts ...Option) error {
	deps, err := assembleDeps(e, opts...)
	if err != nil {
		return err
	}
	return Register(deps)
}

// assembleDeps 从引擎装配 ComponentDeps（无任何全局副作用，可单测）。
// 全局注册发生在 Register。
func assembleDeps(e service.WorkflowEngine, opts ...Option) (ComponentDeps, error) {
	if e == nil {
		return ComponentDeps{}, errors.New("components: engine is nil")
	}
	if !e.IsRunning() {
		return ComponentDeps{}, errors.New("components: engine not started; call engine.Start(ctx) before RegisterFromEngine")
	}
	o := &bootstrapOptions{}
	for _, opt := range opts {
		opt(o)
	}
	deps := ComponentDeps{
		TaskService:           e.GetTaskServiceInternal(),
		IdentityService:       e.GetIdentityService(),
		RuntimeService:        e.GetRuntimeServiceInternal(),
		CCTaskCreatedListener: e.GetCCTaskCreatedListener(),
		TaskEventListener:     e.GetTaskEventListener(),
		ServiceFuncs:          o.serviceFuncs,
	}
	if o.automationExecutor != nil {
		deps.AutomationExecutor = o.automationExecutor
	} else {
		deps.AutomationExecutor = e.GetRuleChainExecutor()
	}
	return deps, nil
}
