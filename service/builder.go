/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package service

import (
	"errors"
	"fmt"

	"github.com/rulego/gflow-engine/config"
	"github.com/rulego/gflow-engine/utils/lock"
)

// WorkflowEngineBuilder 工作流引擎构建器接口
// 用于配置和构建工作流引擎实例
type WorkflowEngineBuilder interface {
	// SetName 设置引擎名称
	SetName(name string) WorkflowEngineBuilder

	// SetConfig 设置配置
	SetConfig(config *config.Config) WorkflowEngineBuilder

	// SetTaskService 设置任务服务实现
	SetTaskService(service TaskService) WorkflowEngineBuilder

	// SetProcessService 设置流程服务实现
	SetProcessService(service ProcessService) WorkflowEngineBuilder

	// SetRuntimeService 设置运行时服务实现
	SetRuntimeService(service RuntimeService) WorkflowEngineBuilder

	// SetHistoryService 设置历史服务实现
	SetHistoryService(service HistoryService) WorkflowEngineBuilder

	// SetIdentityService 设置身份服务实现
	//
	// 上层应用（如 gflow）必须通过此方法注入生产级的 IdentityService 实现。
	// 如果不注入，引擎启动时会使用内置的 Mock 实现（基于内存的测试数据），
	// 导致 UserTaskNode 中按角色/部门/主管解析审批人等功能无法获取真实数据。
	SetIdentityService(service IdentityService) WorkflowEngineBuilder

	// SetDialectProvider 设置自定义方言提供者
	SetDialectProvider(provider DialectProvider) WorkflowEngineBuilder

	// SetIDGenerator 设置ID生成器实现
	SetIDGenerator(generator IDGenerator) WorkflowEngineBuilder

	// SetLocker 设置分布式锁实现
	SetLocker(locker lock.Locker) WorkflowEngineBuilder

	// SetRuleChainExecutor 设置规则链执行器（可选）
	// 用于审批流程完成后触发规则链执行
	SetRuleChainExecutor(executor RuleChainExecutor) WorkflowEngineBuilder

	// SetCCTaskCreatedListener 注册 CC 抄送任务创建事件监听器（可选）。
	// 引擎在 ccTask 节点每次成功创建一条抄送任务后调用 listener。
	// 不设置（nil）时引擎仅写 wf_task，不发出任何事件。
	// 上层应用（如 gflow）通常在此回调里写 notifications 表。
	SetCCTaskCreatedListener(listener CCTaskCreatedListener) WorkflowEngineBuilder

	// AddCCTaskCreatedListener 追加一个 CC 事件监听器（可多次调用）。
	// 与 SetCCTaskCreatedListener 共存：Set 的先执行，追加的按注册顺序随后。
	AddCCTaskCreatedListener(listener CCTaskCreatedListener) WorkflowEngineBuilder

	// SetTaskEventListener 注册任务事件监听器（可选）。
	// 引擎在 userTask 创建、转办、驳回、终止、完成等关键节点调用 listener。
	// 不设置（nil）时引擎仅写 wf_task，不发出任何事件。
	// 上层应用（如 gflow）通常在此回调里写 notifications 表 + 推 WebSocket。
	SetTaskEventListener(listener TaskEventListener) WorkflowEngineBuilder

	// AddTaskEventListener 追加一个任务事件监听器（可多次调用）。
	// 与 SetTaskEventListener 共存：Set 的先执行，追加的按注册顺序随后。
	// 适用场景：通知、审计、WebSocket 推送分属不同模块，各自注册互不覆盖。
	AddTaskEventListener(listener TaskEventListener) WorkflowEngineBuilder

	// RequireIdentityService makes Build fail if no IdentityService was set
	// or if the one set is the engine's built-in mock. Production entrypoints
	// should call this; tests and examples should not.
	//
	// Rationale: IdentityServiceImpl (the mock) returns synthetic users for
	// ANY input, so deployments that forget to inject a real one silently
	// route approvals to fake user001-user005. This switch turns that footgun
	// into a hard error at startup.
	RequireIdentityService() WorkflowEngineBuilder

	// Build 构建工作流引擎实例。
	// 注意：Build 返回的引擎尚未启动——数据库连接在 Start 内才建立，
	// 启动前调用 GetDB()/各服务查询会得到空数据或未定义行为。
	// 标准时序：NewWorkflowEngineBuilder → Set* 配置 → Build() → engine.Start(ctx)。
	Build() (WorkflowEngine, error)
}

// WorkflowEngineBuilderImpl 工作流引擎构建器实现类
// 用于配置和构建工作流引擎实例
type WorkflowEngineBuilderImpl struct {
	name                  string
	config                *config.Config
	taskService           TaskService
	processService        ProcessService
	runtimeService        RuntimeService
	historyService        HistoryService
	identityService       IdentityService
	dialectProviders      []DialectProvider
	idGenerator           IDGenerator
	locker                lock.Locker
	ruleChainExecutor     RuleChainExecutor
	ccTaskCreatedListener CCTaskCreatedListener
	taskEventListener     TaskEventListener
	// Add* 追加的额外监听器，Build 时排在 Set 注册的之后
	extraCCListeners   []CCTaskCreatedListener
	extraTaskListeners []TaskEventListener
	// requireRealIdentity, when true, makes Build reject nil or mock IdentityService.
	// Set via RequireIdentityService(); production entrypoints should opt in.
	requireRealIdentity bool
}

// NewWorkflowEngineBuilder 创建工作流引擎构建器
func NewWorkflowEngineBuilder() WorkflowEngineBuilder {
	return &WorkflowEngineBuilderImpl{
		name: "DefaultWorkflowEngine",
	}
}

// SetName 设置引擎名称
func (b *WorkflowEngineBuilderImpl) SetName(name string) WorkflowEngineBuilder {
	b.name = name
	return b
}

// SetConfig 设置数据库配置
func (b *WorkflowEngineBuilderImpl) SetConfig(c *config.Config) WorkflowEngineBuilder {
	b.config = c
	return b
}

// SetTaskService 设置任务服务实现
func (b *WorkflowEngineBuilderImpl) SetTaskService(service TaskService) WorkflowEngineBuilder {
	b.taskService = service
	return b
}

// SetProcessService 设置流程服务实现
func (b *WorkflowEngineBuilderImpl) SetProcessService(service ProcessService) WorkflowEngineBuilder {
	b.processService = service
	return b
}

// SetRuntimeService 设置运行时服务实现
func (b *WorkflowEngineBuilderImpl) SetRuntimeService(service RuntimeService) WorkflowEngineBuilder {
	b.runtimeService = service
	return b
}

// SetHistoryService 设置历史服务实现
func (b *WorkflowEngineBuilderImpl) SetHistoryService(service HistoryService) WorkflowEngineBuilder {
	b.historyService = service
	return b
}

// SetIdentityService 设置身份服务实现
func (b *WorkflowEngineBuilderImpl) SetIdentityService(service IdentityService) WorkflowEngineBuilder {
	b.identityService = service
	return b
}

// SetDialectProvider 设置自定义方言提供者
func (b *WorkflowEngineBuilderImpl) SetDialectProvider(provider DialectProvider) WorkflowEngineBuilder {
	if provider != nil {
		b.dialectProviders = append(b.dialectProviders, provider)
	}
	return b
}

// SetIDGenerator 设置ID生成器实现（作用于本引擎实例的实体 ID）。
// 注意：不再隐式改写进程级 DefaultIDGenerator——多引擎实例共存时互不污染；
// 若希望事件派发（EventID）也使用自定义生成器，请显式调用 SetDefaultIDGenerator。
func (b *WorkflowEngineBuilderImpl) SetIDGenerator(generator IDGenerator) WorkflowEngineBuilder {
	b.idGenerator = generator
	return b
}

// SetLocker 设置分布式锁实现
func (b *WorkflowEngineBuilderImpl) SetLocker(locker lock.Locker) WorkflowEngineBuilder {
	b.locker = locker
	return b
}

// SetRuleChainExecutor 设置规则链执行器（可选）
// 用于审批流程完成后触发规则链执行
func (b *WorkflowEngineBuilderImpl) SetRuleChainExecutor(executor RuleChainExecutor) WorkflowEngineBuilder {
	b.ruleChainExecutor = executor
	return b
}

// SetCCTaskCreatedListener 注册 CC 抄送任务创建事件监听器（可选）。
func (b *WorkflowEngineBuilderImpl) SetCCTaskCreatedListener(listener CCTaskCreatedListener) WorkflowEngineBuilder {
	b.ccTaskCreatedListener = listener
	return b
}

// AddCCTaskCreatedListener 追加 CC 事件监听器（可多次调用）。
func (b *WorkflowEngineBuilderImpl) AddCCTaskCreatedListener(listener CCTaskCreatedListener) WorkflowEngineBuilder {
	if listener != nil {
		b.extraCCListeners = append(b.extraCCListeners, listener)
	}
	return b
}

// SetTaskEventListener 设置任务事件监听器
func (b *WorkflowEngineBuilderImpl) SetTaskEventListener(listener TaskEventListener) WorkflowEngineBuilder {
	b.taskEventListener = listener
	return b
}

// AddTaskEventListener 追加任务事件监听器（可多次调用）。
func (b *WorkflowEngineBuilderImpl) AddTaskEventListener(listener TaskEventListener) WorkflowEngineBuilder {
	if listener != nil {
		b.extraTaskListeners = append(b.extraTaskListeners, listener)
	}
	return b
}

// RequireIdentityService makes Build fail if no IdentityService was set or if
// the one set is the engine's built-in mock. Production entrypoints should
// call this; tests and examples should not.
//
// Rationale: the built-in mock returns synthetic users for ANY input, so
// deployments that forget to inject a real one silently route approvals to
// fake user001-user005. Flipping this switch turns the footgun into a hard
// startup error.
func (b *WorkflowEngineBuilderImpl) RequireIdentityService() WorkflowEngineBuilder {
	b.requireRealIdentity = true
	return b
}

// Build 构建工作流引擎实例
func (b *WorkflowEngineBuilderImpl) Build() (WorkflowEngine, error) {
	// 验证必要的配置
	if b.name == "" {
		return nil, fmt.Errorf("engine name cannot be empty")
	}

	// 注册自定义方言提供者（重复注册静默跳过，其余错误上抛）
	for _, provider := range b.dialectProviders {
		if err := RegisterDialectProvider(provider); err != nil &&
			!errors.Is(err, ErrDialectAlreadyRegistered) {
			return nil, fmt.Errorf("register dialect provider: %w", err)
		}
	}

	engine := &WorkflowEngineImpl{
		name:    b.name,
		version: Version,
		running: false,
	}

	if b.config != nil {
		engine.setConfig(b.config)
	}

	// 设置各个服务
	if b.taskService != nil {
		engine.setTaskService(b.taskService)
	}

	if b.processService != nil {
		engine.setProcessService(b.processService)
	}

	if b.runtimeService != nil {
		engine.setRuntimeService(b.runtimeService)
	}

	if b.historyService != nil {
		engine.setHistoryService(b.historyService)
	}

	if b.identityService != nil {
		engine.setIdentityService(b.identityService)
	}

	// Production guard: if RequireIdentityService was called, reject nil or
	// mock wiring. The fallback to NewIdentityServiceWithQuery happens later
	// inside WorkflowEngineImpl.initServices(); checking here catches both
	// "forgot to call SetIdentityService" and "set the mock explicitly" before
	// the engine reaches Start().
	if b.requireRealIdentity {
		if b.identityService == nil || isMockIdentity(b.identityService) {
			return nil, fmt.Errorf("real IdentityService required (Builder.RequireIdentityService was called) but mock or nil was provided")
		}
	}

	if b.idGenerator != nil {
		engine.setIDGenerator(b.idGenerator)
	}
	if b.locker != nil {
		engine.setLocker(b.locker)
	}
	if b.ruleChainExecutor != nil {
		engine.setRuleChainExecutor(b.ruleChainExecutor)
	}
	engine.setCCTaskCreatedListener(b.ccTaskCreatedListener)
	engine.setTaskEventListener(b.taskEventListener)
	for _, l := range b.extraCCListeners {
		engine.addCCTaskCreatedListener(l)
	}
	for _, l := range b.extraTaskListeners {
		engine.addTaskEventListener(l)
	}
	return engine, nil
}
