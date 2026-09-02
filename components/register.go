package components

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/sirupsen/logrus"

	// 注册 rulego-components-ai 的 ai/agent 节点。
	// AIAgentNode 通过 ctx.TellFlow 调用智能体规则链，
	// 智能体定义本身是含 ai/agent 节点的子规则链，
	// 因此 gflow-engine 必须能识别 ai/agent 类型。
	_ "github.com/rulego/rulego-components-ai/agent"
)

// registerNode 幂等注册节点。rulego.Registry 是进程级全局单例，
// 应用启动与集成测试都会重复调用注册，"already exists" 是预期跳过；
// 其余错误（非重复注册）如实上抛。
func registerNode(node types.Node) error {
	if err := rulego.Registry.Register(node); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			logrus.WithError(err).Debugf("node %s already registered, skip (idempotent)", node.Type())
			return nil
		}
		return err
	}
	return nil
}

// ComponentDeps 工作流组件注册依赖。
// 字段按名填充，避免长位置参数（多个同类型接口）传错位。
type ComponentDeps struct {
	// TaskService 任务服务（必填；内部接口：组件推进流程时需要会签子任务管理/
	// 节点任务归档等引擎内部机制，经 engine.GetTaskServiceInternal() 获取）
	TaskService service.TaskServiceInternal
	// IdentityService 身份解析服务。生产必须注入对接真实用户/角色/部门数据的实现；
	// 内置 Mock 只用于测试，用 Builder.RequireIdentityService 可在构建期拦截。
	IdentityService service.IdentityService
	// RuntimeService 运行时服务（必填；内部接口：组件需要 ExecuteNext 续跑、
	// 子流程启动等引擎内部机制，经 engine.GetRuntimeServiceInternal() 获取）
	RuntimeService service.RuntimeServiceInternal
	// CCTaskCreatedListener 抄送任务创建事件监听器（可选）。nil 时 CCTaskNode
	// 仅写 wf_task，不发出任何事件。
	CCTaskCreatedListener service.CCTaskCreatedListener
	// TaskEventListener 任务生命周期事件监听器（分配/完成/拒绝等，可选）。
	// nil 时 UserTaskNode 的 fireTaskEvent / fireRejectedEvent 会早退。
	TaskEventListener service.TaskEventListener
	// AutomationExecutor AutomationCallNode 跨池调用自动化规则链的执行器（可选）。
	// nil 时节点仍注册但运行时 TellFailure（bridge 未启用场景）。
	AutomationExecutor service.RuleChainExecutor
	// AttachmentResolver 附件解析器（可选）。AIAgentNode 组装多模态输入时把
	// 附件解析成模型可用形态（图片绝对路径/文档文本）。nil 时附件保持
	// 纯文本行为（文件名+地址），完全向后兼容。
	AttachmentResolver service.AttachmentResolver
	// ServiceFuncs 服务任务函数（可选）。宿主业务函数随组件引导一并注册，
	// 等价于逐个调 Services.Register；统一入口是为了集成方一次调用完成装配。
	// 运行时 ServiceTaskNode 按 functionName 在注册表查找，未注册的函数在部署期即被拦截。
	ServiceFuncs []ServiceFunc
}

// Register 引擎统一装配入口：注册全部 BPM 节点类型并注入依赖，
// 同时注册宿主提供的服务任务函数（deps.ServiceFuncs）。
// 宿主应用启动时调用一次；幂等，重复调用安全。
// 必填依赖（TaskService/IdentityService/RuntimeService）为 nil 时报错拒绝，
// 绝不把 nil 依赖注册进全局 Registry 占坑。
func Register(deps ComponentDeps) error {
	if err := validateDeps(deps); err != nil {
		return err
	}
	warnIfDepsMismatch(deps)

	taskService := deps.TaskService
	identityService := deps.IdentityService
	runtimeService := deps.RuntimeService
	ccTaskCreatedListener := deps.CCTaskCreatedListener
	taskEventListener := deps.TaskEventListener

	// 注入全局自动化执行器（AutomationCallNode.New 引用）
	SetAutomationExecutor(deps.AutomationExecutor)

	// 注入全局附件解析器（AIAgentNode 多模态输入引用；nil 合法）
	SetAttachmentResolver(deps.AttachmentResolver)

	// 注入 serviceTask 函数名校验器：注册表（components.Services）在本包，
	// service 层部署期校验经回调取用，避免 service→components 循环依赖。
	service.SetServiceFunctionChecker(func(name string) bool {
		_, ok := Services.Get(name)
		return ok
	})

	// 注册用户任务节点。
	// registry 保留首次注册的节点实例（绑定首次调用方的依赖），
	// 同一进程内所有引擎必须共享同一套依赖。
	userTaskNode := &UserTaskNode{
		TaskService:       taskService,
		IdentityService:   identityService,
		RuntimeService:    runtimeService,
		TaskEventListener: taskEventListener,
	}
	if err := registerNode(userTaskNode); err != nil {
		return err
	}

	// 注册抄送任务节点
	ccTaskNode := &CCTaskNode{
		TaskService:     taskService,
		OnCCTaskCreated: ccTaskCreatedListener,
	}
	if err := registerNode(ccTaskNode); err != nil {
		return err
	}

	// 注册 automation 节点（跨池调用 rulego-server 自动化规则链）
	if err := registerNode(&AutomationCallNode{}); err != nil {
		return err
	}

	// 注册 subProcess 节点（子流程：call activity，启动独立子 BPM 实例，嵌套审批闭环）
	subProcessNode := &SubProcessNode{RuntimeService: runtimeService}
	if err := registerNode(subProcessNode); err != nil {
		return err
	}

	// 注册 AI 智能体节点（调用智能体规则链，组装上下文 + 路由输出）
	aiAgentNode := &AIAgentNode{
		RuntimeService: runtimeService,
		TaskService:    taskService,
	}
	if err := registerNode(aiAgentNode); err != nil {
		return err
	}

	// 注册 HTTP 调用节点（httpCall）。同步发起 HTTP 请求，
	// 响应按 outputMappings / 默认全平铺合并进 msg.Data（保留表单字段）。
	if err := registerNode(&HttpCallNode{}); err != nil {
		return err
	}

	// 注册服务任务节点（serviceTask）。函数实现经 components.Services.Register
	// 注册（委托 action.Functions），运行时由 embed 的 FunctionsNode.OnMsg 查找调用。
	// type="serviceTask" 可能已由其它 init() 注册过，重复注册是预期（幂等）。
	if err := registerNode(&ServiceTaskNode{}); err != nil {
		return err
	}

	// 注册发起审批节点（startProcess）。规则链内以指定发起人启动 BPM 流程实例，
	// 定时链经此实现"定时自动发起审批"。BPM 设计器面板不含此节点（硬编码清单），
	// 仅经全局 Registry 出现在规则链编辑器组件面板。
	if err := registerNode(&StartProcessNode{RuntimeService: runtimeService}); err != nil {
		return err
	}

	// 注册发起人（开始）节点（startTask）。节点仅作流程起点标记，无依赖。
	if err := registerNode(&StartTaskNode{}); err != nil {
		return err
	}

	// 注册宿主提供的服务任务函数（元数据进设计器目录，实现进运行时注册表）。
	RegisterServiceFuncs(deps.ServiceFuncs)

	return nil
}

// RegisterServiceFuncs 批量注册服务任务函数：Fn 为 nil 的条目跳过
// （元数据先行、实现后补的场景），其余注册进设计器目录与运行时注册表。
// 供 Register 统一调用，也可在组件引导之外单独使用。
func RegisterServiceFuncs(funcs []ServiceFunc) {
	for _, sf := range funcs {
		if sf.Fn == nil {
			continue
		}
		Services.Register(sf.Def, sf.Fn)
	}
}

// ---- 装配防御：必填校验 + 首次依赖记录/不一致告警 ----

// validateDeps 校验必填依赖非 nil。节点按值捕获依赖后注册进全局 Registry，
// nil 依赖一旦占坑，后续正确依赖的注册会被幂等跳过，故障静默且不可恢复——
// 必须在注册前拦下。
func validateDeps(deps ComponentDeps) error {
	if deps.TaskService == nil {
		return errors.New("components: TaskService is required (engine.GetTaskServiceInternal() returned nil? start the engine first)")
	}
	if deps.IdentityService == nil {
		return errors.New("components: IdentityService is required (engine.GetIdentityService() returned nil?)")
	}
	if deps.RuntimeService == nil {
		return errors.New("components: RuntimeService is required (engine.GetRuntimeServiceInternal() returned nil? start the engine first)")
	}
	return nil
}

var (
	recordedDepsMu  sync.Mutex
	recordedDeps    ComponentDeps
	hasRecordedDeps bool
)

// warnIfDepsMismatch 记录首次注册依赖；后续注册依赖与首次不一致时大声告警。
// rulego.Registry 保留首次注册的节点实例（绑定首次调用方的依赖），
// 同一进程内所有引擎必须共享同一套依赖——不一致说明调用方传错了依赖或
// 在错误时机调用了 Register，新依赖不会生效，必须留痕。
func warnIfDepsMismatch(deps ComponentDeps) {
	recordedDepsMu.Lock()
	defer recordedDepsMu.Unlock()
	if !hasRecordedDeps {
		recordedDeps = deps
		hasRecordedDeps = true
		return
	}
	if changed := depsChanged(recordedDeps, deps); len(changed) > 0 {
		logrus.WithField("changedDeps", strings.Join(changed, ",")).
			Warn("components.Register: deps differ from first registration; rulego Registry keeps the first-bound node instances, new deps are ignored")
	}
}

// depsChanged 返回两次依赖中身份不同的字段名列表。
func depsChanged(prev, next ComponentDeps) []string {
	var changed []string
	track := func(field string, a, b any) {
		if depKey(a) != depKey(b) {
			changed = append(changed, field)
		}
	}
	track("TaskService", prev.TaskService, next.TaskService)
	track("IdentityService", prev.IdentityService, next.IdentityService)
	track("RuntimeService", prev.RuntimeService, next.RuntimeService)
	track("CCTaskCreatedListener", prev.CCTaskCreatedListener, next.CCTaskCreatedListener)
	track("TaskEventListener", prev.TaskEventListener, next.TaskEventListener)
	track("AutomationExecutor", prev.AutomationExecutor, next.AutomationExecutor)
	return changed
}

// depKey 生成依赖的身份键：指针类实现用 类型@地址，可比较值类型用值本身；
// 不可比较值类型退化为仅类型名（身份判定宁漏报不误报）。
func depKey(v any) string {
	if v == nil {
		return "<nil>"
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.UnsafePointer, reflect.Chan, reflect.Map, reflect.Func:
		return fmt.Sprintf("%s@%x", rv.Type(), rv.Pointer())
	default:
		if rv.Comparable() {
			return fmt.Sprintf("%s|%v", rv.Type(), v)
		}
		return fmt.Sprintf("%s@<uncomparable>", rv.Type())
	}
}
