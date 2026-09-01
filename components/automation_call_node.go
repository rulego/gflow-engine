package components

import (
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
)

// AutomationCallNodeConfig 跨系统自动化调用配置
//
// 与 subProcess 的选型区别：automation 是非阻塞的 fire-and-forget——触发目标规则链
// 成功即算成功并 TellSuccess，无执行结果回流主流程，失败也不重试；需要结果驱动
// 后续审批走向时请用 subProcess 子流程节点。
//
// 注意失败语义：触发失败走 TellFailure，链路上无 Failure 出边时由引擎把整个
// 流程实例置为失败终止。把自动化当"顺带发通知"用的流程应确保目标链稳定可用。
type AutomationCallNodeConfig struct {
	TargetId string `json:"targetId" label:"Target Chain ID" desc:"自动化规则链 ID" required:"true"`
}

// AutomationCallNode 通过注入的 RuleChainExecutor 跨引擎池调用 rulego-server 自动化规则链。
// executor.Execute 非阻塞，触发成功即 TellSuccess。
type AutomationCallNode struct {
	Config         AutomationCallNodeConfig
	CurrentNodeDef types.RuleNode
	Executor       service.RuleChainExecutor
}

func (x *AutomationCallNode) Type() string { return "automation" }

func (x *AutomationCallNode) New() types.Node {
	return &AutomationCallNode{Executor: getAutomationExecutor()}
}

func (x *AutomationCallNode) Init(ruleConfig types.Config, cfg types.Configuration) error {
	if err := maps.Map2Struct(cfg, &x.Config); err != nil {
		return err
	}
	x.CurrentNodeDef = base.NodeUtils.GetSelfDefinition(cfg)
	if x.Config.TargetId == "" {
		// 发布校验会拦，这里兜底告警（直接部署外部 DSL 的路径没有设计器校验）
		logrus.WithField("nodeId", x.GetSelfId()).
			Warn("automation node has empty targetId; it will fail at runtime")
	}
	return nil
}

func (x *AutomationCallNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	defer recoverNodePanic(ctx, msg, "automation", x.GetSelfId())
	if x.Executor == nil {
		ctx.TellFailure(msg, fmt.Errorf("automation executor not configured"))
		return
	}
	if x.Config.TargetId == "" {
		ctx.TellFailure(msg, fmt.Errorf("automation node targetId is empty"))
		return
	}
	start := time.Now()
	err := x.Executor.Execute(x.Config.TargetId, msg)
	logrus.WithFields(logrus.Fields{
		"nodeType":   x.Type(),
		"nodeId":     x.GetSelfId(),
		"targetId":   x.Config.TargetId,
		"instanceId": metaValue(msg, constants.KeyInstanceID),
		"taskId":     metaValue(msg, constants.KeyTaskID),
		"durationMs": time.Since(start).Milliseconds(),
		"status":     map[bool]string{true: AuditStatusSuccess, false: AuditStatusFailed}[err == nil],
	}).Info("AutomationCallNode execution")
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	ctx.TellSuccess(msg)
}

func (x *AutomationCallNode) Destroy() {}

// GetSelfId 当前节点 ID（CurrentNodeDef 缺失时退回节点类型）
func (x *AutomationCallNode) GetSelfId() string {
	return selfID(x.CurrentNodeDef, x.Type())
}

// globalAutomationExecutor 包级默认执行器，AutomationCallNode 与 AIAgentNode 共用。
// 由 register.go 经 SetAutomationExecutor 注入；读写经锁保护，运行期替换安全。
var (
	automationExecutorMu     sync.RWMutex
	globalAutomationExecutor service.RuleChainExecutor
)

// SetAutomationExecutor 注入默认规则链执行器，供 AutomationCallNode / AIAgentNode 的 New() 引用。
func SetAutomationExecutor(executor service.RuleChainExecutor) {
	automationExecutorMu.Lock()
	defer automationExecutorMu.Unlock()
	globalAutomationExecutor = executor
}

// getAutomationExecutor 并发安全读取当前默认执行器（可能为 nil）。
func getAutomationExecutor() service.RuleChainExecutor {
	automationExecutorMu.RLock()
	defer automationExecutorMu.RUnlock()
	return globalAutomationExecutor
}
