package service

import (
	"context"

	"github.com/rulego/rulego/api/types"
)

// RuntimeServiceInternal 引擎内部机制使用的运行时服务接口，宿主不应调用。
// 在 RuntimeService 的基础上暴露规则链池（enginePool）与流程续跑（ExecuteNext）等
// 引擎内部机制——它们由 subProcess/userTask 等工作流组件与引擎自身在推进过程中回调，
// 调用时机强绑定内部状态机（行锁事务、AfterCommit 顺序等），宿主直接调用会导致
// 实例状态错乱或并发推进。公共 RuntimeService 不 import rulego；rulego 类型只经
// 本接口与 impl 暴露。RuntimeServiceImpl 同时满足 RuntimeService 与本接口。
type RuntimeServiceInternal interface {
	RuntimeService

	// GetExecution 根据ID获取执行实例
	GetExecution(ctx context.Context, processID string) (types.RuleEngine, error)

	// PreloadChain 预加载流程定义对应的规则链到【租户专属】enginePool（不启动实例）。
	// 用途：subProcess 节点通过 targetId（ruleChain.id 别名）TellFlow 调用子链时，
	// 子链必须已注册进池。Deploy 时预加载子流程链，使其对父流程的 subProcess 可见。
	// 幂等：与 initExecution/start 时的注册一致，重复预加载返回已存在的引擎。
	PreloadChain(tenantID, processID, processDef string) error

	// EvictStaleChain 驱逐过期版本链（best-effort，无返回）：若 processID 对应版本
	// 【非最新版】且【无 active 实例】，从租户池移除其链+别名。
	// 维持「同 ProcessKey 一版本 active」不变量。
	// 触发时机：Deploy 新版后（驱逐上一版）；实例完成/终止后（驱逐该版本若无其它活实例）。
	// 安全：GetExecution 自愈会按需重注册，故驱逐即使时序竞争也不影响正确性。
	EvictStaleChain(ctx context.Context, tenantID, processID string)

	// ResolveSubProcessTarget 把 subProcess 的 targetId（子链 ruleChain.id）解析为子流程定义ID。
	// 注册表在 PreloadChain 时按 (tenant, ruleChain.id) 建立。解析不到返回 ok=false。
	ResolveSubProcessTarget(tenantID, ruleChainID string) (processDefID string, ok bool)

	// StartSubProcessInstance 启动一个子流程实例（call activity 语义）：
	// child.ParentID=parentInstanceID；记 parentNodeID 用于完成回调恢复父流程。
	// 返回子实例ID。父流程在 subProcess 节点挂起，等子完成后续跑。
	StartSubProcessInstance(ctx context.Context, parentInstanceID, parentNodeID, childProcessDefID string, variables map[string]interface{}) (string, error)

	// SubProcessChildState 查询父实例下的子流程状态：是否有 active 子实例、是否有已完成（归档）子实例。
	// subProcess 节点 OnMsg 状态机据此决定 续跑/挂起/启动；查询失败返回 err（节点走 Failure 边）。
	SubProcessChildState(ctx context.Context, parentInstanceID string) (active, completed bool, err error)

	// SubProcessChildTerminated 子流程是否有被终止(reject terminate)的归档子实例,
	// 供父 subProcess 节点区分"子正常完成走 Success" vs "子被终止走 Failure"。
	SubProcessChildTerminated(ctx context.Context, parentInstanceID string) (bool, error)

	// ExecuteNext 从 startNodeId 继续推进流程实例（任务完成/跳转/恢复的统一续跑入口）。
	//       processInstanceID - 流程实例ID, startNodeId - 续跑起点节点ID,
	//       variables - 本次下传变量（空时回退到实例存储的启动变量）
	ExecuteNext(ctx context.Context, processInstanceID, startNodeId string, variables map[string]interface{}) error
}
