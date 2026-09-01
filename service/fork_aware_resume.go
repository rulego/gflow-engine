// This file implements fork-aware ExecuteNext for the parallel/inclusive gateway
// pattern (fork/inclusive → suspend nodes → join). See docs/parallel-limitations.md
// for the historical context.
//
// 原理：approve 一个 userTask 后调用 ExecuteNext(startNodeId=userTask)，不能直接
// 走 types.WithStartNode 单节点路径——那会让 join 的 TellCollect 丢失 fork 父
// 上下文，永远收不齐兄弟分支的消息，实例卡在 active。
//
// 因此先检测 startNodeId 是否处于 fork-join 拓扑中。如果是，且所有兄弟分支的
// 暂停型节点都已完成，则用 types.WithRestoreNodes 触发 multi-node 恢复路径。
// processRestoreNodes 会自动通过 LCA 重建 fork 父上下文（waitingCount = N），
// 让 join 在同一个 RunSnapshot/Observer 内收齐消息。这与 RestoreProcessInstance
// 走的是同一条路径。
//
// 暂停型节点（suspendNode）= 让流程暂停等待外部触发的节点：
//   - userTask：等待用户审批
//   - aiAgent（同步模式）：等待 LLM 返回（异步模式不暂停，主流程立即继续）
//   - delay：等待指定时间
//
// 不支持的拓扑（hard fail，避免静默卡死）：
//   - 嵌套 fork：outer_fork → [task, inner_fork → [...]] → outer_join
//   - 分支中没有任何暂停型节点

package service

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
)

// suspendNodeTypes 是会让流程暂停、需要外部触发的节点类型集合。
// 这些节点在 multi-node 恢复时都需要重新 OnMsg 检查"是否已完成"。
//
// aiAgent 仅在同步模式（configuration.async=false 或缺省）下才真正暂停
// 主流程。异步模式立即 TellSuccess、主流程不暂停，由 isSuspendNode 单独判断
// 跳过，避免恢复时重发 LLM 请求造成重复扣费。
var suspendNodeTypes = map[string]bool{
	constants.NodeTypeUserTask: true,
	constants.NodeTypeAIAgent:  true,
	constants.NodeTypeDelay:    true,
}

// forkResumeDecision describes how ExecuteNext should drive the workflow
// after a suspend node is approved/resumed.
type forkResumeDecision int

const (
	// forkResumeSingle: startNodeId is not part of a detectable fork-join
	// pattern. Use the single-node ExecuteNext path (WithStartNode).
	forkResumeSingle forkResumeDecision = iota
	// forkResumeMulti: startNodeId is the last sibling under a fork whose
	// branch suspend nodes are all completed. Drive multi-node restore via
	// WithRestoreNodes so the join can collect all branch messages in one
	// RunSnapshot. reqs carries per-branch NodeRequests with each branch's
	// task variables for proper merge semantics.
	forkResumeMulti
	// forkResumeSkip: startNodeId is part of a fork-join but other sibling
	// branches are still pending. Do nothing — the last sibling to complete
	// will trigger forkResumeMulti. pendingBranches lists which branches are
	// still incomplete (for observability/logging).
	forkResumeSkip
)

// ErrUnsupportedForkTopology 表示流程定义包含当前 fork-aware resume 不支持的
// 拓扑（嵌套 fork、分支无暂停节点等）。返回此错误而不是静默 fallback，让上层
// 能立刻发现问题而不是看到实例卡死。
var ErrUnsupportedForkTopology = fmt.Errorf("unsupported fork topology")

// forkGraphCache 缓存 processID → *forkGraph。避免每次 ExecuteNext 重新解析 JSON。
//
// 失效：InvalidateForkGraphCache(processID)，ProcessService.Delete / Update（就地改
// definition_json，processID 不变）都会调用。正常 Deploy 以新版本（新 processID）发布，
// 不会命中老缓存。
var forkGraphCache sync.Map

// forkGraphInvalidateHook 跨副本失效广播钩子（双实例时由 gflow 注入：PUBLISH 到 redis）。
// nil（单机或未注入）时 InvalidateForkGraphCache 仅清本地。
// 读写加锁：允许运行期替换钩子而不触发数据竞争。
var (
	forkGraphHookMu         sync.RWMutex
	forkGraphInvalidateHook func(processID string)
)

// SetForkGraphInvalidateHook 注入跨副本失效广播钩子（gflow 启动时调）。
// 钩子负责把失效事件广播到集群其他副本（如 Redis pub/sub）。
func SetForkGraphInvalidateHook(h func(processID string)) {
	forkGraphHookMu.Lock()
	defer forkGraphHookMu.Unlock()
	forkGraphInvalidateHook = h
}

// getForkGraphHook 并发安全读取当前钩子（可能为 nil）。
func getForkGraphHook() func(processID string) {
	forkGraphHookMu.RLock()
	defer forkGraphHookMu.RUnlock()
	return forkGraphInvalidateHook
}

// InvalidateForkGraphCache 从缓存中移除指定 processID 的 forkGraph，并触发跨副本
// 失效广播（双实例下让其他副本也清掉，避免 ProcessService.Update 改 definition_json
// 后其他副本仍用老拓扑判断恢复路径）。ProcessService.Delete / Update 调用此函数。
func InvalidateForkGraphCache(processID string) {
	if processID == "" {
		return
	}
	forkGraphCache.Delete(processID)
	if hook := getForkGraphHook(); hook != nil {
		hook(processID)
	}
}

// ApplyRemoteInvalidate 仅清本地缓存（不广播），供跨副本订阅收到远程失效消息时调用，
// 避免收到自己广播的消息又触发广播形成循环。
func ApplyRemoteInvalidate(processID string) {
	if processID == "" {
		return
	}
	forkGraphCache.Delete(processID)
}

// forkResumeOutcome 封装 analyzeForkResume 的返回。multi 路径下 reqs 是
// WithRestoreNodes 的输入；skip 路径下 pendingBranches 用于日志。
type forkResumeOutcome struct {
	decision        forkResumeDecision
	reqs            []types.NodeRequest
	pendingBranches []string // 仅 forkResumeSkip 时有意义
}

// analyzeForkResume walks the rule chain definition to detect a fork-join
// pattern around startNodeId and decides how ExecuteNext should resume.
//
// 错误处理策略：
//   - 解析失败、DAO 错误：fallback 到 single-node（保守，不阻塞线性流程）
//   - 不支持的拓扑（嵌套 fork / 分支无暂停节点）：返回 ErrUnsupportedForkTopology
//     让上层报错给用户（避免静默卡死）
func (s *RuntimeServiceImpl) analyzeForkResume(
	ctx context.Context,
	inst *model.WfInstance,
	startNodeId string,
) (*forkResumeOutcome, error) {
	if inst == nil || inst.ProcessID == "" || startNodeId == "" {
		return &forkResumeOutcome{decision: forkResumeSingle}, nil
	}
	if s.processDAO == nil || s.taskDAO == nil {
		return &forkResumeOutcome{decision: forkResumeSingle}, nil
	}

	graph, err := s.loadForkGraph(ctx, inst.ProcessID)
	if err != nil {
		logrus.WithError(err).WithField("processId", inst.ProcessID).
			Warn("fork-aware resume: failed to load process definition; falling back to single-node path")
		return &forkResumeOutcome{decision: forkResumeSingle}, nil
	}

	forkID := graph.findForkAncestor(startNodeId)
	if forkID == "" {
		return &forkResumeOutcome{decision: forkResumeSingle}, nil
	}

	// 拒绝嵌套 fork：当前算法对嵌套 fork 行为不正确。
	if graph.hasNestedFork(forkID) {
		return nil, fmt.Errorf("%w: nested fork detected under fork %s; "+
			"flatten the workflow or avoid fork inside fork branches", ErrUnsupportedForkTopology, forkID)
	}

	siblings := graph.children[forkID]
	if len(siblings) < 2 {
		// Fork with fewer than 2 children — not a real fan-out.
		return &forkResumeOutcome{decision: forkResumeSingle}, nil
	}

	branchRoots := sortedKeys(siblings)

	tasks, err := s.taskDAO.GetByProcessInstanceID(ctx, inst.ID)
	if err != nil {
		logrus.WithError(err).WithField("instanceId", inst.ID).
			Warn("fork-aware resume: failed to load tasks; falling back to single-node path")
		return &forkResumeOutcome{decision: forkResumeSingle}, nil
	}
	// index: taskDefKey -> []*WfTask. 一个节点可能对应多条 wf_task（会签子任务、
	// 顺序审批多轮等）。分支出口视为 completed 仅当该 key 下所有行都 Completed。
	tasksByKey := make(map[string][]*model.WfTask, len(tasks))
	for _, t := range tasks {
		if t.TaskDefKey == "" {
			continue
		}
		tasksByKey[t.TaskDefKey] = append(tasksByKey[t.TaskDefKey], t)
	}

	// 一条分支可能有多个串联 suspend 节点（如 task_a1 → task_a2 → join）。
	// 先判断 startNodeId 是否是自己分支里"最后一个 suspend 节点"。如果不是
	// （分支里还有后续 suspend，比如只 approve 了 task_a1），不能用 multi-node
	// 恢复也不能 skip——必须走单节点路径让 task_a1.OnMsg 自然驱动 task_a2 创建，
	// 否则 task_a2 永远不会被创建，分支卡死。
	branchRootOfStart := graph.findBranchRoot(startNodeId, forkID)
	if branchRootOfStart != "" {
		allSuspendsInStartBranch := graph.findAllSuspendNodes(branchRootOfStart)
		if len(allSuspendsInStartBranch) > 0 {
			lastSuspendInStartBranch := allSuspendsInStartBranch[len(allSuspendsInStartBranch)-1]
			if startNodeId != lastSuspendInStartBranch {
				logrus.WithField("startNodeId", startNodeId).
					WithField("lastSuspendInBranch", lastSuspendInStartBranch).
					Info("fork-aware resume: startNodeId is not the last suspend in its branch; driving chain via single-node path to advance to next suspend")
				return &forkResumeOutcome{decision: forkResumeSingle}, nil
			}
		}
	}

	// 找每条分支的第一个暂停型节点（最靠近 fork 的那个）。
	// 重新驱动它能让分支自然 TellSuccess 走完整条链到 join（包括后续串联的
	// suspend 节点，它们也会被 OnMsg 检查并 TellSuccess）。
	//
	// 混用分支（suspend 分支 + 纯同步分支）的处理：纯同步分支（没有任何
	// userTask/aiAgent/delay）在初始 OnMsg 时已执行完毕，其 msg 已发到 join，
	// 但因为后续 suspend 分支暂停而没有触发 join callback。multi-node 恢复时
	// 不需要再驱动 sync-only 分支，只需驱动含 suspend 的分支即可让 join 收齐
	// 消息触发 callback。sync-only 分支的副作用节点（如果有的话）已经执行过，
	// 不能也不需要再跑。
	// 包容分支（inclusive）按条件部分激活：路由未命中的分支从未 OnMsg，
	// 其子树不会有任何 wf_task 行。这些分支既不算 pending，也不能进 multi-node
	// 恢复（否则会把没走过的分支凭空执行一遍）。fork（并行）总是全量激活。
	notActivated := make(map[string]bool, len(branchRoots))
	if graph.nodeType[forkID] == constants.NodeTypeInclusive {
		for _, root := range branchRoots {
			if !graph.branchHasAnyTask(root, tasksByKey) {
				notActivated[root] = true
			}
		}
		if len(notActivated) > 0 {
			logrus.WithField("forkId", forkID).
				WithField("notActivatedBranches", sortedKeys(notActivated)).
				Info("fork-aware resume: inclusive branches not activated by routing; excluded from resume")
		}
	}

	exits := make([]string, 0, len(branchRoots))
	skippedSyncBranches := make([]string, 0)
	for _, root := range branchRoots {
		if notActivated[root] {
			// 未激活分支：路由未命中，没有执行过也没有任务，直接排除。
			continue
		}
		exit := graph.findFirstSuspendNode(root)
		if exit == "" {
			// 纯同步分支：在初始 OnMsg 时已跑完，跳过它。
			// 该分支的局部 msg 不会出现在 join 的 merge 结果里，
			// 但分支里的同步节点写入的变量都在 instance.Variables 里（共享 defaultMsg）。
			skippedSyncBranches = append(skippedSyncBranches, root)
			continue
		}
		exits = append(exits, exit)
	}

	// 所有分支都是纯同步：不应该调到 ExecuteNext（没有 suspend 节点可以 approve），
	// 兜底走 single-node 路径，让规则引擎按原逻辑处理。
	if len(exits) == 0 {
		logrus.WithField("forkId", forkID).
			WithField("syncBranches", skippedSyncBranches).
			Info("fork-aware resume: all branches are sync-only; falling back to single-node path")
		return &forkResumeOutcome{decision: forkResumeSingle}, nil
	}

	if len(skippedSyncBranches) > 0 {
		logrus.WithField("forkId", forkID).
			WithField("skippedSyncBranches", skippedSyncBranches).
			WithField("suspendBranches", exits).
			Info("fork-aware resume: mixed branches detected; sync-only branches skipped in multi-node restore")
	}

	// 检查每条分支的"所有 suspend 节点"是否都 completed（不仅是第一个）。
	// 比如 task_a1 → task_a2 → join，光 task_a1 completed 不够，task_a2 也要 completed。
	// 否则 multi-node 恢复时 task_a1.OnMsg → TellSuccess → task_a2.OnMsg 看到 task_a2
	// 还 Active → 不会 TellSuccess → join 收不齐 → 实例卡死。
	pendingBranches := make([]string, 0)
	for _, root := range branchRoots {
		if notActivated[root] {
			continue // 包容分支未激活：没有任务不是未完成，是没走过
		}
		allSuspends := graph.findAllSuspendNodes(root)
		if len(allSuspends) == 0 {
			continue // sync-only 分支，已 skip
		}
		for _, suspendNodeId := range allSuspends {
			rows, ok := tasksByKey[suspendNodeId]
			if !ok || len(rows) == 0 || !allRowsCompleted(rows) {
				pendingBranches = append(pendingBranches, suspendNodeId)
			}
		}
	}
	if len(pendingBranches) > 0 {
		return &forkResumeOutcome{decision: forkResumeSkip, pendingBranches: pendingBranches}, nil
	}

	// 全部分支出口 completed → multi-node 恢复。
	//
	// 分支局部变量：用 ExecuteNodeWithMsg 替代 ExecuteNode，每个分支带自己的
	// task.Variables 而非共享 defaultMsg，这样 join 合并时拿得到分支局部变量。
	// 与 RestoreProcessInstance / ForceResumeInstance 走同一路径。
	//
	// rulego 引擎的 TellCollect 会把每条分支的 msg 收集到
	// nodeInMsgList[joinNodeId]，per-branch msg 完全支持。
	reqs := make([]types.NodeRequest, 0, len(exits))
	for _, exitNodeId := range exits {
		reqs = append(reqs, restoreNodeRequest(exitNodeId,
			s.buildBranchResumeMsg(inst, exitNodeId, tasksByKey[exitNodeId])))
	}
	return &forkResumeOutcome{decision: forkResumeMulti, reqs: reqs}, nil
}

// loadForkGraph 从缓存或解析流程定义。流程定义不可变，缓存永久有效。
func (s *RuntimeServiceImpl) loadForkGraph(ctx context.Context, processID string) (*forkGraph, error) {
	if cached, ok := forkGraphCache.Load(processID); ok {
		return cached.(*forkGraph), nil
	}
	processDef, err := s.processDAO.Get(ctx, processID)
	if err != nil {
		return nil, fmt.Errorf("get process definition: %w", err)
	}
	if processDef == nil {
		return nil, fmt.Errorf("process definition not found: %s", processID)
	}
	graph, err := parseForkGraph(processDef)
	if err != nil {
		return nil, fmt.Errorf("parse rule chain: %w", err)
	}
	forkGraphCache.Store(processID, graph)
	return graph, nil
}

// buildBranchResumeMsg 构造 multi-node 恢复时单个分支用的 msg。
//
// 变量优先级：
//  1. task.Variables（分支局部变量，最准确）
//  2. instance.Variables（流程变量，fallback）
//
// metadata 复用 ExecuteNext 主 msg 的字段（tenant/instance/process/owner）。
//
// 不注入 task_id（与 RestoreProcessInstance 不同）：
// multi-node 恢复时 suspend 节点已 Completed，userTask 走自己的 task 复用
// 逻辑不需要 task_id；注入的 task_id 会通过 userTask.TellSuccess(msg.Copy())
// 传播到 join → end，让 end 节点的 TaskCreator.Before 误以为"task 已存在"，
// 跳过 CreateTask，导致 wf_hi_task 缺少 end 记录。
// 这里只设置基础 metadata，task_id 由各节点自己管理。
//
// 对 delay 类型的 task，额外注入 _delayOffsetMs（从 CreatedAt 计算的已等待
// 毫秒数），让 delay 节点恢复时跳过已等待时间，不会从头重新计时。逻辑参照
// RestoreProcessInstance（runtime_service_impl.go::RestoreProcessInstance）。
func (s *RuntimeServiceImpl) buildBranchResumeMsg(
	inst *model.WfInstance,
	exitNodeId string,
	rows []*model.WfTask,
) types.RuleMsg {
	var variablesStr string
	if len(rows) > 0 && rows[0].Variables != nil && *rows[0].Variables != "" {
		variablesStr = *rows[0].Variables
	} else if inst.Variables != nil {
		variablesStr = *inst.Variables
	} else {
		variablesStr = "{}"
	}

	md := types.NewMetadata()
	md.PutValue(constants.KeyTenantID, inst.TenantID)
	md.PutValue(constants.KeyInstanceID, inst.ID)
	if inst.BusinessKey != nil {
		md.PutValue(constants.KeyBusinessKey, *inst.BusinessKey)
	}
	md.PutValue(constants.KeyOwner, inst.CreatedBy)
	md.PutValue(constants.KeyProcessID, inst.ProcessID)
	if len(rows) > 0 {
		// delay 节点恢复时跳过已等待的时间。和 RestoreProcessInstance 的
		// delay 偏移逻辑保持一致。
		if rows[0].TaskType == constants.TaskTypeDelay && !rows[0].CreatedAt.IsZero() {
			offset := time.Since(rows[0].CreatedAt).Milliseconds()
			if offset < 0 {
				offset = 0
			}
			md.PutValue(constants.KeyDelayOffsetMs, fmt.Sprintf("%d", offset))
		}
	}
	return types.NewMsg(0, "wf_fork_resume", types.JSON, md, variablesStr)
}

// allRowsCompleted 检查一个 taskDefKey 下所有 wf_task 是否都 Completed。
// 用于会签/顺序审批场景下判断"节点整体完成"。
func allRowsCompleted(rows []*model.WfTask) bool {
	if len(rows) == 0 {
		return false
	}
	for _, r := range rows {
		if r.Status != string(enums.TaskStatusCompleted) {
			return false
		}
	}
	return true
}

// forkGraph is a parsed view of the rule chain definition with just enough
// information to detect fork-join topology.
type forkGraph struct {
	nodeType    map[string]string          // nodeId -> type
	parents     map[string]map[string]bool // nodeId -> set of parent nodeIds
	children    map[string]map[string]bool // nodeId -> set of child nodeIds
	asyncAgents map[string]bool            // nodeId -> true if aiAgent async mode (skip as suspend)
}

// parseForkGraph builds a forkGraph from the process definition. Handles
// both the flat types.RuleChain JSON and the designer envelope
// (form/flow/ruleChain/metadata) via WfProcess.ToRuleChain.
//
// aiAgent 的 async 标志从 configuration.async 读取。async=true 的 aiAgent
// 不算暂停型节点（主流程立即 TellSuccess，不需要等待）。
func parseForkGraph(processDef *model.WfProcess) (*forkGraph, error) {
	chain, err := processDef.ToRuleChain()
	if err != nil {
		return nil, fmt.Errorf("parse rule chain: %w", err)
	}
	g := &forkGraph{
		nodeType:    make(map[string]string, len(chain.Metadata.Nodes)),
		parents:     make(map[string]map[string]bool),
		children:    make(map[string]map[string]bool),
		asyncAgents: make(map[string]bool),
	}
	for _, n := range chain.Metadata.Nodes {
		if n.Id == "" {
			continue
		}
		g.nodeType[n.Id] = n.Type
		if n.Type == constants.NodeTypeAIAgent && isAiAgentAsync(n.Configuration) {
			g.asyncAgents[n.Id] = true
		}
	}
	addEdge := func(from, to string) {
		if g.children[from] == nil {
			g.children[from] = make(map[string]bool)
		}
		g.children[from][to] = true
		if g.parents[to] == nil {
			g.parents[to] = make(map[string]bool)
		}
		g.parents[to][from] = true
	}
	for _, c := range chain.Metadata.Connections {
		if c.FromId == "" || c.ToId == "" {
			continue
		}
		addEdge(c.FromId, c.ToId)
	}
	return g, nil
}

// isAiAgentAsync reads configuration.async as bool. Defends against
// missing key (treated as sync, the documented default in ai_agent_node.go),
// wrong type (cast.ToBool semantics: strings like "true" → true).
func isAiAgentAsync(cfg types.Configuration) bool {
	if cfg == nil {
		return false
	}
	v, ok := cfg["async"]
	if !ok {
		return false
	}
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return x == "true" || x == "True" || x == "TRUE"
	default:
		return false
	}
}

// isSuspendNode 判断节点是否为"暂停型"：会让主流程等待，需要 multi-node 恢复
// 时重新 OnMsg。对 aiAgent 单独处理：async=true 不算暂停（主流程不停）。
func (g *forkGraph) isSuspendNode(nodeId string) bool {
	t := g.nodeType[nodeId]
	if !suspendNodeTypes[t] {
		return false
	}
	if t == constants.NodeTypeAIAgent && g.asyncAgents[nodeId] {
		return false
	}
	return true
}

// isBranchingNode 判断节点类型是否为"分支网关"：会同时激活多条分支，需要 join 汇聚。
// 当前支持 fork（并行分支）和 inclusive（包容分支）。switch（条件路由）是排他路由，
// 只走一条分支，不需要 join，不算分支网关。
func isBranchingNode(nodeType string) bool {
	return nodeType == "fork" || nodeType == constants.NodeTypeInclusive
}

// findForkAncestor BFS up the parent chain to find the nearest ancestor
// (excluding startNodeId itself) that is a branching node (fork or inclusive).
// Returns "" if none.
func (g *forkGraph) findForkAncestor(start string) string {
	if start == "" {
		return ""
	}
	visited := map[string]bool{start: true}
	queue := []string{start}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur != start && isBranchingNode(g.nodeType[cur]) {
			return cur
		}
		for _, p := range sortedKeys(g.parents[cur]) {
			if !visited[p] {
				visited[p] = true
				queue = append(queue, p)
			}
		}
	}
	return ""
}

// hasNestedFork 检查 forkID 的子树里是否还有其他分支网关节点（fork 或 inclusive）。
// 嵌套分支网关当前不支持，hard fail 让用户感知。
// 扫描止于 join：join 之后已脱离本 fork 的分支体，顺序并行块
// （fork1→join1→fork2）属受支持拓扑，不是「fork 分支内再 fork」。
func (g *forkGraph) hasNestedFork(forkID string) bool {
	if forkID == "" {
		return false
	}
	visited := map[string]bool{forkID: true}
	queue := []string{}
	for _, c := range sortedKeys(g.children[forkID]) {
		if g.nodeType[c] == "join" {
			continue
		}
		queue = append(queue, c)
		visited[c] = true
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if isBranchingNode(g.nodeType[cur]) {
			return true
		}
		for _, c := range sortedKeys(g.children[cur]) {
			// join 是分支汇聚点，越过它会把合流之后的节点误算进本 fork 的子树
			//（与 branchHasAnyTask/findFirstSuspendNode/findAllSuspendNodes 同一边界）
			if !visited[c] && g.nodeType[c] != "join" {
				visited[c] = true
				queue = append(queue, c)
			}
		}
	}
	return false
}

// findFirstSuspendNode BFS down from root, returns the first suspend node
// (userTask / sync aiAgent / delay) encountered. Returns "" if none in the
// subtree.
//
// "First" via BFS = the suspend node closest to the fork in hop count. We pick
// the highest suspend node (closest to fork) on purpose: re-driving it lets
// the rest of the chain (deeper suspend nodes, sync nodes, ...) propagate
// naturally via TellSuccess → join.
//
// async=true 的 aiAgent 不算暂停节点（主流程不停），会被跳过，
// 避免恢复时重发 LLM 请求造成重复扣费。
func (g *forkGraph) findFirstSuspendNode(root string) string {
	if root == "" {
		return ""
	}
	visited := map[string]bool{root: true}
	queue := []string{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if g.isSuspendNode(cur) {
			return cur
		}
		for _, k := range sortedKeys(g.children[cur]) {
			// join 是分支汇聚点，越过它会把 join 之后续接链路的节点误算进本分支
			//（与 branchHasAnyTask 同一边界）
			if !visited[k] && g.nodeType[k] != "join" {
				visited[k] = true
				queue = append(queue, k)
			}
		}
	}
	return ""
}

// findAllSuspendNodes BFS down from root, returns ALL suspend nodes in BFS
// order (closest to fork first). Used to:
//   - 判断 startNodeId 是否是分支里最后一个 suspend（如果不是，走单节点路径
//     驱动 chain）
//   - 判断一条分支是否真的"全部 suspend 都 completed"（不仅是第一个）
//
// 返回顺序：按 BFS 访问顺序，离 root 近的排前面。在树形流程（每个节点最多
// 一个父节点）里，这就是按"距 fork 的跳数"排序。
func (g *forkGraph) findAllSuspendNodes(root string) []string {
	if root == "" {
		return nil
	}
	var suspends []string
	visited := map[string]bool{root: true}
	queue := []string{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if g.isSuspendNode(cur) {
			suspends = append(suspends, cur)
		}
		for _, k := range sortedKeys(g.children[cur]) {
			// join 是分支汇聚点，越过它会把 join 之后续接链路的 suspend 节点
			// 误算进本分支——导致“本分支最后一个 suspend”被解析成合流之后的
			// 节点，两路分支完成后 join 收不齐消息，实例永远卡在 active
			//（与 branchHasAnyTask 同一边界）
			if !visited[k] && g.nodeType[k] != "join" {
				visited[k] = true
				queue = append(queue, k)
			}
		}
	}
	return suspends
}

// restoreNodeRequest 构造 multi-node 恢复的节点请求。
// 关键：RelationTypes 必须是非 nil 空切片。rulego 的 SetExecuteNodes 对
// 「单节点 + RelationTypes==nil」会降级为 WithStartNode 式自启动（清空
// restoreNodeInfo，不重建分支父上下文）——join 的 TellCollect 丢失 LCA
// 上下文，包容分支仅一条分支激活时实例永远卡在 active。空切片（len==0）
// 既绕开该捷径，又让 processRestoreNodes 的 isFirst=len(RelationTypes)==0
// 保持「执行节点自身」语义。
func restoreNodeRequest(nodeId string, msg types.RuleMsg) types.NodeRequest {
	return types.NodeRequest{
		NodeId:        nodeId,
		RelationTypes: []string{},
		Msg:           &msg,
	}
}

// branchHasAnyTask 判断分支子树内是否有任何节点产生过 wf_task 行。
// 用于 inclusive 部分激活场景识别"路由未命中的分支"：这种分支从未 OnMsg，
// 其子树不会有任何任务记录。遍历以 join 节点为边界（join 是分支汇聚点，
// 越过它会误把 join 之后链路的任务算进来）。
func (g *forkGraph) branchHasAnyTask(root string, tasksByKey map[string][]*model.WfTask) bool {
	if root == "" {
		return false
	}
	visited := map[string]bool{root: true}
	queue := []string{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if rows, ok := tasksByKey[cur]; ok && len(rows) > 0 {
			return true
		}
		for k := range g.children[cur] {
			if visited[k] || g.nodeType[k] == "join" {
				continue
			}
			visited[k] = true
			queue = append(queue, k)
		}
	}
	return false
}

// findBranchRoot 从 startNodeId 往上找，返回 forkID 的直接子节点（即 startNodeId
// 所在分支的"根"）。如果 startNodeId 本身就是 forkID 的直接子节点，返回 startNodeId。
// 用于判断 startNodeId 是否是分支里最后一个 suspend：需要从分支根开始 BFS 找所有 suspend。
//
// 流程拓扑假设：树形（每个节点最多一个父节点）。DAG 场景下结果不确定，但
// BPMN-style 流程定义都是树形。
func (g *forkGraph) findBranchRoot(startNodeId, forkID string) string {
	if startNodeId == "" || forkID == "" {
		return ""
	}
	if startNodeId == forkID {
		return ""
	}
	// startNodeId 是 forkID 的直接子节点？
	if g.parents[startNodeId] != nil {
		if g.parents[startNodeId][forkID] {
			return startNodeId
		}
	}
	// 否则往上找，直到找到 forkID 的直接子节点
	visited := map[string]bool{startNodeId: true}
	queue := []string{startNodeId}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur != startNodeId && g.parents[cur] != nil && g.parents[cur][forkID] {
			return cur
		}
		// 注：startNodeId 自身的 parents 已在循环前的直接子节点检查中覆盖，
		// 这里无需再判 p == forkID
		for p := range g.parents[cur] {
			if !visited[p] {
				visited[p] = true
				queue = append(queue, p)
			}
		}
	}
	return ""
}

// sortedKeys 返回 map key 排序后的切片，保证 BFS 遍历顺序确定。
func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
