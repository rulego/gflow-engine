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

package components

import (
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"
)

// handleRejection 处理审批拒绝后的流程走向
//
// 拒绝处理不依赖流程设计器画 Failure 出边（未画该边时消息会丢失，实例永远卡在
// active 状态），而是直接驱动实例状态。
//
// 策略解析：
//   - 空字符串 / "terminate"：直接调用 RuntimeService.TerminateProcessInstance（默认）
//   - "rejectToStarter"：调用 ExecuteNext 跳到开始节点
//   - "rejectToPrev"：调用 ExecuteNext 跳到上一个 userTask 节点
//   - "rejectToNode"：调用 ExecuteNext 跳到 additionalInfo.rejectTargetNode 指定的节点
//   - 其他未知值：兜底 terminate，避免实例卡死
//
// 跳转失败时的兜底（hasRejectEdge）：若节点定义了 Reject/Failure 出边则走 rulego
// 分支，否则 terminate。
func (n *UserTaskNode) handleRejection(ctx types.RuleContext, msg types.RuleMsg, instanceID string) {
	strategy := strings.TrimSpace(n.Config.RejectStrategy)
	// rejectType 当前版本不生效（见 Config.RejectType 注释），日志标注 ignored 防误读
	logrus.Infof("Node %s rejected, strategy=%q, rejectType=%q (ignored), instance=%s",
		n.GetSelfId(), strategy, n.Config.RejectType, instanceID)

	switch strategy {
	case "", RejectStrategyTerminate:
		// 默认策略：终止流程。
		// 仅在真正终止路径触发 rejected 事件，避免后续 jump 回退到 terminate 时误通知。
		n.fireRejectedEvent(ctx, msg, instanceID, "审批驳回")
		terminateInstance(n.RuntimeService, n.GetSelfId(), ctx, msg, instanceID, constants.EndReasonPrefixRejected+"：终止流程")
		return
	case RejectStrategyRejectToStarter:
		n.fireRejectedEvent(ctx, msg, instanceID, "审批驳回，回退至发起人")
		n.jumpToStartNode(ctx, msg, instanceID)
		return
	case RejectStrategyRejectToPrev:
		n.fireRejectedEvent(ctx, msg, instanceID, "审批驳回，回退至上一审批节点")
		n.jumpToPrevUserTask(ctx, msg, instanceID)
		return
	case RejectStrategyRejectToNode:
		if strings.TrimSpace(n.Config.RejectTargetNode) == "" {
			logrus.Warnf("Node %s strategy=rejectToNode but rejectTargetNode empty, fallback", n.GetSelfId())
			n.fallbackRejection(ctx, msg, instanceID, constants.EndReasonPrefixRejected+"：未配置 rejectTargetNode，降级处理")
			return
		}
		n.fireRejectedEvent(ctx, msg, instanceID, "审批驳回，回退至指定节点 "+n.Config.RejectTargetNode)
		n.jumpToNode(ctx, msg, instanceID, n.Config.RejectTargetNode)
		return
	}

	// 未识别的策略值：兜底 terminate，避免实例卡死
	logrus.Warnf("Node %s has unknown rejectStrategy=%q, terminating as fallback", n.GetSelfId(), strategy)
	n.fireRejectedEvent(ctx, msg, instanceID, "审批驳回")
	terminateInstance(n.RuntimeService, n.GetSelfId(), ctx, msg, instanceID, constants.EndReasonPrefixRejected+"：未识别的驳回策略，默认终止")
}

// fireRejectedEvent 异步触发 rejected 事件；terminate 与 jump 回退路径均派发，
// Reason 区分回退目标，TaskDefKey 定位被驳回的节点。
func (n *UserTaskNode) fireRejectedEvent(ctx types.RuleContext, msg types.RuleMsg, instanceID, reason string) {
	if n.TaskEventListener == nil {
		return
	}
	processID := n.getProcessID(msg)
	tenantID := metaValue(msg, constants.KeyTenantID)
	// 查发起人：通过 RuntimeService 获取实例的 StartUserID
	var startUserID string
	if n.RuntimeService != nil {
		if inst, err := n.RuntimeService.GetProcessInstance(ctx.GetContext(), service.ActorFromCtx(ctx.GetContext()), instanceID); err == nil && inst != nil {
			startUserID = inst.StartUserID
		}
	}
	// 驳回人：链执行 ctx 继承自 API 请求，携带 Actor
	fromUser := operatorFromCtx(ctx.GetContext())
	// TaskID 尽力回查当前节点任务，任务已归档则留空
	taskDefKey := n.GetSelfId()
	var taskID string
	if n.TaskService != nil {
		if t, err := n.TaskService.GetTaskByDefKey(ctx.GetContext(), instanceID, taskDefKey); err == nil && t != nil {
			taskID = t.ID
		}
	}
	listener := n.TaskEventListener
	evt := service.TaskEvent{
		Type:       service.TaskEventRejected,
		TaskID:     taskID,
		TaskDefKey: taskDefKey,
		InstanceID: instanceID,
		ProcessID:  processID,
		TenantID:   tenantID,
		TaskName:   n.GetSelfName(),
		FromUser:   fromUser,
		Reason:     reason,
		Timestamp:  time.Now(),
	}
	if startUserID != "" {
		evt.ToUsers = []string{startUserID}
	}
	service.DispatchTaskEvent(listener, evt, ctx.GetContext())
}

// hasRejectEdge 通过 ChainCtx.Definition() 查询当前节点是否有 Reject 出边
//
// 安全兜底用途：
//   - handleRejection 的默认策略是 terminate，不依赖 Reject 边
//   - 但当 jumpToNode/jumpToStartNode 等运行时跳转失败时，调用本方法检查
//     节点是否定义了 Reject 出边；若存在则走 rulego 自定义分支，
//     让流程设计师有机会自定义错误处理；不存在才降级 terminate
//   - 这样即使 rejectStrategy 配置错误或目标节点丢失，也不会让实例永久卡死
func (n *UserTaskNode) hasRejectEdge(ctx types.RuleContext) bool {
	def := getRuleChainDefinition(ctx)
	if def == nil {
		return false
	}
	selfID := ctx.GetSelfId()
	for _, conn := range def.Metadata.Connections {
		if conn.FromId == selfID && (conn.Type == RelationReject || conn.Type == types.Failure) {
			return true
		}
	}
	return false
}

// fallbackRejection 跳转失败时的兜底处理：优先走 Reject 出边，否则 terminate
func (n *UserTaskNode) fallbackRejection(ctx types.RuleContext, msg types.RuleMsg, instanceID, reason string) {
	if n.hasRejectEdge(ctx) {
		logrus.Warnf("Node %s falling back to Reject edge after reject jump failure", n.GetSelfId())
		ctx.TellNext(msg, RelationReject)
		return
	}
	terminateInstance(n.RuntimeService, n.GetSelfId(), ctx, msg, instanceID, reason)
}

// jumpToStartNode 跳回流程开始节点（取 Metadata.FirstNodeIndex 对应的节点作为开始）
func (n *UserTaskNode) jumpToStartNode(ctx types.RuleContext, msg types.RuleMsg, instanceID string) {
	if n.RuntimeService == nil {
		logrus.Errorf("RuntimeService not injected, cannot jump to start node for instance %s", instanceID)
		ctx.TellFailure(msg, fmt.Errorf("reject: runtime service unavailable"))
		return
	}
	startNodeID := getStartNodeID(ctx)
	if startNodeID == "" {
		logrus.Errorf("cannot resolve start node id for instance %s, fallback", instanceID)
		n.fallbackRejection(ctx, msg, instanceID, constants.EndReasonPrefixRejected+"：开始节点缺失，降级处理")
		return
	}
	n.jumpToNode(ctx, msg, instanceID, startNodeID)
}

// jumpToPrevUserTask 跳到上一个 userTask 节点；找不到则降级
func (n *UserTaskNode) jumpToPrevUserTask(ctx types.RuleContext, msg types.RuleMsg, instanceID string) {
	prevNodeID := n.findPrevUserTaskNodeID(ctx)
	if prevNodeID == "" {
		logrus.Warnf("cannot find previous userTask node for %s, fallback", n.GetSelfId())
		n.fallbackRejection(ctx, msg, instanceID, constants.EndReasonPrefixRejected+"：找不到上一个审批节点，降级处理")
		return
	}
	n.jumpToNode(ctx, msg, instanceID, prevNodeID)
}

// jumpToNode 跳到指定节点
func (n *UserTaskNode) jumpToNode(ctx types.RuleContext, msg types.RuleMsg, instanceID, targetNodeID string) {
	if n.RuntimeService == nil {
		logrus.Errorf("RuntimeService not injected, cannot jump to %s", targetNodeID)
		ctx.TellFailure(msg, fmt.Errorf("reject: runtime service unavailable"))
		return
	}
	// 目标节点存在性校验：ExecuteNext 对不存在的 startNodeId 静默成功（不报错也不路由），
	// 不拦截则 rejectToNode 配错目标时实例永久卡死 active。
	if !n.nodeExists(ctx, targetNodeID) {
		logrus.Warnf("reject jump target node %s not found in definition, fallback", targetNodeID)
		n.fallbackRejection(ctx, msg, instanceID, constants.EndReasonPrefixRejected+"：跳转目标节点不存在，降级处理")
		return
	}
	// 驳回回跳必须清理参与跳转节点上一轮的任务，否则：
	//  1) 路径上节点重入时 getExistingTasks 返回旧 Completed 任务，立即被判定已完成，
	//     节点被静默自动通过，驳回语义被绕过；
	//  2) 驳回节点自身的 Completed(Rejected) 任务不清理时，流程回流到本节点会再次
	//     触发 handleRejection，形成无限驳回回跳循环。
	// 清理范围是"目标节点到当前节点"的重执行区域（目标+可达自身的所有 userTask），
	// 只清 userTask（startTask 是 marker）。区域外的并行分支不受影响——清掉仍在
	// 办理中的兄弟分支任务会让汇合点永远凑不齐。
	if n.TaskService != nil {
		region := rejectResetNodes(nodeGraphFromDefinition(ctx), targetNodeID, n.GetSelfId())
		for _, nodeID := range region {
			if archived, err := n.TaskService.SupersedeNodeTasks(ctx.GetContext(), instanceID, nodeID, "superseded_by_reject_jump"); err != nil {
				// 清理失败不阻断跳转（best-effort），但记录告警便于排查
				logrus.WithError(err).WithField("node", nodeID).
					Warn("SupersedeNodeTasks failed before reject jump; stale tasks may cause silent misjudgment")
			} else if archived > 0 {
				logrus.WithField("node", nodeID).WithField("archived", archived).
					Info("superseded stale tasks before reject jump")
			}
		}
	} else {
		logrus.WithField("targetNode", targetNodeID).
			Warn("TaskService not injected, cannot supersede stale tasks before reject jump")
	}
	if err := n.RuntimeService.ExecuteNext(ctx.GetContext(), instanceID, targetNodeID, nil); err != nil {
		logrus.Errorf("jump to node %s failed: %v", targetNodeID, err)
		// 跳转失败：优先尝试 Reject/Failure 边（设计师预留的错误分支），找不到才 terminate
		n.fallbackRejection(ctx, msg, instanceID, constants.EndReasonPrefixRejected+"：跳转失败，降级处理")
		return
	}
	// 跳转成功后用空 relationType 收尾当前节点，避免触发 Failure 分支重复终止
	ctx.DoOnEnd(msg, nil, "")
}

// nodeGraph 流程定义的邻接表视图，供重执行区域计算使用。
type nodeGraph struct {
	nodeTypes map[string]string   // nodeID -> node type
	forward   map[string][]string // nodeID -> 下游节点ID
	backward  map[string][]string // nodeID -> 上游节点ID
}

// nodeGraphFromDefinition 把规则链定义转成邻接表。定义缺失时返回 nil。
func nodeGraphFromDefinition(ctx types.RuleContext) *nodeGraph {
	def := getRuleChainDefinition(ctx)
	if def == nil {
		return nil
	}
	g := &nodeGraph{
		nodeTypes: make(map[string]string, len(def.Metadata.Nodes)),
		forward:   make(map[string][]string),
		backward:  make(map[string][]string),
	}
	for _, nd := range def.Metadata.Nodes {
		g.nodeTypes[nd.Id] = nd.Type
	}
	for _, conn := range def.Metadata.Connections {
		g.forward[conn.FromId] = append(g.forward[conn.FromId], conn.ToId)
		g.backward[conn.ToId] = append(g.backward[conn.ToId], conn.FromId)
	}
	return g
}

// rejectResetNodes 计算驳回回跳需要清理任务的重执行区域：
// 从 target 正向可达、且能到达 self 的 userTask 节点（含 target 与 self 自身）。
// 区域外节点（如另一条并行分支）不在回流路径上，其任务——尤其是仍在办理中的——
// 必须保持原状，否则汇合点永远凑不齐。
// 环路用 visited 集合防死循环；self 或 target 不在图中时退化为只清理两者自身。
func rejectResetNodes(g *nodeGraph, target, self string) []string {
	if g == nil {
		return nil
	}
	reachableFromTarget := bfsSet(g.forward, target)
	reachesSelf := bfsSet(g.backward, self)
	seen := make(map[string]bool)
	out := make([]string, 0, 4)
	add := func(id string) {
		if id == "" || seen[id] || g.nodeTypes[id] != UserTaskNodeType {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	add(target)
	add(self)
	for id := range reachableFromTarget {
		if reachesSelf[id] {
			add(id)
		}
	}
	return out
}

// bfsSet 从 start 沿邻接表可达的全部节点（含 start 自身，若存在于表中）。
func bfsSet(adj map[string][]string, start string) map[string]bool {
	visited := make(map[string]bool)
	if start == "" {
		return visited
	}
	queue := []string{start}
	visited[start] = true
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range adj[cur] {
			if !visited[next] {
				visited[next] = true
				queue = append(queue, next)
			}
		}
	}
	return visited
}

// nodeExists 判断目标节点是否存在于当前流程定义。jumpToNode 跳转前校验，
// 防 ExecuteNext 对不存在的 startNodeId 静默成功导致实例卡死。
func (n *UserTaskNode) nodeExists(ctx types.RuleContext, nodeID string) bool {
	def := getRuleChainDefinition(ctx)
	if def == nil {
		return false
	}
	for _, nd := range def.Metadata.Nodes {
		if nd.Id == nodeID {
			return true
		}
	}
	return false
}

// findPrevUserTaskNodeID 通过 Definition 反查当前节点的前驱 userTask 节点
// 找不到返回空字符串
func (n *UserTaskNode) findPrevUserTaskNodeID(ctx types.RuleContext) string {
	def := getRuleChainDefinition(ctx)
	if def == nil {
		return ""
	}
	selfID := ctx.GetSelfId()

	// 节点类型映射
	nodeTypeMap := make(map[string]string, len(def.Metadata.Nodes))
	for _, node := range def.Metadata.Nodes {
		nodeTypeMap[node.Id] = node.Type
	}

	// 收集所有指向当前节点的上游节点 ID
	var upstreamIDs []string
	for _, conn := range def.Metadata.Connections {
		if conn.ToId == selfID {
			upstreamIDs = append(upstreamIDs, conn.FromId)
		}
	}

	// 优先返回类型为 userTask 的前驱
	for _, id := range upstreamIDs {
		if nodeTypeMap[id] == UserTaskNodeType {
			return id
		}
	}
	// 退而求其次：返回任意一个非开始节点的前驱。
	// DSL 中开始节点 type 有两种："startTask"（本引擎发起人节点）与
	// "start"（rulego 原生链起点），都要排除——回跳到开始节点等价于
	// rejectToStarter，会使 rejectToPrev 语义错误。
	for _, id := range upstreamIDs {
		if id == "" {
			continue
		}
		if t := nodeTypeMap[id]; t != constants.NodeTypeStart && t != StartTaskNodeType {
			return id
		}
	}
	return ""
}
