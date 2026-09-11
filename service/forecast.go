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
	"sync"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/rulego/api/types"
)

// approverPreviewResolver 审批人预览解析器，由组件层经 SetApproverPreviewResolver
// 注入（组件持有 userTask 配置结构与审批人解析逻辑）。入参为节点 configuration、
// 实例租户/发起人/流程变量，返回该节点的审批人预览。
var (
	approverPreviewResolver   func(cfg map[string]interface{}, tenantID, owner string, variables map[string]interface{}) *ApproverPreview
	approverPreviewResolverMu sync.RWMutex
)

// ApproverPreview 节点审批人预览结果。
type ApproverPreview struct {
	// ApproverType 审批人类型（approver.type 原值）
	ApproverType string
	// Assignees 解析出的审批人 userId 列表
	Assignees []string
	// Unresolved 不可解析原因：initiatorSelect/identityUnavailable/noAssignee；空=已解析
	Unresolved string
}

// SetApproverPreviewResolver 注入审批人预览解析器；传 nil 恢复跳过。并发安全。
func SetApproverPreviewResolver(fn func(cfg map[string]interface{}, tenantID, owner string, variables map[string]interface{}) *ApproverPreview) {
	approverPreviewResolverMu.Lock()
	defer approverPreviewResolverMu.Unlock()
	approverPreviewResolver = fn
}

func approverPreviewResolverForRead() func(cfg map[string]interface{}, tenantID, owner string, variables map[string]interface{}) *ApproverPreview {
	approverPreviewResolverMu.RLock()
	defer approverPreviewResolverMu.RUnlock()
	return approverPreviewResolver
}

// branchingNodeTypes 条件分支节点：出边走向取决于运行时条件求值，预测在此截断。
var branchingNodeTypes = map[string]bool{
	constants.NodeTypeSwitch:        true,
	constants.NodeTypeJsSwitch:      true,
	constants.NodeTypeMsgTypeSwitch: true,
	constants.NodeTypeInclusive:     true,
	"condition":                     true,
}

// BuildUpcomingNodes 从活跃节点出发沿 Success 出边向前遍历链定义，预测后续
// 依次待执行的审批节点。遍历按 BFS 层序（同层按 connections 出现顺序），
// visited 防环。tenantID/owner/variables 供审批人解析。
func BuildUpcomingNodes(chain *types.RuleChain, activeNodeIDs []string, tenantID, owner string, variables map[string]interface{}) []dto.UpcomingNode {
	resolver := approverPreviewResolverForRead()
	if chain == nil || len(activeNodeIDs) == 0 || resolver == nil {
		return nil
	}

	nodeByID := make(map[string]*types.RuleNode, len(chain.Metadata.Nodes))
	for _, n := range chain.Metadata.Nodes {
		nodeByID[n.Id] = n
	}
	successors := make(map[string][]string)
	for _, conn := range chain.Metadata.Connections {
		if conn.Type == types.Success || conn.Type == "" {
			successors[conn.FromId] = append(successors[conn.FromId], conn.ToId)
		}
	}

	var upcoming []dto.UpcomingNode
	activeSet := make(map[string]bool, len(activeNodeIDs))
	for _, id := range activeNodeIDs {
		activeSet[id] = true
	}
	visited := make(map[string]bool, len(nodeByID))
	queue := make([]string, 0, len(activeNodeIDs))
	enqueue := func(ids []string) {
		for _, id := range ids {
			if !visited[id] {
				visited[id] = true
				queue = append(queue, id)
			}
		}
	}
	enqueue(activeNodeIDs)

	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		node, ok := nodeByID[nodeID]
		if !ok {
			continue
		}
		if node.Type == constants.NodeTypeEnd {
			continue
		}
		// 条件分支节点：走向未知，预测截断
		if branchingNodeTypes[node.Type] {
			continue
		}
		// 活跃节点是遍历起点（进行中），不计入预测输出
		if node.Type == constants.NodeTypeUserTask && !activeSet[nodeID] {
			upcoming = append(upcoming, previewUserTaskNode(node, tenantID, owner, variables, resolver))
		}
		enqueue(successors[nodeID])
	}
	return upcoming
}

// previewUserTaskNode 解析单个 userTask 节点的审批人预览；解析失败不阻断遍历。
func previewUserTaskNode(node *types.RuleNode, tenantID, owner string, variables map[string]interface{},
	resolver func(cfg map[string]interface{}, tenantID, owner string, variables map[string]interface{}) *ApproverPreview) dto.UpcomingNode {
	un := dto.UpcomingNode{NodeID: node.Id, NodeName: node.Name}
	cfg := map[string]interface{}(node.Configuration)
	if cfg == nil {
		un.Unresolved = "noAssignee"
		return un
	}
	preview := resolver(cfg, tenantID, owner, variables)
	if preview == nil {
		un.Unresolved = "noAssignee"
		return un
	}
	un.ApproverType = preview.ApproverType
	un.Assignees = preview.Assignees
	un.Unresolved = preview.Unresolved
	return un
}
