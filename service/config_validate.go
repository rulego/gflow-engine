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
	"fmt"
	"strings"
	"sync"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"
)

// ChainGraph 链级拓扑视图：节点集合、类型与双向邻接表。供跨节点引用类校验使用
// （reject.target 的存在性、是否上游节点、回退路径是否跨并行分支），由
// ValidateChainConfigurations 构建后随校验器下发。
type ChainGraph struct {
	NodeIDs     map[string]struct{}
	NodeTypes   map[string]string
	Forward     map[string][]string // nodeID -> 下游节点ID
	Backward    map[string][]string // nodeID -> 上游节点ID
	StartNodeID string              // 链首节点 ID（FirstNodeIndex 解析，toStarter 回退目标）
}

// HasNode 判断节点是否在链内。
func (g *ChainGraph) HasNode(id string) bool {
	if g == nil {
		return false
	}
	_, ok := g.NodeIDs[id]
	return ok
}

// UpstreamReachable 判断 target 是否为 from 的上游节点（沿连接边反向可达，
// from 自身也算）。
func (g *ChainGraph) UpstreamReachable(from, target string) bool {
	if g == nil || from == "" || target == "" {
		return false
	}
	visited := map[string]bool{from: true}
	queue := []string{from}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == target {
			return true
		}
		for _, prev := range g.Backward[cur] {
			if !visited[prev] {
				visited[prev] = true
				queue = append(queue, prev)
			}
		}
	}
	return false
}

// RollbackRegionForked 计算 target→self 的驳回重执行区域（target 正向可达 ∩
// self 反向可达，与运行期 rejectResetNodes 同口径），区域内含并行网关时返回
// true：重入 fork/inclusive 会向各分支重复派发任务，重入 join 会因兄弟分支的
// 消息不再到来而永久等待。
func (g *ChainGraph) RollbackRegionForked(target, self string) bool {
	if g == nil {
		return false
	}
	forwardSeen := map[string]bool{target: true}
	queue := []string{target}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range g.Forward[cur] {
			if !forwardSeen[next] {
				forwardSeen[next] = true
				queue = append(queue, next)
			}
		}
	}
	backwardSeen := map[string]bool{self: true}
	queue = []string{self}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, prev := range g.Backward[cur] {
			if !backwardSeen[prev] {
				backwardSeen[prev] = true
				queue = append(queue, prev)
			}
		}
	}
	for id := range forwardSeen {
		if backwardSeen[id] {
			switch g.NodeTypes[id] {
			case "fork", "join", constants.NodeTypeInclusive:
				return true
			}
		}
	}
	return false
}

// nodeConfigValidator 节点配置校验器，由组件层经 SetNodeConfigValidator 注入，
// 避免 service→components 循环依赖（组件持有各节点配置的结构定义与校验规则）。
// nodeType/nodeID 为节点类型与 ID，cfg 为节点 configuration，graph 为链级拓扑
// （跨节点引用校验用）；返回问题列表（空=通过）。
var (
	nodeConfigValidator   func(nodeType, nodeID string, cfg map[string]interface{}, graph *ChainGraph) []string
	nodeConfigValidatorMu sync.RWMutex
)

// SetNodeConfigValidator 注入节点配置校验器；传 nil 恢复跳过。并发安全。
func SetNodeConfigValidator(fn func(nodeType, nodeID string, cfg map[string]interface{}, graph *ChainGraph) []string) {
	nodeConfigValidatorMu.Lock()
	defer nodeConfigValidatorMu.Unlock()
	nodeConfigValidator = fn
}

func nodeConfigValidatorForRead() func(nodeType, nodeID string, cfg map[string]interface{}, graph *ChainGraph) []string {
	nodeConfigValidatorMu.RLock()
	defer nodeConfigValidatorMu.RUnlock()
	return nodeConfigValidator
}

// ValidateChainConfigurations 校验链内所有节点的 configuration，返回问题列表
// （空=通过）。链加载期 Init 错误只 warn，配置错误（未知取值、必填缺失、互斥组合、
// 跨节点引用悬空）必须在部署期拦截，否则带病落库、运行期才炸。
// 拓扑环检测不依赖节点配置校验器注入：纯环（每节点都有出边）到不了 end 节点，
// 部署后运行期无限派发，userTask 同样不豁免。
func ValidateChainConfigurations(chain *types.RuleChain) []string {
	if chain == nil {
		return nil
	}
	graph := buildChainGraph(chain)
	var issues []string
	if cycle := detectChainCycle(graph); len(cycle) > 0 {
		issues = append(issues, fmt.Sprintf("cyclic chain detected: %s", strings.Join(cycle, " -> ")))
	}
	validator := nodeConfigValidatorForRead()
	if validator == nil {
		return issues
	}
	for _, node := range chain.Metadata.Nodes {
		// configuration 为 nil 也必须过校验器：userTask 等审批节点空配置
		// 恰是最需要拦截的形态（运行期 no assignees），Map2Struct(nil) 零值可正常报错。
		cfg := map[string]interface{}(node.Configuration)
		for _, msg := range validator(node.Type, node.Id, cfg, graph) {
			issues = append(issues, fmt.Sprintf("node %s(%s, type=%s): %s", node.Id, node.Name, node.Type, msg))
		}
	}
	return issues
}

// detectChainCycle DFS 三色标记检测有向环，返回环上节点序列（首尾相接），
// 无环返回 nil。环意味着部分节点到不了 end，部署期必须拒绝。
func detectChainCycle(g *ChainGraph) []string {
	if g == nil {
		return nil
	}
	const (
		white = 0 // 未访问
		gray  = 1 // 在当前 DFS 栈上
		black = 2 // 已完成
	)
	color := make(map[string]int, len(g.NodeIDs))
	stack := make([]string, 0, len(g.NodeIDs))
	var cycle []string
	var dfs func(id string) bool
	dfs = func(id string) bool {
		color[id] = gray
		stack = append(stack, id)
		for _, next := range g.Forward[id] {
			if _, ok := g.NodeIDs[next]; !ok {
				continue // 悬空边的存在性由跨节点引用校验负责
			}
			switch color[next] {
			case gray:
				// 回边命中：从栈中 next 的位置截出整条环
				for i, nid := range stack {
					if nid == next {
						cycle = append(append([]string(nil), stack[i:]...), next)
						return true
					}
				}
				return true
			case white:
				if dfs(next) {
					return true
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = black
		return false
	}
	for id := range g.NodeIDs {
		if color[id] == white && dfs(id) {
			return cycle
		}
	}
	return nil
}

// buildChainGraph 把规则链定义转成拓扑视图。
func buildChainGraph(chain *types.RuleChain) *ChainGraph {
	g := &ChainGraph{
		NodeIDs:   make(map[string]struct{}, len(chain.Metadata.Nodes)),
		NodeTypes: make(map[string]string, len(chain.Metadata.Nodes)),
		Forward:   make(map[string][]string),
		Backward:  make(map[string][]string),
	}
	for _, node := range chain.Metadata.Nodes {
		g.NodeIDs[node.Id] = struct{}{}
		g.NodeTypes[node.Id] = node.Type
	}
	for _, conn := range chain.Metadata.Connections {
		g.Forward[conn.FromId] = append(g.Forward[conn.FromId], conn.ToId)
		g.Backward[conn.ToId] = append(g.Backward[conn.ToId], conn.FromId)
	}
	if idx := chain.Metadata.FirstNodeIndex; idx >= 0 && idx < len(chain.Metadata.Nodes) {
		g.StartNodeID = chain.Metadata.Nodes[idx].Id
	}
	return g
}

// FormatConfigIssues 把配置问题列表拼成单行可读错误（用于 ErrValidation 消息）。
func FormatConfigIssues(issues []string) string {
	return strings.Join(issues, "; ")
}
