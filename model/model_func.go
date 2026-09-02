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

package model

import (
	"encoding/json"
	"fmt"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"
)

// processDefinitionEnvelope 是前端流程设计器保存时输出的
// DefinitionJSON 包裹结构，形如：
//
//	{
//	  "form": {...},
//	  "flow": {...},
//	  "ruleChain": {"id":..., "name":..., "root":..., "debugMode":..., "additionalInfo":...},
//	  "metadata": {"firstNodeIndex":0, "nodes":[...], "connections":[...]}
//	}
//
// 注意：envelope 中的 "ruleChain" 字段值是 RuleChainBaseInfo（id/name/root 等），
// 不是嵌套的完整 types.RuleChain。本结构把它解构后重新组装成 types.RuleChain。
type processDefinitionEnvelope struct {
	Form      json.RawMessage          `json:"form,omitempty"`
	Flow      json.RawMessage          `json:"flow,omitempty"`
	RuleChain *types.RuleChainBaseInfo `json:"ruleChain,omitempty"`
	Metadata  *types.RuleMetadata      `json:"metadata,omitempty"`
}

// ToRuleChain 将流程定义 JSON 转换为 rulego 规则链
// 支持两种 DefinitionJSON 格式：
//  1. 设计器 envelope（顶层 form/flow/ruleChain/metadata 四键）
//  2. 扁平 types.RuleChain（顶层有 ruleChain + metadata 键，与 envelope 无法区分时按此格式解析）
func (p *WfProcess) ToRuleChain() (*types.RuleChain, error) {
	raw := []byte(p.DefinitionJSON)
	if len(raw) == 0 {
		return nil, fmt.Errorf("definition json is empty")
	}

	// 先尝试 envelope 解析
	var env processDefinitionEnvelope
	if err := json.Unmarshal(raw, &env); err == nil {
		// 检测到 envelope：env.RuleChain 或 env.Metadata 至少有一个非空
		if env.RuleChain != nil || env.Metadata != nil {
			chain := &types.RuleChain{}
			if env.RuleChain != nil {
				chain.RuleChain = *env.RuleChain
			}
			if env.Metadata != nil {
				chain.Metadata = *env.Metadata
			}
			// 把 form/flow 等设计器扩展信息塞进 AdditionalInfo，便于后续读取
			if chain.RuleChain.AdditionalInfo == nil {
				chain.RuleChain.AdditionalInfo = map[string]interface{}{}
			}
			if len(env.Form) > 0 {
				var formVal interface{}
				if json.Unmarshal(env.Form, &formVal) == nil {
					chain.RuleChain.AdditionalInfo["form"] = formVal
				}
			}
			if len(env.Flow) > 0 {
				var flowVal interface{}
				if json.Unmarshal(env.Flow, &flowVal) == nil {
					chain.RuleChain.AdditionalInfo["flow"] = flowVal
				}
			}
			return chain, nil
		}
	}

	// 兜底：按扁平 types.RuleChain 解析（顶层有 ruleChain + metadata 字段）
	var ruleChain types.RuleChain
	err := json.Unmarshal(raw, &ruleChain)
	return &ruleChain, err
}

// GetVariablesAsMap 将任务的 variables JSON 解析为 map；未设置或解析失败时返回 nil。
func (t *WfTask) GetVariablesAsMap() map[string]interface{} {
	if t.Variables != nil && *t.Variables != "" {
		var variables map[string]interface{}
		if err := json.Unmarshal([]byte(*t.Variables), &variables); err != nil {
			return nil
		}
		return variables
	}
	return nil
}

// EnsureEndNode 为缺少 end 节点的 DSL 自动补一个 end 节点，并把所有"无出边"的
// 非 end 节点连到它。
//
// 动机：引擎只在 end 节点触发 CompleteProcessInstance（create_task_aspect.go 的
// After 钩子判断 NodeTypeEnd）。DSL 若缺少 end 节点（如外部导入或手工编排的 DSL），
// 最后一个任务节点执行完链就"静默走完"，实例永远停在 active（无任务可办、无终点）。
//
// 本方法在部署/创建时对 DefinitionJSON 做一次性补全：
//  1. 全图无任何 type=="end" 节点时，追加 {id:"node_end", type:"end", name:"结束"}；
//  2. 找出所有未出现在任何 connection.fromId 中的非 end 节点（"悬垂尾节点"），
//     各补一条 {from→end, type:"Success"} 连接——树形模型的链尾/分支尾天然无出边。
//
// 已有 end 节点的 DSL 不动（尊重手工编排）。解析失败不阻断（返回原样）。
// 与 EnsureSwitchDefaultEdges 的顺序：先本方法后它——非穷尽网关的 Default 兜底边
// 才能找到新追加的 end 节点。
func (p *WfProcess) EnsureEndNode() {
	if p.DefinitionJSON == "" {
		return
	}
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(p.DefinitionJSON), &doc); err != nil {
		return
	}
	meta, ok := doc["metadata"].(map[string]interface{})
	if !ok {
		return
	}
	nodes, ok := meta["nodes"].([]interface{})
	if !ok || len(nodes) == 0 {
		return
	}
	conns, _ := meta["connections"].([]interface{})

	hasSource := map[string]bool{} // 出现在某条 connection.fromId 的节点
	hasEnd := false
	for _, n := range nodes {
		if m, ok := n.(map[string]interface{}); ok {
			if t, _ := m["type"].(string); t == constants.NodeTypeEnd {
				hasEnd = true
			}
		}
	}
	for _, c := range conns {
		if m, ok := c.(map[string]interface{}); ok {
			if from, _ := m["fromId"].(string); from != "" {
				hasSource[from] = true
			}
		}
	}
	if hasEnd {
		return
	}

	const endNodeID = "node_end"
	endNode := map[string]interface{}{
		"id":   endNodeID,
		"type": constants.NodeTypeEnd,
		"name": "结束",
	}
	// 追加 end 节点；同 id 已被其它类型节点占用时换后缀避免冲突
	for i := 0; ; i++ {
		id := endNodeID
		if i > 0 {
			id = fmt.Sprintf("%s_%d", endNodeID, i)
		}
		endNode["id"] = id
		dup := false
		for _, n := range nodes {
			if m, ok := n.(map[string]interface{}); ok {
				if nid, _ := m["id"].(string); nid == id {
					dup = true
					break
				}
			}
		}
		if !dup {
			break
		}
	}
	nodes = append(nodes, endNode)

	// 所有"无出边"的非 end 节点连到 end
	for _, n := range nodes {
		m, ok := n.(map[string]interface{})
		if !ok {
			continue
		}
		id, _ := m["id"].(string)
		if id == "" || id == endNode["id"] || hasSource[id] {
			continue
		}
		conns = append(conns, map[string]interface{}{
			"fromId": id, "toId": endNode["id"], "type": "Success",
		})
	}

	meta["nodes"] = nodes
	meta["connections"] = conns
	if b, err := json.Marshal(doc); err == nil {
		p.DefinitionJSON = string(b)
	}
}

// MigrateRouteGateway 把遗留 routeGateway 节点迁移为 switch（部署/装载期兜底，
// 就地改写 DefinitionJSON）。
//
// routeGateway 从未在引擎注册，含它的 DSL 装载必失败；且旧序列化把 cases.then
// 写成分支标题、从不匹配任何连接 type——运行时流量本来就 100% 走唯一的 Success
// 后继。迁移保持该行为等价：
//   - 节点 type: routeGateway → switch
//   - configuration 清空（routeList/cases 全部丢弃；分支条件是摆设，见上）
//   - 该节点的 Success 出边改为 Default 出边（无 case 命中 → Default → 原后继）
//
// 设计器侧重存会经前端 migrateRouteGatewayToSwitch 生成完整可编辑分支；本方法
// 兜底从未重新打开保存的存量定义。幂等；解析失败不阻断（返回原样）。
// 与 EnsureEndNode/EnsureSwitchDefaultEdges 的顺序：先本方法——迁移后的 Default
// 出边已存在，EnsureSwitchDefaultEdges 不会再补 Default→end。
func (p *WfProcess) MigrateRouteGateway() {
	if p.DefinitionJSON == "" {
		return
	}
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(p.DefinitionJSON), &doc); err != nil {
		return
	}
	meta, ok := doc["metadata"].(map[string]interface{})
	if !ok {
		return
	}
	nodes, ok := meta["nodes"].([]interface{})
	if !ok {
		return
	}
	conns, _ := meta["connections"].([]interface{})

	routeIds := map[string]bool{}
	for _, n := range nodes {
		m, ok := n.(map[string]interface{})
		if !ok {
			continue
		}
		if t, _ := m["type"].(string); t != constants.NodeTypeRouteGateway {
			continue
		}
		id, _ := m["id"].(string)
		if id == "" {
			continue
		}
		routeIds[id] = true
		m["type"] = constants.NodeTypeSwitch
		m["configuration"] = map[string]interface{}{}
	}
	if len(routeIds) == 0 {
		return
	}
	for _, c := range conns {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if from, _ := m["fromId"].(string); routeIds[from] {
			m["type"] = "Default"
		}
	}
	if b, err := json.Marshal(doc); err == nil {
		p.DefinitionJSON = string(b)
	}
}

// rulego 的 switch/inclusive 节点无 case 匹配时都会发 Default 关系(switch_node.go/inclusive_node.go)，
// 若 DSL 未布 Default 出边，TellNext 找不到目标→DoOnEnd 静默结束分支→实例停留在 active
// 且无任何待办任务（运行期无法恢复，必须在部署期兜底）。
//
// 本方法在部署/创建时对 DefinitionJSON 做一次性补全：每个 switch/jsSwitch/msgTypeSwitch/inclusive 若无
// Default 出边，补一条 {from→end, type:"Default"}(BPMN default-flow 语义：无 case 命中走 default→完成)。
// 已有 Default 边不动；找不到 end 节点则跳过。解析失败不阻断(返回原样)。
func (p *WfProcess) EnsureSwitchDefaultEdges() {
	if p.DefinitionJSON == "" {
		return
	}
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(p.DefinitionJSON), &doc); err != nil {
		return
	}
	meta, ok := doc["metadata"].(map[string]interface{})
	if !ok {
		return
	}
	nodes, ok := meta["nodes"].([]interface{})
	if !ok {
		return
	}
	conns, _ := meta["connections"].([]interface{})

	var switchIds, endIds []string
	haveDefault := map[string]bool{}
	for _, n := range nodes {
		m, ok := n.(map[string]interface{})
		if !ok {
			continue
		}
		id, _ := m["id"].(string)
		typ, _ := m["type"].(string)
		switch typ {
		case constants.NodeTypeSwitch, constants.NodeTypeJsSwitch, constants.NodeTypeMsgTypeSwitch, constants.NodeTypeInclusive:
			switchIds = append(switchIds, id)
		}
		if typ == constants.NodeTypeEnd {
			endIds = append(endIds, id)
		}
	}
	for _, c := range conns {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if t, _ := m["type"].(string); t == "Default" {
			if from, _ := m["fromId"].(string); from != "" {
				haveDefault[from] = true
			}
		}
	}
	if len(switchIds) == 0 || len(endIds) == 0 {
		return
	}
	target := endIds[len(endIds)-1]
	changed := false
	for _, sid := range switchIds {
		if sid == "" || haveDefault[sid] {
			continue
		}
		conns = append(conns, map[string]interface{}{
			"fromId": sid, "toId": target, "type": "Default",
		})
		changed = true
	}
	if !changed {
		return
	}
	meta["connections"] = conns
	if b, err := json.Marshal(doc); err == nil {
		p.DefinitionJSON = string(b)
	}
}
