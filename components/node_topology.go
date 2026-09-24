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

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"
)

// getRuleChainDefinition 取当前 rule chain 的 Definition（包含节点和连接）
func getRuleChainDefinition(ctx types.RuleContext) *types.RuleChain {
	ruleChainNode := ctx.RuleChain()
	if ruleChainNode == nil {
		return nil
	}
	chainCtx, ok := ruleChainNode.(types.ChainCtx)
	if !ok {
		return nil
	}
	return chainCtx.Definition()
}

// getStartNodeID 取 rule chain 的开始节点 ID（Metadata.FirstNodeIndex 索引对应的节点）
func getStartNodeID(ctx types.RuleContext) string {
	def := getRuleChainDefinition(ctx)
	if def == nil {
		return ""
	}
	idx := def.Metadata.FirstNodeIndex
	if idx < 0 || idx >= len(def.Metadata.Nodes) {
		// 兜底：取第一个节点
		if len(def.Metadata.Nodes) > 0 {
			return def.Metadata.Nodes[0].Id
		}
		return ""
	}
	return def.Metadata.Nodes[idx].Id
}

// terminateInstance 驳回级联终止流程实例的统一实现（userTask/aiAgent 共用）：
//   - RuntimeService 未注入时降级 TellFailure，避免消息静默丢失
//   - 终止前标记事件来源为驳回级联（EventSourceReject），terminated 事件派发时写入 Source
//   - 终止成功后用空 relationType 收尾当前节点：实例已被标记 terminated，
//     不能再路由 Failure，否则下游 Failure 分支会重复触发终止
func terminateInstance(rs service.RuntimeService, nodeID string, ctx types.RuleContext, msg types.RuleMsg, instanceID, reason string) {
	if rs == nil {
		logrus.Errorf("node %s: RuntimeService not injected, cannot terminate instance %s; falling back to TellFailure", nodeID, instanceID)
		ctx.TellFailure(msg, fmt.Errorf("reject: runtime service unavailable"))
		return
	}
	termCtx := service.WithInternalCallingMode(service.WithEventSource(ctx.GetContext(), service.EventSourceReject))
	// 显式 actor：驳回级联终止归属实际驳回人（ctx 身份）；ctx 无身份时按系统动作处理，
	// 并从链元数据补齐租户（TerminateProcessInstance 的租户校验依赖 TenantID）；
	// WithInternalCallingMode 标记为引擎内部级联，跳过实例属主校验（驳回人是对该实例
	// 有审批权的参与人而非发起人，终止是回路语义驱动的副作用，不是越权终止他人实例）。
	actor := service.ActorFromCtx(termCtx)
	if actor.TenantID == "" {
		actor.TenantID = metaValue(msg, constants.KeyTenantID)
	}
	// rulego 根 ctx 不携带调用链身份，操作人与代审被代人从链元数据回读
	// （executeNextLocked 写入），terminated 事件的 FromUser/OnBehalfOf 才不缺失
	if actor.UserID == "" || actor.UserID == constants.UserSystem {
		if op := metaValue(msg, constants.KeyOperator); op != "" {
			actor.UserID = op
			termCtx = service.SetUserToCtx(termCtx, &actor)
		}
	}
	if ob := metaValue(msg, constants.KeyOnBehalfOf); ob != "" {
		termCtx = service.WithOnBehalfOf(termCtx, ob)
	}
	if err := rs.TerminateProcessInstance(termCtx, actor, instanceID, reason); err != nil {
		logrus.Errorf("node %s: terminate instance %s failed: %v", nodeID, instanceID, err)
		ctx.TellFailure(msg, err)
		return
	}
	ctx.DoOnEnd(msg, nil, "")
}
