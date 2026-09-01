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

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/flow"
)

// SubProcessNodeType 子流程节点类型常量（取自 types/constants 的 DSL 节点类型组）。
const SubProcessNodeType = constants.NodeTypeSubProcess

// SubProcessNode 子流程节点：嵌套启动另一条 BPM 工作流（call activity 语义）。
//
// 与 automation 节点语义正交：
//   - "automation" ：非阻塞触发 rulego 规则链（fire-and-forget），无输出回流父流程。
//   - "subProcess" ：启动独立子 BPM 实例，子实例有自己的上下文/任务/生命周期；
//     父流程在 subProcess 节点挂起，子实例完成后回调恢复父流程（嵌套审批闭环）。
//
// 通过 targetId（子链 ruleChain.id）解析子流程定义并启动子实例。
// 变量传递：未显式传变量时，子实例继承父实例的全部流程变量；
// 子流程发起人沿用父实例发起人身份。
// 嵌入 flow.ChainNode 仅复用其 Init（解析 targetId/extend 配置）与 Config 结构，
// OnMsg 完全覆写，extend 等其余 ChainNode 配置项不生效。
type SubProcessNode struct {
	*flow.ChainNode
	RuntimeService service.RuntimeServiceInternal
}

func (x *SubProcessNode) Type() string {
	return SubProcessNodeType
}

func (x *SubProcessNode) New() types.Node {
	return &SubProcessNode{
		ChainNode:      &flow.ChainNode{},
		RuntimeService: x.RuntimeService, // 从注册原型传播到 New 出的实例
	}
}

// OnMsg 启动子流程实例（call activity）。状态机：
//   - 已有完成子实例（重入恢复路径）→ TellNext(Success)，父流程继续到下游。
//   - 已有活跃子实例 → 父挂起等待（不 TellNext）。
//   - 无子实例 → 启动子实例，父挂起。
//
// 子实例完成时由 RuntimeService.CompleteProcessInstance 回调
// ExecuteNext(父实例, 本节点ID) 重入本 OnMsg → 走"已完成"分支续跑。
func (x *SubProcessNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	defer recoverNodePanic(ctx, msg, SubProcessNodeType, ctx.GetSelfId())
	if x.RuntimeService == nil {
		ctx.TellFailure(msg, fmt.Errorf("subProcess node RuntimeService not injected"))
		return
	}
	tenantID := metaValue(msg, constants.KeyTenantID)
	childProcID, ok := x.RuntimeService.ResolveSubProcessTarget(tenantID, x.Config.TargetId)
	if !ok {
		ctx.TellFailure(msg, fmt.Errorf("subProcess target %q not registered (deploy the child process first)", x.Config.TargetId))
		return
	}
	parentInstID := metaValue(msg, constants.KeyInstanceID)
	terminated, err := x.RuntimeService.SubProcessChildTerminated(ctx.GetContext(), parentInstID)
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("subProcess child terminated state unknown: %w", err))
		return
	}
	if terminated {
		ctx.TellFailure(msg, fmt.Errorf("subProcess child terminated")) // 子被终止 → 父走 Failure 边
		return
	}
	active, completed, err := x.RuntimeService.SubProcessChildState(ctx.GetContext(), parentInstID)
	if err != nil {
		// 状态未知时不能盲目启动子实例（可能造成重复子流程），按失败处置
		ctx.TellFailure(msg, fmt.Errorf("subProcess child state unknown: %w", err))
		return
	}
	if completed {
		ctx.TellNext(msg, types.Success) // 子已完成（重入）→ 父继续
		return
	}
	if active {
		return // 子在跑 → 父挂起等待
	}
	// 启动子实例；父挂起（不 TellNext，等子完成回调恢复）
	if _, err := x.RuntimeService.StartSubProcessInstance(ctx.GetContext(), parentInstID, ctx.GetSelfId(), childProcID, nil); err != nil {
		ctx.TellFailure(msg, fmt.Errorf("start subProcess instance failed: %w", err))
	}
}
