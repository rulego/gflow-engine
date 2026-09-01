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
	"github.com/rulego/rulego/api/types"
)

// StartTaskNodeType 发起人（开始）节点类型
const StartTaskNodeType = "startTask"

// 注册方式：与其他 BPM 节点一致，由 Register 显式注册（不在 init() 自动注册），
// 便于测试替换与依赖装配。

// StartTaskNodeConfiguration 发起人节点配置。
// 无配置字段：节点仅作流程起点标记，谁可发起当前不做限制（如需范围鉴权应在流程发起层实现）。
type StartTaskNodeConfiguration struct {
}

type StartTaskNode struct {
	Config StartTaskNodeConfiguration
}

func (x *StartTaskNode) Type() string {
	return StartTaskNodeType
}

// New 创建新实例
func (x *StartTaskNode) New() types.Node {
	return &StartTaskNode{}
}

func (x *StartTaskNode) Init(_ types.Config, _ types.Configuration) error {
	return nil
}

// OnMsg 处理消息
func (x *StartTaskNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	defer recoverNodePanic(ctx, msg, StartTaskNodeType, ctx.GetSelfId())
	ctx.TellSuccess(msg)
}

func (x *StartTaskNode) Destroy() {
}
