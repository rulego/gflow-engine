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
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/action"
)

// ServiceTaskNodeType 服务任务节点类型（取自 types/constants 的 DSL 节点类型组）
const ServiceTaskNodeType = constants.NodeTypeServiceTask

// ServiceTaskNode 服务任务节点。
//
// 通过嵌入 action.FunctionsNode 复用上游全部能力（配置解析、${metadata.key}
// 动态函数名解析、param 传参、路由）。仅覆盖 Type() 注册为 "serviceTask"，
// 并覆盖 New() 把默认 functionName 置空。
//
// 注册方式：不在 init() 自动注册，统一由 Register 显式注册
// （与 userTask/ccTask/aiAgent 一致），便于测试替换与依赖装配。
type ServiceTaskNode struct {
	action.FunctionsNode
	// retryCount 瞬时故障自动重试预算：Init 从 configuration 提取
	// （缺省 2、显式 0=关闭、上限 5）；函数注册方须自证幂等（无幂等键机制）
	retryCount int
}

// Type 返回组件类型，与设计器 NODE_TYPES.SERVICE_TASK 对齐
func (s *ServiceTaskNode) Type() string {
	return ServiceTaskNodeType
}

// New 创建新实例。
//
// 覆写上游默认：FunctionsNode.New() 会把 FunctionName 默认设为 "test"，
// 导致未配置的 serviceTask 节点静默去调用一个名为 "test" 的函数（或报找不到）。
// 这里置空，未配置即明确失败，便于在部署期暴露配置缺失。
func (s *ServiceTaskNode) New() types.Node {
	return &ServiceTaskNode{}
}

// Init 初始化：沿用上游配置解析，另提取 retryCount（FunctionsNodeConfiguration
// 无此字段，map 直读）。
func (s *ServiceTaskNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	if err := s.FunctionsNode.Init(ruleConfig, configuration); err != nil {
		return err
	}
	s.retryCount = retryCountFromConfig(configuration, constants.ServiceTaskDefaultRetryCount)
	return nil
}

// OnMsg 包装上游 FunctionsNode.OnMsg：
//   - panic 兜底：宿主注册的服务函数 panic 不能击穿引擎所在进程（rulego 引擎侧不 recover）
//   - 瞬时故障自动重试：包装 RuleContext 拦截 TellFailure，瞬时技术故障（超时/
//     连接拒绝/5xx）在预算内退避重试，业务失败直接终态；retryCount=0 走原路径
func (s *ServiceTaskNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	defer recoverNodePanic(ctx, msg, ServiceTaskNodeType, ctx.GetSelfId())
	if s.retryCount <= 0 {
		s.FunctionsNode.OnMsg(ctx, msg)
		return
	}
	st := &serviceRetryState{max: s.retryCount, real: ctx}
	st.invoke = func() {
		// 每次重试包一层新 wrapper（共享 st 的计数），TellSuccess 原样透传真实下游
		s.FunctionsNode.OnMsg(&retryServiceContext{RuleContext: ctx, st: st}, msg)
	}
	st.invoke()
}
