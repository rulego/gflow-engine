//go:build !gflow_no_rulego_ai

// rulego_ai_node.go 注册 rulego-components-ai 的 ai/agent 节点:
// AIAgentNode 通过 ctx.TellFlow 调用智能体规则链,智能体定义本身是含
// ai/agent 节点的子规则链,引擎必须能识别该节点类型。
// components-ai 依赖 eino/sonic,32 位平台编译期硬错误。32 位宿主(如边缘
// 网关 armv7)以 -tags gflow_no_rulego_ai 排除本文件,并在宿主侧加一行
// import _ "github.com/rulego/rulego-components-ai/agent/lite"
// 补位注册同名 ai/agent(两实现同名,先注册者生效)。
package components

import _ "github.com/rulego/rulego-components-ai/agent"
