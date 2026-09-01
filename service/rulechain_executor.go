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
	"time"

	"github.com/rulego/rulego/api/types"
)

// RuleChainExecutor 规则链执行器接口
// 由上层应用（gflow）注入实现，nil 则不触发
type RuleChainExecutor interface {
	// Execute 触发规则链（fire-and-forget，底层非阻塞）
	Execute(chainId string, msg types.RuleMsg) error
	// ExecuteAsync 异步执行规则链（不阻塞调用方）
	ExecuteAsync(chainId string, msg types.RuleMsg)
	// ExecuteAndCollect 同步执行并返回链的最终输出消息，超时返回错误。
	// 供需要拿到执行结果（如 AI 智能体输出）的调用方使用。
	ExecuteAndCollect(chainId string, msg types.RuleMsg, timeout time.Duration) (types.RuleMsg, error)
}
