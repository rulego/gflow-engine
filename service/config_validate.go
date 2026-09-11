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

	"github.com/rulego/rulego/api/types"
)

// nodeConfigValidator 节点配置校验器，由组件层经 SetNodeConfigValidator 注入，
// 避免 service→components 循环依赖（组件持有各节点配置的结构定义与校验规则）。
// nodeType 为节点类型，cfg 为节点 configuration，nodeIDs 为链内全部节点 ID 集
// （跨节点引用校验用，如 reject.target）；返回问题列表（空=通过）。
var (
	nodeConfigValidator   func(nodeType string, cfg map[string]interface{}, nodeIDs map[string]struct{}) []string
	nodeConfigValidatorMu sync.RWMutex
)

// SetNodeConfigValidator 注入节点配置校验器；传 nil 恢复跳过。并发安全。
func SetNodeConfigValidator(fn func(nodeType string, cfg map[string]interface{}, nodeIDs map[string]struct{}) []string) {
	nodeConfigValidatorMu.Lock()
	defer nodeConfigValidatorMu.Unlock()
	nodeConfigValidator = fn
}

func nodeConfigValidatorForRead() func(nodeType string, cfg map[string]interface{}, nodeIDs map[string]struct{}) []string {
	nodeConfigValidatorMu.RLock()
	defer nodeConfigValidatorMu.RUnlock()
	return nodeConfigValidator
}

// ValidateChainConfigurations 校验链内所有节点的 configuration，返回问题列表
// （空=通过）。链加载期 Init 错误只 warn，配置错误（未知取值、必填缺失、互斥组合、
// 跨节点引用悬空）必须在部署期拦截，否则带病落库、运行期才炸。
func ValidateChainConfigurations(chain *types.RuleChain) []string {
	if chain == nil {
		return nil
	}
	validator := nodeConfigValidatorForRead()
	if validator == nil {
		return nil
	}
	nodeIDs := make(map[string]struct{}, len(chain.Metadata.Nodes))
	for _, node := range chain.Metadata.Nodes {
		nodeIDs[node.Id] = struct{}{}
	}
	var issues []string
	for _, node := range chain.Metadata.Nodes {
		// configuration 为 nil 也必须过校验器：userTask 等审批节点空配置
		// 恰是最需要拦截的形态（运行期 no assignees），Map2Struct(nil) 零值可正常报错。
		cfg := map[string]interface{}(node.Configuration)
		for _, msg := range validator(node.Type, cfg, nodeIDs) {
			issues = append(issues, fmt.Sprintf("node %s(%s, type=%s): %s", node.Id, node.Name, node.Type, msg))
		}
	}
	return issues
}

// FormatConfigIssues 把配置问题列表拼成单行可读错误（用于 ErrValidation 消息）。
func FormatConfigIssues(issues []string) string {
	return strings.Join(issues, "; ")
}
