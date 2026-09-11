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
	"net/url"
	"strings"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
)

// ValidateNodeConfiguration 按节点类型校验 configuration，返回问题列表（空=通过）。
// nodeID 为当前节点 ID，graph 为链级拓扑，供跨节点引用校验（reject.target 的
// 存在性、是否上游节点、回退路径是否跨并行分支）。
// 供部署期校验入口（service.ValidateChainConfigurations）调用，把配置错误拦在写入前。
func ValidateNodeConfiguration(nodeType, nodeID string, cfg map[string]interface{}, graph *service.ChainGraph) []string {
	switch nodeType {
	case constants.NodeTypeUserTask:
		return validateUserTaskNodeConfig(cfg, nodeID, graph)
	case CCTaskNodeType:
		return validateCCTaskConfig(cfg)
	case AIAgentNodeType:
		return validateAIAgentConfig(cfg)
	case HttpCallNodeType:
		return validateHTTPCallConfig(cfg)
	case StartProcessNodeType:
		return validateStartProcessConfig(cfg)
	case "subProcess", "automation":
		return validateTargetConfig(cfg)
	}
	return nil
}

// validateCCTaskConfig 校验抄送名单非空且模板项可编译：空名单节点必然空跑。
func validateCCTaskConfig(cfg map[string]interface{}) []string {
	var c CCTaskNodeConfiguration
	if err := maps.Map2Struct(cfg, &c); err != nil {
		return []string{"configuration parse: " + err.Error()}
	}
	if len(c.CCUserIds) == 0 {
		return []string{"ccUserIds is empty"}
	}
	for _, item := range c.CCUserIds {
		if strings.TrimSpace(item) == "" {
			return []string{"ccUserIds contains empty item"}
		}
		if _, err := el.NewTemplate(item); err != nil {
			return []string{fmt.Sprintf("ccUserIds item %q compile: %v", item, err)}
		}
	}
	return nil
}

// validateAIAgentConfig 校验智能体节点的必填项与裁决取值。
func validateAIAgentConfig(cfg map[string]interface{}) []string {
	var c AIAgentNodeConfiguration
	if err := maps.Map2Struct(cfg, &c); err != nil {
		return []string{"configuration parse: " + err.Error()}
	}
	var issues []string
	if strings.TrimSpace(c.AgentID) == "" {
		issues = append(issues, "agentId is empty")
	}
	if c.TimeoutSec < 0 {
		issues = append(issues, "timeoutSec must not be negative")
	}
	if c.Decision != nil {
		if !isValidRejectStrategy(c.Decision.RejectStrategy) {
			issues = append(issues, fmt.Sprintf("decision.rejectStrategy %q is not supported", c.Decision.RejectStrategy))
		}
		if !isValidAIAgentUnresolved(c.Decision.Unresolved) {
			issues = append(issues, fmt.Sprintf("decision.unresolved %q is not supported", c.Decision.Unresolved))
		}
	}
	return issues
}

// validateHTTPCallConfig 校验 URL 非空且为 http/https。
func validateHTTPCallConfig(cfg map[string]interface{}) []string {
	var c HttpCallNodeConfiguration
	if err := maps.Map2Struct(cfg, &c); err != nil {
		return []string{"configuration parse: " + err.Error()}
	}
	if strings.TrimSpace(c.Url) == "" {
		return []string{"url is empty"}
	}
	if u, err := url.Parse(c.Url); err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return []string{"url scheme must be http/https"}
	}
	return nil
}

// validateStartProcessConfig 校验发起审批节点的必填项。
func validateStartProcessConfig(cfg map[string]interface{}) []string {
	var c StartProcessNodeConfig
	if err := maps.Map2Struct(cfg, &c); err != nil {
		return []string{"configuration parse: " + err.Error()}
	}
	var issues []string
	if strings.TrimSpace(c.ProcessKey) == "" {
		issues = append(issues, "processKey is empty")
	}
	if strings.TrimSpace(c.Initiator) == "" {
		issues = append(issues, "initiator is empty")
	}
	return issues
}

// validateTargetConfig 校验以 targetId 指向目标链的节点（subProcess/automation）。
func validateTargetConfig(cfg map[string]interface{}) []string {
	target, _ := cfg["targetId"].(string)
	if strings.TrimSpace(target) == "" {
		return []string{"targetId is empty"}
	}
	return nil
}
