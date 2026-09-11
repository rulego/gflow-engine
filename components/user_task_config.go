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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
)

// ApproverConfig 审批人配置，按 Type 消费对应字段。
type ApproverConfig struct {
	// Type 审批人类型：user（指定成员）/role（指定角色，产生待认领任务须经身份服务展开）/
	// dept（指定部门，产生待认领任务）/manager（直接上级，配合 Levels 取第 N 级）/
	// initiatorSelect（发起人自选，Expression 为审批人表达式模板）/
	// initiatorSelf（发起人自己）/multiLevelManager（多级上级，Levels<0 表示直到最上层）
	Type       string   `json:"type"`
	UserIds    []string `json:"userIds"`
	RoleIds    []string `json:"roleIds"`
	DeptIds    []string `json:"deptIds"`
	Levels     int      `json:"levels"`
	Expression string   `json:"expression"`
}

// VoteRule 票签通过阈值（approveMode=vote 专用）。
type VoteRule struct {
	// Type 阈值类型：majority（过半）/percent（按百分比）/count（固定票数）
	Type string `json:"type"`
	// Value percent 取 0~100；count 为票数；majority 不消费
	Value float64 `json:"value"`
}

// RejectConfig 驳回配置。
type RejectConfig struct {
	// Strategy 驳回策略：terminate（终止实例，缺省）/toStarter（跳回开始节点）/
	// toPrev（跳到上一个 userTask 节点）/toNode（跳到 Target 指定节点，
	// Target 必须是链中存在的节点 ID，部署期校验）。
	// 运行期跳转失败时按节点 Reject/Failure 出边兜底，均无则终止。
	Strategy string `json:"strategy"`
	// Target toNode 策略的目标节点 ID
	Target string `json:"target"`
}

// Normalize 归一化 userTask 配置：空缺省值落定。枚举值均为大小写敏感的
// 精确取值（camelCase），这里不做大小写改写。
func (c *UserTaskNodeConfiguration) Normalize() {
	c.ApproveMode = strings.TrimSpace(c.ApproveMode)
	if c.ApproveMode == "" {
		c.ApproveMode = string(enums.ApprovalTypeSingle)
	}
	c.Approver.Type = strings.TrimSpace(c.Approver.Type)
	c.SelfApproval = strings.TrimSpace(c.SelfApproval)
	if c.SelfApproval == "" {
		c.SelfApproval = string(enums.SelfApprovalTypeNone)
	}
	c.Reject.Strategy = strings.TrimSpace(c.Reject.Strategy)
	if c.Reject.Strategy == "" {
		c.Reject.Strategy = RejectStrategyTerminate
	}
	// vote 未配阈值时按过半处理
	if enums.ApprovalType(c.ApproveMode) == enums.ApprovalTypeVote && c.VoteRule == nil {
		c.VoteRule = &VoteRule{Type: string(enums.CountersignTypeMajority)}
	}
	// 主管类层级缺省取第 1 级；multiLevelManager 的负值（审到最上层）保留
	if (c.Approver.Type == string(enums.CandidateTypeDirectManager) ||
		c.Approver.Type == string(enums.CandidateTypeMultiLevelManager)) && c.Approver.Levels == 0 {
		c.Approver.Levels = 1
	}
}

// approvalRuleJSON 生成落库到 wf_task.approval_rule 的规则串：
// 票签为 voteRule 的 JSON（复用会签阈值判定），其余模式为空（service 层按各模式缺省判定）。
func (c *UserTaskNodeConfiguration) approvalRuleJSON() string {
	if enums.ApprovalType(c.ApproveMode) != enums.ApprovalTypeVote || c.VoteRule == nil {
		return ""
	}
	b, err := json.Marshal(c.VoteRule)
	if err != nil {
		return ""
	}
	return string(b)
}

// Validate 校验 userTask 配置，返回问题列表（空=通过）。部署期调用，
// 把配置错误拦在写入前而不是运行期。
func (c *UserTaskNodeConfiguration) Validate() []string {
	var issues []string
	add := func(format string, args ...interface{}) {
		issues = append(issues, fmt.Sprintf(format, args...))
	}

	if !enums.IsValidCandidateType(enums.CandidateType(c.Approver.Type)) {
		add("approver.type %q is not supported", c.Approver.Type)
	} else {
		switch enums.CandidateType(c.Approver.Type) {
		case enums.CandidateTypeUser:
			if len(c.Approver.UserIds) == 0 {
				add("approver.userIds is empty")
			}
		case enums.CandidateTypeRole:
			if len(c.Approver.RoleIds) == 0 {
				add("approver.roleIds is empty")
			}
		case enums.CandidateTypeDept:
			if len(c.Approver.DeptIds) == 0 {
				add("approver.deptIds is empty")
			}
		case enums.CandidateTypeInitiatorSelect:
			if strings.TrimSpace(c.Approver.Expression) == "" {
				add("approver.expression is empty")
			} else if _, err := el.NewTemplate(strings.TrimSpace(c.Approver.Expression)); err != nil {
				add("approver.expression compile: %v", err)
			}
		}
	}

	if !enums.IsValidUserTaskApprovalType(enums.ApprovalType(c.ApproveMode)) {
		add("approveMode %q is not supported", c.ApproveMode)
	}
	if c.VoteRule != nil {
		if enums.ApprovalType(c.ApproveMode) != enums.ApprovalTypeVote {
			add("voteRule only applies to approveMode=vote")
		} else {
			switch enums.CountersignType(c.VoteRule.Type) {
			case enums.CountersignTypeMajority:
			case enums.CountersignTypePercent:
				if c.VoteRule.Value < 0 || c.VoteRule.Value > 100 {
					add("voteRule.value %v out of range for percent", c.VoteRule.Value)
				}
			case enums.CountersignTypeCount:
				if c.VoteRule.Value < 1 {
					add("voteRule.value %v out of range for count", c.VoteRule.Value)
				}
			default:
				add("voteRule.type %q is not supported", c.VoteRule.Type)
			}
		}
	}

	if !enums.IsValidSelfApprovalType(enums.SelfApprovalType(c.SelfApproval)) {
		add("selfApproval %q is not supported", c.SelfApproval)
	} else if enums.CandidateType(c.Approver.Type) == enums.CandidateTypeInitiatorSelf &&
		enums.SelfApprovalType(c.SelfApproval) == enums.SelfApprovalTypeSkip {
		add("approver.type=initiatorSelf with selfApproval=skip leaves no assignee")
	}

	if c.Timeout != nil {
		if c.Timeout.DueInMinutes <= 0 {
			add("timeout.dueInMinutes must be positive")
		}
		switch c.Timeout.Action {
		case "", TimeoutActionRemind, TimeoutActionAutoApprove, TimeoutActionAutoReject:
		default:
			add("timeout.action %q is not supported", c.Timeout.Action)
		}
	}

	if !isValidUserTaskRejectStrategy(c.Reject.Strategy) {
		add("reject.strategy %q is not supported", c.Reject.Strategy)
	}
	if c.Reject.Strategy == RejectStrategyToNode && strings.TrimSpace(c.Reject.Target) == "" {
		add("reject.target is required for reject.strategy=toNode")
	}
	return issues
}

// validateUserTaskNodeConfig 供部署期校验入口调用的 map 形态包装。
// nodeID 与 graph 用于 reject.target 等跨节点引用校验（graph 为 nil 时仅做本节点校验）。
func validateUserTaskNodeConfig(cfg map[string]interface{}, nodeID string, graph *service.ChainGraph) []string {
	var c UserTaskNodeConfiguration
	if err := maps.Map2Struct(cfg, &c); err != nil {
		return []string{"configuration parse: " + err.Error()}
	}
	c.Normalize()
	issues := c.Validate()
	if c.Reject.Strategy == RejectStrategyToNode {
		if target := strings.TrimSpace(c.Reject.Target); target != "" && graph != nil {
			if !graph.HasNode(target) {
				issues = append(issues, fmt.Sprintf("reject.target %q not found in chain nodes", c.Reject.Target))
			} else if !graph.UpstreamReachable(nodeID, target) {
				// 回退目标必须是已走过的上游节点：指向下游会跳过未审节点，
				// 指向并行分支则与本节点没有先后关系。
				issues = append(issues, fmt.Sprintf("reject.target %q is not an upstream node of %q; rollback target must be a passed node", target, nodeID))
			} else if graph.RollbackRegionForked(target, nodeID) {
				// 回退路径穿过 fork 时各分支会被重复派发任务，穿过 join 时
				// 兄弟分支的消息不再到来、汇合点永久等待。
				issues = append(issues, fmt.Sprintf("rollback path from %q to %q crosses fork/join; cross-branch rollback is not supported", target, nodeID))
			}
		}
	}
	return issues
}
