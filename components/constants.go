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

// 驳回策略取值。userTask 与 aiAgent 共享 terminate 语义；
// userTask 额外支持三种回跳策略，aiAgent 额外支持 backToInitiator。
const (
	// RejectStrategyTerminate 终止流程实例（默认策略）
	RejectStrategyTerminate = "terminate"
	// RejectStrategyRejectToStarter 跳回开始节点（userTask）
	RejectStrategyRejectToStarter = "rejectToStarter"
	// RejectStrategyRejectToPrev 跳到上一个 userTask 节点（userTask）
	RejectStrategyRejectToPrev = "rejectToPrev"
	// RejectStrategyRejectToNode 跳到 rejectTargetNode 指定的节点（userTask）
	RejectStrategyRejectToNode = "rejectToNode"
	// RejectStrategyBackToInitiator 退回发起人，即跳到开始节点（aiAgent）
	RejectStrategyBackToInitiator = "backToInitiator"
)

// isValidUserTaskRejectStrategy 判断 userTask 驳回策略是否已知取值（空串合法，等价默认 terminate）。
func isValidUserTaskRejectStrategy(s string) bool {
	switch s {
	case "", RejectStrategyTerminate, RejectStrategyRejectToStarter, RejectStrategyRejectToPrev, RejectStrategyRejectToNode:
		return true
	}
	return false
}

// isValidAIAgentRejectStrategy 判断 aiAgent 驳回策略是否已知取值（空串合法，等价默认 terminate）。
func isValidAIAgentRejectStrategy(s string) bool {
	switch s {
	case "", RejectStrategyTerminate, RejectStrategyBackToInitiator:
		return true
	}
	return false
}

// isValidAIAgentUnresolved 判断 aiAgent 未裁决策略是否已知取值（空串合法，等价默认 human）。
func isValidAIAgentUnresolved(s string) bool {
	switch s {
	case "", UnresolvedStrategyHuman, UnresolvedStrategyPass, UnresolvedStrategyReject:
		return true
	}
	return false
}

// 任务候选实体类型改用 types/enums EntityType*（wf_task_assignee.entity_type）。

// RelationReject 自定义出边关系：业务驳回（与系统错误 types.Failure 分离）。
const RelationReject = "Reject"

// 审计日志状态（aiAgent/automationCall 等节点的执行审计字段取值）。
const (
	// AuditStatusSuccess 执行成功
	AuditStatusSuccess = "success"
	// AuditStatusFailed 执行失败
	AuditStatusFailed = "failed"
)
