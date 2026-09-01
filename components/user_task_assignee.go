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
	"context"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
)

// resolveAssignees 解析审批人
//
// 支持指定审批人、候选用户、候选组的解析。
// 当 CandidateType 为 role/direct_manager/multi_level_manager/department 等类型时，
// 调用 IdentityService 查询真实的用户/角色/部门关系；IdentityService 未注入时
// 这些类型返回错误，由上层在构建期暴露配置缺失。
func (n *UserTaskNode) resolveAssignees(ctx context.Context, tenantID, owner string, variables map[string]interface{}) ([]string, error) {
	var assignees []string
	assigneeSet := make(map[string]bool)

	ct := enums.CandidateType(strings.ToLower(strings.TrimSpace(n.Config.CandidateType)))
	switch ct {
	case enums.CandidateTypeUser:
		for _, uid := range toStringSlice(n.Config.CandidateConfig["userIds"]) {
			assignees = addUnique(assignees, assigneeSet, uid)
		}
	case enums.CandidateTypeRole:
		// IdentityService 未注入时返回错误，构建期暴露配置缺失
		if n.IdentityService == nil {
			return nil, fmt.Errorf("identity service not configured for role-based candidate resolution")
		}
		for _, rid := range toStringSlice(n.Config.CandidateConfig["roleIds"]) {
			if rid == "" {
				continue
			}
			members, err := n.IdentityService.GetUserIDsByRoleID(ctx, tenantID, rid)
			if err != nil {
				logrus.WithError(err).WithField("roleID", rid).Warn("Failed to resolve role members")
				continue
			}
			for _, m := range members {
				assignees = addUnique(assignees, assigneeSet, m)
			}
		}
	case enums.CandidateTypeDirectManager:
		if owner == "" {
			return nil, fmt.Errorf("direct_manager candidate requires process owner in metadata")
		}
		if n.IdentityService == nil {
			return nil, fmt.Errorf("identity service not configured for direct_manager candidate resolution")
		}
		// levels>1 表示取第 N 级主管（设计器"发起人的第 N 级主管"），逐级向上只保留终点
		levels := toInt(n.Config.CandidateConfig["levels"], 1)
		if levels <= 0 {
			levels = 1
		}
		current := owner
		managerID := ""
		for i := 0; i < levels; i++ {
			mgr, err := n.IdentityService.GetUserManagerID(ctx, tenantID, current)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve manager of %s: %w", current, err)
			}
			if mgr == "" {
				return nil, fmt.Errorf("no manager found at level %d of owner %s", i+1, owner)
			}
			managerID = mgr
			current = mgr
		}
		assignees = addUnique(assignees, assigneeSet, managerID)
	case enums.CandidateTypeInitiatorSelect:
		// selected 模板按 msg.xxx 引用流程变量，执行时以 {msg: variables} 提供信封
		if n.initiatorSelectedTemplate != nil {
			if v, err := n.initiatorSelectedTemplate.Execute(map[string]interface{}{types.MsgKey: variables}); err == nil {
				for _, uid := range toStringSlice(v) {
					assignees = addUnique(assignees, assigneeSet, uid)
				}
			} else {
				logrus.WithError(err).Warn("Failed to execute initiator selected template")
			}
		} else {
			logrus.Warn("initiatorSelectedTemplate is nil")
		}
	case enums.CandidateTypeInitiatorSelf:
		assignees = addUnique(assignees, assigneeSet, owner)
	case enums.CandidateTypeMultiLevelManager:
		if owner == "" {
			return nil, fmt.Errorf("multi_level_manager candidate requires process owner in metadata")
		}
		if n.IdentityService == nil {
			return nil, fmt.Errorf("identity service not configured for multi_level_manager candidate resolution")
		}
		// levels>0：固定审批到第 N 级；levels<0：直到最上层（设计器 directorMode=0），
		// 组织关系中没有更上级时自然停止。
		// visited 防组织关系成环（A 的上级是 B、B 的上级是 A）导致死循环
		levels := toInt(n.Config.CandidateConfig["levels"], 1)
		current := owner
		visited := map[string]bool{owner: true}
		for i := 0; levels < 0 || i < levels; i++ {
			mgr, err := n.IdentityService.GetUserManagerID(ctx, tenantID, current)
			if err != nil || mgr == "" {
				break
			}
			if visited[mgr] {
				logrus.WithFields(logrus.Fields{
					"node": n.GetSelfId(), "user": current, "cycleTo": mgr,
				}).Warn("manager chain has a cycle; treating as top of organization")
				break
			}
			visited[mgr] = true
			assignees = addUnique(assignees, assigneeSet, mgr)
			current = mgr
		}
	case enums.CandidateTypeDept:
		if n.IdentityService == nil {
			return nil, fmt.Errorf("identity service not configured for dept-based candidate resolution")
		}
		for _, did := range toStringSlice(n.Config.CandidateConfig["departmentIds"]) {
			if did == "" {
				continue
			}
			members, err := n.IdentityService.GetUserIDsByDepartmentID(ctx, tenantID, did)
			if err != nil {
				logrus.WithError(err).WithField("deptID", did).Warn("Failed to resolve dept members")
				continue
			}
			for _, m := range members {
				assignees = addUnique(assignees, assigneeSet, m)
			}
		}
	default:
		// 未指定类型时，不分配审批人
	}

	if n.Config.SelfApprovalType != "" {
		assignees = n.handleSelfApproval(ctx, tenantID, owner, assignees, variables)
	}

	return assignees, nil
}

// handleSelfApproval 自审处理：审批人列表里包含发起人时按配置调整。
// 未命中自审或配置为 allow 时原样返回。
func (n *UserTaskNode) handleSelfApproval(ctx context.Context, tenantID, owner string, assignees []string, variables map[string]interface{}) []string {
	if owner == "" {
		return assignees
	}
	hasSelfApproval := false
	for _, assignee := range assignees {
		if assignee == owner {
			hasSelfApproval = true
			break
		}
	}
	if !hasSelfApproval {
		return assignees
	}

	switch enums.SelfApprovalType(n.Config.SelfApprovalType) {
	case enums.SelfApprovalTypeAllow:
		return assignees
	case enums.SelfApprovalTypeSkip:
		filteredAssignees := make([]string, 0, len(assignees))
		for _, assignee := range assignees {
			if assignee != owner {
				filteredAssignees = append(filteredAssignees, assignee)
			}
		}
		return filteredAssignees
	case enums.SelfApprovalTypeAutoApprove:
		// 当前行为与 allow 一致：保留发起人，不产生自动通过标记。
		// 依赖"发起人自动通过"语义的场景请勿使用该选项。
		return assignees
	case enums.SelfApprovalTypeDelegateToManager:
		if managerID := n.getDelegateManager(ctx, tenantID, owner, variables); managerID != "" {
			return replaceAssignee(assignees, owner, managerID)
		}
		return assignees
	case enums.SelfApprovalTypeDelegateToDepartmentManager:
		// 委托给部门负责人（不回退到直接上级）。
		// IdentityService 未注入时不做委托，保持原审批人（与 getDelegateManager 的兜底一致）。
		if n.IdentityService == nil {
			return assignees
		}
		if deptID, err := n.IdentityService.GetUserDepartmentID(ctx, tenantID, owner); err == nil && deptID != "" {
			if deptManagerID, err := n.IdentityService.GetDepartmentManagerUserID(ctx, tenantID, deptID); err == nil && deptManagerID != "" {
				return replaceAssignee(assignees, owner, deptManagerID)
			}
		}
		return assignees
	default:
		return assignees
	}
}

// replaceAssignee 把审批人列表中的 from 替换为 to（自审委托场景）。
// to 已在名单中时不再追加——原地替换会产生重复审批人，导致或签/会签
// 给同一人建多个任务（会签要审多次）。
func replaceAssignee(assignees []string, from, to string) []string {
	toExists := false
	for _, assignee := range assignees {
		if assignee == to {
			toExists = true
			break
		}
	}
	filteredAssignees := make([]string, 0, len(assignees))
	for _, assignee := range assignees {
		if assignee == from {
			if !toExists {
				filteredAssignees = append(filteredAssignees, to)
				toExists = true
			}
			continue
		}
		filteredAssignees = append(filteredAssignees, assignee)
	}
	return filteredAssignees
}

// getDelegateManager 解析自审委托对象：直接上级 → 部门负责人 → 变量 manager_<userID> 预设值。
func (n *UserTaskNode) getDelegateManager(ctx context.Context, tenantID, userID string, variables map[string]interface{}) string {
	if n.IdentityService == nil {
		return ""
	}

	if managerID, err := n.IdentityService.GetUserManagerID(ctx, tenantID, userID); err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{"userID": userID, "tenantID": tenantID}).Error("获取用户直接主管失败")
	} else if managerID != "" {
		return managerID
	}

	if departmentID, err := n.IdentityService.GetUserDepartmentID(ctx, tenantID, userID); err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{"userID": userID, "tenantID": tenantID}).Error("获取用户部门ID失败")
	} else if departmentID != "" {
		if departmentManagerID, err := n.IdentityService.GetDepartmentManagerUserID(ctx, tenantID, departmentID); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{"departmentID": departmentID, "tenantID": tenantID}).Error("获取部门主管失败")
		} else {
			return departmentManagerID
		}
	}

	if managerID, ok := variables[fmt.Sprintf("manager_%s", userID)].(string); ok {
		return managerID
	}

	return ""
}
