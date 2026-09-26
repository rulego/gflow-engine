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

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
)

// resolveAssignees 解析审批人：按 approver.type 分发到解析策略（内置类型与
// 宿主注册的自定义类型共用同一注册表，见 approver_resolver.go）。自审处理
// 对全部类型正交，留在节点层。
func (n *UserTaskNode) resolveAssignees(ctx context.Context, tenantID, owner string, variables map[string]interface{}) ([]string, error) {
	var assignees []string
	if r := LookupApproverResolver(enums.CandidateType(n.Config.Approver.Type)); r != nil {
		input := ApproverResolveInput{
			Ctx:      ctx,
			Identity: n.IdentityService,
			TenantID: tenantID,
			Owner:    owner,
			NodeID:   n.GetSelfId(),
			Cfg:      &n.Config.Approver,
			Vars:     variables,
		}
		// 发起人自选模板按 msg.xxx 引用流程变量，执行时以 {msg: variables} 提供信封
		if n.initiatorSelectedTemplate != nil {
			tpl := n.initiatorSelectedTemplate
			input.Expression = func(vars map[string]interface{}) (interface{}, error) {
				return tpl.Execute(map[string]interface{}{types.MsgKey: vars})
			}
		}
		var err error
		assignees, err = r.Resolve(input)
		if err != nil {
			return nil, err
		}
	}

	if sa := enums.SelfApprovalType(n.Config.SelfApproval); sa != "" && sa != enums.SelfApprovalTypeNone {
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

	switch enums.SelfApprovalType(n.Config.SelfApproval) {
	case enums.SelfApprovalTypeSkip:
		filteredAssignees := make([]string, 0, len(assignees))
		for _, assignee := range assignees {
			if assignee != owner {
				filteredAssignees = append(filteredAssignees, assignee)
			}
		}
		return filteredAssignees
	case enums.SelfApprovalTypeAutoApprove:
		// 名单保持原样：发起人照常建任务，自动通过由创建侧 autoApproveOwnerTasks 执行。
		return assignees
	case enums.SelfApprovalTypeDelegateToManager:
		if managerID := n.getDelegateManager(ctx, tenantID, owner, variables); managerID != "" {
			return replaceAssignee(assignees, owner, managerID)
		}
		return assignees
	case enums.SelfApprovalTypeDelegateToDeptManager:
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

// autoApproveOwnerTasks 发起人自动通过：selfApproval=autoApprove 时把受理人为
// 发起人的任务立即按通过完成，归档与会签阈值判定复用用户审批管道。候选池认领型
// 任务（assignee 为空）不命中，认领后正常审批；完成失败时任务保持 active，退化为手动审批。
// 必须在任务创建锁外调用：完成动作经 AfterCommit→ExecuteNext 重入本节点重新拿锁。
func (n *UserTaskNode) autoApproveOwnerTasks(ctx types.RuleContext, msg types.RuleMsg, processInstanceID string) {
	if enums.SelfApprovalType(n.Config.SelfApproval) != enums.SelfApprovalTypeAutoApprove {
		return
	}
	if n.TaskService == nil {
		return
	}
	owner := metaValue(msg, constants.KeyOwner)
	if owner == "" {
		return
	}
	// 会签/票签的受理人挂在带 ParentID 的子任务上，不能复用 getExistingTasks
	//（其 ParentIDIsNull 过滤只返回无办理人的主任务）；含子任务时行数超
	// 默认分页 10 条会把发起人的任务截在第二页，自动通过静默失效，须取全量。
	query := &dto.TaskQuery{
		InstanceID: &processInstanceID,
		TaskDefKey: n.GetSelfId(),
	}
	tasks, err := fetchTasksPageAll(ctx.GetContext(), n.TaskService, service.ActorFromCtx(ctx.GetContext()), query)
	if err != nil {
		logrus.WithError(err).Warnf("auto approve: query tasks of node %s failed, skip", n.GetSelfId())
		return
	}
	for _, t := range tasks {
		if t.Status != string(enums.TaskStatusActive) || t.Assignee == nil || *t.Assignee != owner {
			continue
		}
		req := &service.ApprovalRequest{
			TaskID:         t.ID,
			ApprovalResult: enums.ApprovalResultApproved,
			Comment:        "发起人自动通过（selfApproval=autoApprove）",
			Variables:      map[string]interface{}{},
		}
		if err := n.TaskService.CompleteWithApproval(service.WithInternalCallingMode(ctx.GetContext()), service.SystemActor(), req); err != nil {
			logrus.WithError(err).WithField("taskId", t.ID).
				Warn("auto approve initiator task failed; task left active for manual approval")
		} else {
			logrus.WithFields(logrus.Fields{"taskId": t.ID, "node": n.GetSelfId(), "owner": owner}).
				Info("initiator task auto approved (selfApproval=autoApprove)")
		}
	}
}
