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
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
)

// memberResolverFunc 把候选 ID（角色/部门）展开为用户 ID 列表
type memberResolverFunc func(ctx context.Context, tenantID, candidateID string) ([]string, error)

// createUserTasks 创建用户任务
func (n *UserTaskNode) createUserTasks(ctx types.RuleContext, processInstanceID string, msg types.RuleMsg) error {
	// 同步实例"当前节点"监控字段（仅 active 实例生效）：管理端列表依赖它展示流程位置。
	// 失败只告警，不阻塞任务创建。
	if n.RuntimeService != nil && processInstanceID != "" {
		if err := n.RuntimeService.UpdateInstanceCurrentActivity(ctx.GetContext(), processInstanceID, n.GetSelfId()); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"instance_id": processInstanceID,
				"node_id":     n.GetSelfId(),
			}).Warn("update instance currentActivity failed; task creation continues")
		}
	}
	// 解析任务变量
	variables := extractVariables(ctx, msg)

	// 到期时间（Init 时解析，失败为 nil）
	// 到期时间：timeoutPolicy.dueInMinutes（相对创建时刻）优先，否则用静态 dueDate
	dueDate := n.resolveDueDate()
	tenantID := metaValue(msg, constants.KeyTenantID)
	owner := metaValue(msg, constants.KeyOwner)
	// 解析审批人
	assignees, err := n.resolveAssignees(ctx.GetContext(), tenantID, owner, variables)
	if err != nil {
		return fmt.Errorf("failed to resolve assignees: %w", err)
	}

	if len(assignees) == 0 {
		return fmt.Errorf("no assignees found for task")
	}
	processID := n.getProcessID(msg)
	// 根据审批类型创建任务
	switch enums.ApprovalType(n.Config.ApprovalType) {
	case enums.ApprovalTypeSingle:
		// 单人审批：只创建一个任务，分配给第一个审批人
		// 如果是角色类型，创建代认领任务
		if ct := enums.CandidateType(strings.ToLower(strings.TrimSpace(n.Config.CandidateType))); ct == enums.CandidateTypeRole || ct == enums.CandidateTypeDept {
			return n.createClaimTask(ctx, processInstanceID, processID, tenantID, variables, dueDate)
		}
		return n.createSingleTask(ctx, processInstanceID, processID, tenantID, assignees[0], variables, dueDate)
	case enums.ApprovalTypeOr:
		// 或签：创建多个任务，每个审批人一个
		// 如果是角色类型，创建代认领任务
		if ct := enums.CandidateType(strings.ToLower(strings.TrimSpace(n.Config.CandidateType))); ct == enums.CandidateTypeRole || ct == enums.CandidateTypeDept {
			return n.createClaimTask(ctx, processInstanceID, processID, tenantID, variables, dueDate)
		}
		return n.createMultiTasks(ctx, processInstanceID, processID, tenantID, assignees, variables, dueDate)
	case enums.ApprovalTypeSequential:
		// 顺序审批：先创建第一个审批任务，审批人列表写入变量缓存，
		// 后续 OnMsg 按同一顺序创建下一个任务
		variables[constants.KeySequentialAssignees] = assignees
		return n.createSingleTask(ctx, processInstanceID, processID, tenantID, assignees[0], variables, dueDate)
	case enums.ApprovalTypeCountersign:
		// 会签：创建多个任务，需要所有人都审批
		return n.createCountersignTasks(ctx, processInstanceID, processID, tenantID, assignees, variables, dueDate)
	case enums.ApprovalTypeVote:
		// 票签：复用会签父+子结构，按 approvalRule 阈值(majority/percent/count)判定，达到阈值即结束剩余子任务
		return n.createCountersignTasks(ctx, processInstanceID, processID, tenantID, assignees, variables, dueDate)
	default:
		return fmt.Errorf("unsupported approval type: %s", n.Config.ApprovalType)
	}
}

// createSingleTask 创建单个任务
func (n *UserTaskNode) createSingleTask(ctx types.RuleContext, processInstanceID, processID, tenantID, assignee string, variables map[string]interface{}, dueDate *time.Time) error {
	desc := n.TaskDescription
	vars := serializeVariables(variables)
	task := &model.WfTask{
		ProcessInstanceID: &processInstanceID,
		ProcessID:         processID,
		TaskDefKey:        n.GetSelfId(),
		TaskType:          UserTaskNodeType,
		Name:              n.TaskName,
		Description:       &desc,
		Status:            string(enums.TaskStatusActive),
		Assignee:          &assignee,
		Variables:         &vars,
		ApprovalType:      n.Config.ApprovalType,
		ApprovalRule:      &n.Config.ApprovalRule,
		TenantID:          tenantID,
		FormKey:           formKeyPtr(n.Config.FormKey),
		CreatedBy:         constants.UserSystem,
		CreatedAt:         time.Now(),
		DueDate:           dueDate,
	}

	_, err := n.TaskService.CreateTask(ctx.GetContext(), service.SystemActor(), task)
	if err != nil {
		return err
	}
	// 触发任务分配事件
	n.fireTaskEvent(ctx.GetContext(), service.TaskEvent{
		Type:       service.TaskEventAssigned,
		TaskID:     task.ID,
		TaskDefKey: task.TaskDefKey,
		InstanceID: processInstanceID,
		ProcessID:  processID,
		TenantID:   tenantID,
		TaskName:   n.GetSelfName(),
		ToUsers:    []string{assignee},
		FromUser:   operatorFromCtx(ctx.GetContext()),
		Timestamp:  time.Now(),
	})
	return nil
}

// createMultiTasks 创建多个任务（多人审批）。
// 部分创建失败时删除本次已创建的任务，避免部分参与者已收到待办而节点整体失败。
func (n *UserTaskNode) createMultiTasks(ctx types.RuleContext, processInstanceID, processID, tenantID string, assignees []string, variables map[string]interface{}, dueDate *time.Time) error {
	desc := n.TaskDescription
	vars := serializeVariables(variables)
	createdIDs := make([]string, 0, len(assignees))
	for i, assignee := range assignees {
		task := &model.WfTask{
			ProcessInstanceID: &processInstanceID,
			ProcessID:         processID,
			TaskDefKey:        n.GetSelfId(),
			TaskType:          UserTaskNodeType,
			Name:              fmt.Sprintf("%s_%d", n.TaskName, i+1),
			Description:       &desc,
			Status:            string(enums.TaskStatusActive),
			Assignee:          &assignee,
			Variables:         &vars,
			ApprovalType:      n.Config.ApprovalType,
			ApprovalRule:      &n.Config.ApprovalRule,
			TenantID:          tenantID,
			FormKey:           formKeyPtr(n.Config.FormKey),
			CreatedBy:         constants.UserSystem,
			CreatedAt:         time.Now(),
			DueDate:           dueDate,
		}

		taskID, err := n.TaskService.CreateTask(ctx.GetContext(), service.SystemActor(), task)
		if err != nil {
			n.rollbackTasks(ctx.GetContext(), createdIDs, "rollback partial multi-task creation")
			return fmt.Errorf("failed to create task for assignee %s: %w", assignee, err)
		}
		createdIDs = append(createdIDs, taskID)
		// 触发任务分配事件
		n.fireTaskEvent(ctx.GetContext(), service.TaskEvent{
			Type:       service.TaskEventAssigned,
			TaskID:     task.ID,
			TaskDefKey: task.TaskDefKey,
			InstanceID: processInstanceID,
			ProcessID:  processID,
			TenantID:   tenantID,
			TaskName:   task.Name,
			ToUsers:    []string{assignee},
			FromUser:   operatorFromCtx(ctx.GetContext()),
			Timestamp:  time.Now(),
		})
	}
	return nil
}

// rollbackTasks 删除已创建的任务（部分失败时的补偿），失败仅记录日志。
func (n *UserTaskNode) rollbackTasks(ctx context.Context, taskIDs []string, reason string) {
	for _, id := range taskIDs {
		if err := n.TaskService.DeleteTask(ctx, service.SystemActor(), id, reason); err != nil {
			logrus.WithError(err).WithField("taskId", id).Warn("rollback created task failed")
		}
	}
}

// createClaimTask 创建“待认领”任务
func (n *UserTaskNode) createClaimTask(ctx types.RuleContext, processInstanceID, processID, tenantID string, variables map[string]interface{}, dueDate *time.Time) error {
	desc := n.TaskDescription
	vars := serializeVariables(variables)
	task := &model.WfTask{
		ProcessInstanceID: &processInstanceID,
		ProcessID:         processID,
		TaskDefKey:        n.GetSelfId(),
		TaskType:          UserTaskNodeType,
		Name:              n.TaskName,
		Description:       &desc,
		Status:            string(enums.TaskStatusPending),
		Variables:         &vars,
		ApprovalType:      n.Config.ApprovalType,
		ApprovalRule:      &n.Config.ApprovalRule,
		TenantID:          tenantID,
		FormKey:           formKeyPtr(n.Config.FormKey),
		CreatedBy:         constants.UserSystem,
		CreatedAt:         time.Now(),
		DueDate:           dueDate,
	}

	taskID, err := n.TaskService.CreateTask(ctx.GetContext(), service.SystemActor(), task)
	if err != nil {
		return err
	}

	// 候选成员解析器：IdentityService 未注入时为 nil，仅落库候选不发通知
	var resolveRoleMembers, resolveDeptMembers memberResolverFunc
	if n.IdentityService != nil {
		resolveRoleMembers = n.IdentityService.GetUserIDsByRoleID
		resolveDeptMembers = n.IdentityService.GetUserIDsByDepartmentID
	}

	// 落库 role 候选。AddCandidates 失败时回滚已创建的 task（避免空池任务被任意认领越权）。
	if roleIDs := toStringSlice(n.Config.CandidateConfig["roleIds"]); len(roleIDs) > 0 {
		filtered := make([]string, 0, len(roleIDs))
		for _, rid := range roleIDs {
			if rid != "" {
				filtered = append(filtered, rid)
			}
		}
		if len(filtered) > 0 {
			if cErr := n.TaskService.AddCandidates(ctx.GetContext(), systemActorForTenant(tenantID), taskID, string(enums.EntityTypeRole), filtered); cErr != nil {
				_ = n.TaskService.DeleteTask(ctx.GetContext(), service.SystemActor(), taskID, "candidate write failed")
				return fmt.Errorf("failed to add role candidates for task %s: %w", taskID, cErr)
			}
			n.notifyCandidateCreated(ctx, taskID, processInstanceID, processID, tenantID, filtered, resolveRoleMembers)
		}
	}
	// dept 候选组：落库 department 候选，claim 时由 GetTaskCandidates 展开部门成员。
	if deptIDs := toStringSlice(n.Config.CandidateConfig["departmentIds"]); len(deptIDs) > 0 {
		filtered := make([]string, 0, len(deptIDs))
		for _, did := range deptIDs {
			if did != "" {
				filtered = append(filtered, did)
			}
		}
		if len(filtered) > 0 {
			if cErr := n.TaskService.AddCandidates(ctx.GetContext(), systemActorForTenant(tenantID), taskID, string(enums.EntityTypeDepartment), filtered); cErr != nil {
				_ = n.TaskService.DeleteTask(ctx.GetContext(), service.SystemActor(), taskID, "candidate write failed")
				return fmt.Errorf("failed to add dept candidates for task %s: %w", taskID, cErr)
			}
			n.notifyCandidateCreated(ctx, taskID, processInstanceID, processID, tenantID, filtered, resolveDeptMembers)
		}
	}
	return nil
}

// notifyCandidateCreated 展开候选成员快照，fire CandidateCreated 通知候选成员有新待认领任务。
// resolveMembers 决定按候选 ID（角色或部门）展开成员的方式；IdentityService 未注入时为 nil，跳过通知。
// 创建时刻成员快照；创建后新加入的成员靠待办列表自然发现。
func (n *UserTaskNode) notifyCandidateCreated(ctx types.RuleContext, taskID, processInstanceID, processID, tenantID string, candidateIDs []string, resolveMembers memberResolverFunc) {
	if resolveMembers == nil || len(candidateIDs) == 0 {
		return
	}
	members := make(map[string]struct{})
	for _, id := range candidateIDs {
		if id == "" {
			continue
		}
		ms, err := resolveMembers(ctx.GetContext(), tenantID, id)
		if err != nil {
			// 解析失败跳过该候选但留痕：静默 continue 会让坏角色/部门映射无通知可发
			logrus.WithError(err).WithField("candidateId", id).Warn("resolve candidate members failed; notifications skipped for it")
			continue
		}
		for _, m := range ms {
			if m != "" {
				members[m] = struct{}{}
			}
		}
	}
	if len(members) == 0 {
		return
	}
	toUsers := make([]string, 0, len(members))
	for m := range members {
		toUsers = append(toUsers, m)
	}
	n.fireTaskEvent(ctx.GetContext(), service.TaskEvent{
		Type:       service.TaskEventCandidateCreated,
		TaskID:     taskID,
		TaskDefKey: n.GetSelfId(),
		InstanceID: processInstanceID,
		ProcessID:  processID,
		TenantID:   tenantID,
		TaskName:   n.GetSelfName(),
		ToUsers:    toUsers,
		FromUser:   operatorFromCtx(ctx.GetContext()),
		Timestamp:  time.Now(),
	})
}

// createCountersignTasks 创建会签任务（会签规则解析在 service 层完成）
func (n *UserTaskNode) createCountersignTasks(ctx types.RuleContext, processInstanceID, processID, tenantID string, assignees []string, variables map[string]interface{}, dueDate *time.Time) error {
	desc := n.TaskDescription
	vars := serializeVariables(variables)

	// 创建主任务
	mainTask := &model.WfTask{
		ProcessInstanceID: &processInstanceID,
		ProcessID:         processID,
		TaskDefKey:        n.GetSelfId(),
		TaskType:          UserTaskNodeType,
		Name:              n.TaskName,
		Description:       &desc,
		Status:            string(enums.TaskStatusActive),
		Variables:         &vars,
		ApprovalType:      n.Config.ApprovalType,
		ApprovalRule:      &n.Config.ApprovalRule,
		TenantID:          tenantID,
		FormKey:           formKeyPtr(n.Config.FormKey),
		CreatedBy:         constants.UserSystem,
		CreatedAt:         time.Now(),
		DueDate:           dueDate,
	}

	taskID, err := n.TaskService.CreateTask(ctx.GetContext(), service.SystemActor(), mainTask)
	if err != nil {
		return fmt.Errorf("failed to create main countersign task: %w", err)
	}

	// 子任务创建失败时删除主任务，避免残留无子任务的主任务使流程卡死
	if err := n.TaskService.CreateCountersignSubTasks(ctx.GetContext(), taskID, assignees, n.Config.ApprovalRule); err != nil {
		_ = n.TaskService.DeleteTask(ctx.GetContext(), service.SystemActor(), taskID, "rollback countersign main task")
		return fmt.Errorf("failed to create countersign sub tasks: %w", err)
	}

	return nil
}

// fireTaskEvent 安全地派发任务事件。
// 监听器为空时不操作；panic 时 recover 不影响主流程。
func (n *UserTaskNode) fireTaskEvent(ctx context.Context, evt service.TaskEvent) {
	if n.TaskEventListener == nil {
		return
	}
	// 异步派发：监听器慢 IO 不阻塞链执行；任务行在派发前已落库
	service.DispatchTaskEvent(n.TaskEventListener, evt, ctx)
}
