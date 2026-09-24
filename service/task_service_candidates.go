// This file contains candidate- and approver-management methods on
// TaskServiceImpl: GetTaskCandidates and the node-level approver/approval-status
// readers used by the API layer.
//
// 候选人的写入入口为 AddCandidates/RemoveCandidates 及节点配置的候选人展开；
// 本文件只提供候选人读取与节点审批状态装配。

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/gflow-engine/utils"
)

// expandCandidateMembers 把单条候选实体展开为用户 ID 列表：person 返回自身，
// role/department 经 identity 展开；identity 未注入或展开失败返回 nil（调用方
// 按 fail-closed 原则处理）。
func expandCandidateMembers(ctx context.Context, identity IdentityService, tenantID, entityType, entityID string) []string {
	switch entityType {
	case string(enums.EntityTypePerson):
		if entityID == "" {
			return nil
		}
		return []string{entityID}
	case string(enums.EntityTypeRole):
		if identity == nil {
			return nil
		}
		members, err := identity.GetUserIDsByRoleID(ctx, tenantID, entityID)
		if err != nil {
			logrus.WithError(err).Warn("failed to resolve role members for candidate expansion")
			return nil
		}
		return members
	case string(enums.EntityTypeDepartment):
		if identity == nil {
			return nil
		}
		members, err := identity.GetUserIDsByDepartmentID(ctx, tenantID, entityID)
		if err != nil {
			logrus.WithError(err).Warn("failed to resolve department members for candidate expansion")
			return nil
		}
		return members
	default:
		return nil
	}
}

// collectCandidateMembersExcluding 展开任务候选池成员（role/dept 经 identity 展开 + person），去重并排除指定用户。
// assignees 传 scope.TaskAssignees()：本方法在实例行锁事务内调用，走全局连接会与事务互等连接，
// 单连接配置下死锁到事务超时。
func (s *TaskServiceImpl) collectCandidateMembersExcluding(ctx context.Context, assignees *dao.TaskAssigneeDAO, tenantID, taskID, excludeUserID string) []string {
	rows, err := assignees.GetByTaskID(ctx, tenantID, taskID)
	if err != nil || len(rows) == 0 {
		return nil
	}
	identity := s.workflowEngine.GetIdentityService()
	members := make(map[string]struct{})
	for _, c := range rows {
		if c == nil {
			continue
		}
		for _, m := range expandCandidateMembers(ctx, identity, tenantID, c.EntityType, c.EntityID) {
			if m != "" {
				members[m] = struct{}{}
			}
		}
	}
	delete(members, excludeUserID)
	result := make([]string, 0, len(members))
	for m := range members {
		result = append(result, m)
	}
	return result
}

// GetTaskCandidates 获取任务候选人：读 wf_task_assignee 展开 role/dept 候选。
// tenantID 从实例/任务推断，并校验调用方租户（跨租户拒绝），防止候选人名单
// IDOR 泄露审批人员身份。
func (s *TaskServiceImpl) GetTaskCandidates(ctx context.Context, actor Actor, processInstanceID, taskDefKey string) ([]*dto.NodeApproverDTO, error) {
	ctx = bindActor(ctx, actor)
	if taskDefKey == "" || processInstanceID == "" {
		return nil, fmt.Errorf("taskDefKey and processInstanceID cannot be empty")
	}

	tasks, _, err := s.taskDAO.List(ctx, &dto.TaskQuery{
		TaskDefKey: taskDefKey,
		InstanceID: &processInstanceID,
		PageRequest: dto.PageRequest{
			PageSize: 1,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query tasks for candidates: %w", err)
	}
	tenantID := ""
	if len(tasks) > 0 {
		tenantID = tasks[0].TenantID
	}
	// 租户校验：任务租户非空时拦截跨租户读取（IDOR 防护）；租户为空的脏数据行
	// 会跳过 ensureTenantAccess，对空租户真实用户兜底拒绝。
	if tenantID != "" {
		if err := ensureTenantAccess(ctx, "task", tenantID); err != nil {
			return nil, err
		}
	} else if err := requireNonEmptyTenantForRealUser(&actor); err != nil {
		return nil, err
	}

	rows, err := s.taskAssigneeDAO.GetByInstanceAndDefKey(ctx, tenantID, processInstanceID, taskDefKey)
	if err != nil {
		return nil, fmt.Errorf("failed to query task assignees: %w", err)
	}

	// identity 非 nil 时展开 role/dept；展开失败返回错误而非空池——
	// 本方法的调用方包括实例详情可见性与候选名单展示，展开不了不能当空池放行。
	identity := s.workflowEngine.GetIdentityService()
	expand := func(entityType, entityID string) ([]string, error) {
		switch entityType {
		case string(enums.EntityTypePerson):
			if entityID == "" {
				return nil, nil
			}
			return []string{entityID}, nil
		case string(enums.EntityTypeRole):
			if identity == nil {
				return nil, fmt.Errorf("identity service unavailable, cannot expand role candidate %s", entityID)
			}
			members, err := identity.GetUserIDsByRoleID(ctx, tenantID, entityID)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve role candidate %s: %w", entityID, err)
			}
			return members, nil
		case string(enums.EntityTypeDepartment):
			if identity == nil {
				return nil, fmt.Errorf("identity service unavailable, cannot expand department candidate %s", entityID)
			}
			members, err := identity.GetUserIDsByDepartmentID(ctx, tenantID, entityID)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve department candidate %s: %w", entityID, err)
			}
			return members, nil
		default:
			return nil, nil
		}
	}

	approverMap := make(map[string]*dto.NodeApproverDTO)
	addPerson := func(uid string) {
		if uid == "" {
			return
		}
		if _, ok := approverMap[uid]; !ok {
			approverMap[uid] = &dto.NodeApproverDTO{EntityID: uid, EntityType: string(enums.EntityTypePerson)}
		}
	}
	for _, r := range rows {
		if r == nil {
			continue
		}
		members, err := expand(r.EntityType, r.EntityID)
		if err != nil {
			return nil, err
		}
		for _, m := range members {
			addPerson(m)
		}
	}

	candidates := make([]*dto.NodeApproverDTO, 0, len(approverMap))
	for _, approver := range approverMap {
		candidates = append(candidates, approver)
	}
	return candidates, nil
}

// taskForAdminMutation 候选人池变更的前置校验：要求管理员/系统身份（同
// Reassign/SetAssignee，否则任意同租户用户可加自己为候选再认领劫持任务），
// 并校验任务存在且同租户，两种不满足都按 NotFound 返回，不泄露存在性。
func (s *TaskServiceImpl) taskForAdminMutation(ctx context.Context, actor Actor, taskID string) (*model.WfTask, error) {
	if err := requireAdminIdentity(&actor); err != nil {
		return nil, err
	}
	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil || task.TenantID != actor.TenantID {
		return nil, fmt.Errorf("%w: task", ErrNotFound)
	}
	return task, nil
}

// AddCandidates 批量写入任务候选人（每条 entityID 一条 wf_task_assignee 记录）。
func (s *TaskServiceImpl) AddCandidates(ctx context.Context, actor Actor, taskID, entityType string, entityIDs []string) error {
	ctx = bindActor(ctx, actor)
	if taskID == "" || entityType == "" {
		return fmt.Errorf("taskID and entityType cannot be empty")
	}
	task, err := s.taskForAdminMutation(ctx, actor, taskID)
	if err != nil {
		return err
	}
	tenantID := task.TenantID
	if len(entityIDs) == 0 {
		return nil
	}
	entities := make([]*model.WfTaskAssignee, 0, len(entityIDs))
	for _, eid := range entityIDs {
		if eid == "" {
			continue
		}
		entities = append(entities, &model.WfTaskAssignee{
			ID:         s.idGenerator.GenerateID(),
			TaskID:     taskID,
			EntityType: entityType,
			EntityID:   eid,
			TenantID:   tenantID,
			CreatedAt:  time.Now(),
		})
	}
	if len(entities) == 0 {
		return nil
	}
	if err := s.taskAssigneeDAO.CreateBatch(ctx, entities); err != nil {
		return fmt.Errorf("failed to add candidates: %w", err)
	}
	return nil
}

// RemoveCandidates 移除任务候选人（按 entityType + entityIDs 批量删除，单 SQL 原子）。
func (s *TaskServiceImpl) RemoveCandidates(ctx context.Context, actor Actor, taskID, entityType string, entityIDs []string) error {
	ctx = bindActor(ctx, actor)
	if taskID == "" || entityType == "" {
		return fmt.Errorf("taskID and entityType cannot be empty")
	}
	task, err := s.taskForAdminMutation(ctx, actor, taskID)
	if err != nil {
		return err
	}
	tenantID := task.TenantID
	filtered := make([]string, 0, len(entityIDs))
	for _, eid := range entityIDs {
		if eid != "" {
			filtered = append(filtered, eid)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	if err := s.taskAssigneeDAO.DeleteByTaskAndEntities(ctx, tenantID, taskID, entityType, filtered); err != nil {
		return fmt.Errorf("failed to remove candidates: %w", err)
	}
	return nil
}

// nodeApproverFromTask 按任务 assignee 装配审批人 DTO。
// TODO: EntityName 暂以用户 ID 充当，接入宿主用户服务后可换取真实姓名；
// Comment 同理可从任务评论表补齐。
func nodeApproverFromTask(t *model.WfTask, status string, approvalTime *string, sortOrder int32) *dto.NodeApproverDTO {
	return &dto.NodeApproverDTO{
		EntityID:      *t.Assignee,
		EntityType:    string(enums.EntityTypePerson),
		EntityName:    *t.Assignee,
		CandidateType: "candidate",
		Action:        "assign",
		SortOrder:     sortOrder,
		Status:        status,
		ApprovalTime:  approvalTime,
		CreatedAt:     t.CreatedAt.Format(constants.TimeFormatLayout),
	}
}

// GetNodeApprovalStatus 获取节点审批状态（已审批/待审批名单及审批规则摘要）。
func (s *TaskServiceImpl) GetNodeApprovalStatus(ctx context.Context, actor Actor, taskID string) (*dto.NodeApprovalStatusDTO, error) {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return nil, fmt.Errorf("taskID cannot be empty")
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return nil, fmt.Errorf("%w: task", ErrNotFound)
	}
	// 租户校验：跨租户读取节点审批人名单按拒绝处理（IDOR 防护）
	if err := ensureTenantAccess(ctx, "task", task.TenantID); err != nil {
		return nil, err
	}
	// orphan/draft 任务没有实例，节点审批状态无从谈起
	if task.ProcessInstanceID == nil || *task.ProcessInstanceID == "" {
		return nil, fmt.Errorf("%w: task %s has no process instance", ErrValidation, taskID)
	}

	completedTasks, err := s.getCompletedTasksByDefKey(ctx, *task.ProcessInstanceID, task.TaskDefKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get completed tasks: %w", err)
	}

	subTasks, err := s.taskDAO.GetByParentID(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get sub tasks: %w", err)
	}

	approvedList := make([]*dto.NodeApproverDTO, 0)
	pendingList := make([]*dto.NodeApproverDTO, 0)

	// 已出票按 end_reason 细分通过/驳回：一律标 approved 会把驳回票（含代审
	// 驳回）显示成"已同意"
	completedStatus := func(t *model.WfTask) string {
		if t.EndReason != nil && *t.EndReason == string(enums.ApprovalResultRejected) {
			return string(enums.ApproverStatusRejected)
		}
		return string(enums.ApproverStatusApproved)
	}

	for _, completedTask := range completedTasks {
		if completedTask.Assignee != nil {
			approver := nodeApproverFromTask(completedTask,
				completedStatus(completedTask),
				utils.FormatTimePtr(completedTask.EndedAt), 0)
			approvedList = append(approvedList, approver)
		}
	}

	// 子任务（会签任务）：完成与否决定 approved/pending 归属
	for _, subTask := range subTasks {
		if subTask.Assignee != nil {
			status := string(enums.ApproverStatusPending)
			var approvalTime *string
			if subTask.Status == string(enums.TaskStatusCompleted) {
				status = completedStatus(subTask)
				approvalTime = utils.FormatTimePtr(subTask.EndedAt)
			}

			approver := nodeApproverFromTask(subTask, status, approvalTime, subTask.SequenceOrder)

			if status == string(enums.ApproverStatusPending) {
				pendingList = append(pendingList, approver)
			} else {
				approvedList = append(approvedList, approver)
			}
		}
	}

	// 候选任务（未 claim，无 assignee）：PendingList 用展开后的候选成员展示。
	if task.Assignee == nil || *task.Assignee == "" {
		if task.ProcessInstanceID != nil && *task.ProcessInstanceID != "" && task.TaskDefKey != "" {
			if candidates, cErr := s.GetTaskCandidates(ctx, actor, *task.ProcessInstanceID, task.TaskDefKey); cErr == nil {
				for _, c := range candidates {
					if c == nil || c.EntityID == "" {
						continue
					}
					pendingList = append(pendingList, &dto.NodeApproverDTO{
						EntityID:      c.EntityID,
						EntityType:    string(enums.EntityTypePerson),
						EntityName:    c.EntityID,
						CandidateType: "candidate",
						Action:        "claim",
						Status:        string(enums.ApproverStatusPending),
						CreatedAt:     task.CreatedAt.Format(constants.TimeFormatLayout),
					})
				}
			}
		}
	}

	result := &dto.NodeApprovalStatusDTO{
		TaskID:            taskID,
		TaskDefKey:        task.TaskDefKey,
		TaskName:          task.Name,
		ProcessInstanceID: *task.ProcessInstanceID,
		ApprovedList:      approvedList,
		PendingList:       pendingList,
		TotalCount:        len(approvedList) + len(pendingList),
		ApprovedCount:     len(approvedList),
		PendingCount:      len(pendingList),
		ApprovalRule:      s.getApprovalRuleString(task.ApprovalRule),
		IsCompleted:       task.Status == string(enums.TaskStatusCompleted),
	}

	return result, nil
}

// GetNodeApprovers 获取节点审批人列表（已审批 + 待审批合并）。
func (s *TaskServiceImpl) GetNodeApprovers(ctx context.Context, actor Actor, taskID string) ([]*dto.NodeApproverDTO, error) {
	status, err := s.GetNodeApprovalStatus(ctx, actor, taskID)
	if err != nil {
		return nil, err
	}

	approvers := make([]*dto.NodeApproverDTO, 0, len(status.ApprovedList)+len(status.PendingList))
	approvers = append(approvers, status.ApprovedList...)
	approvers = append(approvers, status.PendingList...)

	return approvers, nil
}

// GetNodeApprovalStatusByProcessInstance 根据流程实例和任务定义Key获取节点审批状态。
func (s *TaskServiceImpl) GetNodeApprovalStatusByProcessInstance(ctx context.Context, actor Actor, processInstanceID, taskDefKey string) (*dto.NodeApprovalStatusDTO, error) {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" || taskDefKey == "" {
		return nil, fmt.Errorf("processInstanceID and taskDefKey cannot be empty")
	}

	query := &dto.TaskQuery{
		InstanceID: &processInstanceID,
		TaskDefKey: taskDefKey,
		PageRequest: dto.PageRequest{
			Status:   []string{string(enums.TaskStatusActive), string(enums.TaskStatusPending)},
			PageSize: 1,
		},
	}

	tasks, _, err := s.QueryTasks(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tasks: %w", err)
	}

	if len(tasks) == 0 {
		return nil, fmt.Errorf("%w: no active task for process instance %s and task def key %s", ErrNotFound, processInstanceID, taskDefKey)
	}

	// 租户校验：跨租户读取节点审批状态按拒绝处理（IDOR 防护）。
	if err := ensureTenantAccess(ctx, "task", tasks[0].TenantID); err != nil {
		return nil, err
	}

	return s.GetNodeApprovalStatus(ctx, actor, tasks[0].ID)
}
