// This file contains the classic add-sign / reduce-sign operations on
// TaskServiceImpl (sibling sign-tasks on the same node, not sub-tasks of a
// parent countersign task). Each routes through WithInstanceTx.

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// authorizeSignOperator 校验加签/减签操作者身份。
// 普通任务：须为 task.Assignee；countersign 父任务（无直接 assignee，审批人在子任务上）：
// 须为该父任务任一子任务的 assignee。防越权加签/减签他人任务。
// 注意：不做超管 bypass——加签/减签是任务参与人动作，即便是超管也须是该节点参与人。
func (s *TaskServiceImpl) authorizeSignOperator(ctx context.Context, task *model.WfTask) error {
	u := GetUserFromCtx(ctx)
	if u == nil {
		return fmt.Errorf("authentication required: %w", ErrPermissionDenied)
	}
	if task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	// 直接 assignee（普通任务）
	if task.Assignee != nil && *task.Assignee == u.UserID {
		return nil
	}
	// countersign 父任务：操作者是任一子任务 assignee 即放行
	if task.ID != "" {
		if subTasks, err := s.taskDAO.GetByParentID(ctx, task.ID); err == nil {
			for _, st := range subTasks {
				if st.Assignee != nil && *st.Assignee == u.UserID {
					return nil
				}
			}
		}
	}
	return fmt.Errorf("only task assignee/countersign participant can add/reduce sign: %w", ErrPermissionDenied)
}

// AddSign 加签（添加额外的审批人）
func (s *TaskServiceImpl) AddSign(ctx context.Context, actor Actor, taskID string, userIDs []string, reason string) error {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}
	if len(userIDs) == 0 {
		return fmt.Errorf("user IDs cannot be empty")
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	// 操作者鉴权：本人 assignee 或 countersign 父任务的子任务 assignee
	if err := s.authorizeSignOperator(ctx, task); err != nil {
		return err
	}

	// 设计器显式禁用 addSign → 拒绝（actionPermissions 解析失败时降级放行）
	if err := s.requireActionEnabled(ctx, task, "addSign"); err != nil {
		return err
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.addSignInternal(ctx, bareScope(s.taskDAO.Query), taskID, userIDs, reason)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.addSignInternal(ctx, scope, taskID, userIDs, reason)
	})
}

func (s *TaskServiceImpl) addSignInternal(ctx context.Context, scope *InstanceScope, taskID string, userIDs []string, reason string) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	now := time.Now()
	desc := fmt.Sprintf("加签任务: %s", reason)
	// 去重：收集该节点已存在的审批人（父任务 assignee + 现有子任务 assignee），避免重复加签
	existing := map[string]bool{}
	if task.Assignee != nil && *task.Assignee != "" {
		existing[*task.Assignee] = true
	}
	if subs, serr := taskDAO.GetByParentID(ctx, taskID); serr == nil {
		for _, st := range subs {
			if st.Assignee != nil && *st.Assignee != "" {
				existing[*st.Assignee] = true
			}
		}
	}
	for _, userID := range userIDs {
		if existing[userID] {
			continue
		}
		signTask := &model.WfTask{
			ID:                s.idGenerator.GenerateTaskID(),
			Name:              fmt.Sprintf("[加签] %s", task.Name),
			Description:       &desc,
			ProcessInstanceID: task.ProcessInstanceID,
			TaskDefKey:        task.TaskDefKey,
			TaskType:          task.TaskType,
			ParentID:          &taskID,
			Assignee:          &userID,
			Owner:             task.Owner,
			Priority:          task.Priority,
			DueDate:           task.DueDate,
			Status:            string(enums.TaskStatusActive),
			ApprovalType:      task.ApprovalType,
			ApprovalRule:      task.ApprovalRule,
			CreatedAt:         time.Now(),
			UpdatedAt:         &now,
			TenantID:          task.TenantID,
		}
		if err := taskDAO.Create(ctx, signTask); err != nil {
			return fmt.Errorf("failed to create sign task for user %s: %w", userID, err)
		}
	}

	// 加签事件：通知被加签人（assigned 语义已由子任务创建覆盖，这里给审计/看板一条明确的加签记录）
	if listener := s.workflowEngine.GetTaskEventListener(); listener != nil {
		instID := ""
		if task.ProcessInstanceID != nil {
			instID = *task.ProcessInstanceID
		}
		operator := ""
		if u := GetUserFromCtx(ctx); u != nil {
			operator = u.UserID
		}
		evt := TaskEvent{
			Type:       TaskEventAddSign,
			TaskID:     task.ID,
			TaskDefKey: task.TaskDefKey,
			InstanceID: instID,
			ProcessID:  task.ProcessID,
			TenantID:   task.TenantID,
			TaskName:   task.Name,
			ToUsers:    append([]string(nil), userIDs...),
			FromUser:   operator,
			Reason:     reason,
			Timestamp:  time.Now(),
		}
		scope.AfterCommit(func() error {
			DispatchTaskEvent(listener, evt, ctx)
			return nil
		})
	}
	return nil
}

// ReduceSign 减签（移除部分审批人）
func (s *TaskServiceImpl) ReduceSign(ctx context.Context, actor Actor, taskID string, userIDs []string, reason string) error {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}
	if len(userIDs) == 0 {
		return fmt.Errorf("user IDs cannot be empty")
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	// 操作者鉴权：本人 assignee 或 countersign 父任务的子任务 assignee
	if err := s.authorizeSignOperator(ctx, task); err != nil {
		return err
	}

	// 设计器显式禁用 reduceSign → 拒绝（actionPermissions 解析失败时降级放行）
	if err := s.requireActionEnabled(ctx, task, "reduceSign"); err != nil {
		return err
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.reduceSignInternal(ctx, bareScope(s.taskDAO.Query), taskID, userIDs, reason)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.reduceSignInternal(ctx, scope, taskID, userIDs, reason)
	})
}

func (s *TaskServiceImpl) reduceSignInternal(ctx context.Context, scope *InstanceScope, taskID string, userIDs []string, reason string) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	for _, userID := range userIDs {
		q := &dto.TaskQuery{
			InstanceID: task.ProcessInstanceID,
			Assignee:   userID,
			PageRequest: dto.PageRequest{
				Status: []string{string(enums.TaskStatusActive)},
			},
		}
		tasks, _, qerr := taskDAO.List(ctx, q)
		if qerr != nil {
			continue
		}
		for _, signTask := range tasks {
			if signTask.ParentID != nil && *signTask.ParentID == taskID {
				// 在持锁事务内直接删除（绕过 DeleteTask 的权限校验，因为这里是
				// 系统行为而非用户操作）
				if derr := taskDAO.Delete(ctx, signTask.ID); derr != nil {
					logrus.Warnf("failed to delete sign task %s during reduce-sign: %v", signTask.ID, derr)
				}
			}
		}
	}
	// 减签事件：记录被移除的审批人（审计/通知用）
	if listener := s.workflowEngine.GetTaskEventListener(); listener != nil {
		instID := ""
		if task.ProcessInstanceID != nil {
			instID = *task.ProcessInstanceID
		}
		operator := ""
		if u := GetUserFromCtx(ctx); u != nil {
			operator = u.UserID
		}
		evt := TaskEvent{
			Type:       TaskEventReduceSign,
			TaskID:     task.ID,
			TaskDefKey: task.TaskDefKey,
			InstanceID: instID,
			ProcessID:  task.ProcessID,
			TenantID:   task.TenantID,
			TaskName:   task.Name,
			ToUsers:    append([]string(nil), userIDs...),
			FromUser:   operator,
			Reason:     reason,
			Timestamp:  time.Now(),
		}
		scope.AfterCommit(func() error {
			DispatchTaskEvent(listener, evt, ctx)
			return nil
		})
	}

	// 减签后重新评估会签/票签节点：剩余子任务满足完成条件则完成；减到 0(无人能审)终止实例。
	// 仅对 countersign/vote 父任务生效；单签/或签/顺序减签为 no-op（无 parent 子任务结构）。
	if (task.ParentID == nil || *task.ParentID == "") &&
		(task.ApprovalType == string(enums.ApprovalTypeCountersign) || task.ApprovalType == string(enums.ApprovalTypeVote)) {
		return s.reevaluateCountersignAfterReduce(ctx, scope, task)
	}
	return nil
}

// reevaluateCountersignAfterReduce 减签后重新评估会签/票签节点完成状态：
// 剩余子任务满足阈值 → 标记 parent 完成 + 聚合变量 + 流转；无剩余子任务(减到 0) → 终止实例。
func (s *TaskServiceImpl) reevaluateCountersignAfterReduce(ctx context.Context, scope *InstanceScope, parentTask *model.WfTask) error {
	taskDAO := scope.Tasks()
	rule := s.getApprovalRuleString(parentTask.ApprovalRule)
	isCompleted, approved, err := s.checkCountersignSubTaskCompletionInternal(ctx, scope, parentTask.ID, rule)
	if err != nil {
		// 无剩余子任务(减到 0)：无人能审，终止实例避免永久卡死
		if parentTask.ProcessInstanceID != nil && *parentTask.ProcessInstanceID != "" {
			evt, terr := terminateProcessInstanceInTx(ctx, s.workflowEngine.GetRuntimeService(), scope.Tx(), *parentTask.ProcessInstanceID, "all approvers reduced during reduce-sign")
			if terr != nil {
				return terr
			}
			if evt != nil && s.workflowEngine.GetTaskEventListener() != nil {
				scope.AfterCommit(func() error {
					DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), *evt, ctx)
					return nil
				})
			}
			return nil
		}
		return nil
	}
	if !isCompleted {
		return nil
	}
	now := time.Now()
	parentTask.Status = string(enums.TaskStatusCompleted)
	parentTask.EndedAt = &now
	endReason := string(enums.ApprovalResultApproved)
	if !approved {
		endReason = string(enums.ApprovalResultRejected)
	}
	parentTask.EndReason = &endReason
	s.mergeCountersignSubTaskVariables(ctx, scope, parentTask)
	if err := taskDAO.Update(ctx, parentTask); err != nil {
		return fmt.Errorf("failed to complete parent after reduce-sign: %w", err)
	}
	inst := parentTask.ProcessInstanceID
	key := parentTask.TaskDefKey
	vars, perr := ParseVariablesJSON(parentTask.Variables)
	if perr != nil || vars == nil {
		vars = map[string]interface{}{}
	}
	scope.AfterCommit(func() error {
		return s.workflowEngine.GetRuntimeServiceInternal().ExecuteNext(ctx, *inst, key, vars)
	})
	return nil
}
