// This file contains task claim / unclaim methods on TaskServiceImpl.
// Claim routes through WithInstanceTx to serialize against concurrent Complete
// operations on the same instance.

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// Claim 声明任务（将任务分配给指定用户）
func (s *TaskServiceImpl) Claim(ctx context.Context, actor Actor, taskID string) error {
	ctx = bindActor(ctx, actor)
	userID := actor.UserID
	if taskID == "" || userID == "" {
		return fmt.Errorf("task ID and user ID cannot be empty")
	}

	// 读一次 task 用于解析 instanceID（廉价读，不需要锁）
	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}

	if instanceID == "" {
		// orphan/draft 任务：没有实例行可以锁定，直接在 service 默认 query 上执行。
		// 这种场景下不存在跨实例协调问题，但仍保留 Internal 函数的幂等性校验。
		return s.claimInternal(ctx, bareScope(s.taskDAO.Query), taskID, userID)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.claimInternal(ctx, scope, taskID, userID)
	})
}

// claimInternal 在已持有实例行锁的事务内执行 Claim 实际逻辑。
// 幂等：如果任务已经是 active 且 assignee 等于 userID，直接返回 nil。
func (s *TaskServiceImpl) claimInternal(ctx context.Context, scope *InstanceScope, taskID, userID string) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 租户校验：认领是跨租户越权的入口——其他租户用户拿到 taskID 即可认领
	// 空候选池任务并推进他人流程。对齐 authorizeSignOperator/GetTask 的标准：
	// 跨租户一律按"不存在"处理（不泄露任务存在性）。
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 幂等：已经是当前用户的 active 任务，直接成功
	if task.Status == string(enums.TaskStatusActive) && task.Assignee != nil && *task.Assignee == userID {
		return nil
	}

	// 检查任务是否可以认领
	if task.Assignee != nil && *task.Assignee != "" {
		return fmt.Errorf("%w: task already assigned", ErrTaskNotClaimable)
	}

	if task.Status != string(enums.TaskStatusPending) {
		return fmt.Errorf("status=%s: %w", task.Status, ErrTaskNotClaimable)
	}

	// 候选人校验：task 级精确校验，读 wf_task_assignee + role/dept 展开命中。
	// 鉴权路径 fail-closed：DAO/identity 故障时拒绝认领（防越权）；候选池为空
	// 表示任务未限定候选人，放行。
	rows, cErr := scope.TaskAssignees().GetByTaskID(ctx, task.TenantID, task.ID)
	if cErr != nil {
		return fmt.Errorf("candidate check unavailable for task %s: %w", taskID, ErrPermissionDenied)
	}
	if len(rows) > 0 {
		identity := s.workflowEngine.GetIdentityService()
		hasGroupEntity := false
		matched := false
		for _, c := range rows {
			if c == nil {
				continue
			}
			switch c.EntityType {
			case string(enums.EntityTypeRole), string(enums.EntityTypeDepartment):
				hasGroupEntity = true
			}
			for _, m := range expandCandidateMembers(ctx, identity, task.TenantID, c.EntityType, c.EntityID) {
				if m == userID {
					matched = true
					break
				}
			}
			if matched {
				break
			}
		}
		if !matched {
			// identity==nil 且池含 role/dept：生产 identity 必须注入，nil 时无法展开 → fail-closed 拒绝（防越权）
			if identity == nil && hasGroupEntity {
				return fmt.Errorf("identity service unavailable, cannot verify group candidate for task %s: %w", taskID, ErrPermissionDenied)
			}
			return fmt.Errorf("user %s is not a candidate of task %s: %w", userID, taskID, ErrPermissionDenied)
		}
	}

	// 更新任务
	task.Assignee = &userID
	task.Status = string(enums.TaskStatusActive)
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	now := time.Now()
	task.UpdatedBy = &username
	task.UpdatedAt = &now

	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to claim task: %w", err)
	}

	// 处理同组其他待认领任务：将它们设为已终止
	if task.ProcessInstanceID != nil && task.TaskDefKey != "" {
		query := &dto.TaskQuery{
			InstanceID: task.ProcessInstanceID,
			TaskDefKey: task.TaskDefKey,
		}
		query.Status = []string{string(enums.TaskStatusPending)}
		otherTasks, _, err := taskDAO.List(ctx, query)
		if err == nil {
			userSystem := constants.UserSystem
			endReason := string(enums.EndReasonClaimedByOther)
			for _, t := range otherTasks {
				if t.ID != task.ID {
					t.Status = string(enums.TaskStatusTerminated)
					t.EndReason = &endReason
					t.UpdatedBy = &userSystem
					t.UpdatedAt = &now
					// 与其他终止路径一致补 EndedAt，否则历史/统计会看到无结束时间的 Terminated 行
					t.EndedAt = &now
					if err := taskDAO.Update(ctx, t); err != nil {
						logrus.Warnf("failed to terminate sibling task %s after claim: %v", t.ID, err)
					}
				}
			}
		}
	}

	// 认领通知：通知其他候选成员任务已被认领。
	// AfterCommit 派发：回滚不产生幽灵通知；候选成员收集也在提交前完成。
	if listener := s.workflowEngine.GetTaskEventListener(); listener != nil {
		if others := s.collectCandidateMembersExcluding(ctx, task.TenantID, task.ID, userID); len(others) > 0 {
			instID := ""
			if task.ProcessInstanceID != nil {
				instID = *task.ProcessInstanceID
			}
			evt := TaskEvent{
				Type:       TaskEventClaimed,
				TaskID:     task.ID,
				TaskDefKey: task.TaskDefKey,
				InstanceID: instID,
				ProcessID:  task.ProcessID,
				TenantID:   task.TenantID,
				TaskName:   task.Name,
				ToUsers:    others,
				FromUser:   userID,
			}
			scope.AfterCommit(func() error {
				DispatchTaskEvent(listener, evt, ctx)
				return nil
			})
		}
	}

	return nil
}

// Unclaim 取消认领任务
func (s *TaskServiceImpl) Unclaim(ctx context.Context, actor Actor, taskID string) error {
	ctx = bindActor(ctx, actor)
	userID := actor.UserID
	if taskID == "" || userID == "" {
		return fmt.Errorf("task ID and user ID cannot be empty")
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		return s.unclaimInternal(ctx, bareScope(s.taskDAO.Query), taskID, userID)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.unclaimInternal(ctx, scope, taskID, userID)
	})
}

// unclaimInternal 在持有实例行锁的事务内执行 Unclaim 实际逻辑。
func (s *TaskServiceImpl) unclaimInternal(ctx context.Context, scope *InstanceScope, taskID, userID string) error {
	taskDAO := scope.Tasks()
	task, err := taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 幂等：已经是 Pending 且无人认领
	if task.Status == string(enums.TaskStatusPending) && (task.Assignee == nil || *task.Assignee == "") {
		return nil
	}

	if task.Status != string(enums.TaskStatusActive) {
		return fmt.Errorf("task is not in active status: %w", ErrValidation)
	}

	// 权限校验：操作人以显式参数 userID 为准，只有当前 assignee 可取消认领
	if task.Assignee == nil || *task.Assignee == "" {
		return fmt.Errorf("task is not assigned to anyone, cannot unclaim")
	}
	if *task.Assignee != userID {
		return fmt.Errorf("task assigned to %s, operator %s: %w", *task.Assignee, userID, ErrPermissionDenied)
	}

	// 委派中的任务：unclaim 应归还原审批人(Owner)，而非扔回候选池导致 owner 悬空
	if task.Owner != nil && *task.Owner != "" {
		task.Assignee = task.Owner
		emptyOwner := ""
		task.Owner = &emptyOwner // gorm Updates(struct) 忽略 nil,用空串指针强制清空 owner(否则残留 → 再 approve 重触发 resolve 永不完成)
		username := ""
		if u := GetUserFromCtx(ctx); u != nil {
			username = u.UserName
		}
		now := time.Now()
		task.UpdatedBy = &username
		task.UpdatedAt = &now
		if err := taskDAO.Update(ctx, task); err != nil {
			return fmt.Errorf("failed to return delegated task on unclaim: %w", err)
		}
		return nil
	}

	// 更新 assignee 为 nil，状态回退为 Pending
	task.Assignee = nil
	task.Status = string(enums.TaskStatusPending)
	now := time.Now()
	username := ""
	if u := GetUserFromCtx(ctx); u != nil {
		username = u.UserName
	}
	task.UpdatedBy = &username
	task.UpdatedAt = &now

	if err := taskDAO.Update(ctx, task); err != nil {
		return fmt.Errorf("failed to unclaim task: %w", err)
	}

	// 恢复同组被终止的任务：将它们恢复为待认领
	if task.ProcessInstanceID != nil && task.TaskDefKey != "" {
		query := &dto.TaskQuery{
			InstanceID: task.ProcessInstanceID,
			TaskDefKey: task.TaskDefKey,
		}
		query.Status = []string{string(enums.TaskStatusTerminated)}
		otherTasks, _, err := taskDAO.List(ctx, query)
		if err == nil {
			userSystem := constants.UserSystem
			for _, t := range otherTasks {
				if t.EndReason != nil && *t.EndReason == string(enums.EndReasonClaimedByOther) {
					t.Status = string(enums.TaskStatusPending)
					t.EndReason = nil
					t.UpdatedBy = &userSystem
					t.UpdatedAt = &now
					if err := taskDAO.Update(ctx, t); err != nil {
						logrus.Warnf("failed to restore sibling task %s after unclaim: %v", t.ID, err)
					}
				}
			}
		}
	}

	// 取消认领事件：任务回到候选池，通知其他候选成员可重新认领
	if listener := s.workflowEngine.GetTaskEventListener(); listener != nil {
		instID := ""
		if task.ProcessInstanceID != nil {
			instID = *task.ProcessInstanceID
		}
		if others := s.collectCandidateMembersExcluding(ctx, task.TenantID, task.ID, userID); len(others) > 0 {
			evt := TaskEvent{
				Type:       TaskEventUnclaimed,
				TaskID:     task.ID,
				TaskDefKey: task.TaskDefKey,
				InstanceID: instID,
				ProcessID:  task.ProcessID,
				TenantID:   task.TenantID,
				TaskName:   task.Name,
				ToUsers:    others,
				FromUser:   userID,
			}
			scope.AfterCommit(func() error {
				DispatchTaskEvent(listener, evt, ctx)
				return nil
			})
		}
	}

	return nil
}
