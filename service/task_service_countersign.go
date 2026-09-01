// This file groups all countersign (会签) machinery on TaskServiceImpl: the
// sub-task family (create / activate-next / check-completion keyed by
// parentTaskID) and the small private helpers (parseCountersignRule,
// getCompletedTasksByDefKey).

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// activateNextSequentialTaskInternal 在已持有实例行锁的事务内执行激活逻辑。
// 由 completeWithApprovalInternal 直接调用（持锁路径），避免重复 FOR UPDATE。
func (s *TaskServiceImpl) activateNextSequentialTaskInternal(ctx context.Context, scope *InstanceScope, processInstanceID, taskDefKey string) error {
	taskDAO := scope.Tasks()

	// 查询所有相关的会签任务，按创建时间排序
	query := &dto.TaskQuery{
		InstanceID: &processInstanceID,
		TaskDefKey: taskDefKey,
		PageRequest: dto.PageRequest{
			OrderBy:   "created_at",
			OrderDesc: false,
		},
	}

	tasks, _, err := taskDAO.List(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query sequential countersign tasks: %w", err)
	}

	// 找到下一个待激活的任务
	for _, task := range tasks {
		if task.Status == string(enums.TaskStatusPending) {
			// 激活这个任务
			task.Status = string(enums.TaskStatusActive)
			now := time.Now()
			task.UpdatedAt = &now

			if err := taskDAO.Update(ctx, task); err != nil {
				return fmt.Errorf("failed to activate next sequential task: %w", err)
			}

			return nil
		}
	}

	// 没有找到待激活的任务
	return nil
}

// parseCountersignRule 解析会签规则
func (s *TaskServiceImpl) parseCountersignRule(approvalRule string) (*dto.CountersignRule, error) {
	if approvalRule == "" {
		return &dto.CountersignRule{Type: string(enums.CountersignTypeAll), Value: 0}, nil
	}
	var rule dto.CountersignRule
	err := json.Unmarshal([]byte(approvalRule), &rule)
	if err != nil {
		return nil, fmt.Errorf("%w: parse countersign rule: %v", ErrCountersignRule, err)
	}

	return &rule, nil
}

// CreateCountersignSubTasks 创建会签子任务
//
// 创建会签子任务走 WithInstanceTx：与同实例的 Complete / Add / Activate 并发时，
// 新插入的子任务必须出现在持锁事务的快照中，否则会签完成判定可能漏掉这些行。
func (s *TaskServiceImpl) CreateCountersignSubTasks(ctx context.Context, parentTaskID string, assignees []string, approvalRule string) error {
	if parentTaskID == "" {
		return fmt.Errorf("parentTaskID cannot be empty")
	}
	if len(assignees) == 0 {
		return fmt.Errorf("assignees cannot be empty")
	}

	// 廉价读：解析 instanceID
	parentTask, err := s.taskDAO.Get(ctx, parentTaskID)
	if err != nil {
		return fmt.Errorf("failed to get parent task: %w", err)
	}
	if parentTask == nil {
		return fmt.Errorf("%w: parent task=%s", ErrNotFound, parentTaskID)
	}

	instanceID := ""
	if parentTask.ProcessInstanceID != nil {
		instanceID = *parentTask.ProcessInstanceID
	}
	if instanceID == "" {
		return s.createCountersignSubTasksInternal(ctx, bareScope(s.taskDAO.Query), parentTaskID, assignees, approvalRule)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.createCountersignSubTasksInternal(ctx, scope, parentTaskID, assignees, approvalRule)
	})
}

// createCountersignSubTasksInternal 在已持有实例行锁的事务内创建会签子任务。
// 重新读取父任务以获取持锁后的最新快照。持锁路径可直接调用此变体。
func (s *TaskServiceImpl) createCountersignSubTasksInternal(ctx context.Context, scope *InstanceScope, parentTaskID string, assignees []string, approvalRule string) error {
	taskDAO := scope.Tasks()

	parentTask, err := taskDAO.Get(ctx, parentTaskID)
	if err != nil {
		return fmt.Errorf("failed to get parent task: %w", err)
	}
	if parentTask == nil {
		return fmt.Errorf("%w: parent task=%s", ErrNotFound, parentTaskID)
	}

	rule, err := s.parseCountersignRule(approvalRule)
	if err != nil {
		return fmt.Errorf("%w: parse countersign rule: %v", ErrCountersignRule, err)
	}
	// 根据会签类型创建子任务
	if rule.IsSequential {
		return s.createSequentialSubTasks(ctx, scope, parentTask, assignees, approvalRule)
	} else {
		return s.createParallelSubTasks(ctx, scope, parentTask, assignees, approvalRule)
	}
}

// createParallelSubTasks 创建并行会签子任务
func (s *TaskServiceImpl) createParallelSubTasks(ctx context.Context, scope *InstanceScope, parentTask *model.WfTask, assignees []string, approvalRule string) error {
	taskDAO := scope.Tasks()
	instanceID := ""
	if parentTask.ProcessInstanceID != nil {
		instanceID = *parentTask.ProcessInstanceID
	}
	listener := s.workflowEngine.GetTaskEventListener()
	for i, assignee := range assignees {
		subTask := &model.WfTask{
			ID:                s.idGenerator.GenerateID(),
			ProcessInstanceID: parentTask.ProcessInstanceID,
			ParentID:          &parentTask.ID,
			TaskDefKey:        parentTask.TaskDefKey,
			TaskType:          parentTask.TaskType,
			ProcessID:         parentTask.ProcessID,
			Variables:         parentTask.Variables,
			Name:              fmt.Sprintf("%s_%s", parentTask.Name, assignee),
			SequenceOrder:     int32(i + 1),
			Status:            string(enums.TaskStatusActive), // 并行会签：所有子任务立即激活
			Assignee:          &assignee,
			ApprovalType:      parentTask.ApprovalType,
			ApprovalRule:      &approvalRule,
			TenantID:          parentTask.TenantID,
			CreatedBy:         constants.UserSystem,
			CreatedAt:         time.Now(),
		}

		if err := taskDAO.Create(ctx, subTask); err != nil {
			return fmt.Errorf("failed to create parallel sub task for %s: %w", assignee, err)
		}

		// 并行会签：每个子任务立即激活，触发 assigned 事件通知审批人
		if listener != nil {
			evt := TaskEvent{
				Type:         TaskEventAssigned,
				TaskID:       subTask.ID,
				TaskDefKey:   subTask.TaskDefKey,
				ParentTaskID: parentTask.ID,
				InstanceID:   instanceID,
				ProcessID:    parentTask.ProcessID,
				TenantID:     parentTask.TenantID,
				TaskName:     subTask.Name,
				ToUsers:      []string{assignee},
				FromUser:     countersignOperator(ctx),
				Timestamp:    time.Now(),
			}
			scope.AfterCommit(func() error {
				DispatchTaskEvent(listener, evt, ctx)
				return nil
			})
		}
	}
	return nil
}

// createSequentialSubTasks 创建顺序会签子任务
func (s *TaskServiceImpl) createSequentialSubTasks(ctx context.Context, scope *InstanceScope, parentTask *model.WfTask, assignees []string, approvalRule string) error {
	taskDAO := scope.Tasks()
	instanceID := ""
	if parentTask.ProcessInstanceID != nil {
		instanceID = *parentTask.ProcessInstanceID
	}
	listener := s.workflowEngine.GetTaskEventListener()
	for i, assignee := range assignees {
		// 只有第一个任务激活，其他任务等待
		status := string(enums.TaskStatusPending)
		isActive := i == 0
		if isActive {
			status = string(enums.TaskStatusActive)
		}

		subTask := &model.WfTask{
			ID:                s.idGenerator.GenerateID(),
			ProcessInstanceID: parentTask.ProcessInstanceID,
			ParentID:          &parentTask.ID,
			TaskDefKey:        parentTask.TaskDefKey,
			TaskType:          parentTask.TaskType,
			ProcessID:         parentTask.ProcessID,
			Variables:         parentTask.Variables,
			Name:              fmt.Sprintf("%s_%s", parentTask.Name, assignee),
			Status:            status,
			Assignee:          &assignee,
			SequenceOrder:     int32(i),
			ApprovalType:      parentTask.ApprovalType,
			TenantID:          parentTask.TenantID,
			CreatedBy:         constants.UserSystem,
			CreatedAt:         time.Now(),
			ApprovalRule:      &approvalRule,
		}

		if err := taskDAO.Create(ctx, subTask); err != nil {
			return fmt.Errorf("failed to create sequential sub task for %s: %w", assignee, err)
		}

		// 顺序会签：仅第一个激活子任务触发 assigned，后续子任务由
		// ActivateNextSequentialSubTask 按需激活时再触发（不在本方法处理）。
		if isActive && listener != nil {
			evt := TaskEvent{
				Type:         TaskEventAssigned,
				TaskID:       subTask.ID,
				TaskDefKey:   subTask.TaskDefKey,
				ParentTaskID: parentTask.ID,
				InstanceID:   instanceID,
				ProcessID:    parentTask.ProcessID,
				TenantID:     parentTask.TenantID,
				TaskName:     subTask.Name,
				ToUsers:      []string{assignee},
				FromUser:     countersignOperator(ctx),
				Timestamp:    time.Now(),
			}
			scope.AfterCommit(func() error {
				DispatchTaskEvent(listener, evt, ctx)
				return nil
			})
		}
	}
	return nil
}

// CheckCountersignSubTaskCompletion 检查会签子任务完成状态
func (s *TaskServiceImpl) CheckCountersignSubTaskCompletion(ctx context.Context, parentTaskID, approvalRule string) (bool, bool, error) {
	if parentTaskID == "" {
		return false, false, fmt.Errorf("parentTaskID cannot be empty")
	}
	return s.checkCountersignSubTaskCompletionInternal(ctx, bareScope(s.taskDAO.Query), parentTaskID, approvalRule)
}

// checkCountersignSubTaskCompletionInternal 在指定 scope 上执行会签完成判定。
// 已持锁路径（如 completeWithApprovalInternal）必须传 tx-bound scope，否则会绕过行锁。
func (s *TaskServiceImpl) checkCountersignSubTaskCompletionInternal(ctx context.Context, scope *InstanceScope, parentTaskID, approvalRule string) (bool, bool, error) {
	if parentTaskID == "" {
		return false, false, fmt.Errorf("parentTaskID cannot be empty")
	}
	taskDAO := scope.Tasks()

	// 获取所有子任务
	subTasks, err := taskDAO.GetByParentID(ctx, parentTaskID)
	if err != nil {
		return false, false, fmt.Errorf("failed to get sub tasks: %w", err)
	}

	if len(subTasks) == 0 {
		return false, false, fmt.Errorf("no sub tasks found for parent task %s", parentTaskID)
	}

	// 解析审批规则
	rule, err := s.parseCountersignRule(approvalRule)
	if err != nil {
		return false, false, fmt.Errorf("%w: parse approval rule: %v", ErrCountersignRule, err)
	}

	// 统计完成情况
	totalCount := len(subTasks)
	completedCount := 0
	approvedCount := 0
	for _, subTask := range subTasks {
		if subTask.Status == string(enums.TaskStatusCompleted) {
			completedCount++
			if subTask.EndReason != nil && *subTask.EndReason == string(enums.ApprovalResultApproved) {
				approvedCount++
			}
		}
	}

	// 根据完成条件判断是否完成
	var isCompleted, isApproved bool
	switch enums.CountersignType(rule.Type) {
	case enums.CountersignTypeAll:
		isCompleted = completedCount == totalCount
		isApproved = approvedCount == completedCount
	case enums.CountersignTypeAny:
		isCompleted = completedCount > 0
		isApproved = approvedCount > 0
	case enums.CountersignTypeMajority:
		// 严格过半(>50%)：N=4 需 3 票，N=3 需 2 票。(N+1)/2 对偶数 N 会差一(N=4 得 2)。
		required := totalCount/2 + 1
		isApproved = approvedCount >= required
		// 阈值类注定才判：已通过 或 剩余全同意仍不足（避免未投能逆转时提前错误 reject）
		isCompleted = isApproved || approvedCount+(totalCount-completedCount) < required
	case enums.CountersignTypePercent:
		// 向上取整：3 人 60% → ceil(1.8)=2，避免阈值过低
		required := max(1, int(math.Ceil(float64(totalCount)*rule.Value/100)))
		isApproved = approvedCount >= required
		isCompleted = isApproved || approvedCount+(totalCount-completedCount) < required
	case enums.CountersignTypeCount:
		required := max(1, int(rule.Value))
		isApproved = approvedCount >= required
		isCompleted = isApproved || approvedCount+(totalCount-completedCount) < required
	default:
		// 默认为全部完成
		isCompleted = completedCount == totalCount
		isApproved = approvedCount == completedCount
	}

	// Note: Parent task completion is handled by the caller (CompleteWithApproval),
	// so we do not call s.Approve here to avoid double-completing the parent task.

	return isCompleted, isApproved, nil
}

// getCompletedTasksByDefKey 获取同一流程实例和任务定义Key的已完成任务
func (s *TaskServiceImpl) getCompletedTasksByDefKey(ctx context.Context, processInstanceID, taskDefKey string) ([]*model.WfTask, error) {
	query := &dto.TaskQuery{
		InstanceID: &processInstanceID,
		TaskDefKey: taskDefKey,
		PageRequest: dto.PageRequest{
			Status:   []string{string(enums.TaskStatusCompleted)},
			PageSize: 100,
		},
	}

	tasks, _, err := s.QueryTasks(ctx, query)
	return tasks, err
}

func countersignOperator(ctx context.Context) string {
	if u := GetUserFromCtx(ctx); u != nil {
		return u.UserID
	}
	return ""
}
