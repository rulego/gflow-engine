// This file contains read-only history lookup methods on TaskServiceImpl:
// listing history tasks for a process instance, looking up a task by its
// definition key, and fetching a single archived task by ID.

package service

import (
	"context"
	"fmt"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
)

// GetHistoryTasksByProcessInstanceID gets all history tasks for a process instance.
// 强制按当前用户租户过滤，防止跨租户越权读取他人实例历史。
func (s *TaskServiceImpl) GetHistoryTasksByProcessInstanceID(ctx context.Context, processInstanceID string) ([]*model.WfTask, error) {
	query := &dto.TaskQuery{
		InstanceID: &processInstanceID,
	}
	if u := GetUserFromCtx(ctx); u != nil {
		query.TenantID = u.TenantID
	}
	tasks, _, err := s.hiTaskDAO.List(ctx, query)
	return tasks, err
}

// GetTaskByDefKey gets a task by its definition key and process instance ID.
func (s *TaskServiceImpl) GetTaskByDefKey(ctx context.Context, processInstanceID, taskDefKey string) (*model.WfTask, error) {
	query := &dto.TaskQuery{
		InstanceID: &processInstanceID,
		TaskDefKey: taskDefKey,
	}
	tasks, _, err := s.taskDAO.List(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return nil, nil
	}
	// 租户校验：ctx 用户租户非空时任务必须同租户，否则按 NotFound 处理
	// （不泄露存在性），与 GetHistoryTask 同口径
	task := tasks[0]
	if u := GetUserFromCtx(ctx); u != nil && u.TenantID != "" && task.TenantID != u.TenantID {
		return nil, fmt.Errorf("%w: task", ErrNotFound)
	}
	return task, nil
}

// GetHistoryTask 根据任务ID获取历史任务详情
func (s *TaskServiceImpl) GetHistoryTask(ctx context.Context, taskID string) (*model.WfTask, error) {
	if taskID == "" {
		return nil, fmt.Errorf("taskID cannot be empty")
	}
	task, err := s.hiTaskDAO.Get(ctx, taskID)
	if err != nil {
		return nil, err
	}
	// 租户校验：防跨租户读他人归档任务（含 Variables/审计字段）
	if task != nil {
		if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
			return nil, fmt.Errorf("%w: task", ErrNotFound)
		}
	}
	return task, nil
}
