// This file contains the task-comment methods on TaskServiceImpl:
// AddTaskComment / GetTaskComments, plus the in-tx recordApprovalComment
// helper used by the approval path to persist comments atomically.

package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rulego/gflow-engine/model"
)

// AddTaskComment 为任务添加审批意见。任务在办（wf_task）或已归档（wf_hi_task）均可评论。
// 评论人以显式参数 actor 为准（与审批动作的显式操作人口径一致，审计记录不丢操作人）；
// ctx 携带同名身份时补充其 UserName，租户与任务不一致时按不存在处理（防跨租户枚举）。
func (s *TaskServiceImpl) AddTaskComment(ctx context.Context, actor Actor, taskID, content string) (string, error) {
	ctx = bindActor(ctx, actor)
	userID := actor.UserID
	if taskID == "" {
		return "", fmt.Errorf("task ID cannot be empty")
	}
	if strings.TrimSpace(content) == "" {
		return "", fmt.Errorf("comment content cannot be empty: %w", ErrValidation)
	}
	if userID == "" {
		return "", ErrAuthenticationRequired
	}
	u := GetUserFromCtx(ctx)

	task, instanceID, err := s.locateTaskAnyState(ctx, taskID)
	if err != nil {
		return "", err
	}
	if u != nil && task.TenantID != u.TenantID {
		return "", fmt.Errorf("%w: task", ErrNotFound)
	}

	userName := ""
	if u != nil && u.UserID == userID {
		userName = u.UserName
	}
	comment := &model.WfTaskComment{
		ID:                s.idGenerator.GenerateID(),
		TaskID:            taskID,
		ProcessInstanceID: instanceID,
		TenantID:          task.TenantID,
		UserID:            userID,
		UserName:          userName,
		Content:           content,
		CreatedAt:         time.Now(),
	}
	if err := s.taskCommentDAO.Create(ctx, comment); err != nil {
		return "", err
	}
	return comment.ID, nil
}

// GetTaskComments 按时间正序获取任务全部评论。
// actor 租户非空时做任务归属校验（跨租户按不存在处理）；空租户视为系统视角。
func (s *TaskServiceImpl) GetTaskComments(ctx context.Context, actor Actor, taskID string) ([]*model.WfTaskComment, error) {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return nil, fmt.Errorf("task ID cannot be empty")
	}
	task, _, err := s.locateTaskAnyState(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if u := GetUserFromCtx(ctx); u != nil && u.TenantID != "" && task.TenantID != u.TenantID {
		return nil, fmt.Errorf("%w: task", ErrNotFound)
	}
	return s.taskCommentDAO.ListByTaskID(ctx, taskID)
}

// locateTaskAnyState 先查在办任务，未命中再查历史任务（评论对已归档任务开放）。
// 返回任务与所属实例ID（历史任务的实例ID取自归档行）。
func (s *TaskServiceImpl) locateTaskAnyState(ctx context.Context, taskID string) (*model.WfTask, string, error) {
	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get task: %w", err)
	}
	if task != nil {
		instanceID := ""
		if task.ProcessInstanceID != nil {
			instanceID = *task.ProcessInstanceID
		}
		return task, instanceID, nil
	}
	hiTask, err := s.hiTaskDAO.Get(ctx, taskID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get historic task: %w", err)
	}
	if hiTask == nil {
		return nil, "", fmt.Errorf("%w: task", ErrNotFound)
	}
	instanceID := ""
	if hiTask.ProcessInstanceID != nil {
		instanceID = *hiTask.ProcessInstanceID
	}
	return hiTask, instanceID, nil
}

// recordApprovalComment 在审批事务内落库审批意见（comment 为空则跳过）。
// 与任务状态变更同事务，避免"审批成功但意见丢失"。
// operator 为审批操作人 ID（显式 request.UserID 优先，回退 ctx 身份）；
// userName 在 ctx 携带同名身份时补充。
func (s *TaskServiceImpl) recordApprovalComment(ctx context.Context, scope *InstanceScope, task *model.WfTask, operator, content string) error {
	if strings.TrimSpace(content) == "" {
		return nil
	}
	userName := ""
	if u := GetUserFromCtx(ctx); u != nil && u.UserID == operator {
		userName = u.UserName
	}
	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	return scope.TaskComments().Create(ctx, &model.WfTaskComment{
		ID:                s.idGenerator.GenerateID(),
		TaskID:            task.ID,
		ProcessInstanceID: instanceID,
		TenantID:          task.TenantID,
		UserID:            operator,
		UserName:          userName,
		Content:           content,
		CreatedAt:         time.Now(),
	})
}
