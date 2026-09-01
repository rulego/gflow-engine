// This file contains the basic task CRUD methods on TaskServiceImpl:
// create, read, query, list active tasks, and delete (with instance-lock routing).

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// CreateTask 创建新任务
func (s *TaskServiceImpl) CreateTask(ctx context.Context, actor Actor, task *model.WfTask) (string, error) {
	ctx = bindActor(ctx, actor)
	if task == nil {
		return "", fmt.Errorf("task cannot be nil")
	}

	// 生成任务ID
	if task.ID == "" {
		task.ID = s.idGenerator.GenerateTaskID()
	}

	// 设置默认状态：如果assignee不为空则状态为TaskStatusActive，否则为TaskStatusPending
	if task.Status == "" {
		if task.Assignee != nil && *task.Assignee != "" {
			task.Status = string(enums.TaskStatusActive)
		} else {
			task.Status = string(enums.TaskStatusPending)
		}
	}

	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	now := time.Now()
	task.UpdatedAt = &now

	if err := s.taskDAO.Create(ctx, task); err != nil {
		return "", fmt.Errorf("failed to create task: %w", err)
	}

	return task.ID, nil
}

// GetTask 根据任务ID获取任务详情
// 加载后强制租户校验，防止跨租户按 ID 枚举越权读取他人任务。
func (s *TaskServiceImpl) GetTask(ctx context.Context, actor Actor, taskID string) (*model.WfTask, error) {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return nil, fmt.Errorf("task ID cannot be empty")
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	if task == nil {
		return nil, fmt.Errorf("%w: task", ErrNotFound)
	}

	// 租户校验：actor 租户非空时任务必须同租户；空租户视为系统视角，跳过校验
	if err := ensureTenantAccess(ctx, "task", task.TenantID); err != nil {
		return nil, err
	}

	return task, nil
}

// QueryTasks 根据条件查询任务列表
func (s *TaskServiceImpl) QueryTasks(ctx context.Context, query *dto.TaskQuery) ([]*model.WfTask, int64, error) {
	// QueryHistory=true 必须路由到历史表，否则归档到 wf_hi_task 的任务从统计中消失，
	// 且"运行时+历史"双查会变成对 wf_task 查两遍(活跃任务重复计数)。与 GetTaskList 同口径。
	if query != nil && query.QueryHistory {
		return s.hiTaskDAO.List(ctx, query)
	}
	return s.taskDAO.List(ctx, query)
}

// GetActiveTasksByProcessInstanceID 根据流程实例ID获取所有活动任务
// 活动任务是指状态为 'ACTIVE' 或 'PENDING' 的任务.
func (s *TaskServiceImpl) GetActiveTasksByProcessInstanceID(ctx context.Context, processInstanceId string) ([]*model.WfTask, error) {
	if processInstanceId == "" {
		return nil, fmt.Errorf("processInstanceId cannot be empty")
	}

	query := &dto.TaskQuery{
		InstanceID: &processInstanceId,
	}
	query.Status = []string{string(enums.TaskStatusActive), string(enums.TaskStatusPending)}

	tasks, _, err := s.taskDAO.List(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query active tasks: %w", err)
	}

	return tasks, nil
}

// DeleteTask 删除任务
//
// 删除走 WithInstanceTx：避免 Delete 与 Complete 在同一 task 上并发，
// 否则一个分支已将该 task 推进到历史表，另一个分支又把它从活动表删除，
// 最终留下重复的终止态记录或丢失审批历史。
func (s *TaskServiceImpl) DeleteTask(ctx context.Context, actor Actor, taskID, reason string) error {
	ctx = bindActor(ctx, actor)
	userID := actor.UserID
	if taskID == "" || userID == "" {
		return fmt.Errorf("taskID and userID cannot be empty")
	}

	// 廉价读：解析 instanceID 并做权限校验，无锁
	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}

	// 权限校验：操作人以显式参数 userID 为准。
	// 系统身份（引擎内部回滚/清理路径，如候选写入失败删任务）免用户级校验。
	if userID == constants.UserSystem {
		if err := s.taskDAO.Delete(ctx, taskID); err != nil {
			return fmt.Errorf("failed to delete task: %w", err)
		}
		return nil
	}
	// 租户校验：ctx 带身份时任务必须同租户（防跨租户删除）
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	isSuperAdmin := false
	if u := GetUserFromCtx(ctx); u != nil {
		isSuperAdmin = u.SuperAdmin
	}
	if task.Assignee != nil && *task.Assignee != "" {
		// 已分配的任务：只有 assignee 可以删除
		if *task.Assignee != userID {
			return fmt.Errorf("task assigned to %s, operator %s: %w", *task.Assignee, userID, ErrPermissionDenied)
		}
	} else if task.ProcessInstanceID != nil && *task.ProcessInstanceID != "" {
		// 未分配的任务：只有流程发起人或管理员可以删除。
		// 查不到实例（含查询失败）一律拒绝——放行会删掉无法溯源的任务。
		instance, err := s.workflowEngine.GetRuntimeService().GetProcessInstance(ctx, ActorFromCtx(ctx), *task.ProcessInstanceID)
		if err != nil || instance == nil {
			return fmt.Errorf("cannot verify instance initiator, refuse to delete: %w", ErrPermissionDenied)
		}
		if instance.StartUserID != userID && !isSuperAdmin {
			return fmt.Errorf("only the process initiator or admin can delete unassigned tasks: %w", ErrPermissionDenied)
		}
	}

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	if instanceID == "" {
		// orphan/draft 任务：没有实例行可以锁定，直接走 Internal
		return s.deleteTaskInternal(ctx, bareScope(s.taskDAO.Query), taskID)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.deleteTaskInternal(ctx, scope, taskID)
	})
}

// deleteTaskInternal 在已持有实例行锁的事务内执行 DeleteTask 实际逻辑。
// 幂等：任务在持锁期间可能已被并发分支删除，再次 Delete 不应失败。
// reason 当前未持久化（保留参数以匹配 public 签名），未来可写入审计日志。
func (s *TaskServiceImpl) deleteTaskInternal(ctx context.Context, scope *InstanceScope, taskID string) error {
	taskDAO := scope.Tasks()
	if err := taskDAO.Delete(ctx, taskID); err != nil {
		return fmt.Errorf("failed to delete task: %w", err)
	}
	return nil
}
