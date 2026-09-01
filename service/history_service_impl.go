package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

var _ HistoryService = (*HistoryServiceImpl)(nil)

// HistoryServiceImpl HistoryService的实现
type HistoryServiceImpl struct {
	hiInstanceDAO *dao.HiInstanceDAO
	hiTaskDAO     *dao.HiTaskDAO
	processDAO    *dao.ProcessDAO
}

// NewHistoryService 创建HistoryService实例
func NewHistoryService() HistoryService {
	return &HistoryServiceImpl{
		hiInstanceDAO: dao.NewHiInstanceDAO(),
		hiTaskDAO:     dao.NewHiTaskDAO(),
		processDAO:    dao.NewProcessDAO(),
	}
}

// NewHistoryServiceWithQuery 用共享 *query.Query（事务内）构造 HistoryService
func NewHistoryServiceWithQuery(q *query.Query) HistoryService {
	return &HistoryServiceImpl{
		hiInstanceDAO: dao.NewHiInstanceDAOWithQuery(q),
		hiTaskDAO:     dao.NewHiTaskDAOWithQuery(q),
		processDAO:    dao.NewProcessDAOWithQuery(q),
	}
}

// GetHistoricProcessInstances 获取历史流程实例列表
func (s *HistoryServiceImpl) GetHistoricProcessInstances(ctx context.Context, actor Actor, query *HistoricProcessInstanceQuery) ([]*HistoricProcessInstance, int64, error) {
	ctx = bindActor(ctx, actor)
	if query == nil {
		query = &HistoricProcessInstanceQuery{}
	}

	if query.PageSize <= 0 {
		query.PageSize = dto.DefaultPageSize
	}
	if query.Page < 1 {
		query.Page = 1
	}

	// 转换查询条件
	instanceQuery := &dto.ProcessInstanceQueryDTO{
		InstanceID:    query.ProcessInstanceID,
		ProcessID:     query.ProcessDefinitionID,
		ProcessKey:    query.ProcessDefinitionKey,
		BusinessKey:   query.BusinessKey,
		StartUserID:   query.StartedBy,
		CreatedAfter:  query.StartedAfter,
		CreatedBefore: query.StartedBefore,
		PageRequest: dto.PageRequest{
			Page:     query.Page,
			PageSize: query.PageSize,
			OrderBy:  query.OrderBy,
		},
	}
	// Finished/Unfinished → 终态/活态状态集（DAO 按状态过滤）
	if query.Finished != nil && *query.Finished {
		instanceQuery.Status = []string{
			string(enums.InstanceStatusCompleted),
			string(enums.InstanceStatusTerminated),
			string(enums.InstanceStatusCancelled),
			string(enums.InstanceStatusFailed),
		}
	} else if query.Unfinished != nil && *query.Unfinished {
		instanceQuery.Status = []string{
			string(enums.InstanceStatusActive),
			string(enums.InstanceStatusSuspended),
			string(enums.InstanceStatusDraft),
		}
	}

	instanceQuery.TenantID = query.TenantID
	// 强制以 actor 租户为查询范围；空租户视为系统视角，不做租户过滤
	if u := GetUserFromCtx(ctx); u != nil && u.TenantID != "" {
		instanceQuery.TenantID = u.TenantID
	}
	instances, total, err := s.hiInstanceDAO.List(ctx, instanceQuery)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query historic process instances: %w", err)
	}

	// 按页批量取流程定义（一次 IN 查询），替代 convert 内逐行 Get 的 N+1
	defs, derr := s.loadProcessDefs(ctx, instances)
	if derr != nil {
		// 批量失败不阻断列表：convert 内还有单行回退
		logrus.WithError(derr).Warn("batch load process definitions failed; falling back to per-row Get")
	}

	historicInstances := make([]*HistoricProcessInstance, len(instances))
	for i, instance := range instances {
		historicInstances[i] = s.convertToHistoricProcessInstance(ctx, instance, defs)
	}

	return historicInstances, total, nil
}

// GetHistoricProcessInstance 根据ID获取历史流程实例
func (s *HistoryServiceImpl) GetHistoricProcessInstance(ctx context.Context, actor Actor, processInstanceID string) (*HistoricProcessInstance, error) {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return nil, fmt.Errorf("process instance ID cannot be empty")
	}

	instance, err := s.hiInstanceDAO.Get(ctx, processInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get historic process instance: %w", err)
	}

	if instance == nil {
		return nil, fmt.Errorf("%w: historic process instance", ErrNotFound)
	}

	if err := ensureTenantAccess(ctx, "historic process instance", instance.TenantID); err != nil {
		return nil, err
	}

	return s.convertToHistoricProcessInstance(ctx, instance, nil), nil
}

// GetHistoricTaskInstances 获取历史任务实例列表
func (s *HistoryServiceImpl) GetHistoricTaskInstances(ctx context.Context, actor Actor, query *HistoricTaskInstanceQuery) ([]*HistoricTaskInstance, int64, error) {
	ctx = bindActor(ctx, actor)
	if query == nil {
		query = &HistoricTaskInstanceQuery{}
	}

	if query.PageSize <= 0 {
		query.PageSize = dto.DefaultPageSize
	}
	if query.Page < 1 {
		query.Page = 1
	}

	// 转换查询条件（分页必须映射进 PageRequest，否则 DAO 回退到第 1 页/每页 10 条）
	taskQuery := dto.TaskQuery{
		TaskID:      query.TaskID,
		InstanceID:  &query.ProcessInstanceID,
		InstanceIDs: query.ProcessInstanceIDs,
		Name:        query.TaskName,
		Assignee:    query.TaskAssignee,
		Owner:       query.TaskOwner,
		TenantID:    query.TenantID,
		// 完成时间窗 → EndedAfter/Before；创建时间窗 → CreatedAfter/Before
		EndedAfter:    query.TaskCompletedAfter,
		EndedBefore:   query.TaskCompletedBefore,
		CreatedAfter:  query.TaskCreatedAfter,
		CreatedBefore: query.TaskCreatedBefore,
		PageRequest: dto.PageRequest{
			Page:      query.Page,
			PageSize:  query.PageSize,
			OrderBy:   query.OrderBy,
			OrderDesc: strings.EqualFold(query.SortOrder, "desc"),
		},
	}

	// 强制以 actor 租户为查询范围；空租户视为系统视角，不做租户过滤
	if u := GetUserFromCtx(ctx); u != nil && u.TenantID != "" {
		taskQuery.TenantID = u.TenantID
	}

	// 查询任务
	tasks, total, err := s.hiTaskDAO.List(ctx, &taskQuery)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query historic task instances: %w", err)
	}

	// 转换为历史任务实例
	historicTasks := make([]*HistoricTaskInstance, len(tasks))
	for i, task := range tasks {
		historicTasks[i] = s.convertToHistoricTaskInstance(task)
	}

	return historicTasks, total, nil
}

// GetHistoricTaskInstance 根据ID获取历史任务实例
func (s *HistoryServiceImpl) GetHistoricTaskInstance(ctx context.Context, actor Actor, taskID string) (*HistoricTaskInstance, error) {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return nil, fmt.Errorf("task ID cannot be empty")
	}

	task, err := s.hiTaskDAO.Get(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get historic task instance: %w", err)
	}

	if task == nil {
		return nil, fmt.Errorf("%w: historic task instance", ErrNotFound)
	}

	if err := ensureTenantAccess(ctx, "historic task instance", task.TenantID); err != nil {
		return nil, err
	}

	return s.convertToHistoricTaskInstance(task), nil
}

// DeleteHistoricProcessInstance 删除历史流程实例（actor 租户非空时校验归属）。
func (s *HistoryServiceImpl) DeleteHistoricProcessInstance(ctx context.Context, actor Actor, processInstanceID string) error {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return fmt.Errorf("process instance ID cannot be empty")
	}

	// 归档记录归属校验后删除
	record, err := s.hiInstanceDAO.Get(ctx, processInstanceID)
	if err != nil {
		return fmt.Errorf("failed to get historic process instance: %w", err)
	}
	if record == nil {
		return fmt.Errorf("%w: historic process instance", ErrNotFound)
	}
	if err := ensureTenantAccess(ctx, "historic process instance", record.TenantID); err != nil {
		return err
	}

	if err := s.hiInstanceDAO.Delete(ctx, processInstanceID); err != nil {
		return fmt.Errorf("failed to delete historic process instance: %w", err)
	}

	return nil
}

// DeleteHistoricTaskInstance 删除历史任务实例（actor 租户非空时校验归属）。
func (s *HistoryServiceImpl) DeleteHistoricTaskInstance(ctx context.Context, actor Actor, taskID string) error {
	ctx = bindActor(ctx, actor)
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}

	// 归档记录归属校验后删除
	record, err := s.hiTaskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get historic task instance: %w", err)
	}
	if record == nil {
		return fmt.Errorf("%w: historic task instance", ErrNotFound)
	}
	if err := ensureTenantAccess(ctx, "historic task instance", record.TenantID); err != nil {
		return err
	}

	if err := s.hiTaskDAO.Delete(ctx, taskID); err != nil {
		return fmt.Errorf("failed to delete historic task instance: %w", err)
	}

	return nil
}

// loadProcessDefs 按实例页批量加载流程定义（一次 IN 查询），消除逐行 Get 的 N+1。
func (s *HistoryServiceImpl) loadProcessDefs(ctx context.Context, instances []*model.WfInstance) (map[string]*model.WfProcess, error) {
	defs := make(map[string]*model.WfProcess)
	if s.processDAO == nil {
		return defs, nil
	}
	ids := make([]string, 0, len(instances))
	for _, inst := range instances {
		if inst != nil && inst.ProcessID != "" {
			if _, ok := defs[inst.ProcessID]; !ok {
				defs[inst.ProcessID] = nil // 占位去重
				ids = append(ids, inst.ProcessID)
			}
		}
	}
	if len(ids) == 0 {
		return defs, nil
	}
	list, err := s.processDAO.GetByIDs(ctx, ids)
	if err != nil {
		return defs, err
	}
	for _, p := range list {
		defs[p.ID] = p
	}
	return defs, nil
}

// convertToHistoricProcessInstance 转换为历史流程实例。
// defs 为批量预取的定义（可为 nil）；未命中时回退单行 Get（单实例查询路径）。
func (s *HistoryServiceImpl) convertToHistoricProcessInstance(ctx context.Context, instance *model.WfInstance, defs map[string]*model.WfProcess) *HistoricProcessInstance {
	// ProcessDefinitionKey / Version 必须从 wf_process 表查得 ——
	// wf_instance.process_id 是流程定义的主键，而非 key。
	// 查不到时回退 Key=ProcessID, Version=0。
	var (
		processKey string
		version    int
	)
	var proc *model.WfProcess
	if instance.ProcessID != "" {
		if defs != nil {
			proc = defs[instance.ProcessID]
		}
		if proc == nil && s.processDAO != nil {
			if p, perr := s.processDAO.Get(ctx, instance.ProcessID); perr == nil {
				proc = p
			}
		}
	}
	if proc != nil {
		processKey = proc.ProcessKey
		version = int(proc.Version)
	}
	if processKey == "" {
		// 退回行为：定义已被删除或查询失败时用 ProcessID 占位，避免前端展示空字符串。
		processKey = instance.ProcessID
	}

	historic := &HistoricProcessInstance{
		ID:                       instance.ID,
		ProcessDefinitionID:      instance.ProcessID,
		ProcessDefinitionKey:     processKey,
		ProcessDefinitionName:    instance.Name,
		ProcessDefinitionVersion: version,
		BusinessKey: func() string {
			if instance.BusinessKey != nil {
				return *instance.BusinessKey
			}
			return ""
		}(),
		StartTime: instance.CreatedAt,
		// StartUserID 是用户 ID 语义字段：优先取 instance.StartUserID（用户 ID），
		// 兜底 CreatedBy（通常与 StartUserID 相同）。
		StartUserID: func() string {
			if instance.StartUserID != "" {
				return instance.StartUserID
			}
			return instance.CreatedBy
		}(),
		StartActivityID:        "",
		SuperProcessInstanceID: "",
		DeleteReason:           "",
		TenantID:               instance.TenantID,
	}

	if instance.EndedAt != nil {
		historic.EndTime = instance.EndedAt
		duration := instance.EndedAt.Sub(instance.CreatedAt).Milliseconds()
		historic.DurationInMillis = &duration
	}

	return historic
}

// convertToHistoricTaskInstance 转换为历史任务实例
func (s *HistoryServiceImpl) convertToHistoricTaskInstance(task *model.WfTask) *HistoricTaskInstance {
	historic := &HistoricTaskInstance{
		ID:                  task.ID,
		ProcessDefinitionID: task.ProcessID,
		ProcessInstanceID: func() string {
			if task.ProcessInstanceID != nil {
				return *task.ProcessInstanceID
			}
			return ""
		}(),
		ExecutionID: "",
		Name:        task.Name,
		Description: func() string {
			if task.Description != nil {
				return *task.Description
			}
			return ""
		}(),
		DeleteReason: "",
		Owner: func() string {
			if task.Owner != nil {
				return *task.Owner
			}
			return ""
		}(),
		Assignee: func() string {
			if task.Assignee != nil {
				return *task.Assignee
			}
			return ""
		}(),
		StartTime:         task.CreatedAt,
		TaskDefinitionKey: task.TaskDefKey,
		FormKey: func() string {
			if task.FormKey != nil {
				return *task.FormKey
			}
			return ""
		}(),
		Priority: int(task.Priority),
		ParentTaskID: func() string {
			if task.ParentID != nil {
				return *task.ParentID
			}
			return ""
		}(),
		Category: "",
		TenantID: task.TenantID,
	}

	if task.EndedAt != nil {
		historic.EndTime = task.EndedAt
		duration := task.EndedAt.Sub(task.CreatedAt).Milliseconds()
		historic.DurationInMillis = &duration
	}

	if task.ClaimedAt != nil {
		historic.ClaimTime = task.ClaimedAt
	}

	if task.DueDate != nil {
		historic.DueDate = task.DueDate
	}

	return historic
}
