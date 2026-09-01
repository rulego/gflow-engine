package dao

import (
	"context"
	"errors"
	"fmt"
	"gorm.io/gen/field"

	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"gorm.io/gorm"
)

// TaskDAO Task数据访问对象
type TaskDAO struct {
	Query *query.Query
}

// NewTaskDAO 创建TaskDAO实例
func NewTaskDAO() *TaskDAO {
	return &TaskDAO{
		Query: query.Q,
	}
}

// NewTaskDAOWithQuery 创建带Query参数的TaskDAO实例
func NewTaskDAOWithQuery(query *query.Query) *TaskDAO {
	return &TaskDAO{
		Query: query,
	}
}

// Create 创建单个Task
func (d *TaskDAO) Create(ctx context.Context, entity *model.WfTask) error {
	if entity == nil {
		return fmt.Errorf("task cannot be nil")
	}

	q := d.Query.WfTask
	return q.WithContext(ctx).Create(entity)
}

// CreateBatch 批量创建Task
func (d *TaskDAO) CreateBatch(ctx context.Context, entities []*model.WfTask) error {
	if len(entities) == 0 {
		return nil
	}

	q := d.Query.WfTask
	return q.WithContext(ctx).Create(entities...)
}

// Get 根据ID获取Task
func (d *TaskDAO) Get(ctx context.Context, id string) (*model.WfTask, error) {
	if id == "" {
		return nil, fmt.Errorf("id cannot be empty")
	}

	q := d.Query.WfTask
	entity, err := q.WithContext(ctx).Where(q.ID.Eq(id)).First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return entity, nil
}

// Update 更新任务
func (d *TaskDAO) Update(ctx context.Context, entity *model.WfTask) error {
	if entity == nil {
		return fmt.Errorf("entity cannot be nil")
	}

	q := d.Query.WfTask
	result, err := q.WithContext(ctx).Where(q.ID.Eq(entity.ID)).Updates(entity)
	if err != nil {
		return err
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("task not found")
	}

	return nil
}

// Delete 删除任务
func (d *TaskDAO) Delete(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("id cannot be empty")
	}

	// 任务与候选池（wf_task_assignee）同事务删除，失败时整体回滚，避免留下孤儿候选行
	return d.Query.Transaction(func(tx *query.Query) error {
		q := tx.WfTask
		result, err := q.WithContext(ctx).Where(q.ID.Eq(id)).Delete()
		if err != nil {
			return err
		}

		if result.RowsAffected == 0 {
			return fmt.Errorf("task not found")
		}

		ca := tx.WfTaskAssignee
		if _, err := ca.WithContext(ctx).Where(ca.TaskID.Eq(id)).Delete(); err != nil {
			return fmt.Errorf("failed to clean task assignees: %w", err)
		}

		return nil
	})
}

// GetByProcessInstanceID 根据流程实例ID获取任务列表
func (d *TaskDAO) GetByProcessInstanceID(ctx context.Context, processInstanceID string) ([]*model.WfTask, error) {
	if processInstanceID == "" {
		return nil, fmt.Errorf("processInstanceID cannot be empty")
	}

	q := d.Query.WfTask
	return q.WithContext(ctx).Where(q.ProcessInstanceID.Eq(processInstanceID)).Find()
}

// GetByParentID 根据父任务ID获取子任务列表
func (d *TaskDAO) GetByParentID(ctx context.Context, parentID string) ([]*model.WfTask, error) {
	if parentID == "" {
		return nil, fmt.Errorf("parentID cannot be empty")
	}

	q := d.Query.WfTask
	return q.WithContext(ctx).Where(q.ParentID.Eq(parentID)).Order(q.SequenceOrder).Find()
}

// List 根据查询条件查询任务列表
func (d *TaskDAO) List(ctx context.Context, query *dto.TaskQuery) ([]*model.WfTask, int64, error) {
	q := d.Query.WfTask
	queryBuilder := q.WithContext(ctx)

	if query.TaskID != "" {
		queryBuilder = queryBuilder.Where(q.ID.Eq(query.TaskID))
	}
	if query.TaskDefKey != "" {
		queryBuilder = queryBuilder.Where(q.TaskDefKey.Eq(query.TaskDefKey))
	}
	if query.InstanceID != nil && *query.InstanceID != "" {
		queryBuilder = queryBuilder.Where(q.ProcessInstanceID.Eq(*query.InstanceID))
	}
	if len(query.InstanceIDs) > 0 {
		queryBuilder = queryBuilder.Where(q.ProcessInstanceID.In(query.InstanceIDs...))
	}
	if query.Assignee != "" {
		queryBuilder = queryBuilder.Where(q.Assignee.Eq(query.Assignee))
	}
	if query.Owner != "" {
		queryBuilder = queryBuilder.Where(q.Owner.Eq(query.Owner))
	}
	if len(query.Status) > 0 {
		queryBuilder = queryBuilder.Where(q.Status.In(query.Status...))
	}
	if query.ApprovalType != "" {
		queryBuilder = queryBuilder.Where(q.ApprovalType.Eq(query.ApprovalType))
	}
	if query.ParentIDIsNull {
		queryBuilder = queryBuilder.Where(q.ParentID.IsNull())
	}
	if query.Keyword != "" {
		// 关键字搜索：在任务名称和描述中搜索（转义通配符）
		keywordPattern := likeContains(query.Keyword)
		queryBuilder = queryBuilder.Where(
			queryBuilder.Where(q.Name.Like(keywordPattern)).Or(q.Description.Like(keywordPattern)),
		)
	}
	if query.TenantID != "" {
		queryBuilder = queryBuilder.Where(q.TenantID.Eq(query.TenantID))
	}
	if query.CreatedAfter != nil {
		queryBuilder = queryBuilder.Where(q.CreatedAt.Gte(*query.CreatedAfter))
	}
	if query.CreatedBefore != nil {
		queryBuilder = queryBuilder.Where(q.CreatedAt.Lte(*query.CreatedBefore))
	}
	if query.EndedAfter != nil {
		queryBuilder = queryBuilder.Where(q.EndedAt.Gte(*query.EndedAfter))
	}
	if query.EndedBefore != nil {
		queryBuilder = queryBuilder.Where(q.EndedAt.Lte(*query.EndedBefore))
	}
	if query.DueDateBefore != nil {
		queryBuilder = queryBuilder.Where(q.DueDate.IsNotNull(), q.DueDate.Lt(*query.DueDateBefore))
	}
	// 候选维度过滤：CandidateUser/CandidateRoleIDs 任一设置时，限定"未指派且命中
	// wf_task_assignee 候选池"的任务（person 直配或 role 引用）。
	// 注意与 InstanceDAO.ListByTaskConditions 的候选分支不同：那边还会命中
	// assignee=user 的已指派任务；可认领集合必须排除已指派任务。
	if query.CandidateUser != "" || len(query.CandidateRoleIDs) > 0 {
		ca := d.Query.WfTaskAssignee
		var poolGroups []field.Expr
		if query.CandidateUser != "" {
			poolGroups = append(poolGroups,
				field.And(ca.EntityType.Eq(string(enums.EntityTypePerson)), ca.EntityID.Eq(query.CandidateUser)))
		}
		if len(query.CandidateRoleIDs) > 0 {
			poolGroups = append(poolGroups,
				field.And(ca.EntityType.Eq(string(enums.EntityTypeRole)), ca.EntityID.In(query.CandidateRoleIDs...)))
		}
		sub := ca.WithContext(ctx).Select(ca.TaskID).Where(field.Or(poolGroups...))
		assigneeFree := field.Or(q.Assignee.IsNull(), q.Assignee.Eq(""))
		queryBuilder = queryBuilder.Where(assigneeFree, field.ContainsSubQuery([]field.Expr{q.ID}, sub.UnderlyingDB()))
	}

	// 获取总数
	total, err := queryBuilder.Count()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count tasks: %w", err)
	}

	offset, size := paginate(&query.PageRequest)
	// 处理排序：OrderBy 未指定或无法识别时回退 created_at，保证分页顺序确定
	var order field.OrderExpr = q.CreatedAt
	if query.OrderBy != "" {
		if f, ok := q.GetFieldByName(query.OrderBy); ok && f != nil {
			order = f
		}
	}
	if query.OrderDesc {
		queryBuilder = queryBuilder.Order(order.Desc())
	} else {
		queryBuilder = queryBuilder.Order(order.Asc())
	}
	// 执行查询
	tasks, err := queryBuilder.Offset(offset).Limit(size).Find()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query tasks: %w", err)
	}

	return tasks, total, nil

}

// BacklogAggRow 积压看板聚合行：process_id + active 计数。
type BacklogAggRow struct {
	ProcessID   string `gorm:"column:process_id"`
	ActiveCount int64  `gorm:"column:active_count"`
}

// AggregateActiveByProcess 按 process_id 聚合 active 任务数，倒序取 top limit。
// 跳过 process_id 为空的行；limit<=0 视为 10。
func (d *TaskDAO) AggregateActiveByProcess(ctx context.Context, tenantID string, limit int) ([]*BacklogAggRow, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenantID cannot be empty")
	}
	if limit <= 0 {
		limit = 10
	}
	db := d.Query.WfTask.UnderlyingDB().WithContext(ctx)
	var rows []*BacklogAggRow
	err := db.Table(model.TableNameWfTask).
		Select("process_id, COUNT(*) AS active_count").
		Where("tenant_id = ? AND status = ? AND process_id <> ?", tenantID, string(enums.TaskStatusActive), "").
		Group("process_id").
		Order("active_count DESC").
		Limit(limit).
		Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate backlog by process: %w", err)
	}
	return rows, nil
}

// ListProcessNamesByID 批量取流程名称（去 N+1）。ids 为空时返回空 map。
func (d *TaskDAO) ListProcessNamesByID(ctx context.Context, ids []string) (map[string]string, error) {
	result := make(map[string]string, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	q := d.Query.WfProcess
	procs, err := q.WithContext(ctx).Where(q.ID.In(ids...)).Find()
	if err != nil {
		return nil, fmt.Errorf("failed to batch load process names: %w", err)
	}
	for _, p := range procs {
		if p != nil {
			result[p.ID] = p.Name
		}
	}
	return result, nil
}
