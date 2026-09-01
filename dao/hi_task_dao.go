package dao

import (
	"context"
	"fmt"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/dto"
	"gorm.io/gen/field"
)

// HiTaskDAO 历史任务数据访问对象
type HiTaskDAO struct {
	Query *query.Query
}

// NewHiTaskDAO 创建HiTaskDAO实例
func NewHiTaskDAO() *HiTaskDAO {
	return &HiTaskDAO{
		Query: query.Q,
	}
}

// NewHiTaskDAOWithQuery 创建带Query参数的HiTaskDAO实例
func NewHiTaskDAOWithQuery(query *query.Query) *HiTaskDAO {
	return &HiTaskDAO{
		Query: query,
	}
}

// Create 创建单个HiTask
func (d *HiTaskDAO) Create(ctx context.Context, entity *model.WfHiTask) error {
	if entity == nil {
		return fmt.Errorf("hi_task cannot be nil")
	}

	q := d.Query.WfHiTask
	return q.WithContext(ctx).Create(entity)
}

// CreateBatch 批量创建HiTask
func (d *HiTaskDAO) CreateBatch(ctx context.Context, entities []*model.WfHiTask) error {
	if len(entities) == 0 {
		return nil
	}

	q := d.Query.WfHiTask
	return q.WithContext(ctx).Create(entities...)
}

// Get 根据ID获取HiTask
func (d *HiTaskDAO) Get(ctx context.Context, id string) (*model.WfTask, error) {
	if id == "" {
		return nil, fmt.Errorf("id cannot be empty")
	}

	q := d.Query.WfHiTask
	var entity model.WfTask
	err := q.WithContext(ctx).Where(q.ID.Eq(id)).Scan(&entity)
	if err != nil {
		return nil, err
	}
	if entity.ID == "" {
		return nil, nil
	}
	return &entity, nil
}

// Update 更新HiTask
func (d *HiTaskDAO) Update(ctx context.Context, entity *model.WfHiTask) error {
	if entity == nil {
		return fmt.Errorf("entity cannot be nil")
	}

	q := d.Query.WfHiTask
	result, err := q.WithContext(ctx).Where(q.ID.Eq(entity.ID)).Updates(entity)
	if err != nil {
		return err
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("hi_task not found")
	}

	return nil
}

// Delete 删除历史任务
func (d *HiTaskDAO) Delete(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("id cannot be empty")
	}

	q := d.Query.WfHiTask
	result, err := q.WithContext(ctx).Where(q.ID.Eq(id)).Delete()
	if err != nil {
		return err
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("hi_task not found")
	}

	return nil
}

// List 根据查询条件查询历史任务列表
func (d *HiTaskDAO) List(ctx context.Context, query *dto.TaskQuery) ([]*model.WfTask, int64, error) {
	q := d.Query.WfHiTask
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
	// 时间窗过滤：与 TaskDAO.List 同口径；weekCompleted/aggregateMonthCompleted
	// 等统计依赖 EndedAfter/EndedBefore 在历史查询路径上同样生效。
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
	var tasks []*model.WfTask
	err = queryBuilder.Offset(offset).Limit(size).Scan(&tasks)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query tasks: %w", err)
	}

	return tasks, total, nil
}
