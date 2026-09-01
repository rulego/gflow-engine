package dao

import (
	"context"
	"fmt"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	"gorm.io/gen/field"
)

// HiInstanceDAO 历史流程实例数据访问对象
type HiInstanceDAO struct {
	Query *query.Query
}

// NewHiInstanceDAO 创建HiInstanceDAO实例
func NewHiInstanceDAO() *HiInstanceDAO {
	return &HiInstanceDAO{
		Query: query.Q,
	}
}

// NewHiInstanceDAOWithQuery 创建带Query参数的HiInstanceDAO实例
func NewHiInstanceDAOWithQuery(query *query.Query) *HiInstanceDAO {
	return &HiInstanceDAO{
		Query: query,
	}
}

// List 查询历史流程实例列表。
// 字段语义与运行时 InstanceDAO.List 保持一致：
// Keyword 对 name/business_key 模糊匹配，BusinessKey 仅对 business_key 模糊匹配。
func (d *HiInstanceDAO) List(ctx context.Context, request *dto.ProcessInstanceQueryDTO) ([]*model.WfInstance, int64, error) {
	q := d.Query.WfHiInstance
	query := q.WithContext(ctx)

	// 租户隔离：与 InstanceDAO.List 同口径（request.TenantID 非空时强制过滤，
	// 空租户视为系统视角）。服务层列表查询会以 actor 租户覆盖该字段。
	if request.TenantID != "" {
		query = query.Where(q.TenantID.Eq(request.TenantID))
	}

	// 按流程实例ID精确匹配
	if request.InstanceID != "" {
		query = query.Where(q.ID.Eq(request.InstanceID))
	}

	// 按流程定义ID精确匹配
	if request.ProcessID != "" {
		query = query.Where(q.ProcessID.Eq(request.ProcessID))
	}

	// 若按流程定义Key筛选，需要与流程定义表做连接
	if request.ProcessKey != "" {
		p := d.Query.WfProcess
		query = query.Join(p, q.ProcessID.EqCol(p.ID), p.ProcessKey.Eq(request.ProcessKey))
	}

	// 按状态筛选（与 InstanceDAO.List 同口径；PageRequest.Status 复用）
	if len(request.Status) > 0 {
		query = query.Where(q.Status.In(request.Status...))
	}

	// 发起人与创建时间窗
	if request.StartUserID != "" {
		query = query.Where(q.StartUserID.Eq(request.StartUserID))
	}
	if request.CreatedAfter != nil {
		query = query.Where(q.CreatedAt.Gte(*request.CreatedAfter))
	}
	if request.CreatedBefore != nil {
		query = query.Where(q.CreatedAt.Lte(*request.CreatedBefore))
	}

	// 关键字搜索：keyword 对 name/business_key 模糊匹配（使用分组条件确保 OR 被括号包裹，转义通配符）
	if request.Keyword != "" {
		kw := likeContains(request.Keyword)
		query = query.Where(query.Where(q.Name.Like(kw)).Or(q.BusinessKey.Like(kw)))
	}

	// businessKey 模糊过滤：仅匹配 business_key（与 InstanceDAO.List 同口径）
	if request.BusinessKey != "" {
		query = query.Where(q.BusinessKey.Like(likeContains(request.BusinessKey)))
	}

	// 统计总数
	total, err := query.Count()
	if err != nil {
		return nil, 0, err
	}

	offset, size := paginate(&request.PageRequest)

	// 排序：OrderBy 未指定或无法识别时回退 created_at，保证分页顺序确定
	var order field.OrderExpr = q.CreatedAt
	if request.OrderBy != "" {
		if f, ok := q.GetFieldByName(request.OrderBy); ok && f != nil {
			order = f
		}
	}

	if request.OrderDesc {
		query = query.Order(order.Desc())
	} else {
		query = query.Order(order.Asc())
	}

	// 查询数据
	var list []*model.WfInstance
	err = query.Offset(offset).Limit(size).Scan(&list)
	if err != nil {
		return nil, 0, err
	}

	return list, total, nil
}

// Create 创建单个HiInstance
func (d *HiInstanceDAO) Create(ctx context.Context, entity *model.WfHiInstance) error {
	if entity == nil {
		return fmt.Errorf("entity cannot be nil")
	}

	q := d.Query.WfHiInstance
	return q.WithContext(ctx).Create(entity)
}

// CreateBatch 批量创建HiInstance
func (d *HiInstanceDAO) CreateBatch(ctx context.Context, entities []*model.WfHiInstance) error {
	if len(entities) == 0 {
		return nil
	}

	q := d.Query.WfHiInstance
	return q.WithContext(ctx).Create(entities...)
}

// Get 根据ID获取HiInstance
func (d *HiInstanceDAO) Get(ctx context.Context, id string) (*model.WfInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("id cannot be empty")
	}

	q := d.Query.WfHiInstance
	var entity model.WfInstance
	err := q.WithContext(ctx).Where(q.ID.Eq(id)).Scan(&entity)
	if err != nil {
		return nil, err
	}
	if entity.ID == "" {
		return nil, nil
	}
	return &entity, nil
}

// HasByParentID 是否存在指定父实例的已归档（已完成）子实例（subProcess 嵌套状态机用）。
func (d *HiInstanceDAO) HasByParentID(ctx context.Context, parentID string) (bool, error) {
	if parentID == "" {
		return false, nil
	}
	q := d.Query.WfHiInstance
	n, err := q.WithContext(ctx).Where(q.ParentID.Eq(parentID)).Count()
	return n > 0, err
}

// HasTerminatedByParentID 查父实例下是否有 status=terminated 的归档子实例
// （子流程被驳回终止归档到 hi_instance）。供 subProcess 父失败传播:
// 子 terminated 时父应走 Failure 边，而非被 HasByParentID 误判为 completed。
func (d *HiInstanceDAO) HasTerminatedByParentID(ctx context.Context, parentID string) (bool, error) {
	if parentID == "" {
		return false, nil
	}
	q := d.Query.WfHiInstance
	n, err := q.WithContext(ctx).Where(q.ParentID.Eq(parentID), q.Status.Eq(string(enums.InstanceStatusTerminated))).Count()
	return n > 0, err
}

// Update 更新HiInstance
func (d *HiInstanceDAO) Update(ctx context.Context, entity *model.WfHiInstance) error {
	if entity == nil {
		return fmt.Errorf("entity cannot be nil")
	}

	q := d.Query.WfHiInstance
	result, err := q.WithContext(ctx).Where(q.ID.Eq(entity.ID)).Updates(entity)
	if err != nil {
		return err
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("hi_instance not found")
	}

	return nil
}

// Delete 删除历史流程实例
func (d *HiInstanceDAO) Delete(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("id cannot be empty")
	}

	q := d.Query.WfHiInstance
	result, err := q.WithContext(ctx).Where(q.ID.Eq(id)).Delete()
	if err != nil {
		return err
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("hi_instance not found")
	}

	return nil
}
