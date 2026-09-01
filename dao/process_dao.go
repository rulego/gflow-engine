package dao

import (
	"context"
	"errors"
	"fmt"
	"github.com/rulego/gflow-engine/types/enums"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/dto"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

// ProcessDAO Process数据访问对象
type ProcessDAO struct {
	Query *query.Query
}

// NewProcessDAO 创建ProcessDAO实例
func NewProcessDAO() *ProcessDAO {
	return &ProcessDAO{
		Query: query.Q,
	}
}

// NewProcessDAOWithQuery 创建带Query参数的ProcessDAO实例
func NewProcessDAOWithQuery(query *query.Query) *ProcessDAO {
	return &ProcessDAO{
		Query: query,
	}
}

// Create 创建单个Process
func (d *ProcessDAO) Create(ctx context.Context, entity *model.WfProcess) error {
	if entity == nil {
		return fmt.Errorf("process cannot be nil")
	}

	q := d.Query.WfProcess
	return q.WithContext(ctx).Create(entity)
}

// CreateBatch 批量创建Process
func (d *ProcessDAO) CreateBatch(ctx context.Context, entities []*model.WfProcess) error {
	if len(entities) == 0 {
		return nil
	}

	q := d.Query.WfProcess
	return q.WithContext(ctx).Create(entities...)
}

// List 分页查询流程定义列表
func (d *ProcessDAO) List(ctx context.Context, request *dto.ProcessQueryRequest) ([]*model.WfProcess, int64, error) {
	q := d.Query.WfProcess
	qu := q.WithContext(ctx)

	// 租户过滤
	if request.TenantID != "" {
		qu = qu.Where(q.TenantID.Eq(request.TenantID))
	}
	if request.Category != "" {
		qu = qu.Where(q.Category.Eq(request.Category))
	}
	if len(request.Status) > 0 {
		qu = qu.Where(q.Status.In(request.Status...))
	}
	if request.ProcessType != "" {
		qu = qu.Where(q.ProcessType.Eq(request.ProcessType))
	}
	if !request.AllVersion {
		// 获取每个（租户, 流程Key）的最新版本。
		// 必须带 tenant_id 维度：uq_process_key_version 约束放开后，
		// 不同租户可使用相同 process_key，仅按 key 聚合会跨租户串版本。
		subQuery := q.WithContext(ctx).
			Select(q.TenantID, q.ProcessKey, q.Version.Max().As("max_version")).
			Group(q.TenantID, q.ProcessKey)

		if request.TenantID != "" {
			subQuery = subQuery.Where(q.TenantID.Eq(request.TenantID))
		}

		// 主查询：连接子查询获取最新版本（使用 Join，并用 field.Expr 引用列）
		latestTenant := field.NewString("latest", "tenant_id")
		latestPk := field.NewString("latest", "process_key")
		latestMaxVer := field.NewInt32("latest", "max_version")
		qu = qu.Join(subQuery.As("latest"),
			q.TenantID.EqCol(latestTenant),
			q.ProcessKey.EqCol(latestPk),
			q.Version.EqCol(latestMaxVer))
	}
	// ProcessKey 精确过滤；Keyword 对 ProcessKey 和 Name 做模糊匹配
	if request.ProcessKey != "" {
		qu = qu.Where(q.ProcessKey.Eq(request.ProcessKey))
	}
	if request.Keyword != "" {
		kw := likeContains(request.Keyword)
		qu = qu.Where(qu.Where(q.ProcessKey.Like(kw)).Or(q.Name.Like(kw)))
	}

	// 统计总数
	total, err := qu.Count()
	if err != nil {
		return nil, 0, err
	}

	offset, size := paginate(&request.PageRequest)
	list, err := qu.Offset(offset).Limit(size).Order(q.CreatedAt.Desc()).Find()
	if err != nil {
		return nil, 0, err
	}

	return list, total, nil
}

// Get 根据ID获取Process
func (d *ProcessDAO) Get(ctx context.Context, id string) (*model.WfProcess, error) {
	if id == "" {
		return nil, fmt.Errorf("id cannot be empty")
	}

	q := d.Query.WfProcess
	entity, err := q.WithContext(ctx).Where(q.ID.Eq(id)).First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return entity, nil
}

// GetByIDs 批量按 ID 查询流程定义（历史列表按页批量回填 key/version，消除逐行 Get 的 N+1）
func (d *ProcessDAO) GetByIDs(ctx context.Context, ids []string) ([]*model.WfProcess, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	q := d.Query.WfProcess
	var list []*model.WfProcess
	if err := q.WithContext(ctx).Where(q.ID.In(ids...)).Scan(&list); err != nil {
		return nil, err
	}
	return list, nil
}

// CountActiveReferencingForm 统计 status=active 且 definition_json 以
// additionalInfo.formKey == formKey 引用指定表单的流程定义数。
// LIKE 通配符转义：formKey 含 % _ \ 时按字面匹配（escapeLike），防越界匹配。
func (d *ProcessDAO) CountActiveReferencingForm(ctx context.Context, tenantID, formKey string) (int64, error) {
	q := d.Query.WfProcess
	pattern := `%"formKey":"` + escapeLike(formKey) + `"%`
	return q.WithContext(ctx).
		Where(q.TenantID.Eq(tenantID)).
		Where(q.Status.Eq(string(enums.ProcessStatusActive))).
		Where(q.DefinitionJSON.Like(pattern)).
		Count()
}

// Update 更新流程定义
func (d *ProcessDAO) Update(ctx context.Context, entity *model.WfProcess) error {
	if entity == nil {
		return fmt.Errorf("entity cannot be nil")
	}

	q := d.Query.WfProcess
	result, err := q.WithContext(ctx).Where(q.ID.Eq(entity.ID)).Updates(entity)
	if err != nil {
		return err
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("process not found")
	}

	return nil
}

// Delete 删除流程定义
func (d *ProcessDAO) Delete(ctx context.Context, tenantID, id string) error {
	if id == "" {
		return fmt.Errorf("id cannot be empty")
	}

	q := d.Query.WfProcess
	result, err := q.WithContext(ctx).Where(q.TenantID.Eq(tenantID), q.ID.Eq(id)).Delete()
	if err != nil {
		return err
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("process not found")
	}

	return nil
}

// GetByKey 根据流程定义Key获取流程定义
func (d *ProcessDAO) GetByKey(ctx context.Context, tenantID, processKey string) (*model.WfProcess, error) {
	if processKey == "" {
		return nil, fmt.Errorf("processKey cannot be empty")
	}

	q := d.Query.WfProcess
	return q.WithContext(ctx).Where(q.TenantID.Eq(tenantID), q.ProcessKey.Eq(processKey)).First()
}

// GetByKeyAndVersion 根据流程定义Key和版本获取流程定义
func (d *ProcessDAO) GetByKeyAndVersion(ctx context.Context, tenantID, processKey string, version int32) (*model.WfProcess, error) {
	if processKey == "" {
		return nil, fmt.Errorf("processKey cannot be empty")
	}

	q := d.Query.WfProcess
	return q.WithContext(ctx).
		Where(q.TenantID.Eq(tenantID), q.ProcessKey.Eq(processKey), q.Version.Eq(version)).First()
}

// GetLatestByKey 根据流程定义Key获取最新版本的流程定义
func (d *ProcessDAO) GetLatestByKey(ctx context.Context, tenantID, processKey string) (*model.WfProcess, error) {
	if processKey == "" {
		return nil, fmt.Errorf("processKey cannot be empty")
	}

	q := d.Query.WfProcess
	query := q.WithContext(ctx).Where(q.ProcessKey.Eq(processKey))

	// 如果提供了租户ID，则添加过滤条件
	if tenantID != "" {
		query = query.Where(q.TenantID.Eq(tenantID))
	}

	return query.Order(q.Version.Desc()).First()
}

// GetAllVersionsByKey 根据流程定义Key获取所有版本的流程定义
func (d *ProcessDAO) GetAllVersionsByKey(ctx context.Context, tenantID, processKey string) ([]*model.WfProcess, error) {
	if processKey == "" {
		return nil, fmt.Errorf("processKey cannot be empty")
	}

	q := d.Query.WfProcess
	return q.WithContext(ctx).
		Where(q.TenantID.Eq(tenantID), q.ProcessKey.Eq(processKey)).
		Order(q.Version.Desc()).
		Find()
}

// UpdateStatus 更新流程定义状态
func (d *ProcessDAO) UpdateStatus(ctx context.Context, id, status string) error {
	if id == "" || status == "" {
		return fmt.Errorf("id and status cannot be empty")
	}

	q := d.Query.WfProcess
	result, err := q.WithContext(ctx).Where(q.ID.Eq(id)).Update(q.Status, status)
	if err != nil {
		return err
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("process not found")
	}

	return nil
}

// ActivateProcess 激活流程定义
func (d *ProcessDAO) ActivateProcess(ctx context.Context, id string) error {
	return d.UpdateStatus(ctx, id, string(enums.ProcessStatusActive))
}

// RetireActivesByKey 把同 (tenantID, processKey) 下除 keepID 外的 active 版本置为 retired。
// 发布/导入新版本时调用，保证「同 key 仅一个 active 版本」。
func (d *ProcessDAO) RetireActivesByKey(ctx context.Context, tenantID, processKey, keepID string) error {
	if tenantID == "" || processKey == "" {
		return fmt.Errorf("tenantID and processKey cannot be empty")
	}
	q := d.Query.WfProcess
	_, err := q.WithContext(ctx).
		Where(q.TenantID.Eq(tenantID), q.ProcessKey.Eq(processKey), q.Status.Eq(string(enums.ProcessStatusActive))).
		Where(q.ID.Neq(keepID)).
		Update(q.Status, string(enums.ProcessStatusRetired))
	return err
}
