package dao

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

// isValidOrderBy validates that an ORDER BY column name contains only safe characters.
var orderByRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

func isValidOrderBy(orderBy string) bool {
	return orderByRegex.MatchString(orderBy)
}

// InstanceDAO Instance数据访问对象
type InstanceDAO struct {
	Query *query.Query
}

// NewInstanceDAO 创建InstanceDAO实例
func NewInstanceDAO() *InstanceDAO {
	return &InstanceDAO{
		Query: query.Q,
	}
}

// NewInstanceDAOWithQuery 创建带Query参数的InstanceDAO实例
func NewInstanceDAOWithQuery(query *query.Query) *InstanceDAO {
	return &InstanceDAO{
		Query: query,
	}
}

// List 查询流程实例列表
func (d *InstanceDAO) List(ctx context.Context, request *dto.ProcessInstanceQueryDTO) ([]*model.WfInstance, int64, error) {
	q := d.Query.WfInstance
	query := q.WithContext(ctx)

	// 预加载流程定义关联（列表/详情需 processName/processKey/processVersion）
	query = query.Preload(q.Process)

	// 租户隔离：request.TenantID 非空时强制过滤。服务层（GetProcessInstanceList 等）
	// 会以 actor 租户覆盖该字段；为空视为系统视角不做限制（与 ensureTenantAccess 同口径）。
	// 缺失该过滤会导致列表与 businessKey 冲突检查跨租户放行。
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

	// 关键字搜索：keyword 对 name/business_key 模糊匹配（转义通配符）
	if request.Keyword != "" {
		kw := likeContains(request.Keyword)
		query = query.Where(query.Where(q.Name.Like(kw)).Or(q.BusinessKey.Like(kw)))
	}

	// businessKey 精确/模糊匹配（保留独立参数，转义通配符）
	if request.BusinessKey != "" {
		query = query.Where(q.BusinessKey.Like(likeContains(request.BusinessKey)))
	}

	// 按状态筛选
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
	list, err := query.Offset(offset).Limit(size).Find()
	if err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

// Create 创建单个Instance
func (d *InstanceDAO) Create(ctx context.Context, entity *model.WfInstance) error {
	if entity == nil {
		return fmt.Errorf("instance cannot be nil")
	}

	q := d.Query.WfInstance
	return q.WithContext(ctx).Create(entity)
}

// CreateBatch 批量创建Instance
func (d *InstanceDAO) CreateBatch(ctx context.Context, entities []*model.WfInstance) error {
	if len(entities) == 0 {
		return nil
	}

	q := d.Query.WfInstance
	return q.WithContext(ctx).Create(entities...)
}

// Get 根据ID获取Instance
func (d *InstanceDAO) Get(ctx context.Context, id string) (*model.WfInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("id cannot be empty")
	}

	q := d.Query.WfInstance
	entity, err := q.WithContext(ctx).Where(q.ID.Eq(id)).First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return entity, nil
}

// Update 更新流程实例
func (d *InstanceDAO) Update(ctx context.Context, entity *model.WfInstance) error {
	if entity == nil {
		return fmt.Errorf("entity cannot be nil")
	}

	q := d.Query.WfInstance
	result, err := q.WithContext(ctx).Where(q.ID.Eq(entity.ID)).Updates(entity)
	if err != nil {
		return err
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("instance not found")
	}

	return nil
}

// Delete 删除流程实例
func (d *InstanceDAO) Delete(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("id cannot be empty")
	}

	q := d.Query.WfInstance
	result, err := q.WithContext(ctx).Where(q.ID.Eq(id)).Delete()
	if err != nil {
		return err
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("instance not found")
	}

	return nil
}

// GetByProcessID 根据流程定义ID获取流程实例
func (d *InstanceDAO) GetByProcessID(ctx context.Context, ProcessID string, limit, offset int) ([]*model.WfInstance, int64, error) {
	if ProcessID == "" {
		return nil, 0, fmt.Errorf("ProcessID cannot be empty")
	}

	q := d.Query.WfInstance
	query := q.WithContext(ctx).Where(q.ProcessID.Eq(ProcessID))

	// 获取总数
	count, err := query.Count()
	if err != nil {
		return nil, 0, err
	}

	// 获取分页数据
	instances, err := query.Order(q.CreatedAt.Desc()).Limit(limit).Offset(offset).Find()
	if err != nil {
		return nil, 0, err
	}

	return instances, count, nil
}

// CountActiveByProcessID 统计某流程定义在指定租户下的活跃（active）实例数。
// 供流程多版本治理使用：某旧版本计数为 0 时，表示该版本已无运行中实例，可安全停用。
func (d *InstanceDAO) CountActiveByProcessID(ctx context.Context, tenantID, processID string) (int64, error) {
	if processID == "" {
		return 0, fmt.Errorf("ProcessID cannot be empty")
	}
	q := d.Query.WfInstance
	query := q.WithContext(ctx).
		Where(q.ProcessID.Eq(processID)).
		Where(q.Status.Eq(string(enums.InstanceStatusActive)))
	if tenantID != "" {
		query = query.Where(q.TenantID.Eq(tenantID))
	}
	return query.Count()
}

// SetParentID 设置实例的 parent_id（subProcess 嵌套：标记子实例归属的父实例）。
func (d *InstanceDAO) SetParentID(ctx context.Context, id, parentID string) error {
	if id == "" {
		return fmt.Errorf("id cannot be empty")
	}
	q := d.Query.WfInstance
	_, err := q.WithContext(ctx).Where(q.ID.Eq(id)).Update(q.ParentID, parentID)
	return err
}

// SetCurrentActivity 更新实例的当前节点（仅 active 实例）。
// 流程推进到 userTask 时由节点回调，保证管理端的"当前节点"列反映真实位置。
// 终态实例（completed/terminated 等）不更新，保留其最终归档时的值。
func (d *InstanceDAO) SetCurrentActivity(ctx context.Context, id, activityKey string) error {
	if id == "" {
		return nil
	}
	q := d.Query.WfInstance
	// 不显式赋 UpdatedAt：gorm 会自动追加 updated_at 赋值，显式再赋会触发
	// "对同一列进行了多次分配" (SQLSTATE 42601)，整条 UPDATE 失败。
	_, err := q.WithContext(ctx).
		Where(q.ID.Eq(id)).
		Where(q.Status.Eq(string(enums.InstanceStatusActive))).
		UpdateSimple(q.CurrentActivity.Value(activityKey))
	return err
}

// HasActiveByParentID 是否存在指定父实例的 active 子实例（subProcess 嵌套状态机用）。
func (d *InstanceDAO) HasActiveByParentID(ctx context.Context, parentID string) (bool, error) {
	if parentID == "" {
		return false, nil
	}
	q := d.Query.WfInstance
	n, err := q.WithContext(ctx).
		Where(q.ParentID.Eq(parentID)).
		Where(q.Status.Eq(string(enums.InstanceStatusActive))).
		Count()
	return n > 0, err
}

// UpdateStatus 更新流程实例状态
func (d *InstanceDAO) UpdateStatus(ctx context.Context, id, status string) error {
	if id == "" || status == "" {
		return fmt.Errorf("id and status cannot be empty")
	}

	q := d.Query.WfInstance
	result, err := q.WithContext(ctx).Where(q.ID.Eq(id)).Update(q.Status, status)
	if err != nil {
		return err
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("instance not found")
	}

	return nil
}

// TerminateInstance 终止流程实例
func (d *InstanceDAO) TerminateInstance(ctx context.Context, id, reason string) error {
	if id == "" {
		return fmt.Errorf("id cannot be empty")
	}

	now := time.Now()
	q := d.Query.WfInstance
	result, err := q.WithContext(ctx).Where(q.ID.Eq(id)).Updates(map[string]interface{}{
		q.Status.ColumnName().String():    enums.InstanceStatusTerminated,
		q.EndedAt.ColumnName().String():   &now,
		q.EndReason.ColumnName().String(): reason,
	})
	if err != nil {
		return err
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("instance not found")
	}

	return nil
}

// instanceStatusCount 按状态分组的实例计数行
type instanceStatusCount struct {
	Status string `gorm:"column:status"`
	Count  int64  `gorm:"column:cnt"`
}

// instanceStatistics 实例统计核心实现：单次 GROUP BY 取出各状态计数，
// 供 GetInstanceStatistics / GetInstanceStatisticsByTenant 复用。
// tenantID/processID/startUserID 均为可选过滤条件。
func (d *InstanceDAO) instanceStatistics(ctx context.Context, tenantID, processID, startUserID string) (map[string]interface{}, error) {
	db := d.Query.WfInstance.UnderlyingDB().WithContext(ctx)
	tx := db.Table(model.TableNameWfInstance).
		Select("status, COUNT(*) AS cnt").
		Group("status")
	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}
	if processID != "" {
		tx = tx.Where("process_id = ?", processID)
	}
	if startUserID != "" {
		tx = tx.Where("created_by = ?", startUserID)
	}

	var rows []instanceStatusCount
	if err := tx.Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("failed to group instance statistics: %w", err)
	}

	counts := make(map[string]int64, len(rows))
	var totalCount int64
	for _, r := range rows {
		counts[r.Status] = r.Count
		totalCount += r.Count
	}

	return map[string]interface{}{
		"total_count":      totalCount,
		"active_count":     counts[string(enums.InstanceStatusActive)],
		"completed_count":  counts[string(enums.InstanceStatusCompleted)],
		"suspended_count":  counts[string(enums.InstanceStatusSuspended)],
		"terminated_count": counts[string(enums.InstanceStatusTerminated)],
	}, nil
}

// GetInstanceStatisticsByTenant 根据租户获取流程实例统计信息
func (d *InstanceDAO) GetInstanceStatisticsByTenant(ctx context.Context, tenantID, ProcessID, startUserID string) (map[string]interface{}, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenantID cannot be empty")
	}

	return d.instanceStatistics(ctx, tenantID, ProcessID, startUserID)
}

// ListByTaskConditions 根据任务条件关联查询流程实例列表（并关联流程定义）
func (d *InstanceDAO) ListByTaskConditions(ctx context.Context, req *dto.TaskQuery) ([]*model.WfInstance, int64, error) {
	if req == nil {
		req = &dto.TaskQuery{}
	}
	if req.TenantID == "" {
		return nil, 0, errors.New("tenantID required")
	}

	// 构建基础 SQL 模板
	// 1. 运行时表查询
	runSQL := `
		SELECT i.* 
		FROM wf_instance i
		JOIN wf_task t ON i.id = t.process_instance_id
		WHERE 1=1
	`
	// 2. 历史表查询
	histSQL := `
		SELECT i.* 
		FROM wf_hi_instance i
		JOIN wf_hi_task t ON i.id = t.process_instance_id
		WHERE 1=1
	`

	var args []interface{}
	var conditions string

	// 构建条件
	if req.TenantID != "" {
		conditions += " AND i.tenant_id = ?"
		args = append(args, req.TenantID)
	}
	if req.Assignee != "" {
		// 候选维度：assignee=user OR (Pending 未签收 且 user 在候选人池 wf_task_assignee)。
		// 候选池匹配 person（entity_id=user）、role（entity_id IN userRoleIDs）与
		// department（entity_id IN userDeptIDs，dept 候选任务落库的是部门实体）。
		if req.CandidateUser != "" {
			poolCond := "(ca.entity_type = 'person' AND ca.entity_id = ?)"
			poolArgs := []interface{}{}
			for _, group := range []struct {
				entityType string
				ids        []string
			}{{"role", req.CandidateRoleIDs}, {"department", req.CandidateDeptIDs}} {
				if len(group.ids) == 0 {
					continue
				}
				ph := strings.TrimSuffix(strings.Repeat("?,", len(group.ids)), ",")
				poolCond += " OR (ca.entity_type = '" + group.entityType + "' AND ca.entity_id IN (" + ph + "))"
				for _, id := range group.ids {
					poolArgs = append(poolArgs, id)
				}
			}
			if len(poolArgs) > 0 {
				poolCond = "(" + poolCond + ")"
			}
			conditions += " AND (t.assignee = ? OR (t.status = ? AND EXISTS (SELECT 1 FROM wf_task_assignee ca WHERE ca.task_id = t.id AND " + poolCond + ")))"
			args = append(args, req.Assignee, string(enums.TaskStatusPending), req.CandidateUser)
			args = append(args, poolArgs...)
		} else {
			conditions += " AND t.assignee = ?"
			args = append(args, req.Assignee)
		}
	}
	if len(req.Status) > 0 {
		conditions += " AND t.status IN (?)"
		args = append(args, req.Status)
	}
	if req.TaskDefKey != "" {
		conditions += " AND t.task_def_key = ?"
		args = append(args, req.TaskDefKey)
	}
	if req.ApprovalType != "" {
		conditions += " AND t.approval_type = ?"
		args = append(args, req.ApprovalType)
	}
	if req.Keyword != "" {
		conditions += " AND (i.name LIKE ? OR i.business_key LIKE ?)"
		kw := likeContains(req.Keyword)
		args = append(args, kw, kw)
	}

	// 合并运行时与历史两个来源。一个实例可能命中多个任务、甚至同时出现在
	// 两个子查询里，用 UNION + COUNT(DISTINCT id) 去重，保证分页总数准确。

	// 构建 Count SQL
	countSQL := fmt.Sprintf(`
		SELECT COUNT(DISTINCT id) FROM (
			%s %s
			UNION
			%s %s
		) AS combined
	`, runSQL, conditions, histSQL, conditions)

	// 复制参数用于 Count 查询 (因为 UNION 用了两次条件，参数也需要两份)
	countArgs := append(args, args...)

	var total int64
	db := d.Query.WfInstance.UnderlyingDB().WithContext(ctx)
	if err := db.Raw(countSQL, countArgs...).Scan(&total).Error; err != nil {
		return nil, 0, err
	}

	if total == 0 {
		return []*model.WfInstance{}, 0, nil
	}

	// 构建分页查询 SQL
	offset, size := paginate(&req.PageRequest)

	orderBy := "created_at"
	if req.OrderBy != "" && isValidOrderBy(req.OrderBy) {
		orderBy = req.OrderBy
	}
	orderDir := "DESC"
	if !req.OrderDesc {
		orderDir = "ASC"
	}

	listSQL := fmt.Sprintf(`
		SELECT * FROM (
			%s %s
			UNION
			%s %s
		) AS combined
		ORDER BY %s %s
		LIMIT %d OFFSET %d
	`, runSQL, conditions, histSQL, conditions, orderBy, orderDir, size, offset)

	// 列表查询参数：两份条件参数
	listArgs := append(args, args...)

	var list []*model.WfInstance
	if err := db.Raw(listSQL, listArgs...).Scan(&list).Error; err != nil {
		return nil, 0, err
	}

	return list, total, nil
}

// GetInstancesUnionPagination 分页获取流程实例列表（合并运行时和历史表，支持多租户）
func (d *InstanceDAO) GetInstancesUnionPagination(ctx context.Context, tenantID, ProcessID, startUserID string, statuses []string, keyword string, startTimeFrom, startTimeTo *time.Time, limit, offset int, instanceID, businessKey string) ([]*model.WfInstance, int64, error) {
	if tenantID == "" {
		return nil, 0, errors.New("tenantID required")
	}
	// 1. 运行时表查询
	runSQL := "SELECT * FROM wf_instance WHERE 1=1"
	// 2. 历史表查询
	histSQL := "SELECT * FROM wf_hi_instance WHERE 1=1"

	var args []interface{}
	var conditions string

	if tenantID != "" {
		conditions += " AND tenant_id = ?"
		args = append(args, tenantID)
	}
	if ProcessID != "" {
		conditions += " AND process_id = ?"
		args = append(args, ProcessID)
	}
	if instanceID != "" {
		conditions += " AND id = ?"
		args = append(args, instanceID)
	}
	if businessKey != "" {
		conditions += " AND business_key = ?"
		args = append(args, businessKey)
	}
	if startUserID != "" {
		conditions += " AND start_user_id = ?"
		args = append(args, startUserID)
	}
	if len(statuses) > 0 {
		placeholders := make([]string, 0, len(statuses))
		for _, s := range statuses {
			if s == "" {
				continue
			}
			placeholders = append(placeholders, "?")
			args = append(args, s)
		}
		if len(placeholders) > 0 {
			conditions += " AND status IN (" + strings.Join(placeholders, ",") + ")"
		}
	}
	if keyword != "" {
		kw := likeContains(keyword)
		conditions += " AND (name LIKE ? OR business_key LIKE ?)"
		args = append(args, kw, kw)
	}
	if startTimeFrom != nil {
		conditions += " AND created_at >= ?"
		args = append(args, *startTimeFrom)
	}
	if startTimeTo != nil {
		conditions += " AND created_at <= ?"
		args = append(args, *startTimeTo)
	}

	// Count SQL：归档在单事务内完成（建历史行+删活行同 tx），两表不会有同 ID 双行，
	// 故用 UNION ALL 免去 UNION 去重的全列比较排序开销。
	countSQL := fmt.Sprintf(`
		SELECT COUNT(*) FROM (
			%s %s
			UNION ALL
			%s %s
		) AS combined
	`, runSQL, conditions, histSQL, conditions)

	countArgs := append(args, args...)

	var total int64
	db := d.Query.WfInstance.UnderlyingDB().WithContext(ctx)
	if err := db.Raw(countSQL, countArgs...).Scan(&total).Error; err != nil {
		return nil, 0, err
	}

	if total == 0 {
		return []*model.WfInstance{}, 0, nil
	}

	// List SQL：与 Count 同口径（UNION ALL）
	listSQL := fmt.Sprintf(`
		SELECT * FROM (
			%s %s
			UNION ALL
			%s %s
		) AS combined
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`, runSQL, conditions, histSQL, conditions)

	listArgs := append(args, args...)
	listArgs = append(listArgs, limit, offset)

	var list []*model.WfInstance
	if err := db.Raw(listSQL, listArgs...).Scan(&list).Error; err != nil {
		return nil, 0, err
	}

	return list, total, nil
}
