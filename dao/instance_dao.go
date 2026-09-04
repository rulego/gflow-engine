package dao

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
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

// taskInstanceQuery 预构建的任务维度实例查询：任务表 JOIN 实例表的运行时/历史两分支同构
type taskInstanceQuery struct {
	runSQL, histSQL, conditions string
	args                        []interface{}
}

// buildTaskInstanceQuery 构建任务维度实例查询的 WHERE 条件（运行时/历史两分支共用同一段条件）
func buildTaskInstanceQuery(req *dto.TaskQuery) taskInstanceQuery {
	tq := taskInstanceQuery{
		runSQL: `
			SELECT i.*
			FROM wf_instance i
			JOIN wf_task t ON i.id = t.process_instance_id
			WHERE 1=1
		`,
		histSQL: `
			SELECT i.*
			FROM wf_hi_instance i
			JOIN wf_hi_task t ON i.id = t.process_instance_id
			WHERE 1=1
		`,
	}

	// 构建条件
	if req.TenantID != "" {
		tq.conditions += " AND i.tenant_id = ?"
		tq.args = append(tq.args, req.TenantID)
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
			tq.conditions += " AND (t.assignee = ? OR (t.status = ? AND EXISTS (SELECT 1 FROM wf_task_assignee ca WHERE ca.task_id = t.id AND " + poolCond + ")))"
			tq.args = append(tq.args, req.Assignee, string(enums.TaskStatusPending), req.CandidateUser)
			tq.args = append(tq.args, poolArgs...)
		} else {
			tq.conditions += " AND t.assignee = ?"
			tq.args = append(tq.args, req.Assignee)
		}
	}
	if len(req.Status) > 0 {
		tq.conditions += " AND t.status IN (?)"
		tq.args = append(tq.args, req.Status)
	}
	// 已删实例（删除时历史行标记 deleted）不进任何任务维度列表：
	// 本条件由运行时/历史两分支共用，任一分支命中都被排除
	tq.conditions += " AND i.status <> ?"
	tq.args = append(tq.args, string(enums.InstanceStatusDeleted))
	if len(req.InstanceStatuses) > 0 {
		tq.conditions += " AND i.status IN (?)"
		tq.args = append(tq.args, req.InstanceStatuses)
	}
	if req.EndReasonPrefix != "" {
		tq.conditions += " AND i.end_reason LIKE ?"
		tq.args = append(tq.args, req.EndReasonPrefix+"%")
	}
	for _, p := range req.EndReasonNotPrefixes {
		if p == "" {
			continue
		}
		tq.conditions += " AND i.end_reason NOT LIKE ?"
		tq.args = append(tq.args, p+"%")
	}
	if req.TaskDefKey != "" {
		tq.conditions += " AND t.task_def_key = ?"
		tq.args = append(tq.args, req.TaskDefKey)
	}
	if req.ApprovalType != "" {
		tq.conditions += " AND t.approval_type = ?"
		tq.args = append(tq.args, req.ApprovalType)
	}
	if req.Keyword != "" {
		// keyword 命中申请标题 / 业务键 / 编号（实例ID）/ 申请人（StartUserIDs，宿主按姓名解析）
		tq.conditions += " AND (i.name LIKE ? OR i.business_key LIKE ? OR i.id LIKE ?"
		kw := likeContains(req.Keyword)
		tq.args = append(tq.args, kw, kw, kw)
		if len(req.StartUserIDs) > 0 {
			ph := strings.TrimSuffix(strings.Repeat("?,", len(req.StartUserIDs)), ",")
			tq.conditions += " OR i.start_user_id IN (" + ph + ")"
			for _, id := range req.StartUserIDs {
				tq.args = append(tq.args, id)
			}
		}
		tq.conditions += ")"
	}
	return tq
}

// ListByTaskConditions 根据任务条件关联查询流程实例列表（并关联流程定义）
func (d *InstanceDAO) ListByTaskConditions(ctx context.Context, req *dto.TaskQuery) ([]*model.WfInstance, int64, error) {
	if req == nil {
		req = &dto.TaskQuery{}
	}
	if req.TenantID == "" {
		return nil, 0, errors.New("tenantID required")
	}
	tq := buildTaskInstanceQuery(req)
	runSQL, histSQL, conditions, args := tq.runSQL, tq.histSQL, tq.conditions, tq.args

	// 合并运行时与历史两个来源。一个实例可能命中多个任务、甚至同时出现在
	// 两个子查询里，用 UNION + COUNT(DISTINCT id) 去重，保证分页总数准确。

	// 构建 Count SQL。内联视图别名不带 AS，部分方言不允许。
	countSQL := fmt.Sprintf(`
		SELECT COUNT(DISTINCT id) FROM (
			%s %s
			UNION
			%s %s
		) combined
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
		) combined
		ORDER BY %s %s
		LIMIT ? OFFSET ?
	`, runSQL, conditions, histSQL, conditions, orderBy, orderDir)

	// 列表查询参数：两份条件参数 + 分页参数
	listArgs := append(args, args...)
	listArgs = append(listArgs, size, offset)

	var list []*model.WfInstance
	if err := db.Raw(listSQL, listArgs...).Scan(&list).Error; err != nil {
		return nil, 0, err
	}

	return list, total, nil
}

// instanceUnionQuery 预构建的运行时/历史实例表合并查询：两分支同构，conditions/args 由分页与计数复用
type instanceUnionQuery struct {
	runSQL, histSQL, conditions string
	args                        []interface{}
}

// buildInstanceUnionQuery 构建实例合并查询的 WHERE 条件（运行时/历史两分支共用同一段条件）。
// statuses 为空时排除软删除行：deleted 对用户不可见，任何列表不应带出。
func buildInstanceUnionQuery(tenantID, processID, startUserID string, statuses []string, keyword string, startTimeFrom, startTimeTo *time.Time, instanceID, businessKey, endReasonPrefix string, endReasonNotPrefixes ...string) instanceUnionQuery {
	uq := instanceUnionQuery{
		runSQL:  "SELECT * FROM wf_instance WHERE 1=1",
		histSQL: "SELECT * FROM wf_hi_instance WHERE 1=1",
	}

	if tenantID != "" {
		uq.conditions += " AND tenant_id = ?"
		uq.args = append(uq.args, tenantID)
	}
	if processID != "" {
		uq.conditions += " AND process_id = ?"
		uq.args = append(uq.args, processID)
	}
	if instanceID != "" {
		uq.conditions += " AND id = ?"
		uq.args = append(uq.args, instanceID)
	}
	if businessKey != "" {
		uq.conditions += " AND business_key = ?"
		uq.args = append(uq.args, businessKey)
	}
	if startUserID != "" {
		uq.conditions += " AND start_user_id = ?"
		uq.args = append(uq.args, startUserID)
	}
	if len(statuses) > 0 {
		placeholders := make([]string, 0, len(statuses))
		for _, s := range statuses {
			if s == "" {
				continue
			}
			placeholders = append(placeholders, "?")
			uq.args = append(uq.args, s)
		}
		if len(placeholders) > 0 {
			uq.conditions += " AND status IN (" + strings.Join(placeholders, ",") + ")"
		}
	} else {
		// 未显式指定状态时排除软删除行：deleted 对用户不可见，任何列表不应带出
		uq.conditions += " AND status <> ?"
		uq.args = append(uq.args, string(enums.InstanceStatusDeleted))
	}
	if endReasonPrefix != "" {
		uq.conditions += " AND end_reason LIKE ?"
		uq.args = append(uq.args, endReasonPrefix+"%")
	}
	for _, p := range endReasonNotPrefixes {
		if p == "" {
			continue
		}
		uq.conditions += " AND end_reason NOT LIKE ?"
		uq.args = append(uq.args, p+"%")
	}
	if keyword != "" {
		// 申请编号即实例ID，纳入 keyword 匹配
		kw := likeContains(keyword)
		uq.conditions += " AND (name LIKE ? OR business_key LIKE ? OR id LIKE ?)"
		uq.args = append(uq.args, kw, kw, kw)
	}
	if startTimeFrom != nil {
		uq.conditions += " AND created_at >= ?"
		uq.args = append(uq.args, *startTimeFrom)
	}
	if startTimeTo != nil {
		uq.conditions += " AND created_at <= ?"
		uq.args = append(uq.args, *startTimeTo)
	}
	return uq
}

// GetInstancesUnionPagination 分页获取流程实例列表（合并运行时和历史表，支持多租户）
func (d *InstanceDAO) GetInstancesUnionPagination(ctx context.Context, tenantID, ProcessID, startUserID string, statuses []string, keyword string, startTimeFrom, startTimeTo *time.Time, limit, offset int, instanceID, businessKey, endReasonPrefix string, endReasonNotPrefixes ...string) ([]*model.WfInstance, int64, error) {
	if tenantID == "" {
		return nil, 0, errors.New("tenantID required")
	}
	uq := buildInstanceUnionQuery(tenantID, ProcessID, startUserID, statuses, keyword, startTimeFrom, startTimeTo, instanceID, businessKey, endReasonPrefix, endReasonNotPrefixes...)

	// Count SQL：归档在单事务内完成（建历史行+删活行同 tx），两表不会有同 ID 双行，
	// 故用 UNION ALL 免去 UNION 去重的全列比较排序开销。
	countSQL := fmt.Sprintf(`
		SELECT COUNT(*) FROM (
			%s %s
			UNION ALL
			%s %s
		) combined
	`, uq.runSQL, uq.conditions, uq.histSQL, uq.conditions)

	countArgs := append(uq.args, uq.args...)

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
		) combined
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`, uq.runSQL, uq.conditions, uq.histSQL, uq.conditions)

	listArgs := append(uq.args, uq.args...)
	listArgs = append(listArgs, limit, offset)

	var list []*model.WfInstance
	if err := db.Raw(listSQL, listArgs...).Scan(&list).Error; err != nil {
		return nil, 0, err
	}

	return list, total, nil
}

// InstanceStatusBucket 状态桶计数定义：Name 为结果键，Statuses/EndReasonPrefix/
// EndReasonNotPrefixes 描述桶的实例条件，与列表接口的实例状态过滤同构。
type InstanceStatusBucket struct {
	Name                 string
	Statuses             []string
	EndReasonPrefix      string
	EndReasonNotPrefixes []string
}

// bucketWhere 桶条件的 SQL 片段与参数（作用于合并后行集的 status/end_reason 列）
func bucketWhere(b InstanceStatusBucket) (string, []interface{}) {
	cond, args := "1=1", []interface{}{}
	if len(b.Statuses) > 0 {
		ph := strings.TrimSuffix(strings.Repeat("?,", len(b.Statuses)), ",")
		cond = "status IN (" + ph + ")"
		for _, s := range b.Statuses {
			args = append(args, s)
		}
		if b.EndReasonPrefix != "" {
			cond += " AND end_reason LIKE ?"
			args = append(args, b.EndReasonPrefix+"%")
		}
		for _, p := range b.EndReasonNotPrefixes {
			if p == "" {
				continue
			}
			cond += " AND end_reason NOT LIKE ?"
			args = append(args, p+"%")
		}
	}
	return cond, args
}

// bucketAlias 聚合列别名：桶名直接作别名可能撞数据库保留字（如 MySQL 的
// TERMINATED），统一加 s_ 前缀保证安全，扫描后再剥掉。
func bucketAlias(name string) (string, error) {
	if !isValidOrderBy(name) {
		return "", fmt.Errorf("invalid bucket name: %q", name)
	}
	return "s_" + name, nil
}

// stripBucketAlias 还原桶名（去掉 bucketAlias 加的安全前缀）
func stripBucketAlias(key string) string {
	return strings.TrimPrefix(key, "s_")
}

// rawCountToInt 聚合计数值转 int64（不同驱动可能回 int64/int/float64/[]byte/string）
func rawCountToInt(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case []byte:
		x, _ := strconv.ParseInt(string(n), 10, 64)
		return x
	case string:
		x, _ := strconv.ParseInt(n, 10, 64)
		return x
	}
	return 0
}

// scanBucketCounts 执行单行聚合并按列名转 map（列名带安全前缀，此处剥离还原桶名）
func scanBucketCounts(ctx context.Context, db *gorm.DB, sql string, args []interface{}) (map[string]int64, error) {
	var row map[string]interface{}
	if err := db.Raw(sql, args...).Scan(&row).Error; err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(row))
	for k, v := range row {
		out[stripBucketAlias(k)] = rawCountToInt(v)
	}
	return out, nil
}

// CountInstancesUnionByBuckets 与 GetInstancesUnionPagination 同口径（同表同条件、UNION ALL），
// 单次扫描按桶条件聚合计数；total 为未按状态过滤的全量。供列表接口搭车下发 chips 计数。
func (d *InstanceDAO) CountInstancesUnionByBuckets(ctx context.Context, tenantID, processID, startUserID, keyword string, startTimeFrom, startTimeTo *time.Time, buckets []InstanceStatusBucket) (map[string]int64, error) {
	if tenantID == "" {
		return nil, errors.New("tenantID required")
	}
	uq := buildInstanceUnionQuery(tenantID, processID, startUserID, nil, keyword, startTimeFrom, startTimeTo, "", "", "")

	// 参数顺序须与 SQL 文本一致：先外层 SELECT 的桶参数，再两个分支各一份条件参数
	selects := []string{"COUNT(*) AS total"}
	args := make([]interface{}, 0, len(uq.args)*2+len(buckets)*2)
	for _, b := range buckets {
		alias, err := bucketAlias(b.Name)
		if err != nil {
			return nil, err
		}
		cond, condArgs := bucketWhere(b)
		selects = append(selects, fmt.Sprintf("COALESCE(SUM(CASE WHEN %s THEN 1 ELSE 0 END), 0) AS %s", cond, alias))
		args = append(args, condArgs...)
	}
	args = append(args, uq.args...)
	args = append(args, uq.args...)

	sql := fmt.Sprintf(`SELECT %s FROM (%s %s UNION ALL %s %s) combined`,
		strings.Join(selects, ", "), uq.runSQL, uq.conditions, uq.histSQL, uq.conditions)
	return scanBucketCounts(ctx, d.Query.WfInstance.UnderlyingDB().WithContext(ctx), sql, args)
}

// CountTaskInstancesByBuckets 与 ListByTaskConditions 同口径（同表同 JOIN 同条件、UNION 去重），
// 按桶条件 COUNT(DISTINCT id) 一次聚合出各状态桶的实例数；total 为未按状态过滤的全量。
func (d *InstanceDAO) CountTaskInstancesByBuckets(ctx context.Context, req *dto.TaskQuery, buckets []InstanceStatusBucket) (map[string]int64, error) {
	if req == nil {
		req = &dto.TaskQuery{}
	}
	if req.TenantID == "" {
		return nil, errors.New("tenantID required")
	}
	tq := buildTaskInstanceQuery(req)

	// 参数顺序须与 SQL 文本一致：先外层 SELECT 的桶参数，再两个分支各一份条件参数
	selects := []string{"COUNT(DISTINCT id) AS total"}
	args := make([]interface{}, 0, len(tq.args)*2+len(buckets)*2)
	for _, b := range buckets {
		alias, err := bucketAlias(b.Name)
		if err != nil {
			return nil, err
		}
		cond, condArgs := bucketWhere(b)
		selects = append(selects, fmt.Sprintf("COUNT(DISTINCT CASE WHEN %s THEN id END) AS %s", cond, alias))
		args = append(args, condArgs...)
	}
	args = append(args, tq.args...)
	args = append(args, tq.args...)

	sql := fmt.Sprintf(`SELECT %s FROM (%s %s UNION %s %s) combined`,
		strings.Join(selects, ", "), tq.runSQL, tq.conditions, tq.histSQL, tq.conditions)
	return scanBucketCounts(ctx, d.Query.WfInstance.UnderlyingDB().WithContext(ctx), sql, args)
}
