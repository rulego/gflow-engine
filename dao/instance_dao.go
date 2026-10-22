package dao

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rulego/gflow-engine/types/constants"
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

// GetActiveByBusinessKey 精确等值查询活表中占用该 businessKey 的实例（不限流程定义）。
// 活表残留状态即 draft/active/suspended（终态归档时活表删行、businessKey 释放），
// 三者皆占用唯一索引 uq_wf_instance_tenant_bizkey。无占用返回 (nil, nil)。
func (d *InstanceDAO) GetActiveByBusinessKey(ctx context.Context, tenantID, businessKey string) (*model.WfInstance, error) {
	if tenantID == "" || businessKey == "" {
		return nil, fmt.Errorf("tenantID and businessKey cannot be empty")
	}
	q := d.Query.WfInstance
	instance, err := q.WithContext(ctx).
		Where(q.TenantID.Eq(tenantID)).
		Where(q.BusinessKey.Eq(businessKey)).
		Where(q.Status.In(
			string(enums.InstanceStatusDraft),
			string(enums.InstanceStatusActive),
			string(enums.InstanceStatusSuspended),
		)).
		First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return instance, nil
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

// taskInstanceQuery 任务维度实例 UNION 查询的分支材料。条件按所属表拆成
// 实例条件（i.*）与任务条件（t.*）两段；任务条件再分共享段（两表同构列）
// 与 run/hist 各自的受理人段——prev_assignee 列仅历史表有，运行分支不得引用。
type taskInstanceQuery struct {
	runSQL, histSQL, traceSQL      string
	instConditions, taskConditions string
	// runTaskCond/histTaskCond 是受理人条件在运行/历史分支各自的文本，
	// 其余任务条件（状态/节点/审批类型）两表同构、进共享段
	runTaskCond, histTaskCond string
	runTaskArgs, histTaskArgs []interface{}
	instArgs, taskArgs        []interface{}
	traceUser                 string
}

// buildTaskInstanceQuery 构建任务维度实例查询的条件。三个分支：运行时（活实例⋈
// 活任务）、历史（历史实例⋈历史任务）、轨迹（活实例⋈历史任务，TraceUser
// 非空时启用）——转办/改派轨迹行落历史表，实例活跃期内只有第三分支能命中。
func buildTaskInstanceQuery(req *dto.TaskQuery) taskInstanceQuery {
	// TraceUser 的轨迹行 OR 只挂在历史侧任务条件上，运行/历史分支的 assignee
	// 匹配却以 Assignee 为准——只传 TraceUser 不传 Assignee 时受理人条件会
	// 缺位（返回全量实例），入口归一化兜住调用口径
	if req.TraceUser != "" && req.Assignee == "" {
		req.Assignee = req.TraceUser
	}
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
		traceSQL: `
			SELECT i.*
			FROM wf_instance i
			JOIN wf_hi_task t ON i.id = t.process_instance_id
			WHERE 1=1
		`,
	}

	// 构建条件
	if req == nil {
		req = &dto.TaskQuery{}
	}
	// —— 实例条件（i.*）——
	if req.TenantID != "" {
		tq.instConditions += " AND i.tenant_id = ?"
		tq.instArgs = append(tq.instArgs, req.TenantID)
	}
	// 已删实例（删除时历史行标记 deleted）不进任何任务维度列表：
	// 本条件由全部分支共用，任一分支命中都被排除
	tq.instConditions += " AND i.status <> ?"
	tq.instArgs = append(tq.instArgs, string(enums.InstanceStatusDeleted))
	if len(req.InstanceStatuses) > 0 {
		tq.instConditions += " AND i.status IN (?)"
		tq.instArgs = append(tq.instArgs, req.InstanceStatuses)
	}
	if req.EndReasonPrefix != "" {
		tq.instConditions += " AND i.end_reason LIKE ?"
		tq.instArgs = append(tq.instArgs, req.EndReasonPrefix+"%")
	}
	for _, p := range req.EndReasonNotPrefixes {
		if p == "" {
			continue
		}
		tq.instConditions += " AND (i.end_reason IS NULL OR i.end_reason NOT LIKE ?)"
		tq.instArgs = append(tq.instArgs, p+"%")
	}
	if req.Keyword != "" {
		// keyword 命中申请标题 / 业务键 / 编号（实例ID）/ 申请人（StartUserIDs，宿主按姓名解析）
		tq.instConditions += " AND (i.name LIKE ? OR i.business_key LIKE ? OR i.id LIKE ?"
		kw := likeContains(req.Keyword)
		tq.instArgs = append(tq.instArgs, kw, kw, kw)
		if len(req.StartUserIDs) > 0 {
			ph := strings.TrimSuffix(strings.Repeat("?,", len(req.StartUserIDs)), ",")
			tq.instConditions += " OR i.start_user_id IN (" + ph + ")"
			for _, id := range req.StartUserIDs {
				tq.instArgs = append(tq.instArgs, id)
			}
		}
		tq.instConditions += ")"
	}
	// —— 任务条件（t.*）——
	if req.Assignee != "" {
		// 候选维度：assignee=user OR (Pending 未签收 且 user 在候选人池 wf_task_assignee)。
		// 候选池匹配 person（entity_id=user）、role（entity_id IN userRoleIDs）与
		// department（entity_id IN userDeptIDs，dept 候选任务落库的是部门实体）。
		// 候选池表只有运行侧语义，两分支同条件即可
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
			cond := " AND (t.assignee = ? OR (t.status = ? AND EXISTS (SELECT 1 FROM wf_task_assignee ca WHERE ca.task_id = t.id AND " + poolCond + ")))"
			args := append([]interface{}{req.Assignee, string(enums.TaskStatusPending), req.CandidateUser}, poolArgs...)
			tq.runTaskCond, tq.histTaskCond = cond, cond
			tq.runTaskArgs, tq.histTaskArgs = args, args
		} else if req.TraceUser != "" {
			// 已办维度：assignee=user OR 流转轨迹行（task_type IN (forward,reassign)
			// 且 prev_assignee=user）。转办/改派都原地换受理人，转办人与被改派的
			// 原受理人在 assignee 口径上不可达，凭轨迹行进已办列表；委派/归还行
			// 刻意不进（存续窗口内详情可读即可）。prev_assignee 仅历史表有——
			// 运行分支只按 assignee 匹配（轨迹行从不落运行表，语义不变），
			// 历史分支带轨迹行 OR，活跃实例的轨迹行由第三分支命中
			tq.runTaskCond = " AND t.assignee = ?"
			tq.runTaskArgs = []interface{}{req.Assignee}
			tq.histTaskCond = " AND (t.assignee = ? OR (t.task_type IN (?,?) AND t.prev_assignee = ?))"
			tq.histTaskArgs = []interface{}{req.Assignee, string(constants.TaskTypeForward), string(constants.TaskTypeReassign), req.TraceUser}
		} else {
			cond, args := " AND t.assignee = ?", []interface{}{req.Assignee}
			tq.runTaskCond, tq.histTaskCond = cond, cond
			tq.runTaskArgs, tq.histTaskArgs = args, args
		}
	}
	if len(req.Status) > 0 {
		tq.taskConditions += " AND t.status IN (?)"
		tq.taskArgs = append(tq.taskArgs, req.Status)
	}
	if req.TaskDefKey != "" {
		tq.taskConditions += " AND t.task_def_key = ?"
		tq.taskArgs = append(tq.taskArgs, req.TaskDefKey)
	}
	if req.ApprovalType != "" {
		tq.taskConditions += " AND t.approval_type = ?"
		tq.taskArgs = append(tq.taskArgs, req.ApprovalType)
	}
	tq.traceUser = req.TraceUser
	return tq
}

// branches 返回 UNION 各分支的完整 SQL 与参数（参数顺序与各自 SQL 文本一致）。
// 运行时/历史分支拼装全部条件（受理人条件各用各的）；轨迹分支只认轨迹行——
// 不带任务条件，避免归档的 returned 等常规历史行借"活实例⋈历史任务"组合
// 混入本来进不了的列表。
func (tq taskInstanceQuery) branches() ([]string, [][]interface{}) {
	runCond := tq.instConditions + tq.taskConditions + tq.runTaskCond
	runArgs := append(append(append([]interface{}{}, tq.instArgs...), tq.taskArgs...), tq.runTaskArgs...)
	histCond := tq.instConditions + tq.taskConditions + tq.histTaskCond
	histArgs := append(append(append([]interface{}{}, tq.instArgs...), tq.taskArgs...), tq.histTaskArgs...)
	sqls := []string{tq.runSQL + runCond, tq.histSQL + histCond}
	argsList := [][]interface{}{runArgs, histArgs}
	if tq.traceUser != "" {
		traceCond := tq.instConditions + " AND t.task_type IN (?,?) AND t.prev_assignee = ?"
		traceArgs := append(append([]interface{}{}, tq.instArgs...), string(constants.TaskTypeForward), string(constants.TaskTypeReassign), tq.traceUser)
		sqls = append(sqls, tq.traceSQL+traceCond)
		argsList = append(argsList, traceArgs)
	}
	return sqls, argsList
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
	branchSQLs, branchArgs := tq.branches()

	// 合并各来源分支。一个实例可能命中多个任务、甚至同时出现在多个子查询里，
	// 用 UNION + COUNT(DISTINCT id) 去重，保证分页总数准确。
	unionSQL := strings.Join(branchSQLs, "\n UNION \n")
	unionArgs := make([]interface{}, 0, 8)
	for _, ba := range branchArgs {
		unionArgs = append(unionArgs, ba...)
	}

	// 构建 Count SQL。内联视图别名不带 AS，部分方言不允许。
	countSQL := fmt.Sprintf("SELECT COUNT(DISTINCT id) FROM (%s) combined", unionSQL)

	var total int64
	db := d.Query.WfInstance.UnderlyingDB().WithContext(ctx)
	if err := db.Raw(countSQL, unionArgs...).Scan(&total).Error; err != nil {
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

	listSQL := fmt.Sprintf("SELECT * FROM (%s) combined ORDER BY %s %s LIMIT ? OFFSET ?",
		unionSQL, orderBy, orderDir)

	listArgs := append(append([]interface{}{}, unionArgs...), size, offset)

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
func buildInstanceUnionQuery(tenantID string, processIDs []string, startUserID string, statuses []string, keyword string, startTimeFrom, startTimeTo *time.Time, instanceID, businessKey, endReasonPrefix string, endReasonNotPrefixes ...string) instanceUnionQuery {
	uq := instanceUnionQuery{
		runSQL:  "SELECT * FROM wf_instance WHERE 1=1",
		histSQL: "SELECT * FROM wf_hi_instance WHERE 1=1",
	}

	if tenantID != "" {
		uq.conditions += " AND tenant_id = ?"
		uq.args = append(uq.args, tenantID)
	}
	if len(processIDs) > 0 {
		placeholders := make([]string, 0, len(processIDs))
		for _, id := range processIDs {
			if id == "" {
				continue
			}
			placeholders = append(placeholders, "?")
			uq.args = append(uq.args, id)
		}
		if len(placeholders) > 0 {
			uq.conditions += " AND process_id IN (" + strings.Join(placeholders, ",") + ")"
		}
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
		uq.conditions += " AND (end_reason IS NULL OR end_reason NOT LIKE ?)"
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

// GetInstancesUnionPagination 分页获取流程实例列表（合并运行时和历史表，支持多租户）。
// processIDs 按流程定义 ID 集合过滤（IN 语义）。
func (d *InstanceDAO) GetInstancesUnionPagination(ctx context.Context, tenantID string, processIDs []string, startUserID string, statuses []string, keyword string, startTimeFrom, startTimeTo *time.Time, limit, offset int, instanceID, businessKey, endReasonPrefix string, endReasonNotPrefixes ...string) ([]*model.WfInstance, int64, error) {
	if tenantID == "" {
		return nil, 0, errors.New("tenantID required")
	}
	uq := buildInstanceUnionQuery(tenantID, processIDs, startUserID, statuses, keyword, startTimeFrom, startTimeTo, instanceID, businessKey, endReasonPrefix, endReasonNotPrefixes...)

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
			cond += " AND (end_reason IS NULL OR end_reason NOT LIKE ?)"
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
	uq := buildInstanceUnionQuery(tenantID, []string{processID}, startUserID, nil, keyword, startTimeFrom, startTimeTo, "", "", "")

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

	// 参数顺序须与 SQL 文本一致：先外层 SELECT 的桶参数，再各分支的条件参数
	branchSQLs, branchArgs := tq.branches()
	selects := []string{"COUNT(DISTINCT id) AS total"}
	args := make([]interface{}, 0, 16)
	for _, b := range buckets {
		alias, err := bucketAlias(b.Name)
		if err != nil {
			return nil, err
		}
		cond, condArgs := bucketWhere(b)
		selects = append(selects, fmt.Sprintf("COUNT(DISTINCT CASE WHEN %s THEN id END) AS %s", cond, alias))
		args = append(args, condArgs...)
	}
	for _, ba := range branchArgs {
		args = append(args, ba...)
	}

	sql := fmt.Sprintf("SELECT %s FROM (%s) combined",
		strings.Join(selects, ", "), strings.Join(branchSQLs, "\n UNION \n"))
	return scanBucketCounts(ctx, d.Query.WfInstance.UnderlyingDB().WithContext(ctx), sql, args)
}
