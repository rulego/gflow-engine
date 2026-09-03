package dao

import (
	"context"
	"fmt"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
)

// TaskAssigneeDAO wf_task_assignee 数据访问对象。
// 存原始角色/部门引用，查询时经 IdentityService 展开（动态语义）。
type TaskAssigneeDAO struct {
	Query *query.Query
}

// NewTaskAssigneeDAO 创建 TaskAssigneeDAO 实例
func NewTaskAssigneeDAO() *TaskAssigneeDAO {
	return &TaskAssigneeDAO{
		Query: query.Q,
	}
}

// NewTaskAssigneeDAOWithQuery 创建带 Query 参数的 TaskAssigneeDAO 实例
func NewTaskAssigneeDAOWithQuery(query *query.Query) *TaskAssigneeDAO {
	return &TaskAssigneeDAO{
		Query: query,
	}
}

// Create 创建单条候选人记录
func (d *TaskAssigneeDAO) Create(ctx context.Context, entity *model.WfTaskAssignee) error {
	if entity == nil {
		return fmt.Errorf("task_assignee cannot be nil")
	}
	q := d.Query.WfTaskAssignee
	return q.WithContext(ctx).Create(entity)
}

// CreateBatch 批量创建候选人记录
func (d *TaskAssigneeDAO) CreateBatch(ctx context.Context, entities []*model.WfTaskAssignee) error {
	if len(entities) == 0 {
		return nil
	}
	q := d.Query.WfTaskAssignee
	return q.WithContext(ctx).Create(entities...)
}

// GetByTaskID 按任务 ID 查询候选人（带租户过滤）
func (d *TaskAssigneeDAO) GetByTaskID(ctx context.Context, tenantID, taskID string) ([]*model.WfTaskAssignee, error) {
	if taskID == "" {
		return nil, fmt.Errorf("taskID cannot be empty")
	}
	q := d.Query.WfTaskAssignee
	query := q.WithContext(ctx).Where(q.TaskID.Eq(taskID))
	if tenantID != "" {
		query = query.Where(q.TenantID.Eq(tenantID))
	}
	return query.Find()
}

// GetByInstanceAndDefKey 按流程实例 + 节点定义 Key 查询候选人。
// 通过 JOIN wf_task 定位同实例同节点的任务候选池。
func (d *TaskAssigneeDAO) GetByInstanceAndDefKey(ctx context.Context, tenantID, processInstanceID, taskDefKey string) ([]*model.WfTaskAssignee, error) {
	if processInstanceID == "" || taskDefKey == "" {
		return nil, fmt.Errorf("processInstanceID and taskDefKey cannot be empty")
	}
	db := d.Query.WfTaskAssignee.UnderlyingDB().WithContext(ctx)
	var rows []*model.WfTaskAssignee
	q := db.Table(model.TableNameWfTaskAssignee+" AS ca").
		Joins("JOIN "+model.TableNameWfTask+" AS t ON t.id = ca.task_id").
		Where("t.process_instance_id = ?", processInstanceID).
		Where("t.task_def_key = ?", taskDefKey)
	if tenantID != "" {
		q = q.Where("ca.tenant_id = ?", tenantID)
	}
	// Scan 查询不到时返回空切片而非 ErrRecordNotFound，无需判 not found
	if err := q.Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("failed to get assignees by instance and def key: %w", err)
	}
	return rows, nil
}

// DeleteByTaskAndEntity 按任务 ID + 实体（type+id）删除候选人（带租户过滤）
func (d *TaskAssigneeDAO) DeleteByTaskAndEntity(ctx context.Context, tenantID, taskID, entityType, entityID string) error {
	if taskID == "" || entityType == "" || entityID == "" {
		return fmt.Errorf("taskID, entityType and entityID cannot be empty")
	}
	q := d.Query.WfTaskAssignee
	query := q.WithContext(ctx).
		Where(q.TaskID.Eq(taskID)).
		Where(q.EntityType.Eq(entityType)).
		Where(q.EntityID.Eq(entityID))
	if tenantID != "" {
		query = query.Where(q.TenantID.Eq(tenantID))
	}
	_, err := query.Delete()
	return err
}

// DeleteByTaskAndEntities 批量删除同任务同类型的多个候选（单 SQL 原子，带租户过滤）
func (d *TaskAssigneeDAO) DeleteByTaskAndEntities(ctx context.Context, tenantID, taskID, entityType string, entityIDs []string) error {
	if taskID == "" || entityType == "" || len(entityIDs) == 0 {
		return fmt.Errorf("taskID, entityType and entityIDs cannot be empty")
	}
	q := d.Query.WfTaskAssignee
	query := q.WithContext(ctx).
		Where(q.TaskID.Eq(taskID)).
		Where(q.EntityType.Eq(entityType)).
		Where(q.EntityID.In(entityIDs...))
	if tenantID != "" {
		query = query.Where(q.TenantID.Eq(tenantID))
	}
	_, err := query.Delete()
	return err
}

// CountCandidateTasks 统计用户作为候选人（person、role 成员或 department 成员）的待办任务数。
// JOIN wf_task 过滤状态/创建/截止时间；候选任务无 assignee，与 assignee 计数不重叠。
func (d *TaskAssigneeDAO) CountCandidateTasks(ctx context.Context, tenantID, userID string, roleIDs, deptIDs, statuses []string, createdAfter, dueBefore *time.Time) (int64, error) {
	if userID == "" {
		return 0, nil
	}
	db := d.Query.WfTaskAssignee.UnderlyingDB().WithContext(ctx)
	cond := "(ca.entity_type = 'person' AND ca.entity_id = ?)"
	args := []interface{}{userID}
	if len(roleIDs) > 0 {
		cond += " OR (ca.entity_type = 'role' AND ca.entity_id IN (?))"
		args = append(args, roleIDs)
	}
	if len(deptIDs) > 0 {
		cond += " OR (ca.entity_type = 'department' AND ca.entity_id IN (?))"
		args = append(args, deptIDs)
	}
	q := db.Table(model.TableNameWfTaskAssignee+" AS ca").
		Joins("JOIN "+model.TableNameWfTask+" AS t ON t.id = ca.task_id").
		Where("("+cond+")", args...).
		Where("(t.assignee IS NULL OR t.assignee = '')")
	if tenantID != "" {
		q = q.Where("ca.tenant_id = ?", tenantID)
	}
	if len(statuses) > 0 {
		q = q.Where("t.status IN (?)", statuses)
	}
	if createdAfter != nil {
		q = q.Where("t.created_at >= ?", *createdAfter)
	}
	if dueBefore != nil {
		q = q.Where("t.due_date < ?", *dueBefore)
	}
	var count int64
	if err := q.Distinct("ca.task_id").Count(&count).Error; err != nil {
		return 0, fmt.Errorf("failed to count candidate tasks: %w", err)
	}
	return count, nil
}
