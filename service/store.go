// service 层消费的存储窄接口：方法集为 service 层的实际调用，具体 *dao.XxxDAO
// 天然满足。替换实现（读写分离、缓存、测试替身）经此接缝接入；当前构造函数
// 固定装配内置 DAO，注入构造待真实需求出现再补。Underlying 提供事务入口所需
// 的 *query.Query（WithInstanceTx 据此开事务）——以方法而非字段暴露，接口
// 实现才不必背负 gorm gen 的查询束结构。}

package service

import (
	"context"
	"time"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/dto"
)

// TaskStore 运行任务表存储。
type TaskStore interface {
	Underlying() *query.Query
	Create(ctx context.Context, entity *model.WfTask) error
	Get(ctx context.Context, id string) (*model.WfTask, error)
	Update(ctx context.Context, entity *model.WfTask) error
	Delete(ctx context.Context, id string) error
	GetByProcessInstanceID(ctx context.Context, processInstanceID string) ([]*model.WfTask, error)
	GetByParentID(ctx context.Context, parentID string) ([]*model.WfTask, error)
	List(ctx context.Context, query *dto.TaskQuery) ([]*model.WfTask, int64, error)
	AggregateActiveByProcess(ctx context.Context, tenantID string, limit int) ([]*dao.BacklogAggRow, error)
	ListProcessNamesByID(ctx context.Context, ids []string) (map[string]string, error)
}

// HiTaskStore 历史任务表存储。
type HiTaskStore interface {
	Underlying() *query.Query
	Create(ctx context.Context, entity *model.WfHiTask) error
	Get(ctx context.Context, id string) (*model.WfTask, error)
	Delete(ctx context.Context, id string) error
	List(ctx context.Context, query *dto.TaskQuery) ([]*model.WfTask, int64, error)
}

// InstanceStore 运行实例表存储。
type InstanceStore interface {
	Underlying() *query.Query
	List(ctx context.Context, request *dto.ProcessInstanceQueryDTO) ([]*model.WfInstance, int64, error)
	Create(ctx context.Context, entity *model.WfInstance) error
	Get(ctx context.Context, id string) (*model.WfInstance, error)
	Update(ctx context.Context, entity *model.WfInstance) error
	GetByProcessID(ctx context.Context, ProcessID string, limit, offset int) ([]*model.WfInstance, int64, error)
	HasActiveByParentID(ctx context.Context, parentID string) (bool, error)
	SetCurrentActivity(ctx context.Context, id, activityKey string) error
	ListByTaskConditions(ctx context.Context, req *dto.TaskQuery) ([]*model.WfInstance, int64, error)
	GetInstancesUnionPagination(ctx context.Context, tenantID, ProcessID, startUserID string, statuses []string, keyword string, startTimeFrom, startTimeTo *time.Time, limit, offset int, instanceID, businessKey, endReasonPrefix string, endReasonNotPrefixes ...string) ([]*model.WfInstance, int64, error)
	CountInstancesUnionByBuckets(ctx context.Context, tenantID, processID, startUserID, keyword string, startTimeFrom, startTimeTo *time.Time, buckets []dao.InstanceStatusBucket) (map[string]int64, error)
	CountTaskInstancesByBuckets(ctx context.Context, req *dto.TaskQuery, buckets []dao.InstanceStatusBucket) (map[string]int64, error)
}

// HiInstanceStore 历史实例表存储。
type HiInstanceStore interface {
	Underlying() *query.Query
	List(ctx context.Context, request *dto.ProcessInstanceQueryDTO) ([]*model.WfInstance, int64, error)
	Get(ctx context.Context, id string) (*model.WfInstance, error)
	GetIncludingDeleted(ctx context.Context, id string) (*model.WfInstance, error)
	HasByParentID(ctx context.Context, parentID string) (bool, error)
	HasTerminatedByParentID(ctx context.Context, parentID string) (bool, error)
	Delete(ctx context.Context, id string) error
}

// ProcessStore 流程定义表存储。
type ProcessStore interface {
	Underlying() *query.Query
	List(ctx context.Context, request *dto.ProcessQueryRequest) ([]*model.WfProcess, int64, error)
	Get(ctx context.Context, id string) (*model.WfProcess, error)
	GetByIDs(ctx context.Context, ids []string) ([]*model.WfProcess, error)
	GetByKeyAndVersion(ctx context.Context, tenantID, processKey string, version int32) (*model.WfProcess, error)
	GetLatestByKey(ctx context.Context, tenantID, processKey string) (*model.WfProcess, error)
	Update(ctx context.Context, entity *model.WfProcess) error
	Delete(ctx context.Context, tenantID, id string) error
	CountActiveReferencingForm(ctx context.Context, tenantID, formKey string) (int64, error)
}

// TaskAssigneeStore 候选池表存储。
type TaskAssigneeStore interface {
	Underlying() *query.Query
	CreateBatch(ctx context.Context, entities []*model.WfTaskAssignee) error
	GetByTaskID(ctx context.Context, tenantID, taskID string) ([]*model.WfTaskAssignee, error)
	GetByInstanceAndDefKey(ctx context.Context, tenantID, processInstanceID, taskDefKey string) ([]*model.WfTaskAssignee, error)
	DeleteByTaskAndEntities(ctx context.Context, tenantID, taskID, entityType string, entityIDs []string) error
	CountCandidateTasks(ctx context.Context, tenantID, userID string, roleIDs, deptIDs, statuses []string, createdAfter, dueBefore *time.Time) (int64, error)
}

// TaskCommentStore 任务评论表存储。
type TaskCommentStore interface {
	Underlying() *query.Query
	Create(ctx context.Context, comment *model.WfTaskComment) error
	ListByTaskID(ctx context.Context, taskID string) ([]*model.WfTaskComment, error)
}

// 编译期锁定：具体 DAO 必须满足对应窄接口，宿主自有实现与内置实现同契约。
var (
	_ TaskStore         = (*dao.TaskDAO)(nil)
	_ HiTaskStore       = (*dao.HiTaskDAO)(nil)
	_ InstanceStore     = (*dao.InstanceDAO)(nil)
	_ HiInstanceStore   = (*dao.HiInstanceDAO)(nil)
	_ ProcessStore      = (*dao.ProcessDAO)(nil)
	_ TaskAssigneeStore = (*dao.TaskAssigneeDAO)(nil)
	_ TaskCommentStore  = (*dao.TaskCommentDAO)(nil)
)
