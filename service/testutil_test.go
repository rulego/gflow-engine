// 包内测试共享的轻量替身：引擎、运行时服务与 ID 生成器。
// 各测试文件自带 SQLite 内存库 helper（建表 DDL 按需裁剪），本文件只放无 DB 依赖的替身。

package service

import (
	"context"
	"sync"
	"time"

	"gorm.io/gorm"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/utils/lock"

	"github.com/rulego/rulego/api/types"
)

// testSeqIDGen 测试用 ID 生成器：时间戳+序号，避免并发碰撞。
type testSeqIDGen struct {
	mu sync.Mutex
	n  int
}

func (g *testSeqIDGen) next() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.n++
	return time.Now().Format("150405.000000000") + "-" + string(rune('a'+g.n%26))
}

func (g *testSeqIDGen) GenerateID() string          { return g.next() }
func (g *testSeqIDGen) GenerateInstanceID() string  { return g.next() }
func (g *testSeqIDGen) GenerateTaskID() string      { return g.next() }
func (g *testSeqIDGen) GenerateProcessID() string   { return g.next() }
func (g *testSeqIDGen) GenerateBusinessKey() string { return g.next() }

// testRuntimeDouble 最小 RuntimeService 替身：
// GetProcessInstance 恒返回 (nil, nil)，用于模拟"实例不存在"。
type testRuntimeDouble struct{}

func (r *testRuntimeDouble) StartProcessInstanceByKey(ctx context.Context, initiator Actor, processDefinitionKey, businessKey string, variables map[string]interface{}, opts ...StartOption) (string, error) {
	return "", nil
}
func (r *testRuntimeDouble) StartProcessInstanceByID(ctx context.Context, initiator Actor, processDefinitionID, businessKey string, variables map[string]interface{}, opts ...StartOption) (string, error) {
	return "", nil
}
func (r *testRuntimeDouble) GetProcessInstance(ctx context.Context, actor Actor, id string) (*model.WfInstance, error) {
	return nil, nil
}
func (r *testRuntimeDouble) GetProcessInstanceList(ctx context.Context, actor Actor, query *dto.ProcessInstanceQueryDTO) ([]*model.WfInstance, int64, error) {
	return nil, 0, nil
}
func (r *testRuntimeDouble) GetProcessInstanceUnionList(ctx context.Context, actor Actor, query *dto.ProcessInstanceQueryDTO) ([]*model.WfInstance, int64, error) {
	return nil, 0, nil
}
func (r *testRuntimeDouble) UpdateInstanceCurrentActivity(ctx context.Context, id, activityKey string) error {
	return nil
}
func (r *testRuntimeDouble) GetStuckProcessInstances(ctx context.Context, tenantID string) ([]*model.WfInstance, error) {
	return nil, nil
}
func (r *testRuntimeDouble) GetExpiredDelayTasks(ctx context.Context, tenantID string) ([]*model.WfTask, error) {
	return nil, nil
}
func (r *testRuntimeDouble) RescueExpiredDelayTask(ctx context.Context, actor Actor, taskID string) error {
	return nil
}
func (r *testRuntimeDouble) ReDriveProcessInstance(ctx context.Context, actor Actor, id string) error {
	return nil
}
func (r *testRuntimeDouble) TerminateProcessInstance(ctx context.Context, actor Actor, id, reason string) error {
	return nil
}
func (r *testRuntimeDouble) DeleteProcessInstance(ctx context.Context, actor Actor, id, reason string) error {
	return nil
}
func (r *testRuntimeDouble) DeleteProcessInstances(ctx context.Context, actor Actor, ids []string, reason string) error {
	return nil
}
func (r *testRuntimeDouble) SuspendProcessInstance(ctx context.Context, actor Actor, id string) error {
	return nil
}
func (r *testRuntimeDouble) ActivateProcessInstance(ctx context.Context, actor Actor, id string) error {
	return nil
}
func (r *testRuntimeDouble) GetProcessInstanceVariables(ctx context.Context, actor Actor, id string) (map[string]interface{}, error) {
	return nil, nil
}
func (r *testRuntimeDouble) GetProcessInstanceVariable(ctx context.Context, actor Actor, id, name string) (interface{}, error) {
	return nil, nil
}
func (r *testRuntimeDouble) SetProcessInstanceVariables(ctx context.Context, actor Actor, id string, vars map[string]interface{}) error {
	return nil
}
func (r *testRuntimeDouble) SetProcessInstanceVariable(ctx context.Context, actor Actor, id, name string, value interface{}) error {
	return nil
}
func (r *testRuntimeDouble) RemoveProcessInstanceVariable(ctx context.Context, actor Actor, id, name string) error {
	return nil
}
func (r *testRuntimeDouble) RestoreProcessInstance(ctx context.Context, actor Actor, id string) error {
	return nil
}
func (r *testRuntimeDouble) RestoreAllProcessInstances(ctx context.Context, actor Actor) error {
	return nil
}
func (r *testRuntimeDouble) GetExecution(ctx context.Context, processID string) (types.RuleEngine, error) {
	return nil, nil
}
func (r *testRuntimeDouble) PreloadChain(tenantID, processID, processDef string) error { return nil }
func (r *testRuntimeDouble) EvictStaleChain(ctx context.Context, tenantID, processID string) {
}
func (r *testRuntimeDouble) ResolveSubProcessTarget(tenantID, ruleChainID string) (string, bool) {
	return "", false
}
func (r *testRuntimeDouble) StartSubProcessInstance(ctx context.Context, parentInstanceID, parentNodeID, childProcessDefID string, variables map[string]interface{}) (string, error) {
	return "", nil
}
func (r *testRuntimeDouble) SubProcessChildState(parentInstanceID string) (bool, bool) {
	return false, false
}
func (r *testRuntimeDouble) SubProcessChildTerminated(parentInstanceID string) bool { return false }
func (r *testRuntimeDouble) ExecuteNext(ctx context.Context, id, node string, vars map[string]interface{}) error {
	return nil
}
func (r *testRuntimeDouble) ForceResumeInstance(ctx context.Context, actor Actor, id string) error {
	return nil
}
func (r *testRuntimeDouble) RestartProcessInstance(ctx context.Context, actor Actor, id, activityID string) (string, error) {
	return "", nil
}
func (r *testRuntimeDouble) CompleteProcessInstance(ctx context.Context, actor Actor, id, reason string) error {
	return nil
}
func (r *testRuntimeDouble) GetTodoProcessInstanceList(ctx context.Context, actor Actor, page, pageSize int, keyword string, startUserIDs []string, orderBy string, orderDesc bool) ([]*model.WfInstance, int64, error) {
	return nil, 0, nil
}
func (r *testRuntimeDouble) GetDoneProcessInstanceList(ctx context.Context, actor Actor, page, pageSize int, keyword string, startUserIDs []string, orderBy string, orderDesc bool, instanceStatus string) ([]*model.WfInstance, int64, error) {
	return nil, 0, nil
}
func (r *testRuntimeDouble) GetCcProcessInstanceList(ctx context.Context, actor Actor, page, pageSize int, keyword string, startUserIDs []string, orderBy string, orderDesc bool) ([]*model.WfInstance, int64, error) {
	return nil, 0, nil
}
func (r *testRuntimeDouble) GetMyApplicationsProcessInstanceList(ctx context.Context, actor Actor, page, pageSize int, keyword, orderBy string, orderDesc bool, instanceStatus string) ([]*model.WfInstance, int64, error) {
	return nil, 0, nil
}
func (r *testRuntimeDouble) CountMyApplications(ctx context.Context, actor Actor, from, to *time.Time) (int64, error) {
	return 0, nil
}
func (r *testRuntimeDouble) CountMyApplicationsByBuckets(ctx context.Context, actor Actor, keyword string) (map[string]int64, error) {
	return nil, nil
}
func (r *testRuntimeDouble) CountDoneByBuckets(ctx context.Context, actor Actor, keyword string, startUserIDs []string) (map[string]int64, error) {
	return nil, nil
}
func (r *testRuntimeDouble) GetProcessInstancesByTaskConditions(ctx context.Context, req *dto.TaskQuery) ([]*model.WfInstance, int64, error) {
	return nil, 0, nil
}
func (r *testRuntimeDouble) GetProcessInstanceDetail(ctx context.Context, actor Actor, id string) (*dto.InstanceDetailResponse, error) {
	return nil, nil
}

// testEngineDouble 最小 WorkflowEngine 替身：可注入事件监听器与运行时替身。
type testEngineDouble struct {
	listener TaskEventListener
	runtime  RuntimeService
}

func (e *testEngineDouble) GetDB() *gorm.DB                                 { return nil }
func (e *testEngineDouble) GetTaskService() TaskService                     { return nil }
func (e *testEngineDouble) GetProcessService() ProcessService               { return nil }
func (e *testEngineDouble) GetRuntimeService() RuntimeService               { return e.runtime }
func (e *testEngineDouble) GetHistoryService() HistoryService               { return nil }
func (e *testEngineDouble) GetIdentityService() IdentityService             { return nil }
func (e *testEngineDouble) GetLocker() lock.Locker                          { return nil }
func (e *testEngineDouble) Start(context.Context) error                     { return nil }
func (e *testEngineDouble) Stop(context.Context) error                      { return nil }
func (e *testEngineDouble) IsRunning() bool                                 { return false }
func (e *testEngineDouble) GetName() string                                 { return "test-double" }
func (e *testEngineDouble) GetVersion() string                              { return "" }
func (e *testEngineDouble) GetIDGenerator() IDGenerator                     { return nil }
func (e *testEngineDouble) GetRuleChainExecutor() RuleChainExecutor         { return nil }
func (e *testEngineDouble) GetCCTaskCreatedListener() CCTaskCreatedListener { return nil }
func (e *testEngineDouble) GetTaskEventListener() TaskEventListener         { return e.listener }
func (e *testEngineDouble) GetTaskServiceInternal() TaskServiceInternal     { return nil }
func (e *testEngineDouble) GetRuntimeServiceInternal() RuntimeServiceInternal {
	return asRuntimeServiceInternal(e.runtime)
}
func (e *testEngineDouble) CountTenantData(ctx context.Context, tenantID string) (map[string]int64, error) {
	return nil, nil
}

// asRuntimeServiceInternal 断言公共 RuntimeService 的实现同时满足内部接口
// （*RuntimeServiceImpl 恒满足；测试替身只实现公共面时返回 nil）。
func asRuntimeServiceInternal(rs RuntimeService) RuntimeServiceInternal {
	if r, ok := rs.(RuntimeServiceInternal); ok {
		return r
	}
	return nil
}

// asTaskServiceInternal 断言公共 TaskService 的实现同时满足内部接口
// （*TaskServiceImpl 恒满足；测试替身只实现公共面时返回 nil）。
func asTaskServiceInternal(ts TaskService) TaskServiceInternal {
	if t, ok := ts.(TaskServiceInternal); ok {
		return t
	}
	return nil
}
