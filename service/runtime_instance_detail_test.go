package service

// Tests for runtime_instance_detail.go: GetProcessInstanceDetail rejects
// same-tenant users who are not participants (IDOR protection).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/gflow-engine/utils/lock"
)

// secFixEngine 提供真实 TaskService/RuntimeService 的最小 mock 引擎。
// GetTaskService / GetRuntimeService 返回注入的真实 service；
// GetProcessService 返回 noop（使详情组装跳过 ProcessDef 分支）。
type secFixEngine struct {
	taskSvc    TaskService
	runtimeSvc RuntimeService
}

func (e *secFixEngine) GetDB() *gorm.DB                                 { return nil }
func (e *secFixEngine) GetTaskService() TaskService                     { return e.taskSvc }
func (e *secFixEngine) GetProcessService() ProcessService               { return noopSecFixProcessService{} }
func (e *secFixEngine) GetRuntimeService() RuntimeService               { return e.runtimeSvc }
func (e *secFixEngine) GetHistoryService() HistoryService               { return nil }
func (e *secFixEngine) GetIdentityService() IdentityService             { return nil }
func (e *secFixEngine) GetLocker() lock.Locker                          { return nil }
func (e *secFixEngine) Start(context.Context) error                     { return nil }
func (e *secFixEngine) Stop(context.Context) error                      { return nil }
func (e *secFixEngine) IsRunning() bool                                 { return false }
func (e *secFixEngine) GetName() string                                 { return "secfix-test" }
func (e *secFixEngine) GetVersion() string                              { return "" }
func (e *secFixEngine) GetIDGenerator() IDGenerator                     { return nil }
func (e *secFixEngine) GetRuleChainExecutor() RuleChainExecutor         { return nil }
func (e *secFixEngine) GetCCTaskCreatedListener() CCTaskCreatedListener { return nil }
func (e *secFixEngine) GetTaskEventListener() TaskEventListener         { return nil }
func (e *secFixEngine) GetTaskServiceInternal() TaskServiceInternal {
	return asTaskServiceInternal(e.taskSvc)
}
func (e *secFixEngine) GetRuntimeServiceInternal() RuntimeServiceInternal {
	return asRuntimeServiceInternal(e.runtimeSvc)
}
func (e *secFixEngine) CountTenantData(ctx context.Context, tenantID string) (map[string]int64, error) {
	return nil, nil
}

// noopSecFixProcessService 使 GetProcessInstanceDetail 在详情组装时跳过 ProcessDef 分支
// （Get 返回 nil,nil → procDef==nil → 不进入 definitionJSON 解析）。
type noopSecFixProcessService struct{}

func (noopSecFixProcessService) Deploy(context.Context, Actor, *model.WfProcess, bool) (*model.WfProcess, error) {
	return nil, nil
}
func (noopSecFixProcessService) Create(context.Context, Actor, *model.WfProcess, bool) (*model.WfProcess, error) {
	return nil, nil
}
func (noopSecFixProcessService) Update(context.Context, Actor, *model.WfProcess) error { return nil }
func (noopSecFixProcessService) List(context.Context, Actor, *dto.ProcessQueryRequest) ([]*model.WfProcess, int64, error) {
	return nil, 0, nil
}
func (noopSecFixProcessService) Get(context.Context, string) (*model.WfProcess, error) {
	return nil, nil
}
func (noopSecFixProcessService) GetByKey(context.Context, string, string) (*model.WfProcess, error) {
	return nil, nil
}
func (noopSecFixProcessService) GetByKeyAndVersion(context.Context, string, string, int32) (*model.WfProcess, error) {
	return nil, nil
}
func (noopSecFixProcessService) GetVersions(context.Context, string, string, int, int) ([]*model.WfProcess, int64, error) {
	return nil, 0, nil
}
func (noopSecFixProcessService) Delete(context.Context, Actor, string) error { return nil }
func (noopSecFixProcessService) Activate(context.Context, Actor, string) (*model.WfProcess, error) {
	return nil, nil
}
func (noopSecFixProcessService) Retire(context.Context, Actor, string) error { return nil }
func (noopSecFixProcessService) UpdateStatus(context.Context, Actor, string, enums.ProcessStatus) error {
	return nil
}
func (noopSecFixProcessService) UpdateStatusByKey(context.Context, Actor, string, enums.ProcessStatus) error {
	return nil
}
func (noopSecFixProcessService) IsFormReferenced(context.Context, string, string) (bool, error) {
	return false, nil
}

// TestGetProcessInstanceDetail_IDOR_RejectsNonParticipant 验证：
// 同租户用户 B 枚举用户 A 的实例 ID 读取详情，因非发起人/非 assignee/非 CC 抄送归属被拒绝。
func TestGetProcessInstanceDetail_IDOR_RejectsNonParticipant(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()

	// 种子：实例属于 userA，租户 t1，状态 active
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID:          "inst-idor",
		ProcessID:   "proc-1",
		Name:        "id_test",
		Status:      string(enums.InstanceStatusActive),
		StartUserID: "userA",
		TenantID:    "t1",
		CreatedBy:   "userA",
		CreatedAt:   time.Now(),
	}))
	// 种子：唯一 task 的 assignee 是 userA（userB 不在任何可见角色里）
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:                "task-idor",
		ProcessInstanceID: secFixStrPtr("inst-idor"),
		TaskDefKey:        "approve",
		Name:              "审批",
		TaskType:          "user_task",
		Status:            string(enums.TaskStatusActive),
		Assignee:          secFixStrPtr("userA"),
		ApprovalType:      string(enums.ApprovalTypeSingle),
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         time.Now(),
	}))

	taskSvc := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), hiTaskDAO: dao.NewHiTaskDAOWithQuery(q)}
	engine := &secFixEngine{taskSvc: taskSvc}
	rs := &RuntimeServiceImpl{
		instanceDAO:    dao.NewInstanceDAOWithQuery(q),
		hiInstanceDAO:  dao.NewHiInstanceDAOWithQuery(q),
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		workflowEngine: engine,
	}
	engine.runtimeSvc = rs

	// userB 同租户，但既非发起人也非 assignee → 必须拒绝
	bCtx := SetUserToCtx(ctx, &Actor{UserID: "userB", TenantID: "t1", UserName: "B"})
	_, err := rs.GetProcessInstanceDetail(bCtx, Actor{UserID: "userB", TenantID: "t1"}, "inst-idor")
	require.Error(t, err, "非参与用户读取他人实例详情应被拒绝")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)

	// 空 UserID（宿主漏传操作人）：fail-closed 拒绝，不得因缺身份而放行可见性校验
	_, err = rs.GetProcessInstanceDetail(ctx, Actor{UserID: "", TenantID: "t1"}, "inst-idor")
	require.Error(t, err, "空 UserID 读取实例详情应被拒绝")
	require.True(t, errors.Is(err, ErrAuthenticationRequired), "期望 ErrAuthenticationRequired，got %v", err)

	// 反向验证：发起人 userA 可读
	aCtx := SetUserToCtx(ctx, &Actor{UserID: "userA", TenantID: "t1", UserName: "A"})
	resp, err := rs.GetProcessInstanceDetail(aCtx, Actor{UserID: "userA", TenantID: "t1"}, "inst-idor")
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "inst-idor", resp.InstanceID)
}

// 候选池成员的详情可见性：待签收任务的候选成员（department/role 展开后）可读
// 实例详情，池外用户拒绝；他人签收后剩余成员失去候选可见性。
func TestGetProcessInstanceDetail_CandidateVisibility(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	now := time.Now()

	identity := newMockIdentity()
	identity.AddMockUser(&User{ID: "alice", TenantID: "t1"})
	identity.AddMockUserDepartment("alice", "dept-1")
	identity.AddMockUser(&User{ID: "bob", TenantID: "t1"})
	identity.AddMockUserDepartment("bob", "dept-1")
	identity.AddMockUser(&User{ID: "carol", TenantID: "t1"})
	identity.AddMockRoleUsers("r1", []string{"carol"})

	// 实例 i1：pending 任务候选池为部门 dept-1
	seedRoleInstance(t, q, "task-dept-vis", "inst-dept-vis")
	require.NoError(t, q.WfTaskAssignee.Create(&model.WfTaskAssignee{
		ID: "pool-dept-vis", TaskID: "task-dept-vis", EntityType: "department", EntityID: "dept-1",
		TenantID: "t1", CreatedAt: now,
	}))
	// 实例 i2：pending 任务候选池为角色 r1
	seedRoleInstance(t, q, "task-role-vis", "inst-role-vis")
	require.NoError(t, q.WfTaskAssignee.Create(&model.WfTaskAssignee{
		ID: "pool-role-vis", TaskID: "task-role-vis", EntityType: "role", EntityID: "r1",
		TenantID: "t1", CreatedAt: now,
	}))

	taskSvc := newCandSvc(q, identity)
	engine := &secFixEngine{taskSvc: taskSvc}
	rs := &RuntimeServiceImpl{
		instanceDAO:    dao.NewInstanceDAOWithQuery(q),
		hiInstanceDAO:  dao.NewHiInstanceDAOWithQuery(q),
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		workflowEngine: engine,
	}
	engine.runtimeSvc = rs

	detail := func(userID, instanceID string) error {
		userCtx := SetUserToCtx(ctx, &Actor{UserID: userID, TenantID: "t1", UserName: userID})
		_, err := rs.GetProcessInstanceDetail(userCtx, Actor{UserID: userID, TenantID: "t1"}, instanceID)
		return err
	}

	// 部门候选：成员可读，池外（仅角色成员）拒绝
	require.NoError(t, detail("alice", "inst-dept-vis"))
	require.ErrorIs(t, detail("carol", "inst-dept-vis"), ErrPermissionDenied)

	// 角色候选：角色成员可读，部门成员拒绝
	require.NoError(t, detail("carol", "inst-role-vis"))
	require.ErrorIs(t, detail("alice", "inst-role-vis"), ErrPermissionDenied)

	// alice 签收部门候选任务后，同部门其他成员失去候选可见性
	require.NoError(t, taskSvc.Claim(SetUserToCtx(ctx, &Actor{UserID: "alice", TenantID: "t1"}),
		Actor{UserID: "alice", TenantID: "t1"}, "task-dept-vis"))
	require.ErrorIs(t, detail("bob", "inst-dept-vis"), ErrPermissionDenied)
}

// errDeptIdentity 部门成员查询恒失败的 identity：展开失败时详情必须拒绝，不得当作空池放行。
type errDeptIdentity struct {
	*IdentityServiceImpl
}

func (errDeptIdentity) GetUserIDsByDepartmentID(context.Context, string, string) ([]string, error) {
	return nil, errors.New("identity backend unavailable")
}

// 候选展开不可用（identity 缺失或查询失败）时，待签收实例详情必须 fail-closed，
// 与认领校验同口径：展开不了不能当作空池对全租户放行。
func TestGetProcessInstanceDetail_CandidateExpandFailureDenied(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	now := time.Now()

	seedRoleInstance(t, q, "task-open", "inst-open")
	require.NoError(t, q.WfTaskAssignee.Create(&model.WfTaskAssignee{
		ID: "pool-open", TaskID: "task-open", EntityType: "department", EntityID: "dept-9",
		TenantID: "t1", CreatedAt: now,
	}))

	outsider := func(rs *RuntimeServiceImpl) error {
		userCtx := SetUserToCtx(ctx, &Actor{UserID: "outsider", TenantID: "t1", UserName: "outsider"})
		_, err := rs.GetProcessInstanceDetail(userCtx, Actor{UserID: "outsider", TenantID: "t1"}, "inst-open")
		return err
	}

	// identity 查询失败
	taskSvc := newCandSvc(q, errDeptIdentity{newMockIdentity()})
	engine := &secFixEngine{taskSvc: taskSvc}
	rs := &RuntimeServiceImpl{
		instanceDAO:    dao.NewInstanceDAOWithQuery(q),
		hiInstanceDAO:  dao.NewHiInstanceDAOWithQuery(q),
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		workflowEngine: engine,
	}
	engine.runtimeSvc = rs
	require.ErrorIs(t, outsider(rs), ErrPermissionDenied, "identity 展开失败时不得放行详情")

	// identity 缺失且池含 group 实体
	taskSvc2 := newCandSvc(q, nil)
	engine2 := &secFixEngine{taskSvc: taskSvc2}
	rs2 := &RuntimeServiceImpl{
		instanceDAO:    dao.NewInstanceDAOWithQuery(q),
		hiInstanceDAO:  dao.NewHiInstanceDAOWithQuery(q),
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		workflowEngine: engine2,
	}
	engine2.runtimeSvc = rs2
	require.ErrorIs(t, outsider(rs2), ErrPermissionDenied, "identity 缺失且池含 role/dept 时不得放行详情")
}
