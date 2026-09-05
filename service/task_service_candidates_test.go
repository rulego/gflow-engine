package service

// Tests for task_service_candidates.go: AddCandidates persistence, claim
// candidate-pool checks (role expansion via identity), and
// GetTaskCandidates / GetClaimableInstanceIDs reads.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/gflow-engine/utils/lock"
)

// candGroupDB 内存 SQLite，建 wf_instance + wf_task + wf_task_assignee。
func candGroupDB(t *testing.T) *query.Query {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:candgroup_test?mode=memory&cache=shared&_busy_timeout=30000"), &gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if c, e := db.DB(); e == nil {
			_ = c.Close()
		}
	})
	ddls := []string{
		`CREATE TABLE IF NOT EXISTS wf_instance (
			id TEXT PRIMARY KEY, process_id TEXT NOT NULL, business_key TEXT, name TEXT NOT NULL,
			status TEXT NOT NULL, variables TEXT, current_activity TEXT,
			priority INTEGER NOT NULL DEFAULT 50, parent_id TEXT, tenant_id TEXT NOT NULL,
			created_by TEXT NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_by TEXT, updated_at DATETIME, end_reason TEXT, duration INTEGER, ended_at DATETIME,
			start_user_id TEXT NOT NULL,
			UNIQUE (tenant_id, business_key)
		)`,
		`CREATE TABLE IF NOT EXISTS wf_task (
			id TEXT PRIMARY KEY, process_instance_id TEXT, process_id TEXT, parent_id TEXT,
			task_def_key TEXT, name TEXT, task_type TEXT, description TEXT, status TEXT,
			assignee TEXT, owner TEXT, due_date DATETIME, priority INTEGER, form_key TEXT,
			variables TEXT, claimed_at DATETIME, sequence_order INTEGER, approval_type TEXT,
			approval_rule TEXT, delegate_from TEXT, delegate_reason TEXT, delegate_time DATETIME,
			ended_at DATETIME, comment TEXT, end_reason TEXT, duration INTEGER,
			tenant_id TEXT, created_by TEXT, created_at DATETIME, updated_by TEXT, updated_at DATETIME
		)`,
		`CREATE TABLE IF NOT EXISTS wf_task_assignee (
			id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
			entity_type TEXT NOT NULL DEFAULT 'role',
			entity_id TEXT NOT NULL,
			tenant_id TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
	}
	for _, ddl := range ddls {
		require.NoError(t, db.Exec(ddl).Error)
	}
	db.Exec("DELETE FROM wf_instance")
	db.Exec("DELETE FROM wf_task")
	db.Exec("DELETE FROM wf_task_assignee")
	return query.Use(db)
}

// candGroupEngine 测试用引擎：可注入 identity（nil 时校验走兜底放行）。
type candGroupEngine struct {
	identity IdentityService
}

func (e candGroupEngine) GetDB() *gorm.DB                                 { return nil }
func (e candGroupEngine) GetTaskService() TaskService                     { return nil }
func (e candGroupEngine) GetProcessService() ProcessService               { return nil }
func (e candGroupEngine) GetRuntimeService() RuntimeService               { return nil }
func (e candGroupEngine) GetHistoryService() HistoryService               { return nil }
func (e candGroupEngine) GetIdentityService() IdentityService             { return e.identity }
func (e candGroupEngine) GetLocker() lock.Locker                          { return nil }
func (e candGroupEngine) Start(context.Context) error                     { return nil }
func (e candGroupEngine) Stop(context.Context) error                      { return nil }
func (e candGroupEngine) IsRunning() bool                                 { return false }
func (e candGroupEngine) GetName() string                                 { return "candgroup-test" }
func (e candGroupEngine) GetVersion() string                              { return "" }
func (e candGroupEngine) GetIDGenerator() IDGenerator                     { return DefaultIDGenerator }
func (e candGroupEngine) GetRuleChainExecutor() RuleChainExecutor         { return nil }
func (e candGroupEngine) GetCCTaskCreatedListener() CCTaskCreatedListener { return nil }
func (e candGroupEngine) GetTaskEventListener() TaskEventListener         { return nil }
func (e candGroupEngine) GetTaskServiceInternal() TaskServiceInternal     { return nil }
func (e candGroupEngine) GetRuntimeServiceInternal() RuntimeServiceInternal {
	return nil
}
func (e candGroupEngine) CountTenantData(ctx context.Context, tenantID string) (map[string]int64, error) {
	return nil, nil
}

// newCandSvc 组装带 taskAssigneeDAO 的 TaskServiceImpl。
func newCandSvc(q *query.Query, identity IdentityService) *TaskServiceImpl {
	return &TaskServiceImpl{
		taskDAO:         dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:       dao.NewHiTaskDAOWithQuery(q),
		taskAssigneeDAO: dao.NewTaskAssigneeDAOWithQuery(q),
		idGenerator:     DefaultIDGenerator,
		workflowEngine:  candGroupEngine{identity: identity},
	}
}

// newMockIdentity 返回空数据的 *IdentityServiceImpl（不引入 NewIdentityService 的默认测试数据）。
func newMockIdentity() *IdentityServiceImpl {
	return &IdentityServiceImpl{}
}

// seedRoleInstance 种一个 pending 待认领任务 + instance 行。
func seedRoleInstance(t *testing.T, q *query.Query, taskID, instanceID string) {
	t.Helper()
	now := time.Now()
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID:          instanceID,
		ProcessID:   "proc-1",
		Name:        "role_task_test",
		Status:      string(enums.InstanceStatusActive),
		StartUserID: "starter",
		TenantID:    "t1",
		CreatedBy:   "starter",
		CreatedAt:   now,
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID:                taskID,
		ProcessInstanceID: secFixStrPtr(instanceID),
		TaskDefKey:        "approve-node",
		Name:              "审批",
		TaskType:          "user_task",
		Status:            string(enums.TaskStatusPending),
		ApprovalType:      string(enums.ApprovalTypeSingle),
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         now,
	}))
}

// TestAddCandidates_WritesRoleRows 验证 AddCandidates 落 N 条 entity_type=role 记录。
func TestAddCandidates_WritesRoleRows(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	seedRoleInstance(t, q, "task-add", "inst-add")

	taskSvc := newCandSvc(q, newMockIdentity())

	// 模拟 createClaimTask 落库 role 候选：2 个 role。
	require.NoError(t, taskSvc.AddCandidates(ctx, Actor{UserID: "system", TenantID: "t1"}, "task-add", "role", []string{"role-1", "role-2", ""}))

	rows, err := taskSvc.taskAssigneeDAO.GetByTaskID(ctx, "t1", "task-add")
	require.NoError(t, err)
	require.Len(t, rows, 2, "空 entityID 应被过滤，剩 2 条 role 记录")
	for _, r := range rows {
		require.Equal(t, "role", r.EntityType)
		require.Equal(t, "t1", r.TenantID)
		require.Equal(t, "task-add", r.TaskID)
	}
}

// TestClaim_RoleMember_Passes 验证 role 成员可认领（identity 展开 role→members 命中）。
func TestClaim_RoleMember_Passes(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	seedRoleInstance(t, q, "task-role", "inst-role")

	identity := newMockIdentity()
	identity.AddMockUser(&User{ID: "member-1", TenantID: "t1"})
	identity.AddMockUser(&User{ID: "member-2", TenantID: "t1"})
	identity.AddMockRoleUsers("role-1", []string{"member-1", "member-2"})

	taskSvc := newCandSvc(q, identity)
	require.NoError(t, taskSvc.AddCandidates(ctx, Actor{UserID: "system", TenantID: "t1"}, "task-role", "role", []string{"role-1"}))

	memberCtx := SetUserToCtx(ctx, &Actor{UserID: "member-1", TenantID: "t1", UserName: "M1"})
	require.NoError(t, taskSvc.Claim(memberCtx, Actor{UserID: "member-1", TenantID: "t1", UserName: "M1"}, "task-role"), "role 成员应可认领")

	persisted, err := taskSvc.GetTask(memberCtx, ActorFromCtx(memberCtx), "task-role")
	require.NoError(t, err)
	require.NotNil(t, persisted.Assignee)
	require.Equal(t, "member-1", *persisted.Assignee)
	require.Equal(t, string(enums.TaskStatusActive), persisted.Status)
}

// TestClaim_NonMember_Rejected 验证非 role 成员被拒绝（堵越权）。
func TestClaim_NonMember_Rejected(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	seedRoleInstance(t, q, "task-reject", "inst-reject")

	identity := newMockIdentity()
	identity.AddMockUser(&User{ID: "member-1", TenantID: "t1"})
	identity.AddMockRoleUsers("role-1", []string{"member-1"})

	taskSvc := newCandSvc(q, identity)
	require.NoError(t, taskSvc.AddCandidates(ctx, Actor{UserID: "system", TenantID: "t1"}, "task-reject", "role", []string{"role-1"}))

	outsideCtx := SetUserToCtx(ctx, &Actor{UserID: "outsider", TenantID: "t1", UserName: "O"})
	err := taskSvc.Claim(outsideCtx, Actor{UserID: "outsider", TenantID: "t1", UserName: "O"}, "task-reject")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)
}

// TestClaim_RoleChange_NewMemberCanClaim 验证角色变动后新成员可认领（动态语义）。
func TestClaim_RoleChange_NewMemberCanClaim(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	seedRoleInstance(t, q, "task-dyn", "inst-dyn")

	identity := newMockIdentity()
	identity.AddMockUser(&User{ID: "original", TenantID: "t1"})
	identity.AddMockRoleUsers("role-1", []string{"original"})

	taskSvc := newCandSvc(q, identity)
	require.NoError(t, taskSvc.AddCandidates(ctx, Actor{UserID: "system", TenantID: "t1"}, "task-dyn", "role", []string{"role-1"}))

	// 角色成员变动：加入 newMember。
	identity.AddMockUser(&User{ID: "newMember", TenantID: "t1"})
	identity.AddMockRoleUsers("role-1", []string{"original", "newMember"})

	newCtx := SetUserToCtx(ctx, &Actor{UserID: "newMember", TenantID: "t1", UserName: "NM"})
	require.NoError(t, taskSvc.Claim(newCtx, Actor{UserID: "newMember", TenantID: "t1", UserName: "NM"}, "task-dyn"), "角色变动后新成员应可认领")
}

// TestClaim_IdentityNil_GroupEntity_Rejected 验证 identity==nil 且池有 role 候选时 fail-closed 拒绝（防越权）。
func TestClaim_IdentityNil_GroupEntity_Rejected(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	seedRoleInstance(t, q, "task-nilid", "inst-nilid")

	// identity==nil：claimInternal 有 role 候选但无法展开 → fail-closed 拒绝（防越权）。
	taskSvc := newCandSvc(q, nil)
	require.NoError(t, taskSvc.AddCandidates(ctx, Actor{UserID: "system", TenantID: "t1"}, "task-nilid", "role", []string{"role-1"}))

	c := SetUserToCtx(ctx, &Actor{UserID: "anyone", TenantID: "t1", UserName: "A"})
	err := taskSvc.Claim(c, Actor{UserID: "anyone", TenantID: "t1", UserName: "A"}, "task-nilid")
	require.Error(t, err, "identity nil + role 候选应 fail-closed 拒绝")
	require.ErrorIs(t, err, ErrPermissionDenied)
}

// TestClaim_PersonCandidate_Matches 验证 entity_type=person 候选精确命中。
func TestClaim_PersonCandidate_Matches(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	seedRoleInstance(t, q, "task-person", "inst-person")

	taskSvc := newCandSvc(q, newMockIdentity())
	require.NoError(t, taskSvc.AddCandidates(ctx, Actor{UserID: "system", TenantID: "t1"}, "task-person", "person", []string{"p1", "p2"}))

	p1Ctx := SetUserToCtx(ctx, &Actor{UserID: "p1", TenantID: "t1", UserName: "P1"})
	require.NoError(t, taskSvc.Claim(p1Ctx, Actor{UserID: "p1", TenantID: "t1", UserName: "P1"}, "task-person"), "person 候选成员应可认领")

	p3Ctx := SetUserToCtx(ctx, &Actor{UserID: "p3", TenantID: "t1", UserName: "P3"})
	err := taskSvc.Claim(p3Ctx, Actor{UserID: "p3", TenantID: "t1", UserName: "P3"}, "task-person")
	// task-person 已被 p1 claim，状态变 active → ErrTaskNotClaimable（非候选拒绝，而是已分配）
	require.Error(t, err)
}

// TestGetTaskCandidates_RoleExpanded 验证 role 任务返回展开后 userIds（非空）。
func TestGetTaskCandidates_RoleExpanded(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	seedRoleInstance(t, q, "task-cand", "inst-cand")

	identity := newMockIdentity()
	identity.AddMockUser(&User{ID: "m1", TenantID: "t1"})
	identity.AddMockUser(&User{ID: "m2", TenantID: "t1"})
	identity.AddMockRoleUsers("role-a", []string{"m1", "m2"})

	taskSvc := newCandSvc(q, identity)
	require.NoError(t, taskSvc.AddCandidates(ctx, Actor{UserID: "system", TenantID: "t1"}, "task-cand", "role", []string{"role-a"}))

	candidates, err := taskSvc.GetTaskCandidates(ctx, "inst-cand", "approve-node")
	require.NoError(t, err)
	require.NotEmpty(t, candidates, "role 任务候选展开后应非空")

	ids := make(map[string]bool)
	for _, c := range candidates {
		require.Equal(t, "person", c.EntityType)
		ids[c.EntityID] = true
	}
	require.True(t, ids["m1"], "应包含 role-a 成员 m1")
	require.True(t, ids["m2"], "应包含 role-a 成员 m2")
}

// TestGetTaskCandidates_IdentityNil 验证 identity==nil 时 person 池正常返回；
// 池含 role/dept 实体时报错（展开不了不能当空池放行，与认领校验同口径）。
func TestGetTaskCandidates_IdentityNil(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	seedRoleInstance(t, q, "task-mix", "inst-mix")

	taskSvc := newCandSvc(q, nil)
	require.NoError(t, taskSvc.AddCandidates(ctx, Actor{UserID: "system", TenantID: "t1"}, "task-mix", "role", []string{"role-a"}))
	require.NoError(t, taskSvc.AddCandidates(ctx, Actor{UserID: "system", TenantID: "t1"}, "task-mix", "person", []string{"p1"}))

	_, err := taskSvc.GetTaskCandidates(ctx, "inst-mix", "approve-node")
	require.Error(t, err, "identity 缺失且池含 role 实体时必须报错")

	require.NoError(t, taskSvc.RemoveCandidates(ctx, Actor{UserID: "system", TenantID: "t1"}, "task-mix", "role", []string{"role-a"}))
	candidates, err := taskSvc.GetTaskCandidates(ctx, "inst-mix", "approve-node")
	require.NoError(t, err)
	require.Len(t, candidates, 1, "纯 person 池不依赖 identity")
	require.Equal(t, "p1", candidates[0].EntityID)
	require.Equal(t, "person", candidates[0].EntityType)
}

// TestGetClaimableInstanceIDs 批量判断可认领实例：
// 只返回存在"无 assignee 且用户在候选人池"任务的实例（单次查询，替代逐实例 N+1）。
func TestGetClaimableInstanceIDs(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	now := time.Now()

	// i1：pending 无 assignee，userX 是 person 候选 → 命中
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-cl1", ProcessInstanceID: secFixStrPtr("i1"), TaskDefKey: "n1",
		Name: "审批", TaskType: "user_task", Status: string(enums.TaskStatusPending),
		TenantID: "t1", CreatedBy: "system", CreatedAt: now,
	}))
	require.NoError(t, q.WfTaskAssignee.Create(&model.WfTaskAssignee{
		ID: "as-1", TaskID: "task-cl1", EntityType: "person", EntityID: "userX", TenantID: "t1",
		CreatedAt: now,
	}))
	// i2：active 已有 assignee → 不命中
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-cl2", ProcessInstanceID: secFixStrPtr("i2"), TaskDefKey: "n2",
		Name: "审批", TaskType: "user_task", Status: string(enums.TaskStatusActive),
		Assignee: secFixStrPtr("someone"), TenantID: "t1", CreatedBy: "system", CreatedAt: now,
	}))

	taskSvc := newCandSvc(q, nil)

	got, err := taskSvc.GetClaimableInstanceIDs(ctx, Actor{UserID: "userX", TenantID: "t1"}, []string{"i1", "i2"})
	require.NoError(t, err)
	require.Equal(t, []string{"i1"}, got)

	// 指定范围外查询：无命中
	got, err = taskSvc.GetClaimableInstanceIDs(ctx, Actor{UserID: "userX", TenantID: "t1"}, []string{"i2"})
	require.NoError(t, err)
	require.Empty(t, got)

	// 参数校验
	_, err = taskSvc.GetClaimableInstanceIDs(ctx, Actor{UserID: "", TenantID: "t1"}, nil)
	require.ErrorIs(t, err, ErrValidation)
}

// TestGetClaimableInstanceIDs_NonCandidateExcluded 验证 GetClaimableInstanceIDs
// 只命中候选池包含用户且未指派的实例：pending 无主但用户非候选的实例不得命中
// （否则待办 needsClaim 标记全错并泄露实例存在性）。
func TestGetClaimableInstanceIDs_NonCandidateExcluded(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	strPtr := func(s string) *string { return &s }

	// 实例 A：pending 无主任务，候选池含 role:r1（用户 u1 持有 r1 → 可认领）
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-A", ProcessID: "p1", Name: "A", Status: string(enums.InstanceStatusActive),
		StartUserID: "starter", TenantID: "t1", CreatedBy: "starter", CreatedAt: time.Now(),
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-A", ProcessInstanceID: strPtr("inst-A"), TaskDefKey: "n1", Name: "A审批",
		TaskType: "user_task", Status: string(enums.TaskStatusPending), ApprovalType: "or",
		TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))
	require.NoError(t, q.WfTaskAssignee.Create(&model.WfTaskAssignee{
		ID: "ca-1", TaskID: "task-A", EntityType: "role", EntityID: "r1", TenantID: "t1", CreatedAt: time.Now(),
	}))

	// 实例 B：pending 无主任务，但候选池为空（任何用户都非候选）
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-B", ProcessID: "p1", Name: "B", Status: string(enums.InstanceStatusActive),
		StartUserID: "starter", TenantID: "t1", CreatedBy: "starter", CreatedAt: time.Now(),
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-B", ProcessInstanceID: strPtr("inst-B"), TaskDefKey: "n1", Name: "B审批",
		TaskType: "user_task", Status: string(enums.TaskStatusPending), ApprovalType: "or",
		TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))

	// 实例 C：pending 但已指派给他人——即便 u1 在候选池也不算可认领
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "inst-C", ProcessID: "p1", Name: "C", Status: string(enums.InstanceStatusActive),
		StartUserID: "starter", TenantID: "t1", CreatedBy: "starter", CreatedAt: time.Now(),
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-C", ProcessInstanceID: strPtr("inst-C"), TaskDefKey: "n1", Name: "C审批",
		TaskType: "user_task", Status: string(enums.TaskStatusPending), ApprovalType: "or",
		Assignee: strPtr("someone-else"), TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
	}))
	require.NoError(t, q.WfTaskAssignee.Create(&model.WfTaskAssignee{
		ID: "ca-2", TaskID: "task-C", EntityType: "person", EntityID: "u1", TenantID: "t1", CreatedAt: time.Now(),
	}))

	identity := newMockIdentity()
	// u1 持有角色 r1（candidateRoleIDs 经 GetRoleIDsByUserID 反查）
	identity.users = map[string]*User{
		"u1": {ID: "u1", Email: "u1@test", TenantID: "t1"},
		"u2": {ID: "u2", Email: "u2@test", TenantID: "t1"},
	}
	identity.roleUsers = map[string][]string{"r1": {"u1"}}
	svc := newCandSvc(q, identity)

	// u1 持有角色 r1 → 只命中 inst-A；inst-B（非候选）与 inst-C（已指派）必须排除
	ids, err := svc.GetClaimableInstanceIDs(ctx, Actor{UserID: "u1", TenantID: "t1"}, []string{"inst-A", "inst-B", "inst-C"})
	require.NoError(t, err)
	require.Equal(t, []string{"inst-A"}, ids, "仅候选池命中且未指派的实例可认领")

	// 无角色的用户 u2：连 inst-A 也不得命中
	ids2, err := svc.GetClaimableInstanceIDs(ctx, Actor{UserID: "u2", TenantID: "t1"}, []string{"inst-A", "inst-B"})
	require.NoError(t, err)
	require.Empty(t, ids2, "非候选用户不得命中任何实例")
}

// TestGetClaimableInstanceIDs_DeptCandidate dept 候选任务落库的是 department 实体，
// 可认领判断须按用户的部门 ID 匹配，否则成员看不到待签收任务。
func TestGetClaimableInstanceIDs_DeptCandidate(t *testing.T) {
	q := candGroupDB(t)
	ctx := context.Background()
	now := time.Now()

	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: "i-dept", ProcessID: "proc-1", Name: "dept_task_test",
		Status: string(enums.InstanceStatusActive), StartUserID: "starter",
		TenantID: "t1", CreatedBy: "starter", CreatedAt: now,
	}))
	require.NoError(t, q.WfTask.Create(&model.WfTask{
		ID: "task-dept", ProcessInstanceID: secFixStrPtr("i-dept"), TaskDefKey: "n1",
		Name: "审批", TaskType: "user_task", Status: string(enums.TaskStatusPending),
		TenantID: "t1", CreatedBy: "system", CreatedAt: now,
	}))
	require.NoError(t, q.WfTaskAssignee.Create(&model.WfTaskAssignee{
		ID: "as-dept", TaskID: "task-dept", EntityType: "department", EntityID: "dept-1",
		TenantID: "t1", CreatedAt: now,
	}))

	identity := newMockIdentity()
	identity.AddMockUser(&User{ID: "member", TenantID: "t1"})
	identity.AddMockUserDepartment("member", "dept-1")
	identity.AddMockUser(&User{ID: "outsider", TenantID: "t1"})

	taskSvc := newCandSvc(q, identity)

	got, err := taskSvc.GetClaimableInstanceIDs(ctx, Actor{UserID: "member", TenantID: "t1"}, []string{"i-dept"})
	require.NoError(t, err)
	require.Equal(t, []string{"i-dept"}, got)

	got, err = taskSvc.GetClaimableInstanceIDs(ctx, Actor{UserID: "outsider", TenantID: "t1"}, []string{"i-dept"})
	require.NoError(t, err)
	require.Empty(t, got)
}
