package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"gorm.io/gorm"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"
)

func scopeChain(t *testing.T, additionalInfo string) *types.RuleChain {
	t.Helper()
	dsl := `{"ruleChain":{"id":"p1","name":"x","additionalInfo":` + additionalInfo + `},"metadata":{"nodes":[],"connections":[]}}`
	chain, err := (&model.WfProcess{DefinitionJSON: dsl}).ToRuleChain()
	if err != nil {
		t.Fatalf("ToRuleChain: %v", err)
	}
	return chain
}

func TestParseStarterScope(t *testing.T) {
	// 未配置 / additionalInfo 空 → all（存量流程全员可发起，行为不变）
	if got := ParseStarterScope(scopeChain(t, `{}`)); got.Type != ScopeTypeAll {
		t.Errorf("empty additionalInfo: got %q, want all", got.Type)
	}
	if got := ParseStarterScope(&types.RuleChain{}); got.Type != ScopeTypeAll {
		t.Errorf("nil chain: got %q, want all", got.Type)
	}
	// 未知类型回退 all（前向兼容）
	if got := ParseStarterScope(scopeChain(t, `{"starterScope":{"type":"dept"}}`)); got.Type != ScopeTypeAll {
		t.Errorf("unknown type: got %q, want all fallback", got.Type)
	}
	got := ParseStarterScope(scopeChain(t, `{"starterScope":{"type":"user","userIds":["u1","u2"]}}`))
	if got.Type != ScopeTypeUser || len(got.UserIDs) != 2 {
		t.Errorf("user scope parse mismatch: %+v", got)
	}
}

func TestMatchStarterScope(t *testing.T) {
	all := StarterScope{Type: ScopeTypeAll}
	user := StarterScope{Type: ScopeTypeUser, UserIDs: []string{"u1", "u2"}}
	role := StarterScope{Type: ScopeTypeRole, RoleIDs: []string{"r1", "r2"}}
	// 空 ID 列表 = 设计器明确"未选任何人"：全员拒绝（而非全员放行）
	userEmpty := StarterScope{Type: ScopeTypeUser}
	roleEmpty := StarterScope{Type: ScopeTypeRole}

	cases := []struct {
		name       string
		scope      StarterScope
		userID     string
		userRoleID []string
		want       bool
	}{
		{"all 放行任意用户", all, "anyone", nil, true},
		{"user 在名单内", user, "u1", nil, true},
		{"user 不在名单内", user, "u3", nil, false},
		{"user 空名单拒绝", userEmpty, "u1", nil, false},
		{"role 命中", role, "u9", []string{"r2", "r7"}, true},
		{"role 未命中", role, "u9", []string{"r7"}, false},
		{"role 用户无角色", role, "u9", nil, false},
		{"role identity 查询失败(nil)拒绝(fail-closed)", role, "u9", nil, false},
		{"role 空名单拒绝", roleEmpty, "u9", []string{"r1"}, false},
	}
	for _, c := range cases {
		if got := MatchStarterScope(c.scope, c.userID, c.userRoleID); got != c.want {
			t.Errorf("%s: MatchStarterScope(%+v, %s, %v) = %v, want %v",
				c.name, c.scope, c.userID, c.userRoleID, got, c.want)
		}
	}
}

// 发起入口强校验：范围外用户启动实例被拒（ErrPermissionDenied → 宿主 403）。
// 使用与 enginepool 测试相同的裸 RS + SQLite 内存库；user 范围不依赖 IdentityService。
func TestStartProcessInstanceByID_StarterScopeDeny(t *testing.T) {
	rs, db := newPoolTestRS(t)
	def := `{"ruleChain":{"id":"scoped","name":"scoped","root":true,"additionalInfo":{"starterScope":{"type":"user","userIds":["u1"]}}},"metadata":{"nodes":[{"id":"n1","type":"functions","name":"n","configuration":{"functionName":"pooltest_noop"}}],"connections":[]}}`
	if err := db.Exec(`INSERT INTO wf_process (id, process_key, name, version, definition_json, status, tenant_id, created_by, process_type, icon) VALUES ('p_scoped','scoped','scoped',1,?, 'active', 't1', 'tester', 'main', '')`, def).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err := rs.StartProcessInstanceByID(context.Background(), Actor{UserID: "u2", TenantID: "t1"}, "p_scoped", "", nil)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected ErrPermissionDenied for out-of-scope user, got: %v", err)
	}

	// 范围内用户必须通过本关：裸 RS 后续装配缺失会 panic/报错，但不应再是权限错误
	func() {
		defer func() { _ = recover() }()
		_, err := rs.StartProcessInstanceByID(context.Background(), Actor{UserID: "u1", TenantID: "t1"}, "p_scoped", "", nil)
		if errors.Is(err, ErrPermissionDenied) {
			t.Errorf("in-scope user should pass the starter-scope gate, got: %v", err)
		}
	}()
}

// scopeIdentityEngine 只实现 GetIdentityService：checkStarterScope 的 role 分支
// 只经它取身份服务，其余引擎方法在测试路径上不会被触达。
type scopeIdentityEngine struct {
	WorkflowEngine
	identity IdentityService
}

func (e scopeIdentityEngine) GetIdentityService() IdentityService { return e.identity }

// 发起入口的定义状态校验：retired/draft 定义拒绝发起新实例（ErrValidation → 400），
// active 定义放行。
func TestStartProcessInstanceByID_DefinitionStatusGuard(t *testing.T) {
	rs, db := newPoolTestRS(t)
	def := `{"ruleChain":{"id":"st_g","name":"st_g","root":true},"metadata":{"nodes":[{"id":"n1","type":"functions","name":"n","configuration":{"functionName":"pooltest_noop"}}],"connections":[]}}`
	seed := func(id, key, status string) {
		t.Helper()
		if err := db.Exec(`INSERT INTO wf_process (id, process_key, name, version, definition_json, status, tenant_id, created_by, process_type, icon) VALUES (?,?,?,1,?,?, 't1', 'tester', 'main', '')`,
			id, key, key, def, status).Error; err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	seed("p_retired", "stg_retired", "retired")
	seed("p_draftdef", "stg_draft", "draft")

	for _, id := range []string{"p_retired", "p_draftdef"} {
		if _, err := rs.StartProcessInstanceByID(context.Background(), Actor{UserID: "u1", TenantID: "t1"}, id, "", nil); !errors.Is(err, ErrValidation) {
			t.Errorf("%s: expected ErrValidation for non-active definition, got: %v", id, err)
		}
	}

	// active 定义须放行（裸 RS 后续装配缺失会 panic/报错，但不应是状态校验拒绝）
	func() {
		seed("p_active", "stg_active", "active")
		defer func() { _ = recover() }()
		_, err := rs.StartProcessInstanceByID(context.Background(), Actor{UserID: "u1", TenantID: "t1"}, "p_active", "", nil)
		if errors.Is(err, ErrValidation) {
			t.Errorf("active definition should pass the status check, got: %v", err)
		}
	}()
}

// TestStartProcessInstanceByID_StarterScopeRoleDeny role 范围端到端：
// 用户角色经 IdentityService 解析，命中放行、未命中/解析为空一律拒绝。
func TestStartProcessInstanceByID_StarterScopeRoleDeny(t *testing.T) {
	rs, db := newPoolTestRS(t)
	identity := newMockIdentity()
	identity.users = map[string]*User{
		"u-in":  {ID: "u-in", Email: "in@test", TenantID: "t1"},
		"u-out": {ID: "u-out", Email: "out@test", TenantID: "t1"},
	}
	identity.roleUsers = map[string][]string{"r1": {"u-in"}}
	rs.workflowEngine = scopeIdentityEngine{identity: identity}

	def := `{"ruleChain":{"id":"role_scoped","name":"role_scoped","root":true,"additionalInfo":{"starterScope":{"type":"role","roleIds":["r1"]}}},"metadata":{"nodes":[{"id":"n1","type":"functions","name":"n","configuration":{"functionName":"pooltest_noop"}}],"connections":[]}}`
	if err := db.Exec(`INSERT INTO wf_process (id, process_key, name, version, definition_json, status, tenant_id, created_by, process_type, icon) VALUES ('p_role','role_scoped','role_scoped',1,?, 'active', 't1', 'tester', 'main', '')`, def).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	// 不持角色 r1 → 拒绝
	_, err := rs.StartProcessInstanceByID(context.Background(), Actor{UserID: "u-out", TenantID: "t1"}, "p_role", "", nil)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected ErrPermissionDenied for user without role, got: %v", err)
	}

	// 持角色 r1 → 过闸（后续装配缺失允许报错，但不得是权限错误）
	func() {
		defer func() { _ = recover() }()
		_, err := rs.StartProcessInstanceByID(context.Background(), Actor{UserID: "u-in", TenantID: "t1"}, "p_role", "", nil)
		if errors.Is(err, ErrPermissionDenied) {
			t.Errorf("in-role user should pass the starter-scope gate, got: %v", err)
		}
	}()
}

// TestStartProcessInstanceByID_StarterScopeRoleIdentityMissing identity 未注入
// 时 role 范围无法证明用户在范围内，必须拒绝（fail-closed），而非放行。
func TestStartProcessInstanceByID_StarterScopeRoleIdentityMissing(t *testing.T) {
	rs, db := newPoolTestRS(t)
	rs.workflowEngine = scopeIdentityEngine{identity: nil}

	def := `{"ruleChain":{"id":"role_scoped","name":"role_scoped","root":true,"additionalInfo":{"starterScope":{"type":"role","roleIds":["r1"]}}},"metadata":{"nodes":[],"connections":[]}}`
	if err := db.Exec(`INSERT INTO wf_process (id, process_key, name, version, definition_json, status, tenant_id, created_by, process_type, icon) VALUES ('p_role','role_scoped','role_scoped',1,?, 'active', 't1', 'tester', 'main', '')`, def).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	if _, err := rs.StartProcessInstanceByID(context.Background(), Actor{UserID: "u1", TenantID: "t1"}, "p_role", "", nil); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected ErrPermissionDenied when identity service missing, got: %v", err)
	}
}

// seedDraftInstance 插入一条 draft 实例（startUserID 为原创建者）。
func seedDraftInstance(t *testing.T, db interface {
	Exec(string, ...interface{}) *gorm.DB
}, id, processID, tenant, startUserID string) {
	t.Helper()
	if err := db.Exec(`INSERT INTO wf_instance (id, process_id, name, status, tenant_id, created_by, start_user_id) VALUES (?,?, 'd', 'draft', ?, ?, ?)`,
		id, processID, tenant, startUserID, startUserID).Error; err != nil {
		t.Fatalf("seed draft %s: %v", id, err)
	}
}

// TestActivateDraftProcessInstance_StarterScope 草稿激活 = 正式发起：
// ① 非创建者不得激活他人草稿；② 范围收紧后，原创建者（现已范围外）激活被拒；
// ③ 范围内创建者激活过闸；④ 跨租户先撞租户校验（不泄露草稿归属）；
// ⑤ 系统身份越过创建者闸；⑥ 系统身份仍受范围闸约束；⑦ 孤儿草稿明确报错。
// 挂起恢复路径不受影响（不在此测）。
func TestActivateDraftProcessInstance_StarterScope(t *testing.T) {
	rs, db := newPoolTestRS(t)
	identity := newMockIdentity()
	identity.users = map[string]*User{
		"u1": {ID: "u1", Email: "u1@test", TenantID: "t1"},
		"u2": {ID: "u2", Email: "u2@test", TenantID: "t1"},
	}
	rs.workflowEngine = scopeIdentityEngine{identity: identity}

	def := `{"ruleChain":{"id":"scoped","name":"scoped","root":true,"additionalInfo":{"starterScope":{"type":"user","userIds":["u1"]}}},"metadata":{"nodes":[{"id":"n1","type":"functions","name":"n","configuration":{"functionName":"pooltest_noop"}}],"connections":[]}}`
	if err := db.Exec(`INSERT INTO wf_process (id, process_key, name, version, definition_json, status, tenant_id, created_by, process_type, icon) VALUES ('p_scoped','scoped','scoped',1,?, 'active', 't1', 'tester', 'main', '')`, def).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}
	seedDraftInstance(t, db, "d_creator", "p_scoped", "t1", "u1")
	seedDraftInstance(t, db, "d_other", "p_scoped", "t1", "u1")
	seedDraftInstance(t, db, "d_out", "p_scoped", "t1", "u2")

	// ① 非创建者 u2 激活 u1 的草稿 → 拒绝
	err := rs.ActivateProcessInstance(context.Background(), Actor{UserID: "u2", TenantID: "t1"}, "d_other")
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected ErrPermissionDenied for non-creator activation, got: %v", err)
	}

	// ② 创建者 u2 激活自己的草稿，但 u2 已不在范围（范围建后收紧）→ 拒绝
	err = rs.ActivateProcessInstance(context.Background(), Actor{UserID: "u2", TenantID: "t1"}, "d_out")
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected ErrPermissionDenied for out-of-scope creator, got: %v", err)
	}

	// ③ 范围内创建者 u1 激活自己的草稿 → 过闸（激活放行，后续引擎装配不在此断言）
	func() {
		defer func() { _ = recover() }()
		err := rs.ActivateProcessInstance(context.Background(), Actor{UserID: "u1", TenantID: "t1"}, "d_creator")
		if errors.Is(err, ErrPermissionDenied) {
			t.Errorf("in-scope creator should pass the draft activation gate, got: %v", err)
		}
	}()

	// ④ 跨租户用户激活 → 先撞租户校验：错误必须是租户不符，而非"仅创建者"
	err = rs.ActivateProcessInstance(context.Background(), Actor{UserID: "u9", TenantID: "t2"}, "d_other")
	if !errors.Is(err, ErrPermissionDenied) || !strings.Contains(err.Error(), "another tenant") {
		t.Fatalf("expected tenant-mismatch denial for cross-tenant activation, got: %v", err)
	}

	// ⑤ 系统身份（同租户）不冒充创建者，越过创建者闸（后续装配失败容忍）
	func() {
		defer func() { _ = recover() }()
		err := rs.ActivateProcessInstance(context.Background(), Actor{UserID: constants.UserSystem, UserName: constants.UserSystem, TenantID: "t1"}, "d_other")
		if errors.Is(err, ErrPermissionDenied) {
			t.Errorf("system actor should bypass the creator-only gate, got: %v", err)
		}
	}()

	// ⑥ 系统身份越过创建者闸后，范围闸照拦：创建者 u2 已出范围 → 拒绝
	err = rs.ActivateProcessInstance(context.Background(), Actor{UserID: constants.UserSystem, UserName: constants.UserSystem, TenantID: "t1"}, "d_out")
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected ErrPermissionDenied for system activation of out-of-scope creator, got: %v", err)
	}

	// ⑦ 孤儿草稿（流程定义已删）：明确报错，而非静默跳过范围校验
	seedDraftInstance(t, db, "d_orphan", "p_missing", "t1", "u1")
	if err := rs.ActivateProcessInstance(context.Background(), Actor{UserID: "u1", TenantID: "t1"}, "d_orphan"); err == nil {
		t.Fatal("expected error for orphan draft (process definition missing), got nil")
	}
}
