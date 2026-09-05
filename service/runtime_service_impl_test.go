package service

import (
	"context"
	"sync"
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
)

// Tests for runtime_service_impl.go parameter validation.
// These do NOT require a database connection.

func newRuntimeServiceForTest() *RuntimeServiceImpl {
	return &RuntimeServiceImpl{}
}

// expectPanic runs fn and fails the test if it does NOT panic.
func expectPanicRuntime(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("%s: expected panic", name)
		}
	}()
	fn()
}

// ---------------------------------------------------------------------------
// StartProcessInstanceByKey
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_StartByKey_EmptyKey(t *testing.T) {
	s := newRuntimeServiceForTest()
	_, err := s.StartProcessInstanceByKey(context.Background(), Actor{}, "", "", nil)
	if err == nil {
		t.Error("expected error for empty processDefinitionKey")
	}
}

// ---------------------------------------------------------------------------
// StartProcessInstanceByID
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_StartByID_EmptyID(t *testing.T) {
	s := newRuntimeServiceForTest()
	_, err := s.StartProcessInstanceByID(context.Background(), Actor{}, "", "", nil)
	if err == nil {
		t.Error("expected error for empty processDefinitionID")
	}
}

func TestRuntimeServiceImpl_StartByID_NilProcessDAO_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "StartByID", func() {
		s.StartProcessInstanceByID(context.Background(), Actor{TenantID: "t1"}, "proc-1", "", nil)
	})
}

// ---------------------------------------------------------------------------
// GetProcessInstance
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_GetProcessInstance_EmptyID(t *testing.T) {
	s := newRuntimeServiceForTest()
	_, err := s.GetProcessInstance(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty processInstanceID")
	}
}

func TestRuntimeServiceImpl_GetProcessInstance_NilDAO_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "GetProcessInstance", func() {
		s.GetProcessInstance(context.Background(), Actor{}, "inst-1")
	})
}

// ---------------------------------------------------------------------------
// DeleteProcessInstance
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_DeleteProcessInstance_EmptyID(t *testing.T) {
	s := newRuntimeServiceForTest()
	err := s.DeleteProcessInstance(context.Background(), Actor{}, "", "reason")
	if err == nil {
		t.Error("expected error for empty processInstanceID")
	}
}

// ---------------------------------------------------------------------------
// DeleteProcessInstances
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_DeleteProcessInstances_EmptyList(t *testing.T) {
	s := newRuntimeServiceForTest()
	err := s.DeleteProcessInstances(context.Background(), Actor{}, []string{}, "reason")
	if err != nil {
		t.Errorf("unexpected error for empty list: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TerminateProcessInstance
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_TerminateProcessInstance_EmptyID(t *testing.T) {
	s := newRuntimeServiceForTest()
	err := s.TerminateProcessInstance(context.Background(), Actor{}, "", "reason")
	if err == nil {
		t.Error("expected error for empty processInstanceID")
	}
}

// ---------------------------------------------------------------------------
// SuspendProcessInstance
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_SuspendProcessInstance_EmptyID(t *testing.T) {
	s := newRuntimeServiceForTest()
	err := s.SuspendProcessInstance(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty processInstanceID")
	}
}

// ---------------------------------------------------------------------------
// ActivateProcessInstance
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_ActivateProcessInstance_EmptyID(t *testing.T) {
	s := newRuntimeServiceForTest()
	err := s.ActivateProcessInstance(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty processInstanceID")
	}
}

// ---------------------------------------------------------------------------
// GetProcessInstanceVariables
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_GetVariables_EmptyID(t *testing.T) {
	s := newRuntimeServiceForTest()
	_, err := s.GetProcessInstanceVariables(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty processInstanceID")
	}
}

// ---------------------------------------------------------------------------
// GetProcessInstanceVariable
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_GetVariable_EmptyID(t *testing.T) {
	s := newRuntimeServiceForTest()
	_, err := s.GetProcessInstanceVariable(context.Background(), Actor{}, "", "var1")
	if err == nil {
		t.Error("expected error for empty processInstanceID")
	}
}

// ---------------------------------------------------------------------------
// SetProcessInstanceVariables
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_SetVariables_EmptyID(t *testing.T) {
	s := newRuntimeServiceForTest()
	err := s.SetProcessInstanceVariables(context.Background(), Actor{}, "", map[string]interface{}{"k": "v"})
	if err == nil {
		t.Error("expected error for empty processInstanceID")
	}
}

// ---------------------------------------------------------------------------
// SetProcessInstanceVariable
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_SetVariable_NilLock_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "SetVariable", func() {
		s.SetProcessInstanceVariable(context.Background(), Actor{}, "inst-1", "var1", "val1")
	})
}

// ---------------------------------------------------------------------------
// RemoveProcessInstanceVariable
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_RemoveVariable_NilLock_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "RemoveVariable", func() {
		s.RemoveProcessInstanceVariable(context.Background(), Actor{}, "inst-1", "var1")
	})
}

// ---------------------------------------------------------------------------
// CompleteProcessInstance
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_CompleteProcessInstance_EmptyID(t *testing.T) {
	s := newRuntimeServiceForTest()
	err := s.CompleteProcessInstance(context.Background(), Actor{}, "", "done")
	if err == nil {
		t.Error("expected error for empty processInstanceID")
	}
}

// ---------------------------------------------------------------------------
// RestartProcessInstance
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_RestartProcessInstance_NotImplemented(t *testing.T) {
	s := newRuntimeServiceForTest()
	_, err := s.RestartProcessInstance(context.Background(), Actor{}, "inst-1", "act-1")
	if err == nil {
		t.Error("expected not-implemented error")
	}
}

// ---------------------------------------------------------------------------
// ExecuteNext
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_ExecuteNext_NilDAO_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "ExecuteNext", func() {
		s.ExecuteNext(context.Background(), "inst-1", "node1", nil)
	})
}

// ---------------------------------------------------------------------------
// GetProcessInstanceList
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_GetProcessInstanceList_NilDAO_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "GetProcessInstanceList", func() {
		s.GetProcessInstanceList(context.Background(), Actor{}, nil)
	})
}

// ---------------------------------------------------------------------------
// RestoreProcessInstance
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_RestoreProcessInstance_NilDAO_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "RestoreProcessInstance", func() {
		s.RestoreProcessInstance(context.Background(), Actor{}, "inst-1")
	})
}

// ---------------------------------------------------------------------------
// RestoreAllProcessInstances
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_RestoreAllProcessInstances_NilDAO_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "RestoreAllProcessInstances", func() {
		s.RestoreAllProcessInstances(context.Background(), SystemActor())
	})
}

// ---------------------------------------------------------------------------
// GetProcessInstanceDetail
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_GetProcessInstanceDetail_NilDAO_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "GetProcessInstanceDetail", func() {
		s.GetProcessInstanceDetail(context.Background(), Actor{UserID: "user1"}, "inst-1")
	})
}

// ---------------------------------------------------------------------------
// Todo/Done/Cc/MyApplications list
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_GetTodoList_NilDAO_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "GetTodoList", func() {
		s.GetTodoProcessInstanceList(context.Background(), Actor{UserID: "u1", TenantID: "t1"}, 1, 10, "", nil, "", false)
	})
}

func TestRuntimeServiceImpl_GetDoneList_NilDAO_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "GetDoneList", func() {
		s.GetDoneProcessInstanceList(context.Background(), Actor{UserID: "u1", TenantID: "t1"}, 1, 10, "", nil, "", false, "")
	})
}

func TestRuntimeServiceImpl_GetCcList_NilDAO_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "GetCcList", func() {
		s.GetCcProcessInstanceList(context.Background(), Actor{UserID: "u1", TenantID: "t1"}, 1, 10, "", nil, "", false)
	})
}

func TestRuntimeServiceImpl_GetMyApplications_NilDAO_Panics(t *testing.T) {
	s := &RuntimeServiceImpl{}
	expectPanicRuntime(t, "GetMyApplications", func() {
		s.GetMyApplicationsProcessInstanceList(context.Background(), Actor{UserID: "u1", TenantID: "t1"}, 1, 10, "", "", false, "")
	})
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

func TestNewRuntimeService_NilEngine(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from nil engine")
		}
	}()
	NewRuntimeService(nil)
}

// ---------------------------------------------------------------------------
// Interface compliance
// ---------------------------------------------------------------------------

func TestRuntimeServiceImpl_ImplementsInterface(t *testing.T) {
	var _ RuntimeService = (*RuntimeServiceImpl)(nil)
}

// ---------------------------------------------------------------------------
// ExecuteNext / SetProcessInstanceVariables（SQLite 内存库）
// ---------------------------------------------------------------------------

// rtImplTestDB 建 wf_instance 内存表，返回 query.Query。
func rtImplTestDB(t *testing.T) *query.Query {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:rtimpl_test?mode=memory&cache=shared&_busy_timeout=30000"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if c, e := db.DB(); e == nil {
			_ = c.Close()
		}
	})
	require.NoError(t, db.Exec(`CREATE TABLE IF NOT EXISTS wf_instance (
		id TEXT PRIMARY KEY, process_id TEXT NOT NULL, business_key TEXT, name TEXT NOT NULL,
		status TEXT NOT NULL, variables TEXT, current_activity TEXT,
		priority INTEGER NOT NULL DEFAULT 50, parent_id TEXT, tenant_id TEXT NOT NULL,
		created_by TEXT NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_by TEXT, updated_at DATETIME, end_reason TEXT, duration INTEGER,
		ended_at DATETIME, start_user_id TEXT NOT NULL,
		UNIQUE (tenant_id, business_key))`).Error)
	require.NoError(t, db.Exec("DELETE FROM wf_instance").Error)
	return query.Use(db)
}

// 实例已归档/删除时，ExecuteNext 应幂等返回 nil，而不是报 not found
// （并发审批的尾随驱动据此安静退出）。
func TestExecuteNext_MissingInstanceReturnsNil(t *testing.T) {
	q := rtImplTestDB(t)
	svc := &RuntimeServiceImpl{instanceDAO: dao.NewInstanceDAOWithQuery(q)}

	err := svc.ExecuteNext(context.Background(), "inst-not-exist", "node1", nil)
	require.NoError(t, err)
}

// 批量写变量走行锁事务：并发写不丢更新。
func TestSetProcessInstanceVariables_ConcurrentMerge(t *testing.T) {
	q := rtImplTestDB(t)
	instDAO := dao.NewInstanceDAOWithQuery(q)
	svc := &RuntimeServiceImpl{instanceDAO: instDAO}
	ctx := context.Background()

	require.NoError(t, instDAO.Create(ctx, &model.WfInstance{
		ID: "inst-vars", ProcessID: "proc-1", Name: "vars", Status: string(enums.InstanceStatusActive),
		TenantID: "t1", CreatedBy: "sys", StartUserID: "starter", CreatedAt: time.Now(),
	}))

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "k" + string(rune('a'+i))
			// SQLite 共享缓存并发写事务会报 deadlock（生产 PG/MySQL 下
			// FOR UPDATE 排队等待），重试模拟排队语义
			var err error
			for attempt := 0; attempt < 8; attempt++ {
				err = svc.SetProcessInstanceVariables(ctx, Actor{UserID: "sys", TenantID: "t1"}, "inst-vars", map[string]interface{}{key: i})
				if err == nil {
					return
				}
				time.Sleep(15 * time.Millisecond)
			}
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	got, err := svc.GetProcessInstanceVariables(ctx, ActorFromCtx(ctx), "inst-vars")
	require.NoError(t, err)
	require.Len(t, got, n, "并发批量写入不应互相覆盖")
}

// ---------------------------------------------------------------------------
// activateProcessInstanceInternal：草稿激活只允许上报一次
// ---------------------------------------------------------------------------

// 草稿激活的「本次是否真正把 draft 翻成 active」必须由持锁事务内的实际状态迁移
// 决定，不能靠事务外用 `Status==active && CurrentActivity==nil` 反推。
//
// 反推为什么不成立：第一次激活提交事务后，引擎 OnMsg 在事务外执行，
// CurrentActivity 要等 userTask 回调才落库。这个窗口里第二个并发请求拿到的
// 实例正好是 active + CurrentActivity=nil，被误判成「本次激活了草稿」，于是
// 第二次驱动引擎——起始后继任务重复创建、流转紊乱。
// activateTestEngine 让激活末尾的事件派发拿到"无监听器"，
// 而不是撞上桩里未实现的内嵌接口。
type activateTestEngine struct{ scopeIdentityEngine }

func (activateTestEngine) GetTaskEventListener() TaskEventListener { return nil }

func TestActivateDraftInternal_ReportsDraftStartOnlyOnce(t *testing.T) {
	rs, db := newPoolTestRS(t)
	rs.workflowEngine = activateTestEngine{scopeIdentityEngine{identity: newMockIdentity()}}

	def := `{"ruleChain":{"id":"p_twice","name":"twice","root":true},"metadata":{"nodes":[{"id":"n1","type":"functions","name":"n","configuration":{"functionName":"pooltest_noop"}}],"connections":[]}}`
	if err := db.Exec(`INSERT INTO wf_process (id, process_key, name, version, definition_json, status, tenant_id, created_by, process_type, icon) VALUES ('p_twice','twice','twice',1,?, 'active', 't1', 'tester', 'main', '')`, def).Error; err != nil {
		t.Fatalf("seed process: %v", err)
	}
	seedDraftInstance(t, db, "d_twice", "p_twice", "t1", "u1")

	ctx := bindActor(context.Background(), Actor{UserID: "u1", TenantID: "t1"})

	// activate 跑一次持锁激活，返回内部判定的「本次是否为草稿激活」
	activate := func() (bool, error) {
		var wasDraft bool
		err := WithInstanceTx(ctx, rs.instanceDAO.Query, "d_twice", func(scope *InstanceScope) error {
			_, started, err := rs.activateProcessInstanceInternal(ctx, scope, "d_twice")
			wasDraft = started
			return err
		})
		return wasDraft, err
	}

	// 第一次：draft → active，是真正的草稿激活
	wasDraft, err := activate()
	if err != nil {
		t.Fatalf("first activation: %v", err)
	}
	if !wasDraft {
		t.Fatal("first activation should report a real draft start")
	}

	// 实例此时 active 且 CurrentActivity 仍为 nil——正是引擎回调落库前的窗口
	inst, err := rs.instanceDAO.Get(ctx, "d_twice")
	if err != nil {
		t.Fatalf("reload instance: %v", err)
	}
	if inst.Status != string(enums.InstanceStatusActive) || inst.CurrentActivity != nil {
		t.Fatalf("precondition: expected active + nil CurrentActivity, got status=%s currentActivity=%v",
			inst.Status, inst.CurrentActivity)
	}

	// 第二次：没有发生任何状态迁移 → 不得报告草稿激活（否则二次驱动引擎）
	wasDraft, err = activate()
	if err != nil {
		t.Fatalf("second activation should be idempotent, got: %v", err)
	}
	if wasDraft {
		t.Fatal("second activation must not report a draft start (would double-drive the engine)")
	}
}

// ---------------------------------------------------------------------------
// DeleteProcessInstance：草稿硬删 / 非草稿归档（SQLite 内存库）
// ---------------------------------------------------------------------------

// 草稿未流转，删除即物理删除：运行时行删除，不落历史行
func TestDeleteProcessInstance_DraftHardDelete(t *testing.T) {
	engine := buildEngineForEvents(t, &recordingListener{})
	rs := engine.GetRuntimeService()
	db := engine.GetDB()

	require.NoError(t, db.Create(&model.WfInstance{
		ID: "inst-del-draft", ProcessID: "proc-test", Name: "draft",
		Status: string(enums.InstanceStatusDraft), TenantID: "tenant-test",
		StartUserID: "starter", CreatedBy: "starter", CreatedAt: time.Now(),
	}).Error)

	count := func(table, where string, args ...interface{}) int64 {
		var n int64
		require.NoError(t, db.Table(table).Where(where, args...).Count(&n).Error)
		return n
	}

	actor := *newUserIdentity("starter")
	ctx := SetUserToCtx(context.Background(), newUserIdentity("starter"))
	require.NoError(t, rs.DeleteProcessInstance(ctx, actor, "inst-del-draft", "用户手动删除"))

	require.Equal(t, int64(0), count("wf_instance", "id = ?", "inst-del-draft"))
	require.Equal(t, int64(0), count("wf_hi_instance", "id = ?", "inst-del-draft"))
}

// 非草稿删除维持归档语义：运行时行删除，历史行 status=deleted，任务归档后删除
func TestDeleteProcessInstance_ActiveArchivesAsDeleted(t *testing.T) {
	engine := buildEngineForEvents(t, &recordingListener{})
	rs := engine.GetRuntimeService()
	db := engine.GetDB()

	seedInstance(t, engine, "inst-del-act", "starter")
	seedActiveTask(t, engine, "inst-del-act", "userTask1", "alice")

	count := func(table, where string, args ...interface{}) int64 {
		var n int64
		require.NoError(t, db.Table(table).Where(where, args...).Count(&n).Error)
		return n
	}

	actor := *newUserIdentity("starter")
	ctx := SetUserToCtx(context.Background(), newUserIdentity("starter"))
	require.NoError(t, rs.DeleteProcessInstance(ctx, actor, "inst-del-act", "用户手动删除"))

	require.Equal(t, int64(0), count("wf_instance", "id = ?", "inst-del-act"))
	require.Equal(t, int64(1), count("wf_hi_instance", "id = ? AND status = ? AND end_reason = ?",
		"inst-del-act", string(enums.InstanceStatusDeleted), "用户手动删除"))
	require.Equal(t, int64(1), count("wf_hi_task", "process_instance_id = ?", "inst-del-act"))
	require.Equal(t, int64(0), count("wf_task", "process_instance_id = ?", "inst-del-act"))
}
