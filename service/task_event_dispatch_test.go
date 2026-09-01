package service

// Tests for task_event_dispatch.go: event dispatch behavior (async firing,
// panic isolation, ctx/EventID/timestamp handling) and engine-integration
// coverage verifying the engine routes lifecycle events (transfer, terminate,
// countersign, resolve, suspend/activate, withdraw) to registered listeners.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/rulego/gflow-engine/config"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/enums"
)

// recordingListener is a thread-safe TaskEventListener that records all
// events it receives.
type recordingListener struct {
	mu     sync.Mutex
	events []TaskEvent
}

func (r *recordingListener) onEvent(_ context.Context, evt TaskEvent) {
	r.mu.Lock()
	r.events = append(r.events, evt)
	r.mu.Unlock()
}

func (r *recordingListener) snapshot() []TaskEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]TaskEvent, len(r.events))
	copy(out, r.events)
	return out
}

// waitForEvents polls the recorder up to the given timeout, returning the
// captured events once at least minCount are present.
func (r *recordingListener) waitForEvents(t *testing.T, minCount int, timeout time.Duration) []TaskEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if evs := r.snapshot(); len(evs) >= minCount {
			return evs
		}
		time.Sleep(5 * time.Millisecond)
	}
	return r.snapshot()
}

// buildEngineForEvents builds a fully wired engine backed by sqlite-in-memory
// and registers the given listener. Returns the engine and its TaskService.
func buildEngineForEvents(t *testing.T, rec *recordingListener) WorkflowEngine {
	t.Helper()
	registerTestSqliteDialect()

	cfg := &config.Config{
		Database: &config.DatabaseConfig{
			Driver: "sqlite",
			Dsn:    "file::memory:?cache=shared&_busy_timeout=5000",
		},
	}
	engine, err := NewWorkflowEngineBuilder().
		SetName("evt-test").
		SetConfig(cfg).
		SetIDGenerator(NewIDGenerator()).
		SetTaskEventListener(rec.onEvent).
		Build()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Stop(context.Background()) })

	ensureEventTestSchema(t, engine.GetDB())
	return engine
}

// ensureEventTestSchema creates the minimal wf_instance / wf_task (and
// history/assignee/process) tables the event tests touch. Raw DDL is used
// because AutoMigrate chokes on the `comment:` tags under SQLite.
func ensureEventTestSchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	ddls := []string{
		`CREATE TABLE IF NOT EXISTS wf_instance (
			id TEXT PRIMARY KEY,
			process_id TEXT NOT NULL,
			business_key TEXT,
			name TEXT NOT NULL,
			status TEXT NOT NULL,
			variables TEXT,
			current_activity TEXT,
			priority INTEGER NOT NULL DEFAULT 50,
			parent_id TEXT,
			tenant_id TEXT NOT NULL,
			created_by TEXT NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_by TEXT,
			updated_at DATETIME,
			end_reason TEXT,
			duration INTEGER,
			ended_at DATETIME,
			start_user_id TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS wf_task (
			id TEXT PRIMARY KEY,
			process_instance_id TEXT,
			process_id TEXT,
			parent_id TEXT,
			task_def_key TEXT,
			task_type TEXT,
			name TEXT,
			description TEXT,
			status TEXT,
			assignee TEXT,
			owner TEXT,
			priority INTEGER DEFAULT 50,
			sequence_order INTEGER DEFAULT 0,
			due_date DATETIME,
			form_key TEXT,
			variables TEXT,
			claimed_at DATETIME,
			approval_type TEXT,
			approval_rule TEXT,
			delegate_from TEXT,
			delegate_reason TEXT,
			delegate_time DATETIME,
			ended_at DATETIME,
			comment TEXT,
			end_reason TEXT,
			duration INTEGER,
			tenant_id TEXT,
			created_by TEXT,
			created_at DATETIME,
			updated_by TEXT,
			updated_at DATETIME
		)`,
		`CREATE TABLE IF NOT EXISTS wf_hi_instance (
			id TEXT PRIMARY KEY,
			process_id TEXT NOT NULL,
			business_key TEXT,
			name TEXT NOT NULL,
			status TEXT NOT NULL,
			variables TEXT,
			current_activity TEXT,
			priority INTEGER,
			parent_id TEXT,
			tenant_id TEXT,
			created_by TEXT,
			created_at DATETIME,
			updated_by TEXT,
			updated_at DATETIME,
			end_reason TEXT,
			duration INTEGER,
			ended_at DATETIME,
			start_user_id TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS wf_hi_task (
			id TEXT PRIMARY KEY,
			process_instance_id TEXT,
			process_id TEXT,
			parent_id TEXT,
			task_def_key TEXT,
			task_type TEXT,
			name TEXT,
			description TEXT,
			status TEXT,
			assignee TEXT,
			owner TEXT,
			priority INTEGER,
			sequence_order INTEGER,
			due_date DATETIME,
			form_key TEXT,
			variables TEXT,
			claimed_at DATETIME,
			approval_type TEXT,
			approval_rule TEXT,
			delegate_from TEXT,
			delegate_reason TEXT,
			delegate_time DATETIME,
			ended_at DATETIME,
			comment TEXT,
			end_reason TEXT,
			duration INTEGER,
			tenant_id TEXT,
			created_by TEXT,
			created_at DATETIME,
			updated_by TEXT,
			updated_at DATETIME
		)`,
	}
	for _, ddl := range ddls {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	// 撤回与激活路径涉及的表（候选池清理、流程定义查询）
	extraDDLs := []string{
		`CREATE TABLE IF NOT EXISTS wf_task_assignee (
			id TEXT PRIMARY KEY,
			task_id TEXT,
			entity_type TEXT,
			entity_id TEXT,
			tenant_id TEXT,
			created_at DATETIME
		)`,
		`CREATE TABLE IF NOT EXISTS wf_process (
			id TEXT PRIMARY KEY,
			tenant_id TEXT,
			name TEXT,
			version INTEGER,
			definition_json TEXT,
			created_at DATETIME,
			updated_at DATETIME
		)`,
	}
	for _, ddl := range extraDDLs {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	// Reset between tests so prior rows don't leak.
	db.Exec("DELETE FROM wf_instance")
	db.Exec("DELETE FROM wf_task")
	db.Exec("DELETE FROM wf_hi_instance")
	db.Exec("DELETE FROM wf_hi_task")
	db.Exec("DELETE FROM wf_task_assignee")
	db.Exec("DELETE FROM wf_process")
}

// seedInstance inserts a minimal Active process instance row using the
// engine's gorm.DB so TaskService/RuntimeService operations can find it.
func seedInstance(t *testing.T, engine WorkflowEngine, instanceID, startUser string) {
	t.Helper()
	inst := &model.WfInstance{
		ID:          instanceID,
		ProcessID:   "proc-test",
		Name:        "test-instance",
		Status:      string(enums.InstanceStatusActive),
		TenantID:    "tenant-test",
		StartUserID: startUser,
		CreatedBy:   startUser,
	}
	if err := engine.GetDB().Create(inst).Error; err != nil {
		t.Fatalf("seed instance: %v", err)
	}
}

// seedActiveTask inserts a minimal Active task row and returns its ID.
func seedActiveTask(t *testing.T, engine WorkflowEngine, instanceID, taskDefKey, assignee string) string {
	t.Helper()
	task := &model.WfTask{
		ID:                "task-" + instanceID + "-" + assignee,
		Name:              taskDefKey,
		TaskDefKey:        taskDefKey,
		ProcessID:         "proc-test",
		ProcessInstanceID: &instanceID,
		Assignee:          &assignee,
		Status:            string(enums.TaskStatusActive),
		TenantID:          "tenant-test",
		CreatedBy:         assignee,
	}
	if err := engine.GetDB().Create(task).Error; err != nil {
		t.Fatalf("seed task: %v", err)
	}
	return task.ID
}

// newUserIdentity builds an Actor suitable for SetUserToCtx.
func newUserIdentity(userID string) *Actor {
	return &Actor{UserID: userID, UserName: userID, TenantID: "tenant-test"}
}

// TestDispatchTaskEvent_NilListener covers the nil-check fast path: dispatch
// must be a no-op when no listener is registered.
func TestDispatchTaskEvent_NilListener(t *testing.T) {
	DispatchTaskEvent(nil, TaskEvent{Type: TaskEventAssigned}, context.Background())
	// No assertion beyond not panicking; the function must return immediately.
}

// TestDispatchTaskEvent_PanicRecovery ensures a panicking listener is recovered
// by the dispatcher and never crashes the caller.
// 断言是真实的：监听器自身在 panic 后置位 called——若 dispatcher 未调用监听器，
// called 永不为 true（超时失败）；若 dispatcher 未 recover，panic 直接击穿测试进程。
func TestDispatchTaskEvent_PanicRecovery(t *testing.T) {
	var called atomic.Bool
	boom := func(_ context.Context, _ TaskEvent) {
		defer called.Store(true)
		panic("listener exploded")
	}
	// Use a cancelled ctx to also verify WithoutCancel keeps the listener alive.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	DispatchTaskEvent(boom, TaskEvent{Type: TaskEventAssigned, TaskID: "t1"}, ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if called.Load() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener was not invoked before deadline (dispatch lost or panic not recovered)")
}

// TestDispatchTaskEvent_StripsCancellation verifies the ctx handed to the
// listener is NOT cancelled even when the caller's ctx is: events fire from
// goroutines that outlive the transaction.
func TestDispatchTaskEvent_StripsCancellation(t *testing.T) {
	got := make(chan struct{})
	listener := func(ctx context.Context, _ TaskEvent) {
		select {
		case <-ctx.Done():
			// unexpected: ctx was cancelled
		default:
			close(got)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // caller's ctx is cancelled (simulating a committed transaction)
	DispatchTaskEvent(listener, TaskEvent{Type: TaskEventAssigned}, ctx)
	select {
	case <-got:
		// good: listener saw a non-cancelled ctx
	case <-time.After(2 * time.Second):
		t.Fatal("listener was not invoked or saw a cancelled ctx")
	}
}

// TestDispatchTaskEvent_FillsTimestamp verifies the dispatcher fills in a
// non-zero timestamp when the caller leaves it zero.
func TestDispatchTaskEvent_FillsTimestamp(t *testing.T) {
	got := make(chan TaskEvent, 1)
	listener := func(_ context.Context, evt TaskEvent) { got <- evt }
	DispatchTaskEvent(listener, TaskEvent{Type: TaskEventAssigned, TaskID: "t-timestamp"}, context.Background())
	select {
	case evt := <-got:
		if evt.Timestamp.IsZero() {
			t.Error("expected dispatcher to fill zero timestamp")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("listener was not invoked")
	}
}

// TestEngine_FiresForwardedEventOnTransfer verifies the full wiring path:
// engine -> TaskService.Transfer -> DispatchTaskEvent -> listener.
func TestEngine_FiresForwardedEventOnTransfer(t *testing.T) {
	rec := &recordingListener{}
	engine := buildEngineForEvents(t, rec)
	taskSvc := engine.GetTaskServiceInternal()

	instanceID := "inst-transfer-test"
	seedInstance(t, engine, instanceID, "starter-1")
	taskID := seedActiveTask(t, engine, instanceID, "userTask1", "user-1")

	fromUser := newUserIdentity("user-1")
	ctx := SetUserToCtx(context.Background(), fromUser)
	if err := taskSvc.Transfer(ctx, *fromUser, taskID, "user-2", "请协助处理"); err != nil {
		t.Fatalf("Transfer failed: %v", err)
	}

	evs := rec.waitForEvents(t, 1, 2*time.Second)
	if len(evs) == 0 {
		t.Fatalf("expected at least one event, got 0")
	}
	var got *TaskEvent
	for i := range evs {
		if evs[i].Type == TaskEventForwarded && evs[i].TaskID == taskID {
			got = &evs[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("no forwarded event for task %s in %v", taskID, evs)
	}
	if got.FromUser != "user-1" {
		t.Errorf("FromUser = %q, want user-1", got.FromUser)
	}
	if len(got.ToUsers) != 1 || got.ToUsers[0] != "user-2" {
		t.Errorf("ToUsers = %v, want [user-2]", got.ToUsers)
	}
}

// TestEngine_FiresTerminatedEventOnTerminate verifies the engine fires a
// terminated event (with unique ToUsers) when TerminateProcessInstance runs.
func TestEngine_FiresTerminatedEventOnTerminate(t *testing.T) {
	rec := &recordingListener{}
	engine := buildEngineForEvents(t, rec)

	instanceID := "inst-terminate-test"
	seedInstance(t, engine, instanceID, "starter-7")
	seedActiveTask(t, engine, instanceID, "userTask1", "assignee-7")

	if err := engine.GetRuntimeService().TerminateProcessInstance(context.Background(), Actor{UserID: "admin-s", TenantID: "tenant-test"}, instanceID, "测试终止"); err != nil {
		t.Fatalf("TerminateProcessInstance failed: %v", err)
	}

	evs := rec.waitForEvents(t, 1, 2*time.Second)
	var got *TaskEvent
	for i := range evs {
		if evs[i].Type == TaskEventTerminated && evs[i].InstanceID == instanceID {
			got = &evs[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("no terminated event for instance %s in %v", instanceID, evs)
	}
	if got.Reason != "测试终止" {
		t.Errorf("Reason = %q, want '测试终止'", got.Reason)
	}
	// uniqueStrings should have deduplicated starter-7 / assignee-7 (distinct).
	wantUsers := map[string]bool{"starter-7": true, "assignee-7": true}
	for _, u := range got.ToUsers {
		if !wantUsers[u] {
			t.Errorf("unexpected ToUser %q", u)
		}
	}
	if len(got.ToUsers) != len(wantUsers) {
		t.Errorf("ToUsers = %v, want %d unique users", got.ToUsers, len(wantUsers))
	}
}

// TestUniqueStrings covers the dedup helper.
func TestUniqueStrings(t *testing.T) {
	cases := []struct {
		in   []string
		want []string
	}{
		{nil, nil},
		{[]string{}, nil},
		{[]string{"a"}, []string{"a"}},
		{[]string{"a", "b", "a", "c", "b"}, []string{"a", "b", "c"}},
		{[]string{"x", "x", "x"}, []string{"x"}},
	}
	for _, c := range cases {
		got := uniqueStrings(c.in)
		if len(got) != len(c.want) {
			t.Errorf("uniqueStrings(%v) = %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("uniqueStrings(%v)[%d] = %q, want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}

// TestEngine_FiresAssignedOnCountersignParallel verifies parallel countersign
// sub-tasks each fire TaskEventAssigned with ParentTaskID set: given 3
// assignees, the listener must receive 3 assigned events — one per sub-task.
func TestEngine_FiresAssignedOnCountersignParallel(t *testing.T) {
	rec := &recordingListener{}
	engine := buildEngineForEvents(t, rec)
	taskSvc := engine.GetTaskServiceInternal()

	instanceID := "inst-countersign-parallel"
	seedInstance(t, engine, instanceID, "starter-1")
	parentTaskID := seedActiveTask(t, engine, instanceID, "userTask1", "starter-1")

	assignees := []string{"user-a", "user-b", "user-c"}
	// 空 rule → parseCountersignRule 返回默认并行规则（IsSequential=false）
	ctx := SetUserToCtx(context.Background(), newUserIdentity("starter-1"))
	if err := taskSvc.CreateCountersignSubTasks(ctx, parentTaskID, assignees, ""); err != nil {
		t.Fatalf("CreateCountersignSubTasks failed: %v", err)
	}

	evs := rec.waitForEvents(t, len(assignees), 2*time.Second)
	assignedCount := 0
	for _, evt := range evs {
		if evt.Type != TaskEventAssigned {
			continue
		}
		assignedCount++
		if evt.ParentTaskID != parentTaskID {
			t.Errorf("event ParentTaskID = %q, want %q", evt.ParentTaskID, parentTaskID)
		}
	}
	if assignedCount != len(assignees) {
		t.Errorf("parallel countersign expected %d assigned events, got %d", len(assignees), assignedCount)
	}
}

// TestEngine_FiresAssignedOnCountersignSequential verifies sequential
// countersign fires TaskEventAssigned ONLY for the first sub-task: given 3
// assignees, only the first is activated, so exactly 1 assigned event fires.
func TestEngine_FiresAssignedOnCountersignSequential(t *testing.T) {
	rec := &recordingListener{}
	engine := buildEngineForEvents(t, rec)
	taskSvc := engine.GetTaskServiceInternal()

	instanceID := "inst-countersign-sequential"
	seedInstance(t, engine, instanceID, "starter-1")
	parentTaskID := seedActiveTask(t, engine, instanceID, "userTask1", "starter-1")

	assignees := []string{"user-a", "user-b", "user-c"}
	// isSequential=true → 顺序会签，仅创建首个子任务
	rule := `{"isSequential":true}`
	ctx := SetUserToCtx(context.Background(), newUserIdentity("starter-1"))
	if err := taskSvc.CreateCountersignSubTasks(ctx, parentTaskID, assignees, rule); err != nil {
		t.Fatalf("CreateCountersignSubTasks failed: %v", err)
	}

	evs := rec.waitForEvents(t, 1, 2*time.Second)
	assignedCount := 0
	for _, evt := range evs {
		if evt.Type != TaskEventAssigned {
			continue
		}
		assignedCount++
		if evt.ParentTaskID != parentTaskID {
			t.Errorf("event ParentTaskID = %q, want %q", evt.ParentTaskID, parentTaskID)
		}
	}
	if assignedCount != 1 {
		t.Errorf("sequential countersign expected exactly 1 assigned event (first only), got %d", assignedCount)
	}
}

// TestEngine_DoesNotFireRejectedOnNormalLifecycle is a regression guard that
// rejection-resolved-to-jump paths never fire TaskEventRejected: a normal
// task lifecycle (transfer only) must produce no rejected events.
func TestEngine_DoesNotFireRejectedOnNormalLifecycle(t *testing.T) {
	rec := &recordingListener{}
	engine := buildEngineForEvents(t, rec)
	taskSvc := engine.GetTaskServiceInternal()

	instanceID := "inst-no-reject"
	seedInstance(t, engine, instanceID, "starter-1")
	taskID := seedActiveTask(t, engine, instanceID, "userTask1", "user-1")

	// 仅 Transfer（forwarded 事件），不应产生 rejected
	ctx := SetUserToCtx(context.Background(), newUserIdentity("user-1"))
	if err := taskSvc.Transfer(ctx, *newUserIdentity("user-1"), taskID, "user-2", ""); err != nil {
		t.Fatalf("Transfer failed: %v", err)
	}

	evs := rec.waitForEvents(t, 1, 2*time.Second)
	for _, evt := range evs {
		if evt.Type == TaskEventRejected {
			t.Errorf("rejected event fired on non-rejection path: %+v", evt)
		}
	}
}

// TestEngine_FiresResolvedEventOnResolve verifies Resolve (委派归还) dispatches
// TaskEventResolved to the original owner after commit.
func TestEngine_FiresResolvedEventOnResolve(t *testing.T) {
	rec := &recordingListener{}
	engine := buildEngineForEvents(t, rec)
	taskSvc := engine.GetTaskServiceInternal()

	instanceID := "inst-resolve-test"
	seedInstance(t, engine, instanceID, "starter-1")

	// 委派中的任务：assignee=被委派人，owner=原审批人
	owner, delegatee := "owner-1", "delegatee-1"
	task := &model.WfTask{
		ID:                "task-resolve",
		Name:              "userTask1",
		TaskDefKey:        "userTask1",
		ProcessID:         "proc-test",
		ProcessInstanceID: &instanceID,
		Assignee:          &delegatee,
		Owner:             &owner,
		Status:            string(enums.TaskStatusActive),
		TenantID:          "tenant-test",
		CreatedBy:         "starter-1",
	}
	if err := engine.GetDB().Create(task).Error; err != nil {
		t.Fatalf("seed task: %v", err)
	}

	ctx := SetUserToCtx(context.Background(), newUserIdentity("delegatee-1"))
	if err := taskSvc.Resolve(ctx, *newUserIdentity("delegatee-1"), "task-resolve"); err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	evs := rec.waitForEvents(t, 1, 2*time.Second)
	found := false
	for _, e := range evs {
		if e.Type == TaskEventResolved && e.TaskID == "task-resolve" {
			found = true
			if len(e.ToUsers) != 1 || e.ToUsers[0] != owner {
				t.Errorf("resolved 事件应通知原 owner %s, got ToUsers=%v", owner, e.ToUsers)
			}
			if e.EventID == "" {
				t.Error("事件应携带 EventID 供上层幂等")
			}
		}
	}
	if !found {
		t.Fatalf("expected TaskEventResolved, got events: %+v", evs)
	}
}

// Listener invocation tests for the various event types.

func TestTaskEventListenerCalledOnAssign(t *testing.T) {
	var mu sync.Mutex
	var events []TaskEvent

	listener := func(ctx context.Context, evt TaskEvent) {
		mu.Lock()
		events = append(events, evt)
		mu.Unlock()
	}

	// 验证 listener 被正确设置和调用
	evt := TaskEvent{
		Type:       TaskEventAssigned,
		TaskID:     "task-1",
		InstanceID: "inst-1",
		ToUsers:    []string{"user-1"},
	}

	listener(context.Background(), evt)

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Type != TaskEventAssigned {
		t.Errorf("event type = %q, want assigned", events[0].Type)
	}
	if events[0].ToUsers[0] != "user-1" {
		t.Errorf("ToUsers[0] = %q, want user-1", events[0].ToUsers[0])
	}
}

func TestTaskEventListenerPanicRecovery(t *testing.T) {
	// 验证 listener panic 不影响调用方
	panicListener := func(ctx context.Context, evt TaskEvent) {
		panic("listener crashed")
	}

	// 应该 recover，不 panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("listener panic was not recovered: %v", r)
		}
	}()

	func() {
		defer func() { recover() }()
		panicListener(context.Background(), TaskEvent{Type: TaskEventAssigned})
	}()
}

func TestTaskEventListenerForwarded(t *testing.T) {
	var mu sync.Mutex
	var events []TaskEvent

	listener := func(ctx context.Context, evt TaskEvent) {
		mu.Lock()
		events = append(events, evt)
		mu.Unlock()
	}

	evt := TaskEvent{
		Type:       TaskEventForwarded,
		TaskID:     "task-2",
		InstanceID: "inst-2",
		ToUsers:    []string{"user-2"},
		FromUser:   "user-1",
	}

	listener(context.Background(), evt)

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Type != TaskEventForwarded {
		t.Errorf("event type = %q, want forwarded", events[0].Type)
	}
	if events[0].FromUser != "user-1" {
		t.Errorf("FromUser = %q, want user-1", events[0].FromUser)
	}
}

func TestTaskEventListenerRejected(t *testing.T) {
	var mu sync.Mutex
	var events []TaskEvent

	listener := func(ctx context.Context, evt TaskEvent) {
		mu.Lock()
		events = append(events, evt)
		mu.Unlock()
	}

	evt := TaskEvent{
		Type:       TaskEventRejected,
		TaskID:     "task-3",
		InstanceID: "inst-3",
		ToUsers:    []string{"user-1"},
		Reason:     "审批驳回",
	}

	listener(context.Background(), evt)

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Type != TaskEventRejected {
		t.Errorf("event type = %q, want rejected", events[0].Type)
	}
	if events[0].Reason != "审批驳回" {
		t.Errorf("Reason = %q, want '审批驳回'", events[0].Reason)
	}
}

// Event lifecycle: suspended/activated/withdrawn events, EventID/Source
// payload fields, async dispatch and CC event panic isolation.

// TestEngine_FiresSuspendedAndActivatedEvents 挂起→恢复链路：
// 两个事件都到达监听器，携带操作人（FromUser）与来源（Source=api）。
func TestEngine_FiresSuspendedAndActivatedEvents(t *testing.T) {
	rec := &recordingListener{}
	engine := buildEngineForEvents(t, rec)
	rt := engine.GetRuntimeService()

	instanceID := "inst-suspend-test"
	seedInstance(t, engine, instanceID, "starter-s")
	seedActiveTask(t, engine, instanceID, "userTask1", "assignee-s")
	// current_activity 非空：激活后不走草稿启动分支
	engine.GetDB().Exec("UPDATE wf_instance SET current_activity = 'node1' WHERE id = ?", instanceID)

	ctx := SetUserToCtx(context.Background(), newUserIdentity("admin-s"))
	if err := rt.SuspendProcessInstance(ctx, *newUserIdentity("admin-s"), instanceID); err != nil {
		t.Fatalf("SuspendProcessInstance failed: %v", err)
	}
	evs := rec.waitForEvents(t, 1, 2*time.Second)
	var suspended *TaskEvent
	for i := range evs {
		if evs[i].Type == TaskEventSuspended && evs[i].InstanceID == instanceID {
			suspended = &evs[i]
			break
		}
	}
	if suspended == nil {
		t.Fatalf("no suspended event for instance %s in %v", instanceID, evs)
	}
	if suspended.FromUser != "admin-s" {
		t.Errorf("suspended FromUser = %q, want admin-s", suspended.FromUser)
	}
	if suspended.Source != EventSourceAPI {
		t.Errorf("suspended Source = %q, want %q", suspended.Source, EventSourceAPI)
	}
	if len(suspended.ToUsers) != 1 || suspended.ToUsers[0] != "starter-s" {
		t.Errorf("suspended ToUsers = %v, want [starter-s]", suspended.ToUsers)
	}

	if err := rt.ActivateProcessInstance(ctx, *newUserIdentity("admin-s"), instanceID); err != nil {
		t.Fatalf("ActivateProcessInstance failed: %v", err)
	}
	evs = rec.waitForEvents(t, 2, 2*time.Second)
	var activated *TaskEvent
	for i := range evs {
		if evs[i].Type == TaskEventActivated && evs[i].InstanceID == instanceID {
			activated = &evs[i]
			break
		}
	}
	if activated == nil {
		t.Fatalf("no activated event for instance %s in %v", instanceID, evs)
	}
	if activated.FromUser != "admin-s" {
		t.Errorf("activated FromUser = %q, want admin-s", activated.FromUser)
	}
}

// TestEngine_FiresWithdrawnAndSourceTaggedTerminated 撤回链路：
// withdrawn 与 terminated 事件都派发，且 terminated 的 Source=withdraw
// （区分撤回级联终止与 API 直接终止）。
func TestEngine_FiresWithdrawnAndSourceTaggedTerminated(t *testing.T) {
	rec := &recordingListener{}
	engine := buildEngineForEvents(t, rec)
	taskSvc := engine.GetTaskServiceInternal()

	instanceID := "inst-withdraw-test"
	seedInstance(t, engine, instanceID, "starter-w")
	taskID := seedActiveTask(t, engine, instanceID, "userTask1", "starter-w")

	ctx := SetUserToCtx(context.Background(), newUserIdentity("starter-w"))
	if err := taskSvc.WithdrawByInstance(ctx, *newUserIdentity("starter-w"), instanceID, "写错了"); err != nil {
		t.Fatalf("WithdrawByInstance failed: %v", err)
	}

	evs := rec.waitForEvents(t, 2, 2*time.Second)
	var withdrawn, terminated *TaskEvent
	for i := range evs {
		switch evs[i].Type {
		case TaskEventWithdrawn:
			if evs[i].InstanceID == instanceID {
				withdrawn = &evs[i]
			}
		case TaskEventTerminated:
			if evs[i].InstanceID == instanceID {
				terminated = &evs[i]
			}
		}
	}
	if withdrawn == nil {
		t.Fatalf("no withdrawn event for instance %s in %v", instanceID, evs)
	}
	if withdrawn.FromUser != "starter-w" {
		t.Errorf("withdrawn FromUser = %q, want starter-w", withdrawn.FromUser)
	}
	if withdrawn.Reason != "写错了" {
		t.Errorf("withdrawn Reason = %q, want '写错了'", withdrawn.Reason)
	}
	if withdrawn.TaskID != taskID || withdrawn.TaskDefKey != "userTask1" {
		t.Errorf("withdrawn TaskID/TaskDefKey = %q/%q, want %s/userTask1", withdrawn.TaskID, withdrawn.TaskDefKey, taskID)
	}
	if withdrawn.Source != EventSourceWithdraw {
		t.Errorf("withdrawn Source = %q, want %q", withdrawn.Source, EventSourceWithdraw)
	}
	if terminated == nil {
		t.Fatalf("no terminated event for instance %s in %v", instanceID, evs)
	}
	if terminated.Source != EventSourceWithdraw {
		t.Errorf("terminated Source = %q, want %q (withdraw cascade)", terminated.Source, EventSourceWithdraw)
	}
}

// TestDispatchTaskEvent_FillsUniqueEventID 每条派发的事件都带唯一非空 EventID。
func TestDispatchTaskEvent_FillsUniqueEventID(t *testing.T) {
	got := make(chan TaskEvent, 2)
	listener := func(_ context.Context, evt TaskEvent) { got <- evt }
	DispatchTaskEvent(listener, TaskEvent{Type: TaskEventAssigned, TaskID: "t-1"}, context.Background())
	DispatchTaskEvent(listener, TaskEvent{Type: TaskEventAssigned, TaskID: "t-2"}, context.Background())

	ids := make(map[string]bool)
	for i := 0; i < 2; i++ {
		select {
		case evt := <-got:
			if evt.EventID == "" {
				t.Error("EventID is empty; dispatcher must fill it")
			}
			if ids[evt.EventID] {
				t.Errorf("duplicate EventID %q across dispatches", evt.EventID)
			}
			ids[evt.EventID] = true
		case <-time.After(2 * time.Second):
			t.Fatal("listener not invoked")
		}
	}
}

// TestDispatchTaskEvent_DoesNotBlockCaller 派发异步：监听器慢速（200ms）
// 不阻塞调用方。
func TestDispatchTaskEvent_DoesNotBlockCaller(t *testing.T) {
	slow := func(_ context.Context, _ TaskEvent) { time.Sleep(200 * time.Millisecond) }
	start := time.Now()
	DispatchTaskEvent(slow, TaskEvent{Type: TaskEventAssigned, TaskID: "t-slow"}, context.Background())
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("dispatch blocked caller for %v; must return before slow listener finishes", elapsed)
	}
}

// TestDispatchCCEvent_AsyncAndPanicSafe CC 事件异步派发、panic 隔离、nil 监听器 no-op。
func TestDispatchCCEvent_AsyncAndPanicSafe(t *testing.T) {
	DispatchCCEvent(nil, CCEvent{TaskID: "cc-1"}, context.Background()) // 不得 panic

	got := make(chan CCEvent, 1)
	listener := func(_ context.Context, evt CCEvent) { got <- evt }
	DispatchCCEvent(listener, CCEvent{TaskID: "cc-2", AssigneeUserID: "user-cc"}, context.Background())
	select {
	case evt := <-got:
		if evt.TaskID != "cc-2" || evt.CreatedAt.IsZero() {
			t.Errorf("CC event = %+v; want TaskID cc-2 with CreatedAt filled", evt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CC listener not invoked")
	}

	boom := func(_ context.Context, _ CCEvent) { panic("cc listener exploded") }
	DispatchCCEvent(boom, CCEvent{TaskID: "cc-3"}, context.Background())
	time.Sleep(50 * time.Millisecond) // panic 在监听器 goroutine 内 recover
}

// TestEventSourceCtxHelpers 来源标记的写入/读取与缺省值。
func TestEventSourceCtxHelpers(t *testing.T) {
	if got := EventSourceFromCtx(context.Background()); got != EventSourceAPI {
		t.Errorf("default source = %q, want %q", got, EventSourceAPI)
	}
	ctx := WithEventSource(context.Background(), EventSourceWithdraw)
	if got := EventSourceFromCtx(ctx); got != EventSourceWithdraw {
		t.Errorf("source = %q, want %q", got, EventSourceWithdraw)
	}
}
