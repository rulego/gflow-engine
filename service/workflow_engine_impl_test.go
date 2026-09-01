package service

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/rulego/gflow-engine/config"
)

// Tests for workflow_engine_impl.go.
// Covers lifecycle and validation NOT already tested in builder_test.go.

// ---------------------------------------------------------------------------
// Start - nil config
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_Start_NilConfig(t *testing.T) {
	e := NewWorkflowEngine("test", nil)
	err := e.Start(context.Background())
	if err == nil {
		t.Error("expected error starting with nil config")
	}
}

// ---------------------------------------------------------------------------
// Start - config with nil database
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_Start_NilDatabase(t *testing.T) {
	cfg := &config.Config{
		Database: nil,
	}
	e := NewWorkflowEngine("test", cfg)
	err := e.Start(context.Background())
	if err == nil {
		t.Error("expected error for nil database config")
	}
}

// ---------------------------------------------------------------------------
// Stop - not running
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_Stop_NotRunning(t *testing.T) {
	e := NewWorkflowEngine("test", nil)
	err := e.Stop(context.Background())
	if err == nil {
		t.Error("expected error stopping non-running engine")
	}
}

// ---------------------------------------------------------------------------
// initServices - custom implementations must satisfy the Internal interfaces
// ---------------------------------------------------------------------------

// 只满足公共接口的自定义实现（内嵌 nil 接口补齐方法集）必须在启动期被拒绝：
// 引擎内部级联经 Internal 接口调用这两个服务，缺失会在运行期以 nil 接口 panic。
func TestWorkflowEngineImpl_InitServices_RejectsPublicOnlyImpls(t *testing.T) {
	q := secFixDB(t)

	// runtime 只实现公共接口 → 拒绝
	e := &WorkflowEngineImpl{query: q}
	e.taskService = NewTaskServiceWithQuery(q, e)
	e.runtimeService = struct{ RuntimeService }{}
	err := e.initServices()
	if err == nil || !strings.Contains(err.Error(), "RuntimeServiceInternal") {
		t.Fatalf("expected RuntimeServiceInternal error, got %v", err)
	}

	// task 只实现公共接口 → 拒绝
	e2 := &WorkflowEngineImpl{query: q}
	e2.runtimeService = NewRuntimeServiceWithQuery(q, e2)
	e2.taskService = struct{ TaskService }{}
	err = e2.initServices()
	if err == nil || !strings.Contains(err.Error(), "TaskServiceInternal") {
		t.Fatalf("expected TaskServiceInternal error, got %v", err)
	}

	// 未注入（默认实现）→ 通过，默认实现天然满足 Internal 接口
	e3 := &WorkflowEngineImpl{query: q}
	if err := e3.initServices(); err != nil {
		t.Fatalf("default impls must satisfy Internal interfaces, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Double stop
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_DoubleStop(t *testing.T) {
	e := &WorkflowEngineImpl{name: "test", running: false}
	err := e.Stop(context.Background())
	if err == nil {
		t.Error("expected error on first stop (not running)")
	}
}

// ---------------------------------------------------------------------------
// IsRunning - initial state
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_IsRunning_Initial(t *testing.T) {
	e := NewWorkflowEngine("test", nil)
	if e.IsRunning() {
		t.Error("engine should not be running initially")
	}
}

// ---------------------------------------------------------------------------
// GetName / GetVersion
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_GetName(t *testing.T) {
	e := NewWorkflowEngine("my-engine", nil)
	if e.GetName() != "my-engine" {
		t.Errorf("GetName = %q, want 'my-engine'", e.GetName())
	}
}

func TestWorkflowEngineImpl_GetVersion(t *testing.T) {
	e := NewWorkflowEngine("test", nil)
	if e.GetVersion() != Version {
		t.Errorf("GetVersion = %q, want %q", e.GetVersion(), Version)
	}
}

// ---------------------------------------------------------------------------
// GetIDGenerator - nil when not set
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_GetIDGenerator_Nil(t *testing.T) {
	e := NewWorkflowEngine("test", nil)
	if gen := e.GetIDGenerator(); gen != nil {
		t.Errorf("expected nil IDGenerator, got %v", gen)
	}
}

// ---------------------------------------------------------------------------
// GetServices - nil when not set
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_GetServices_Nil(t *testing.T) {
	e := NewWorkflowEngine("test", nil)
	if e.GetTaskService() != nil {
		t.Error("expected nil TaskService")
	}
	if e.GetProcessService() != nil {
		t.Error("expected nil ProcessService")
	}
	if e.GetRuntimeService() != nil {
		t.Error("expected nil RuntimeService")
	}
	if e.GetHistoryService() != nil {
		t.Error("expected nil HistoryService")
	}
	if e.GetIdentityService() != nil {
		t.Error("expected nil IdentityService")
	}
	if e.GetLocker() != nil {
		t.Error("expected nil Locker")
	}
	if e.GetRuleChainExecutor() != nil {
		t.Error("expected nil RuleChainExecutor")
	}
}

// ---------------------------------------------------------------------------
// IsRunning - thread safety
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_IsRunning_ThreadSafe(t *testing.T) {
	e := NewWorkflowEngine("test", nil)
	done := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		go func() {
			_ = e.IsRunning()
			done <- true
		}()
	}
	for i := 0; i < 20; i++ {
		<-done
	}
}

// ---------------------------------------------------------------------------
// setName / setConfig / setIDGenerator / setLocker - via reflection-free approach
// We test these through the public builder API indirectly.
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_SetConfig_ThenStart_InvalidDriver(t *testing.T) {
	e := &WorkflowEngineImpl{name: "test"}
	cfg := &config.Config{
		Database: &config.DatabaseConfig{
			Driver: "unsupported",
			Dsn:    "test",
		},
	}
	e.setConfig(cfg)
	err := e.Start(context.Background())
	if err == nil {
		t.Error("expected error for unsupported driver")
	}
}

func TestWorkflowEngineImpl_SetName(t *testing.T) {
	e := &WorkflowEngineImpl{}
	e.setName("custom")
	if e.GetName() != "custom" {
		t.Errorf("GetName = %q, want 'custom'", e.GetName())
	}
}

func TestWorkflowEngineImpl_SetIDGenerator(t *testing.T) {
	e := &WorkflowEngineImpl{}
	gen := NewIDGenerator()
	e.setIDGenerator(gen)
	if e.GetIDGenerator() != gen {
		t.Error("expected IDGenerator to be set")
	}
}

func TestWorkflowEngineImpl_SetLocker(t *testing.T) {
	e := &WorkflowEngineImpl{}
	e.setLocker(nil)
	if e.GetLocker() != nil {
		t.Error("expected nil locker")
	}
}

func TestWorkflowEngineImpl_SetRuleChainExecutor(t *testing.T) {
	e := &WorkflowEngineImpl{}
	e.setRuleChainExecutor(nil)
	if e.GetRuleChainExecutor() != nil {
		t.Error("expected nil executor")
	}
}

// ---------------------------------------------------------------------------
// setTaskService / setProcessService / setRuntimeService / etc.
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_SetServices(t *testing.T) {
	e := &WorkflowEngineImpl{}

	e.setTaskService(nil)
	if e.GetTaskService() != nil {
		t.Error("expected nil TaskService")
	}

	e.setProcessService(nil)
	if e.GetProcessService() != nil {
		t.Error("expected nil ProcessService")
	}

	e.setRuntimeService(nil)
	if e.GetRuntimeService() != nil {
		t.Error("expected nil RuntimeService")
	}

	e.setHistoryService(nil)
	if e.GetHistoryService() != nil {
		t.Error("expected nil HistoryService")
	}

	e.setIdentityService(nil)
	if e.GetIdentityService() != nil {
		t.Error("expected nil IdentityService")
	}
}

// ---------------------------------------------------------------------------
// Start with invalid driver
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_Start_InvalidDriver(t *testing.T) {
	cfg := &config.Config{
		Database: &config.DatabaseConfig{
			Driver: "unsupported_db",
			Dsn:    "test",
		},
	}
	e := &WorkflowEngineImpl{name: "test", config: cfg}
	err := e.Start(context.Background())
	if err == nil {
		t.Error("expected error for unsupported driver")
	}
}

// ---------------------------------------------------------------------------
// Start/Stop concurrent safety
// ---------------------------------------------------------------------------

func TestWorkflowEngineImpl_ConcurrentIsRunning(t *testing.T) {
	e := &WorkflowEngineImpl{name: "test", running: true}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = e.IsRunning()
			_ = e.GetName()
			_ = e.GetVersion()
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// NewWorkflowEngine constructor
// ---------------------------------------------------------------------------

func TestNewWorkflowEngine_Constructor(t *testing.T) {
	e := NewWorkflowEngine("test", nil)
	if e == nil {
		t.Fatal("expected non-nil engine")
	}
	if e.GetName() != "test" {
		t.Errorf("GetName = %q, want 'test'", e.GetName())
	}
	if e.IsRunning() {
		t.Error("should not be running")
	}
}

// Set + Add 注册的多个任务事件监听器必须全部收到事件（组合派发）。
func TestWorkflowEngineImpl_MultiTaskEventListeners(t *testing.T) {
	e := &WorkflowEngineImpl{}
	var mu sync.Mutex
	got := map[string]int{}
	mk := func(tag string) TaskEventListener {
		return func(ctx context.Context, evt TaskEvent) {
			mu.Lock()
			got[tag]++
			mu.Unlock()
		}
	}
	e.setTaskEventListener(mk("set"))
	e.addTaskEventListener(mk("add1"))
	e.addTaskEventListener(mk("add2"))

	listener := e.GetTaskEventListener()
	if listener == nil {
		t.Fatal("注册监听器后 getter 不应返回 nil")
	}
	listener(context.Background(), TaskEvent{Type: TaskEventAssigned, TaskID: "t1"})

	for _, tag := range []string{"set", "add1", "add2"} {
		if got[tag] != 1 {
			t.Errorf("监听器 %s 应收到 1 次事件，实际 %d", tag, got[tag])
		}
	}

	// 未注册任何监听器时 getter 返回 nil
	empty := &WorkflowEngineImpl{}
	if empty.GetTaskEventListener() != nil {
		t.Error("未注册监听器时应返回 nil")
	}
	if empty.GetCCTaskCreatedListener() != nil {
		t.Error("未注册 CC 监听器时应返回 nil")
	}
}
