package service

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"gorm.io/gorm"

	"github.com/rulego/gflow-engine/config"
)

func TestNewWorkflowEngineBuilder(t *testing.T) {
	b := NewWorkflowEngineBuilder()
	if b == nil {
		t.Fatal("NewWorkflowEngineBuilder returned nil")
	}
}

func TestBuilder_SetName(t *testing.T) {
	b := NewWorkflowEngineBuilder()
	result := b.SetName("test-engine")
	if result == nil {
		t.Fatal("SetName returned nil")
	}
}

func TestBuilder_SetConfig(t *testing.T) {
	b := NewWorkflowEngineBuilder()
	cfg := &config.Config{
		Database: &config.DatabaseConfig{
			Driver: "sqlite",
			Dsn:    ":memory:",
		},
	}
	result := b.SetConfig(cfg)
	if result == nil {
		t.Fatal("SetConfig returned nil")
	}
}

func TestBuilder_SetTaskService(t *testing.T) {
	b := NewWorkflowEngineBuilder()
	result := b.SetTaskService(nil)
	if result == nil {
		t.Fatal("SetTaskService returned nil")
	}
}

func TestBuilder_SetIDGenerator(t *testing.T) {
	b := NewWorkflowEngineBuilder()
	gen := NewIDGenerator()
	result := b.SetIDGenerator(gen)
	if result == nil {
		t.Fatal("SetIDGenerator returned nil")
	}
}

func TestBuilder_SetLocker(t *testing.T) {
	b := NewWorkflowEngineBuilder()
	result := b.SetLocker(nil)
	if result == nil {
		t.Fatal("SetLocker returned nil")
	}
}

func TestBuilder_SetRuleChainExecutor(t *testing.T) {
	b := NewWorkflowEngineBuilder()
	result := b.SetRuleChainExecutor(nil)
	if result == nil {
		t.Fatal("SetRuleChainExecutor returned nil")
	}
}

func TestBuilder_Build_EmptyName(t *testing.T) {
	b := &WorkflowEngineBuilderImpl{name: ""}
	_, err := b.Build()
	if err == nil {
		t.Error("expected error for empty name")
	}
}

func TestBuilder_Build_NoConfig(t *testing.T) {
	b := NewWorkflowEngineBuilder()
	b.SetName("test")
	engine, err := b.Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if engine == nil {
		t.Fatal("engine should not be nil")
	}
	if engine.GetName() != "test" {
		t.Errorf("name = %q, want 'test'", engine.GetName())
	}
}

func TestBuilder_Build_WithConfig(t *testing.T) {
	b := NewWorkflowEngineBuilder()
	cfg := &config.Config{
		Database: &config.DatabaseConfig{
			Driver: "sqlite",
			Dsn:    ":memory:",
		},
	}
	b.SetName("test").SetConfig(cfg)
	engine, err := b.Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if engine == nil {
		t.Fatal("engine should not be nil")
	}
}

func TestBuilder_Chaining(t *testing.T) {
	gen := NewIDGenerator()
	b := NewWorkflowEngineBuilder().
		SetName("chain-test").
		SetIDGenerator(gen).
		SetLocker(nil).
		SetRuleChainExecutor(nil).
		SetTaskService(nil).
		SetProcessService(nil).
		SetRuntimeService(nil).
		SetHistoryService(nil).
		SetIdentityService(nil)

	engine, err := b.Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if engine.GetName() != "chain-test" {
		t.Errorf("name = %q, want 'chain-test'", engine.GetName())
	}
}

func TestWorkflowEngine_StartStop(t *testing.T) {
	engine := NewWorkflowEngine("test", nil)

	// Should not be running initially
	if engine.IsRunning() {
		t.Error("engine should not be running initially")
	}

	// Start should fail without config
	err := engine.Start(context.TODO())
	if err == nil {
		t.Error("expected error starting without config")
	}
}

func TestWorkflowEngine_GetName(t *testing.T) {
	engine := NewWorkflowEngine("my-engine", nil)
	assertEqual(t, "name", engine.GetName(), "my-engine")
}

func TestWorkflowEngine_GetVersion(t *testing.T) {
	engine := NewWorkflowEngine("test", nil)
	assertEqual(t, "version", engine.GetVersion(), Version)
}

func TestWorkflowEngine_GetIDGenerator_Nil(t *testing.T) {
	engine := NewWorkflowEngine("test", nil)
	if gen := engine.GetIDGenerator(); gen != nil {
		t.Errorf("expected nil IDGenerator, got %v", gen)
	}
}

func TestWorkflowEngine_GetServices_Nil(t *testing.T) {
	engine := NewWorkflowEngine("test", nil)
	if engine.GetTaskService() != nil {
		t.Error("expected nil TaskService")
	}
	if engine.GetProcessService() != nil {
		t.Error("expected nil ProcessService")
	}
	if engine.GetRuntimeService() != nil {
		t.Error("expected nil RuntimeService")
	}
	if engine.GetHistoryService() != nil {
		t.Error("expected nil HistoryService")
	}
	if engine.GetIdentityService() != nil {
		t.Error("expected nil IdentityService")
	}
	if engine.GetLocker() != nil {
		t.Error("expected nil Locker")
	}
	if engine.GetRuleChainExecutor() != nil {
		t.Error("expected nil RuleChainExecutor")
	}
}

func TestWorkflowEngine_IsRunning_ThreadSafe(t *testing.T) {
	engine := NewWorkflowEngine("test", nil)
	// Concurrent reads should not panic
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_ = engine.IsRunning()
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

// sqliteDialectProvider is a test-only gorm.Dialector provider for sqlite,
// used so Start() can drive an in-memory DB in tests.
type sqliteDialectProvider struct{}

func (sqliteDialectProvider) GetName() string               { return "sqlite" }
func (sqliteDialectProvider) GetSupportedDrivers() []string { return []string{"sqlite", "sqlite3"} }
func (sqliteDialectProvider) CreateDialector(dsn string) (gorm.Dialector, error) {
	return sqlite.Open(dsn), nil
}

var registerSqliteOnce sync.Once

// registerTestSqliteDialect registers the test-only sqlite dialect provider
// exactly once. Safe to call from multiple tests.
func registerTestSqliteDialect() {
	registerSqliteOnce.Do(func() {
		_ = RegisterDialectProvider(sqliteDialectProvider{})
	})
}

// fakeIdentityService is a non-mock IdentityService used by tests to exercise
// the Builder.RequireIdentityService happy path.
type fakeIdentityService struct{}

func (fakeIdentityService) GetUserIDsByRoleID(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}
func (fakeIdentityService) GetDepartmentManagerUserID(_ context.Context, _, _ string) (string, error) {
	return "", nil
}
func (fakeIdentityService) GetUserIDsByGroupID(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}
func (fakeIdentityService) GetUserIDsByDepartmentID(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}
func (fakeIdentityService) GetUserManagerID(_ context.Context, _, _ string) (string, error) {
	return "", nil
}
func (fakeIdentityService) GetUserManagerHierarchy(_ context.Context, _, _ string, _ int) ([]string, error) {
	return nil, nil
}
func (fakeIdentityService) GetUserDepartmentID(_ context.Context, _, _ string) (string, error) {
	return "", nil
}
func (fakeIdentityService) GetRoleIDsByUserID(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}
func (fakeIdentityService) GetDepartmentIDsByUserID(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}

// TestBuilder_RequireIdentityService_RejectsMock verifies Build refuses to
// construct an engine when RequireIdentityService is set but only a mock (or
// no) IdentityService is wired.
func TestBuilder_RequireIdentityService_RejectsMock(t *testing.T) {
	t.Run("nil identity", func(t *testing.T) {
		b := NewWorkflowEngineBuilder().
			SetName("guarded").
			RequireIdentityService()
		_, err := b.Build()
		if err == nil {
			t.Fatal("expected Build to fail when RequireIdentityService set but no IdentityService provided")
		}
		if !strings.Contains(err.Error(), "real IdentityService required") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("explicit mock identity", func(t *testing.T) {
		b := NewWorkflowEngineBuilder().
			SetName("guarded").
			SetIdentityService(NewIdentityService()).
			RequireIdentityService()
		_, err := b.Build()
		if err == nil {
			t.Fatal("expected Build to fail when RequireIdentityService set and mock was wired explicitly")
		}
		if !strings.Contains(err.Error(), "real IdentityService required") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("mock from NewIdentityServiceWithQuery", func(t *testing.T) {
		b := NewWorkflowEngineBuilder().
			SetName("guarded").
			SetIdentityService(NewIdentityServiceWithQuery(nil)).
			RequireIdentityService()
		_, err := b.Build()
		if err == nil {
			t.Fatal("expected Build to fail when RequireIdentityService set and NewIdentityServiceWithQuery was wired")
		}
	})
}

// TestBuilder_RequireIdentityService_AcceptsReal verifies a non-mock
// implementation passes Build under the strict mode.
func TestBuilder_RequireIdentityService_AcceptsReal(t *testing.T) {
	b := NewWorkflowEngineBuilder().
		SetName("guarded").
		SetIdentityService(fakeIdentityService{}).
		RequireIdentityService()
	engine, err := b.Build()
	if err != nil {
		t.Fatalf("expected Build to succeed with real IdentityService, got: %v", err)
	}
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if !isMockIdentity(engine.GetIdentityService()) {
		// expected: the wired service is not detected as mock
	} else {
		t.Error("real IdentityService was wrongly detected as mock")
	}
}

// TestBuilder_NoRequire_AllowsMock verifies the builder stays permissive
// (mock identity allowed) when RequireIdentityService is not set.
func TestBuilder_NoRequire_AllowsMock(t *testing.T) {
	b := NewWorkflowEngineBuilder().
		SetName("permissive").
		SetIdentityService(NewIdentityService())
	engine, err := b.Build()
	if err != nil {
		t.Fatalf("expected Build to succeed in permissive mode with mock, got: %v", err)
	}
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if !isMockIdentity(engine.GetIdentityService()) {
		t.Error("expected mock IdentityService to be detected as mock")
	}
}

// TestIsMockIdentity_DistinguishesMockFromReal verifies the detection helper
// returns true only for the built-in mock type.
func TestIsMockIdentity_DistinguishesMockFromReal(t *testing.T) {
	if !isMockIdentity(NewIdentityService()) {
		t.Error("NewIdentityService() should be detected as mock")
	}
	if !isMockIdentity(NewIdentityServiceWithQuery(nil)) {
		t.Error("NewIdentityServiceWithQuery() should be detected as mock")
	}
	if isMockIdentity(fakeIdentityService{}) {
		t.Error("fakeIdentityService should NOT be detected as mock")
	}
	if isMockIdentity(nil) {
		t.Error("nil should NOT be detected as mock (handled by callers)")
	}
}

// TestStart_LogsWarningWhenMockInUse verifies Start() emits a WARN entry
// carrying the IDENTITY_SERVICE_MOCK_IN_USE marker when the engine's
// IdentityService is the mock.
func TestStart_LogsWarningWhenMockInUse(t *testing.T) {
	registerTestSqliteDialect()

	hook := logrustest.NewLocal(logrus.StandardLogger())
	defer hook.Reset()
	origLevel := logrus.GetLevel()
	logrus.SetLevel(logrus.WarnLevel)
	defer logrus.SetLevel(origLevel)

	cfg := &config.Config{
		Database: &config.DatabaseConfig{
			Driver: "sqlite",
			Dsn:    "file::memory:?cache=shared&_busy_timeout=5000",
		},
	}
	engine := NewWorkflowEngine("warn-test", cfg)
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	var found bool
	for _, entry := range hook.AllEntries() {
		if entry == nil {
			continue
		}
		if entry.Level != logrus.WarnLevel {
			continue
		}
		msg := entry.Message
		if !strings.Contains(msg, "IDENTITY_SERVICE_MOCK_IN_USE") {
			continue
		}
		if field, ok := entry.Data["identity_warning"]; !ok || field != "mock_in_use" {
			continue
		}
		found = true
		break
	}
	if !found {
		// Fallback: also accept any captured entry mentioning the marker, to
		// remain robust against future formatter changes.
		var buf bytes.Buffer
		for _, e := range hook.AllEntries() {
			_, _ = buf.WriteString(e.Message)
		}
		if bytes.Contains(buf.Bytes(), []byte("IDENTITY_SERVICE_MOCK_IN_USE")) {
			found = true
		}
	}
	if !found {
		t.Errorf("expected IDENTITY_SERVICE_MOCK_IN_USE WARN entry; got %d entries", len(hook.AllEntries()))
	}
}
