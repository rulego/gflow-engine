package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/enums"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// newTestDB prepares an in-memory sqlite DB with the instance/task tables
// for WithInstanceTx tests. Raw DDL is used instead of AutoMigrate (which
// chokes on `comment:` tags under SQLite), and query.SetDefault is avoided
// so this file doesn't pollute global state on failures.
func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared&_busy_timeout=5000"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, e := db.DB(); e == nil {
			_ = sqlDB.Close()
		}
	})

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
			start_user_id TEXT NOT NULL,
			UNIQUE (tenant_id, business_key)
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
	}
	for _, ddl := range ddls {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	// Reset the shared in-memory DB between tests so prior rows don't leak.
	db.Exec("DELETE FROM wf_instance")
	db.Exec("DELETE FROM wf_task")
	return db
}

// insertTestInstance inserts an instance row and returns the *query.Query
// bound to the same DB.
func insertTestInstance(t *testing.T, db *gorm.DB, id, status string) *query.Query {
	t.Helper()
	q := query.Use(db)
	inst := &model.WfInstance{
		ID:          id,
		ProcessID:   "proc-1",
		Name:        "test",
		Status:      status,
		StartUserID: "u1",
		TenantID:    "t1",
	}
	if err := db.Create(inst).Error; err != nil {
		t.Fatalf("seed instance %s: %v", id, err)
	}
	return q
}

func TestWithInstanceTx_EmptyInstanceID_Errors(t *testing.T) {
	db := newTestDB(t)
	q := query.Use(db)
	err := WithInstanceTx(context.Background(), q, "", func(scope *InstanceScope) error {
		t.Fatal("fn should not be called when instanceID is empty")
		return nil
	})
	if err == nil {
		t.Fatal("expected error for empty instanceID")
	}
	if !strings.Contains(err.Error(), "instanceID is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestWithInstanceTx_NilQuery_Errors(t *testing.T) {
	err := WithInstanceTx(context.Background(), nil, "inst-1", func(scope *InstanceScope) error { return nil })
	if err == nil {
		t.Fatal("expected error for nil query")
	}
	if !strings.Contains(err.Error(), "query is nil") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestWithInstanceTx_NilFn_Errors(t *testing.T) {
	db := newTestDB(t)
	q := insertTestInstance(t, db, "inst-1", string(enums.InstanceStatusActive))
	err := WithInstanceTx(context.Background(), q, "inst-1", nil)
	if err == nil {
		t.Fatal("expected error for nil fn")
	}
	if !strings.Contains(err.Error(), "fn is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestWithInstanceTx_NotFound(t *testing.T) {
	db := newTestDB(t)
	q := query.Use(db)
	err := WithInstanceTx(context.Background(), q, "does-not-exist", func(scope *InstanceScope) error {
		t.Fatal("fn should not be called when instance row is missing")
		return nil
	})
	if err == nil {
		t.Fatal("expected error for missing instance")
	}
	if !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("expected ErrInstanceNotFound, got: %v", err)
	}
}

func TestWithInstanceTx_TerminalState_Rejects(t *testing.T) {
	cases := []string{
		string(enums.InstanceStatusCompleted),
		string(enums.InstanceStatusTerminated),
		string(enums.InstanceStatusCancelled),
		string(enums.InstanceStatusFailed),
	}
	for _, status := range cases {
		t.Run(status, func(t *testing.T) {
			db := newTestDB(t)
			q := insertTestInstance(t, db, "inst-"+status, status)
			err := WithInstanceTx(context.Background(), q, "inst-"+status, func(scope *InstanceScope) error {
				t.Fatal("fn should not be called on terminal instance")
				return nil
			})
			if err == nil {
				t.Fatal("expected error for terminal instance")
			}
			if !errors.Is(err, ErrInstanceTerminal) {
				t.Fatalf("expected ErrInstanceTerminal, got: %v", err)
			}
		})
	}
}

func TestWithInstanceTx_Active_RunsFn(t *testing.T) {
	db := newTestDB(t)
	q := insertTestInstance(t, db, "inst-1", string(enums.InstanceStatusActive))

	called := false
	err := WithInstanceTx(context.Background(), q, "inst-1", func(scope *InstanceScope) error {
		tx := scope.Tx()
		called = true
		// Inside the tx the row is locked. Verify we can still read it back
		// via the tx-scoped query.
		got, gerr := tx.WfInstance.WithContext(context.Background()).
			Where(tx.WfInstance.ID.Eq("inst-1")).First()
		if gerr != nil {
			return gerr
		}
		if got == nil || got.ID != "inst-1" {
			return errors.New("could not read instance inside tx")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithInstanceTx returned error: %v", err)
	}
	if !called {
		t.Fatal("fn was never invoked")
	}
}

// TestWithInstanceTx_Serializes verifies that two concurrent WithInstanceTx
// calls on the SAME instance execute fn sequentially — the second only runs
// after the first releases the lock at commit.
//
// SQLite doesn't do row-level locking, but it DOES enforce serialization at
// the connection level via a single writer transaction. By restricting the
// pool to exactly one connection (SetMaxOpenConns(1)), we force concurrent
// transactions through the same connection, which means GORM's underlying
// database/sql layer will block goroutine B until goroutine A's transaction
// commits. The observable guarantee we check is that fns do NOT overlap in
// time — the same guarantee FOR UPDATE would give us on PostgreSQL/MySQL.
func TestWithInstanceTx_Serializes(t *testing.T) {
	db := newTestDB(t)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get *sql.DB: %v", err)
	}
	// Force single-connection pool so concurrent transactions block on the
	// connection itself (mimicking row-lock contention on real databases).
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	q := insertTestInstance(t, db, "inst-1", string(enums.InstanceStatusActive))

	var (
		mu        sync.Mutex
		overlaps  int32
		activeNow int32
		maxActive int32
		execOrder []int
	)

	run := func(idx int) error {
		return WithInstanceTx(context.Background(), q, "inst-1", func(scope *InstanceScope) error {
			cur := atomic.AddInt32(&activeNow, 1)
			defer atomic.AddInt32(&activeNow, -1)
			// Track high-water mark of concurrent execution.
			for {
				maxv := atomic.LoadInt32(&maxActive)
				if cur <= maxv || atomic.CompareAndSwapInt32(&maxActive, maxv, cur) {
					break
				}
			}
			if cur > 1 {
				atomic.AddInt32(&overlaps, 1)
			}
			mu.Lock()
			execOrder = append(execOrder, idx)
			mu.Unlock()
			// Hold the lock briefly so the other goroutine definitely
			// has to wait if it were allowed to run concurrently.
			time.Sleep(50 * time.Millisecond)
			return nil
		})
	}

	const n = 4
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs <- run(idx)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("WithInstanceTx failed: %v", err)
		}
	}

	if got := atomic.LoadInt32(&maxActive); got > 1 {
		t.Fatalf("expected serialized execution (maxActive=1), got maxActive=%d overlaps=%d", got, atomic.LoadInt32(&overlaps))
	}
	if len(execOrder) != n {
		t.Fatalf("expected %d executions, got %d", n, len(execOrder))
	}
}

// TestWithInstanceTx_RollbackOnFnError verifies that an error returned by fn
// aborts the transaction (caller-visible error AND no side-effect commits).
func TestWithInstanceTx_RollbackOnFnError(t *testing.T) {
	db := newTestDB(t)
	q := insertTestInstance(t, db, "inst-1", string(enums.InstanceStatusActive))

	want := errors.New("boom")
	err := WithInstanceTx(context.Background(), q, "inst-1", func(scope *InstanceScope) error {
		tx := scope.Tx()
		// Mutate the row inside the tx; rollback should discard it.
		if _, e := tx.WfInstance.WithContext(context.Background()).
			Where(tx.WfInstance.ID.Eq("inst-1")).
			Update(tx.WfInstance.Name, "mutated"); e != nil {
			return e
		}
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("expected wrapped error %v, got %v", want, err)
	}

	// Confirm the mutation did not persist.
	var got model.WfInstance
	if err := db.First(&got, "id = ?", "inst-1").Error; err != nil {
		t.Fatalf("reload instance: %v", err)
	}
	if got.Name == "mutated" {
		t.Fatalf("tx should have rolled back; name still updated to %q", got.Name)
	}
}

// TestWithInstanceTx_EmitsForUpdateSQL confirms the underlying *gorm.DB call
// actually attaches a SELECT ... FOR UPDATE clause to the statement.
//
// SQLite silently drops the Locking clause (it has no row-level locks), so
// the exact statement shape WithInstanceTx uses is replayed against the
// postgres dialector in DryRun mode, which just returns the SQL GORM would
// have emitted without connecting.
func TestWithInstanceTx_EmitsForUpdateSQL(t *testing.T) {
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN: "host=localhost user=test password=test dbname=test port=5432 sslmode=disable",
	}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		// gorm.Open can return a non-nil db even when the initial connection
		// fails (it lazily connects). Only bail if db itself is nil.
		if db == nil {
			t.Fatalf("open postgres dialector: %v", err)
		}
	}

	stmt := db.Session(&gorm.Session{DryRun: true}).
		Table("wf_instance").
		Where("id = ?", "inst-1").
		Clauses(clause.Locking{Strength: "UPDATE"}).
		First(&model.WfInstance{}).
		Statement
	sqlOut := stmt.SQL.String()
	if !strings.Contains(strings.ToUpper(sqlOut), "FOR UPDATE") {
		t.Fatalf("expected FOR UPDATE clause in built SQL, got:\n%s", sqlOut)
	}
}

func TestIsTerminalInstanceStatus(t *testing.T) {
	terminal := []string{
		string(enums.InstanceStatusCompleted),
		string(enums.InstanceStatusTerminated),
		string(enums.InstanceStatusCancelled),
		string(enums.InstanceStatusFailed),
	}
	nonTerminal := []string{
		string(enums.InstanceStatusActive),
		string(enums.InstanceStatusSuspended),
		string(enums.InstanceStatusDraft),
		"",
		"unknown-status",
	}
	for _, s := range terminal {
		if !IsTerminalInstanceStatus(s) {
			t.Errorf("expected %q to be terminal", s)
		}
	}
	for _, s := range nonTerminal {
		if IsTerminalInstanceStatus(s) {
			t.Errorf("expected %q to NOT be terminal", s)
		}
	}
}

// TestWithInstanceTx_RespectsCallerDeadline 验证调用方设置的短 deadline 会被尊重，
// 超时后返回 ErrInstanceLockTimeout（而非无限阻塞到 DB 的 lock_wait_timeout）。
func TestWithInstanceTx_RespectsCallerDeadline(t *testing.T) {
	db := newTestDB(t)
	q := insertTestInstance(t, db, "inst-1", string(enums.InstanceStatusActive))

	// 用一个很短的 deadline（50ms），fn 内 sleep 200ms 超过它，
	// 随后做一次 DB 读——ctx 已超时，DB 操作应返回 DeadlineExceeded。
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := WithInstanceTx(ctx, q, "inst-1", func(scope *InstanceScope) error {
		tx := scope.Tx()
		time.Sleep(200 * time.Millisecond) // 超过 deadline
		// 这次读会因 ctx 超时而失败
		_, e := tx.WfInstance.WithContext(ctx).
			Where(tx.WfInstance.ID.Eq("inst-1")).First()
		return e
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, ErrInstanceLockTimeout) {
		t.Errorf("expected ErrInstanceLockTimeout, got: %v", err)
	}
	// 应该在 deadline（50ms）附近返回，而不是撑到默认 30s
	if elapsed > 2*time.Second {
		t.Errorf("expected quick return near deadline (~50ms), took %v", elapsed)
	}
}

// TestWithInstanceTx_NoDeadline_NormalPath 验证 ctx 无 deadline 时套默认超时
// 不影响正常快速操作。
func TestWithInstanceTx_NoDeadline_NormalPath(t *testing.T) {
	db := newTestDB(t)
	q := insertTestInstance(t, db, "inst-1", string(enums.InstanceStatusActive))

	// 无 deadline 的 ctx → 内部套 defaultInstanceLockTimeout，但操作很快完成
	called := false
	err := WithInstanceTx(context.Background(), q, "inst-1", func(scope *InstanceScope) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("normal fast path should succeed, got: %v", err)
	}
	if !called {
		t.Fatal("fn was never invoked")
	}
}

// bare scope（无实例行可锁的变更路径）上注册的 AfterCommit 必须立即执行，
// 否则事件/副作用会被静默丢弃。
func TestBareScope_AfterCommitRunsImmediately(t *testing.T) {
	scope := bareScope(nil)
	ran := false
	scope.AfterCommit(func() error {
		ran = true
		return nil
	})
	if !ran {
		t.Fatal("bare scope 的 AfterCommit 必须立即执行")
	}
}

// Concurrency suite: SQLite with a single-connection pool serializes
// concurrent transactions on one connection, matching the behavioral
// guarantee FOR UPDATE gives on PostgreSQL/MySQL — no duplicate writes,
// idempotency holds, concurrent ops don't interleave.

// newConcurrencyDB builds an in-memory DB + minimal tables for concurrency
// tests on the instance/task tables.
func newConcurrencyDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared&_busy_timeout=30000"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, e := db.DB(); e == nil {
			_ = sqlDB.Close()
		}
	})
	for _, ddl := range []string{
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
			start_user_id TEXT NOT NULL,
			UNIQUE (tenant_id, business_key)
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
	} {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	db.Exec("DELETE FROM wf_instance")
	db.Exec("DELETE FROM wf_task")
	return db
}

// singleConnPool restricts the pool so concurrent transactions are forced
// through one connection, mirroring the row-lock contention behavior of
// PostgreSQL/MySQL in our SQLite test harness.
func singleConnPool(t *testing.T, db *gorm.DB) {
	t.Helper()
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get *sql.DB: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
}

// seedInstanceAndTask creates an Active instance + a userTask under it.
// Returns the *query.Query for use in WithInstanceTx.
func seedInstanceAndTask(t *testing.T, db *gorm.DB, instID, taskID, taskStatus string) (*query.Query, *model.WfInstance, *model.WfTask) {
	t.Helper()
	q := query.Use(db)
	now := time.Now()
	inst := &model.WfInstance{
		ID:          instID,
		ProcessID:   "proc-1",
		Name:        "inst",
		Status:      string(enums.InstanceStatusActive),
		TenantID:    "t1",
		CreatedBy:   "u1",
		CreatedAt:   now,
		StartUserID: "u1",
	}
	if err := db.Create(inst).Error; err != nil {
		t.Fatalf("seed instance: %v", err)
	}
	task := &model.WfTask{
		ID:                taskID,
		ProcessInstanceID: &instID,
		TaskDefKey:        "node-1",
		TaskType:          "userTask",
		Name:              "task",
		Status:            taskStatus,
		CreatedAt:         now,
	}
	if err := db.Create(task).Error; err != nil {
		t.Fatalf("seed task: %v", err)
	}
	return q, inst, task
}

// TestConcurrency_SuspendVsClaim: concurrent suspend/claim operations on one
// instance must serialize.
func TestConcurrency_SuspendVsClaim(t *testing.T) {
	db := newConcurrencyDB(t)
	singleConnPool(t, db)
	q, _, _ := seedInstanceAndTask(t, db, "inst-1", "task-1", string(enums.TaskStatusPending))

	var (
		activeNow int32
		maxActive int32
	)
	op := func() error {
		return WithInstanceTx(context.Background(), q, "inst-1", func(scope *InstanceScope) error {
			cur := atomic.AddInt32(&activeNow, 1)
			defer atomic.AddInt32(&activeNow, -1)
			for {
				m := atomic.LoadInt32(&maxActive)
				if cur <= m || atomic.CompareAndSwapInt32(&maxActive, m, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			return nil
		})
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- op() }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent op failed: %v", err)
		}
	}
	if atomic.LoadInt32(&maxActive) > 1 {
		t.Fatalf("expected serialized execution (maxActive=1), got %d", atomic.LoadInt32(&maxActive))
	}
}

// TestConcurrency_ActivateDraft_DoubleClick: double-submitting a draft
// activation must start the instance exactly once.
func TestConcurrency_ActivateDraft_DoubleClick(t *testing.T) {
	db := newConcurrencyDB(t)
	singleConnPool(t, db)
	q := query.Use(db)
	now := time.Now()
	inst := &model.WfInstance{
		ID:          "inst-draft",
		ProcessID:   "proc-1",
		Name:        "draft",
		Status:      string(enums.InstanceStatusDraft),
		TenantID:    "t1",
		CreatedBy:   "u1",
		CreatedAt:   now,
		StartUserID: "u1",
	}
	if err := db.Create(inst).Error; err != nil {
		t.Fatalf("seed draft: %v", err)
	}

	// Two concurrent goroutines both try to flip draft → active via WithInstanceTx.
	var (
		wg          sync.WaitGroup
		successes   int32
		notDraftErr int32
	)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := WithInstanceTx(context.Background(), q, "inst-draft", func(scope *InstanceScope) error {
				tx := scope.Tx()
				got, err := tx.WfInstance.WithContext(context.Background()).Where(tx.WfInstance.ID.Eq("inst-draft")).First()
				if err != nil {
					return err
				}
				// Idempotency: only flip if still draft
				if got.Status != string(enums.InstanceStatusDraft) {
					return fmt.Errorf("not draft")
				}
				got.Status = string(enums.InstanceStatusActive)
				_, err = tx.WfInstance.WithContext(context.Background()).
					Where(tx.WfInstance.ID.Eq("inst-draft")).
					Update(tx.WfInstance.Status, string(enums.InstanceStatusActive))
				return err
			})
			if err == nil {
				atomic.AddInt32(&successes, 1)
			} else if err.Error() == "not draft" {
				atomic.AddInt32(&notDraftErr, 1)
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&successes); got != 1 {
		t.Fatalf("expected exactly 1 success (no duplicate activate), got %d", got)
	}
	// The other goroutine must see "not draft" (idempotent rejection), not error.
	if got := atomic.LoadInt32(&notDraftErr); got != 1 {
		t.Fatalf("expected exactly 1 idempotent rejection, got %d", got)
	}

	// Verify the instance is exactly Active once.
	got, err := q.WfInstance.WithContext(context.Background()).Where(q.WfInstance.ID.Eq("inst-draft")).First()
	if err != nil {
		t.Fatalf("reload instance: %v", err)
	}
	if got.Status != string(enums.InstanceStatusActive) {
		t.Fatalf("expected instance Active, got %s", got.Status)
	}
}

// TestConcurrency_CompleteInstance_DoubleComplete: duplicate concurrent
// completion of the same task must be idempotent (exactly one write).
func TestConcurrency_CompleteInstance_DoubleComplete(t *testing.T) {
	db := newConcurrencyDB(t)
	singleConnPool(t, db)
	q, _, _ := seedInstanceAndTask(t, db, "inst-1", "task-1", string(enums.TaskStatusActive))

	// Two concurrent CompleteWithApproval calls on the same task.
	// We can't easily build a full TaskServiceImpl here without engine wiring,
	// so we simulate the same pattern WithInstanceTx enforces: each call enters
	// the tx, reads task, applies the idempotent-completed check, writes.
	var (
		wg              sync.WaitGroup
		completedWrites int32
	)
	doComplete := func() {
		defer wg.Done()
		err := WithInstanceTx(context.Background(), q, "inst-1", func(scope *InstanceScope) error {
			tx := scope.Tx()
			tk, err := tx.WfTask.WithContext(context.Background()).Where(tx.WfTask.ID.Eq("task-1")).First()
			if err != nil {
				return err
			}
			// Idempotent: skip if already completed
			if tk.Status == string(enums.TaskStatusCompleted) {
				return nil
			}
			tk.Status = string(enums.TaskStatusCompleted)
			now := time.Now()
			tk.EndedAt = &now
			_, err = tx.WfTask.WithContext(context.Background()).
				Where(tx.WfTask.ID.Eq("task-1")).
				Updates(map[string]interface{}{
					tx.WfTask.Status.ColumnName().String():  string(enums.TaskStatusCompleted),
					tx.WfTask.EndedAt.ColumnName().String(): &now,
				})
			if err == nil {
				atomic.AddInt32(&completedWrites, 1)
			}
			return err
		})
		if err != nil {
			t.Errorf("WithInstanceTx failed: %v", err)
		}
	}
	wg.Add(2)
	go doComplete()
	go doComplete()
	wg.Wait()

	if got := atomic.LoadInt32(&completedWrites); got != 1 {
		t.Fatalf("expected exactly 1 completion write (idempotent), got %d", got)
	}

	tk, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("task-1")).First()
	if err != nil {
		t.Fatalf("reload task: %v", err)
	}
	if tk.Status != string(enums.TaskStatusCompleted) {
		t.Fatalf("task should be Completed, got %s", tk.Status)
	}
}

// TestConcurrency_Restart_NoCorruption: concurrent restarts must each write
// consistently under the row lock without corrupting the original instance.
func TestConcurrency_Restart_NoCorruption(t *testing.T) {
	db := newConcurrencyDB(t)
	singleConnPool(t, db)
	q, _, _ := seedInstanceAndTask(t, db, "inst-orig", "task-orig", string(enums.InstanceStatusActive))

	// Simulate the locked read-modify-write cycle restart performs on the
	// original instance: read status, derive a "restart" by flipping a flag.
	var (
		wg     sync.WaitGroup
		flips  int32
		stamps int32
	)
	doRestart := func() {
		defer wg.Done()
		err := WithInstanceTx(context.Background(), q, "inst-orig", func(scope *InstanceScope) error {
			tx := scope.Tx()
			got, err := tx.WfInstance.WithContext(context.Background()).Where(tx.WfInstance.ID.Eq("inst-orig")).First()
			if err != nil {
				return err
			}
			// Mark a restart stamp on the instance variables.
			vars := fmt.Sprintf(`{"restarted":true,"stamp":%d}`, time.Now().UnixNano())
			got.Variables = &vars
			_, err = tx.WfInstance.WithContext(context.Background()).
				Where(tx.WfInstance.ID.Eq("inst-orig")).
				Update(tx.WfInstance.Variables, vars)
			if err == nil {
				atomic.AddInt32(&flips, 1)
				atomic.AddInt32(&stamps, 1)
			}
			return err
		})
		if err != nil {
			t.Errorf("WithInstanceTx failed: %v", err)
		}
	}
	wg.Add(3)
	go doRestart()
	go doRestart()
	go doRestart()
	wg.Wait()

	if got := atomic.LoadInt32(&flips); got != 3 {
		t.Fatalf("expected 3 successful writes (each in its own tx), got %d", got)
	}
	// Verify the instance is still readable & has exactly one variables field.
	got, err := q.WfInstance.WithContext(context.Background()).Where(q.WfInstance.ID.Eq("inst-orig")).First()
	if err != nil {
		t.Fatalf("reload instance: %v", err)
	}
	if got.Variables == nil || !strings.Contains(*got.Variables, "restarted") {
		t.Fatalf("instance lost its restart stamp: %v", got.Variables)
	}
}

// TestConcurrency_UnclaimVsClaim: a claim racing an unclaim must leave the
// task in a consistent status.
func TestConcurrency_UnclaimVsClaim(t *testing.T) {
	db := newConcurrencyDB(t)
	singleConnPool(t, db)
	q, _, _ := seedInstanceAndTask(t, db, "inst-1", "task-1", string(enums.TaskStatusActive))

	// Track the final task status. Both ops serialize; we just verify no
	// partial / corrupted state results.
	var wg sync.WaitGroup
	mutator := func(target string) {
		defer wg.Done()
		err := WithInstanceTx(context.Background(), q, "inst-1", func(scope *InstanceScope) error {
			tx := scope.Tx()
			tk, err := tx.WfTask.WithContext(context.Background()).Where(tx.WfTask.ID.Eq("task-1")).First()
			if err != nil {
				return err
			}
			tk.Status = target
			_, err = tx.WfTask.WithContext(context.Background()).
				Where(tx.WfTask.ID.Eq("task-1")).
				Update(tx.WfTask.Status, target)
			return err
		})
		if err != nil {
			t.Errorf("WithInstanceTx failed: %v", err)
		}
	}
	wg.Add(2)
	go mutator(string(enums.TaskStatusPending))
	go mutator(string(enums.TaskStatusActive))
	wg.Wait()

	tk, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("task-1")).First()
	if err != nil {
		t.Fatalf("reload task: %v", err)
	}
	if tk.Status != string(enums.TaskStatusActive) && tk.Status != string(enums.TaskStatusPending) {
		t.Fatalf("task ended in unexpected status: %s", tk.Status)
	}
}

// TestConcurrency_Terminal_RejectsConcurrentMutations: all concurrent
// mutations of a terminal instance are rejected.
func TestConcurrency_Terminal_RejectsConcurrentMutations(t *testing.T) {
	db := newConcurrencyDB(t)
	q := query.Use(db)
	now := time.Now()
	inst := &model.WfInstance{
		ID:          "inst-term",
		ProcessID:   "proc-1",
		Name:        "term",
		Status:      string(enums.InstanceStatusCompleted),
		TenantID:    "t1",
		CreatedBy:   "u1",
		CreatedAt:   now,
		StartUserID: "u1",
	}
	if err := db.Create(inst).Error; err != nil {
		t.Fatalf("seed terminal instance: %v", err)
	}

	var (
		wg       sync.WaitGroup
		rejected int32
	)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := WithInstanceTx(context.Background(), q, "inst-term", func(scope *InstanceScope) error {
				return nil
			})
			if errors.Is(err, ErrInstanceTerminal) {
				atomic.AddInt32(&rejected, 1)
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&rejected); got != 4 {
		t.Fatalf("expected all 4 ops rejected as terminal, got %d", got)
	}
}

// TestConcurrency_NoDuplicateTaskCreation: rows do not duplicate under
// contention — only the first creator wins, the rest skip idempotently.
func TestConcurrency_NoDuplicateTaskCreation(t *testing.T) {
	db := newConcurrencyDB(t)
	singleConnPool(t, db)
	q, _, _ := seedInstanceAndTask(t, db, "inst-1", "task-1", string(enums.InstanceStatusActive))

	// 10 goroutines each try to create a sibling task under the same instance,
	// but only the first should succeed (others see the row exists and skip).
	var (
		wg         sync.WaitGroup
		created    int32
		skipped    int32
		targetTask = "sibling-1"
	)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := WithInstanceTx(context.Background(), q, "inst-1", func(scope *InstanceScope) error {
				tx := scope.Tx()
				// Check if sibling already exists
				rows, err := tx.WfTask.WithContext(context.Background()).
					Where(tx.WfTask.ID.Eq(targetTask)).Count()
				if err != nil {
					return err
				}
				if rows > 0 {
					atomic.AddInt32(&skipped, 1)
					return nil
				}
				now := time.Now()
				inst := "inst-1"
				err = tx.WfTask.WithContext(context.Background()).Create(&model.WfTask{
					ID:                targetTask,
					ProcessInstanceID: &inst,
					TaskDefKey:        "node-2",
					Name:              "sibling",
					Status:            string(enums.TaskStatusActive),
					CreatedAt:         now,
				})
				if err == nil {
					atomic.AddInt32(&created, 1)
				}
				return err
			})
			if err != nil {
				t.Errorf("WithInstanceTx failed: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&created); got != 1 {
		t.Fatalf("expected exactly 1 sibling created, got %d", got)
	}
	if got := atomic.LoadInt32(&skipped); got != 9 {
		t.Fatalf("expected exactly 9 idempotent skips, got %d", got)
	}
}

// Real-DB lock verification (optional): 用真实 PostgreSQL/MySQL 验证
// WithInstanceTx 的 SELECT ... FOR UPDATE 在多连接下串行化同一行的并发事务。
// 设置 TEST_PG_DSN / TEST_MYSQL_DSN 指向一个可建库的实例（任意现存库即可），
// 测试自建并清理 gflow_locktest 库；未设置则整体跳过。
// 临时库名刻意区别于真实库名 gflow：withRealLockDB 会 DROP 重建该库，
// 若同名会把共享实例上的真实 gflow 库连带数据删掉。

const lockTestDBName = "gflow_locktest"

var pgDBNameRe = regexp.MustCompile(`(?i)\bdbname=\S+`)

// pickRealDBDSN 返回第一个配置了的真实 DB 目标。
func pickRealDBDSN() (driver, dsn string, ok bool) {
	if d := os.Getenv("TEST_PG_DSN"); d != "" {
		return "postgres", d, true
	}
	if d := os.Getenv("TEST_MYSQL_DSN"); d != "" {
		return "mysql", d, true
	}
	return "", "", false
}

func openDialector(driver, dsn string) (*gorm.DB, error) {
	var dialector gorm.Dialector
	switch driver {
	case "postgres":
		dialector = postgres.Open(dsn)
	case "mysql":
		dialector = mysql.Open(dsn)
	default:
		return nil, fmt.Errorf("unsupported driver %q", driver)
	}
	return gorm.Open(dialector, &gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
}

// withDBName 把 DSN 里的库名替换为 name，用于切到临时库。
func withDBName(dsn, driver, name string) string {
	switch driver {
	case "postgres":
		if pgDBNameRe.MatchString(dsn) {
			return pgDBNameRe.ReplaceAllString(dsn, "dbname="+name)
		}
		return dsn + " dbname=" + name
	case "mysql":
		// user:pass@tcp(host:port)/dbname?params
		if idx := strings.Index(dsn, "/"); idx >= 0 {
			rest := dsn[idx+1:]
			params := ""
			if qpos := strings.Index(rest, "?"); qpos >= 0 {
				params = rest[qpos:]
			}
			return dsn[:idx+1] + name + params
		}
		return dsn
	}
	return dsn
}

// instanceLockDDL 返回目标方言下 wf_instance 建表语句。
func instanceLockDDL(driver string) []string {
	switch driver {
	case "postgres":
		return []string{`CREATE TABLE wf_instance (
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
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_by TEXT,
			updated_at TIMESTAMP,
			end_reason TEXT,
			duration INTEGER,
			ended_at TIMESTAMP,
			start_user_id TEXT NOT NULL
		)`}
	case "mysql":
		return []string{`CREATE TABLE wf_instance (
			id VARCHAR(64) PRIMARY KEY,
			process_id VARCHAR(128) NOT NULL,
			business_key VARCHAR(128),
			name VARCHAR(255) NOT NULL,
			status VARCHAR(32) NOT NULL,
			variables TEXT,
			current_activity VARCHAR(128),
			priority INT NOT NULL DEFAULT 50,
			parent_id VARCHAR(64),
			tenant_id VARCHAR(64) NOT NULL,
			created_by VARCHAR(64) NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_by VARCHAR(64),
			updated_at DATETIME,
			end_reason TEXT,
			duration INT,
			ended_at DATETIME,
			start_user_id VARCHAR(64) NOT NULL
		)`}
	}
	return nil
}

// withRealLockDB 自建临时库 gflow_locktest，在其内建表后跑 fn，最后清理。
// CREATE/DROP DATABASE 不能进事务，直接走底层 *sql.DB。
func withRealLockDB(t *testing.T, driver, dsn string, fn func(t *testing.T, db *gorm.DB)) {
	t.Helper()

	admin, err := openDialector(driver, dsn)
	if err != nil || admin == nil {
		t.Fatalf("connect admin (%s): %v", driver, err)
	}
	adminSQL, err := admin.DB()
	if err != nil {
		t.Fatalf("get admin *sql.DB: %v", err)
	}
	if _, err := adminSQL.Exec("DROP DATABASE IF EXISTS " + lockTestDBName); err != nil {
		t.Fatalf("drop stale %s: %v", lockTestDBName, err)
	}
	if _, err := adminSQL.Exec("CREATE DATABASE " + lockTestDBName); err != nil {
		t.Fatalf("create %s: %v", lockTestDBName, err)
	}

	db, err := openDialector(driver, withDBName(dsn, driver, lockTestDBName))
	if err != nil || db == nil {
		t.Fatalf("connect %s: %v", lockTestDBName, err)
	}
	// 多连接池：串行化只能来自 FOR UPDATE，不能来自连接池（对照 SQLite 单连接测试）。
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(8)
	sqlDB.SetMaxIdleConns(8)

	t.Cleanup(func() {
		if c, e := db.DB(); e == nil {
			_ = c.Close()
		}
		if _, err := adminSQL.Exec("DROP DATABASE IF EXISTS " + lockTestDBName); err != nil {
			t.Logf("cleanup drop %s failed (leaked, harmless in dev): %v", lockTestDBName, err)
		}
		_ = adminSQL.Close()
	})

	for _, ddl := range instanceLockDDL(driver) {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table (%s): %v", driver, err)
		}
	}

	fn(t, db)
}

// TestWithInstanceTx_RealDB_ForUpdateSerializes 在真实 DB 多连接池下，验证对同一实例的
// 并发 WithInstanceTx 被 FOR UPDATE 串行化（同一时刻最多一个 fn 在跑）。
func TestWithInstanceTx_RealDB_ForUpdateSerializes(t *testing.T) {
	driver, dsn, ok := pickRealDBDSN()
	if !ok {
		t.Skip("set TEST_PG_DSN or TEST_MYSQL_DSN to run the real-DB FOR UPDATE test")
	}

	withRealLockDB(t, driver, dsn, func(t *testing.T, db *gorm.DB) {
		q := query.Use(db)
		now := time.Now()
		if err := db.Create(&model.WfInstance{
			ID:          "lock-real",
			ProcessID:   "p1",
			Name:        "x",
			Status:      string(enums.InstanceStatusActive),
			TenantID:    "t1",
			CreatedBy:   "u1",
			CreatedAt:   now,
			StartUserID: "u1",
		}).Error; err != nil {
			t.Fatalf("seed instance: %v", err)
		}

		var (
			wg        sync.WaitGroup
			activeNow int32
			maxActive int32
		)
		op := func() error {
			return WithInstanceTx(context.Background(), q, "lock-real", func(scope *InstanceScope) error {
				cur := atomic.AddInt32(&activeNow, 1)
				defer atomic.AddInt32(&activeNow, -1)
				for {
					m := atomic.LoadInt32(&maxActive)
					if cur <= m || atomic.CompareAndSwapInt32(&maxActive, m, cur) {
						break
					}
				}
				time.Sleep(40 * time.Millisecond) // 持锁一会儿，让并发持有者可被观测
				return nil
			})
		}

		const n = 4
		errs := make(chan error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); errs <- op() }()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("WithInstanceTx failed: %v", err)
			}
		}

		if got := atomic.LoadInt32(&maxActive); got != 1 {
			t.Fatalf("FOR UPDATE 应串行化同实例并发: want maxActive=1, got %d "+
				"(多连接池下出现并发说明行锁未在 %s 上生效)", got, driver)
		}
	})
}
