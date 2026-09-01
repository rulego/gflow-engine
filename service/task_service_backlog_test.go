package service

import (
	"context"
	"sort"
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

// backlogTestDB 在内存 SQLite 上建 wf_task + wf_process，返回 TaskDAO。
func backlogTestDB(t *testing.T) *dao.TaskDAO {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:backlog_test?mode=memory&cache=shared&_busy_timeout=30000"), &gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if c, e := db.DB(); e == nil {
			_ = c.Close()
		}
	})
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS wf_task (
			id TEXT PRIMARY KEY,
			process_instance_id TEXT,
			process_id TEXT,
			parent_id TEXT,
			task_def_key TEXT,
			name TEXT,
			task_type TEXT,
			description TEXT,
			status TEXT,
			assignee TEXT,
			owner TEXT,
			due_date DATETIME,
			priority INTEGER,
			form_key TEXT,
			variables TEXT,
			claimed_at DATETIME,
			sequence_order INTEGER,
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
		`CREATE TABLE IF NOT EXISTS wf_process (
			id TEXT PRIMARY KEY,
			process_key TEXT NOT NULL,
			name TEXT NOT NULL,
			version INTEGER NOT NULL DEFAULT 1,
			category TEXT,
			description TEXT,
			definition_json TEXT,
			status TEXT NOT NULL DEFAULT 'active',
			publish_time DATETIME,
			tenant_id TEXT NOT NULL,
			created_by TEXT,
			created_at DATETIME,
			updated_by TEXT,
			updated_at DATETIME,
			ext TEXT,
			process_type TEXT,
			icon TEXT
		)`,
	} {
		require.NoError(t, db.Exec(ddl).Error)
	}
	// 共享内存库在连接关闭前清表，避免用例间脏数据。
	require.NoError(t, db.Exec("DELETE FROM wf_task").Error)
	require.NoError(t, db.Exec("DELETE FROM wf_process").Error)
	q := query.Use(db)
	return dao.NewTaskDAOWithQuery(q)
}

// seedBacklogTasks 写入一组任务（active/completed 混合、跨多个 process_id）。
func seedBacklogTasks(t *testing.T, d *dao.TaskDAO) {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	statusActive := string(enums.TaskStatusActive)
	statusDone := string(enums.TaskStatusCompleted)
	tasks := []*model.WfTask{
		{ID: "t1", ProcessID: "proc-A", Status: statusActive, TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now},
		{ID: "t2", ProcessID: "proc-A", Status: statusActive, TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now},
		{ID: "t3", ProcessID: "proc-A", Status: statusActive, TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now},
		{ID: "t4", ProcessID: "proc-B", Status: statusActive, TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now},
		{ID: "t5", ProcessID: "proc-B", Status: statusActive, TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now},
		{ID: "t6", ProcessID: "proc-C", Status: statusActive, TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now},
		{ID: "t7", ProcessID: "proc-A", Status: statusDone, TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now},      // 不计入
		{ID: "t8", ProcessID: "", Status: statusActive, TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now},          // 空 process_id 跳过
		{ID: "t9", ProcessID: "proc-A", Status: statusActive, TenantID: "other", TaskDefKey: "n", Name: "n", TaskType: "user_task", CreatedAt: now}, // 跨租户不计入
	}
	for _, tk := range tasks {
		require.NoError(t, d.Create(ctx, tk))
	}
}

// 稳定排序辅助：按 ActiveCount 降序、并列按 ProcessDefID 升序。
func sortByCountThenID(items []*BacklogItem) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].ActiveCount != items[j].ActiveCount {
			return items[i].ActiveCount > items[j].ActiveCount
		}
		return items[i].ProcessDefID < items[j].ProcessDefID
	})
}

func TestAggregateActiveByProcess_GroupsDesc(t *testing.T) {
	d := backlogTestDB(t)
	seedBacklogTasks(t, d)

	rows, err := d.AggregateActiveByProcess(context.Background(), "t1", 10)
	require.NoError(t, err)
	// 聚合后排序不保证稳定（同 count 并列），用辅助函数再排一次便于断言。
	items := make([]*BacklogItem, 0, len(rows))
	for _, r := range rows {
		items = append(items, &BacklogItem{ProcessDefID: r.ProcessID, ActiveCount: r.ActiveCount})
	}
	sortByCountThenID(items)

	require.Len(t, items, 3)
	require.Equal(t, "proc-A", items[0].ProcessDefID)
	require.EqualValues(t, 3, items[0].ActiveCount)
	require.Equal(t, "proc-B", items[1].ProcessDefID)
	require.EqualValues(t, 2, items[1].ActiveCount)
	require.Equal(t, "proc-C", items[2].ProcessDefID)
	require.EqualValues(t, 1, items[2].ActiveCount)
}

func TestAggregateActiveByProcess_RespectsLimit(t *testing.T) {
	d := backlogTestDB(t)
	seedBacklogTasks(t, d)

	rows, err := d.AggregateActiveByProcess(context.Background(), "t1", 2)
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestAggregateActiveByProcess_DefaultLimit(t *testing.T) {
	d := backlogTestDB(t)
	seedBacklogTasks(t, d)
	// limit<=0 走默认 10，3 个 process 全返回
	rows, err := d.AggregateActiveByProcess(context.Background(), "t1", 0)
	require.NoError(t, err)
	require.Len(t, rows, 3)
}

func TestAggregateActiveByProcess_Empty(t *testing.T) {
	d := backlogTestDB(t)
	rows, err := d.AggregateActiveByProcess(context.Background(), "t1", 10)
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestAggregateActiveByProcess_EmptyTenant(t *testing.T) {
	d := backlogTestDB(t)
	if _, err := d.AggregateActiveByProcess(context.Background(), "", 10); err == nil {
		t.Fatal("expected error for empty tenant")
	}
}

func TestListProcessNamesByID_Batch(t *testing.T) {
	d := backlogTestDB(t)
	ctx := context.Background()
	// 直接写 wf_process
	q := d.Query
	require.NoError(t, q.WfProcess.WithContext(ctx).Create(&model.WfProcess{ID: "proc-A", Name: "请假流程", TenantID: "t1", Version: 1, Status: "active"}))
	require.NoError(t, q.WfProcess.WithContext(ctx).Create(&model.WfProcess{ID: "proc-B", Name: "报销流程", TenantID: "t1", Version: 1, Status: "active"}))

	names, err := d.ListProcessNamesByID(ctx, []string{"proc-A", "proc-B", "proc-missing"})
	require.NoError(t, err)
	require.Equal(t, "请假流程", names["proc-A"])
	require.Equal(t, "报销流程", names["proc-B"])
	_, ok := names["proc-missing"]
	require.False(t, ok)
}

func TestListProcessNamesByID_Empty(t *testing.T) {
	d := backlogTestDB(t)
	names, err := d.ListProcessNamesByID(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, names)
}

// TestGetBacklogByProcess_SQL 端到端验证 service 层走 DAO 聚合 + 批量取名称。
func TestGetBacklogByProcess_SQL(t *testing.T) {
	d := backlogTestDB(t)
	ctx := context.Background()
	seedBacklogTasks(t, d)
	q := d.Query
	require.NoError(t, q.WfProcess.WithContext(ctx).Create(&model.WfProcess{ID: "proc-A", Name: "请假", TenantID: "t1", Version: 1, Status: "active"}))
	require.NoError(t, q.WfProcess.WithContext(ctx).Create(&model.WfProcess{ID: "proc-B", Name: "报销", TenantID: "t1", Version: 1, Status: "active"}))

	svc := &TaskServiceImpl{taskDAO: d, workflowEngine: &noopBacklogEngine{}}
	items, err := svc.GetBacklogByProcess(ctx, Actor{TenantID: "t1"})
	require.NoError(t, err)
	sortByCountThenID(items)

	require.Len(t, items, 3)
	require.Equal(t, "proc-A", items[0].ProcessDefID)
	require.EqualValues(t, 3, items[0].ActiveCount)
	require.Equal(t, "请假", items[0].ProcessName)
	require.Equal(t, "proc-B", items[1].ProcessDefID)
	require.Equal(t, "报销", items[1].ProcessName)
}

func TestGetBacklogByProcess_EmptyTenant(t *testing.T) {
	d := backlogTestDB(t)
	svc := &TaskServiceImpl{taskDAO: d, workflowEngine: &noopBacklogEngine{}}
	if _, err := svc.GetBacklogByProcess(context.Background(), Actor{}); err == nil {
		t.Fatal("expected error for empty tenant")
	}
}

// noopBacklogEngine 占位引擎：GetBacklogByProcess 已不调用 GetProcessService，
// 但 TaskServiceImpl.workflowEngine 字段不可为 nil，给个空实现避免 panic。
type noopBacklogEngine struct{}

func (noopBacklogEngine) GetDB() *gorm.DB                                 { return nil }
func (noopBacklogEngine) GetTaskService() TaskService                     { return nil }
func (noopBacklogEngine) GetProcessService() ProcessService               { return nil }
func (noopBacklogEngine) GetRuntimeService() RuntimeService               { return nil }
func (noopBacklogEngine) GetHistoryService() HistoryService               { return nil }
func (noopBacklogEngine) GetIdentityService() IdentityService             { return nil }
func (noopBacklogEngine) GetLocker() lock.Locker                          { return nil }
func (noopBacklogEngine) Start(context.Context) error                     { return nil }
func (noopBacklogEngine) Stop(context.Context) error                      { return nil }
func (noopBacklogEngine) IsRunning() bool                                 { return false }
func (noopBacklogEngine) GetName() string                                 { return "noop" }
func (noopBacklogEngine) GetVersion() string                              { return "" }
func (noopBacklogEngine) GetIDGenerator() IDGenerator                     { return nil }
func (noopBacklogEngine) GetRuleChainExecutor() RuleChainExecutor         { return nil }
func (noopBacklogEngine) GetCCTaskCreatedListener() CCTaskCreatedListener { return nil }
func (noopBacklogEngine) GetTaskEventListener() TaskEventListener         { return nil }
func (noopBacklogEngine) GetTaskServiceInternal() TaskServiceInternal     { return nil }
func (noopBacklogEngine) GetRuntimeServiceInternal() RuntimeServiceInternal {
	return nil
}
func (noopBacklogEngine) CountTenantData(ctx context.Context, tenantID string) (map[string]int64, error) {
	return nil, nil
}
