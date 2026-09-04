package service

import (
	"context"
	"errors"
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
	utils2 "github.com/rulego/gflow-engine/utils"
	"github.com/rulego/gflow-engine/utils/lock"
)

// reassignStrPtr 字符串指针辅助
func reassignStrPtr(s string) *string { return &s }

// 解析 task.Variables JSON 到 map
func reassignVars(t *testing.T, task *model.WfTask) map[string]interface{} {
	t.Helper()
	vars := make(map[string]interface{})
	if task.Variables != nil && *task.Variables != "" {
		if err := utils2.FromJSON(*task.Variables, &vars); err != nil {
			t.Fatalf("unmarshal variables: %v", err)
		}
	}
	return vars
}

func TestReassignTask_UpdatesAssigneeAndVariables(t *testing.T) {
	now := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	task := &model.WfTask{
		ID:       "task-1",
		Status:   string(enums.TaskStatusActive),
		Assignee: reassignStrPtr("userA"),
	}
	svc := &TaskServiceImpl{}

	err := svc.applyReassign(task, "admin1", "userB", "负载调整", now)
	if err != nil {
		t.Fatalf("applyReassign failed: %v", err)
	}

	if task.Assignee == nil || *task.Assignee != "userB" {
		t.Errorf("expected assignee=userB, got %v", task.Assignee)
	}
	vars := reassignVars(t, task)
	if v, _ := vars["reassign_from"].(string); v != "userA" {
		t.Errorf("expected reassign_from=userA, got %v", vars["reassign_from"])
	}
	if v, _ := vars["reassign_to"].(string); v != "userB" {
		t.Errorf("expected reassign_to=userB, got %v", vars["reassign_to"])
	}
	if v, _ := vars["reassign_operator"].(string); v != "admin1" {
		t.Errorf("expected reassign_operator=admin1, got %v", vars["reassign_operator"])
	}
	if v, _ := vars["reassign_reason"].(string); v != "负载调整" {
		t.Errorf("expected reassign_reason=负载调整, got %v", vars["reassign_reason"])
	}
	if _, ok := vars["reassign_time"]; !ok {
		t.Errorf("expected reassign_time key present, got %v", vars)
	}
}

func TestReassignTask_RejectsNonActive(t *testing.T) {
	task := &model.WfTask{
		ID:       "task-2",
		Status:   string(enums.TaskStatusCompleted),
		Assignee: reassignStrPtr("userA"),
	}
	svc := &TaskServiceImpl{}
	err := svc.applyReassign(task, "admin1", "userB", "", time.Now())
	if err == nil {
		t.Fatal("expected error for non-active task")
	}
	if !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation, got %v", err)
	}
}

func TestReassignTask_EmptyID(t *testing.T) {
	svc := &TaskServiceImpl{}
	_, err := svc.Reassign(context.Background(), Actor{UserID: "admin1"}, "", "userB", "reason")
	if err == nil {
		t.Fatal("expected error for empty taskID")
	}
}

func TestReassignTask_EmptyNewAssignee(t *testing.T) {
	svc := &TaskServiceImpl{}
	_, err := svc.Reassign(context.Background(), Actor{UserID: "admin1"}, "task-1", "", "reason")
	if err == nil {
		t.Fatal("expected error for empty newAssignee")
	}
}

// ===== 集成测试：端到端覆盖 svc.Reassign（真 DAO + 监听器断言） =====

// reassignTestDB 在内存 SQLite 上建 wf_task，返回 TaskDAO。
func reassignTestDB(t *testing.T) *dao.TaskDAO {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:reassign_test?mode=memory&cache=shared&_busy_timeout=30000"), &gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if c, e := db.DB(); e == nil {
			_ = c.Close()
		}
	})
	require.NoError(t, db.Exec(`CREATE TABLE IF NOT EXISTS wf_task (
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
	)`).Error)
	require.NoError(t, db.Exec("DELETE FROM wf_task").Error)
	return dao.NewTaskDAOWithQuery(query.Use(db))
}

// reassignListenerEngine 注入一个捕获事件监听器的占位引擎。
type reassignListenerEngine struct {
	listener TaskEventListener
}

func (e *reassignListenerEngine) GetDB() *gorm.DB                                 { return nil }
func (e *reassignListenerEngine) GetTaskService() TaskService                     { return nil }
func (e *reassignListenerEngine) GetProcessService() ProcessService               { return nil }
func (e *reassignListenerEngine) GetRuntimeService() RuntimeService               { return nil }
func (e *reassignListenerEngine) GetHistoryService() HistoryService               { return nil }
func (e *reassignListenerEngine) GetIdentityService() IdentityService             { return nil }
func (e *reassignListenerEngine) GetLocker() lock.Locker                          { return nil }
func (e *reassignListenerEngine) Start(context.Context) error                     { return nil }
func (e *reassignListenerEngine) Stop(context.Context) error                      { return nil }
func (e *reassignListenerEngine) IsRunning() bool                                 { return false }
func (e *reassignListenerEngine) GetName() string                                 { return "reassign-test" }
func (e *reassignListenerEngine) GetVersion() string                              { return "" }
func (e *reassignListenerEngine) GetIDGenerator() IDGenerator                     { return nil }
func (e *reassignListenerEngine) GetRuleChainExecutor() RuleChainExecutor         { return nil }
func (e *reassignListenerEngine) GetCCTaskCreatedListener() CCTaskCreatedListener { return nil }
func (e *reassignListenerEngine) GetTaskEventListener() TaskEventListener         { return e.listener }
func (e *reassignListenerEngine) GetTaskServiceInternal() TaskServiceInternal     { return nil }
func (e *reassignListenerEngine) GetRuntimeServiceInternal() RuntimeServiceInternal {
	return nil
}
func (e *reassignListenerEngine) CountTenantData(ctx context.Context, tenantID string) (map[string]int64, error) {
	return nil, nil
}

// TestReassignTask_EndToEnd 验证 svc.Reassign 端到端：写 assignee + Variables 5 键 + 发 TaskEventForwarded。
func TestReassignTask_EndToEnd(t *testing.T) {
	d := reassignTestDB(t)
	ctx := context.Background()
	now := time.Now()

	// 种子：active 任务，assignee=userA，process_instance_id 留空 → requireActionEnabled 直接放行
	oldAssignee := "userA"
	require.NoError(t, d.Create(ctx, &model.WfTask{
		ID:         "task-e2e",
		Status:     string(enums.TaskStatusActive),
		Assignee:   &oldAssignee,
		TenantID:   "t1",
		TaskDefKey: "approve",
		Name:       "审批",
		TaskType:   "user_task",
		CreatedAt:  now,
		CreatedBy:  "system",
	}))

	var mu sync.Mutex
	var got []TaskEvent
	listener := func(ctx context.Context, evt TaskEvent) {
		mu.Lock()
		got = append(got, evt)
		mu.Unlock()
	}
	svc := &TaskServiceImpl{taskDAO: d, workflowEngine: &reassignListenerEngine{listener: listener}}

	old, err := svc.Reassign(ctx, Actor{UserID: "admin1", TenantID: "t1", SuperAdmin: true}, "task-e2e", "userB", "负载调整")
	require.NoError(t, err)
	require.Equal(t, "userA", old)

	// 校验落库后的 task：assignee 改为 userB、Variables 含 5 键
	persisted, err := d.Get(ctx, "task-e2e")
	require.NoError(t, err)
	require.NotNil(t, persisted)
	require.NotNil(t, persisted.Assignee)
	require.Equal(t, "userB", *persisted.Assignee)

	vars := reassignVars(t, persisted)
	require.Equal(t, "admin1", vars["reassign_operator"])
	require.Equal(t, "userA", vars["reassign_from"])
	require.Equal(t, "userB", vars["reassign_to"])
	require.Equal(t, "负载调整", vars["reassign_reason"])
	_, hasTime := vars["reassign_time"]
	require.True(t, hasTime, "reassign_time key present")

	// 校验监听器收到 TaskEventForwarded
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1, "listener should receive exactly one event")
	require.Equal(t, TaskEventForwarded, got[0].Type)
	require.Equal(t, "task-e2e", got[0].TaskID)
	require.Equal(t, []string{"userB"}, got[0].ToUsers)
	require.Equal(t, "admin1", got[0].FromUser)
	require.Equal(t, "负载调整", got[0].Reason)
}

// TestReassignTask_EndToEnd_RejectsNonActive 验证非 active 状态在 DB 落库层面被拒绝。
func TestReassignTask_EndToEnd_RejectsNonActive(t *testing.T) {
	d := reassignTestDB(t)
	ctx := context.Background()
	now := time.Now()
	require.NoError(t, d.Create(ctx, &model.WfTask{
		ID: "task-done", Status: string(enums.TaskStatusCompleted),
		Assignee: reassignStrPtr("userA"), TenantID: "t1", TaskDefKey: "n", Name: "n", TaskType: "user_task",
		CreatedAt: now, CreatedBy: "system",
	}))
	svc := &TaskServiceImpl{taskDAO: d, workflowEngine: &reassignListenerEngine{}}
	_, err := svc.Reassign(ctx, Actor{UserID: "admin1", TenantID: "t1", SuperAdmin: true}, "task-done", "userB", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrValidation))
}

// TestReassignTask_NonAdminDenied 验证：普通同租户用户（无 SuperAdmin，也非系统身份）改派被拒绝。
// 鉴权在 DB 读之前执行，故无需建表。
func TestReassignTask_NonAdminDenied(t *testing.T) {
	svc := &TaskServiceImpl{}
	_, err := svc.Reassign(context.Background(), Actor{UserID: "userB", TenantID: "t1"}, "task-1", "userC", "reason")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPermissionDenied), "非管理员改派必须拒绝，got %v", err)
}

// TestReassignTask_EmptyOperatorDenied 验证：空操作人身份（无 UserID）改派被拒绝。
func TestReassignTask_EmptyOperatorDenied(t *testing.T) {
	svc := &TaskServiceImpl{}
	_, err := svc.Reassign(context.Background(), Actor{TenantID: "t1"}, "task-1", "userC", "reason")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrAuthenticationRequired), "空身份改派必须拒绝，got %v", err)
}
