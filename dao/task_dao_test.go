package dao

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
)

// Tests for task_dao.go parameter validation.

func TestTaskDAO_Create_NilEntity(t *testing.T) {
	d := &TaskDAO{}
	err := d.Create(context.Background(), nil)
	if err == nil {
		t.Error("expected error for nil entity")
	}
}

func TestTaskDAO_CreateBatch_EmptyList(t *testing.T) {
	d := &TaskDAO{}
	err := d.CreateBatch(context.Background(), nil)
	if err != nil {
		t.Errorf("unexpected error for nil list: %v", err)
	}
}

func TestTaskDAO_CreateBatch_EmptySlice(t *testing.T) {
	d := &TaskDAO{}
	err := d.CreateBatch(context.Background(), []*model.WfTask{})
	if err != nil {
		t.Errorf("unexpected error for empty slice: %v", err)
	}
}

func TestTaskDAO_Get_EmptyID(t *testing.T) {
	d := &TaskDAO{}
	_, err := d.Get(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty ID")
	}
}

func TestTaskDAO_Get_NilQuery_Panics(t *testing.T) {
	d := &TaskDAO{}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from nil Query")
		}
	}()
	d.Get(context.Background(), "task-1")
}

func seedTask(t *testing.T, d *TaskDAO, id, instanceID, name string, createdAt time.Time) {
	t.Helper()
	entity := &model.WfTask{
		ID:         id,
		Name:       name,
		TaskType:   "user_task",
		TaskDefKey: "node1",
		Status:     "active",
		TenantID:   "tenant1",
		CreatedAt:  createdAt,
	}
	if instanceID != "" {
		entity.ProcessInstanceID = &instanceID
	}
	if err := d.Create(context.Background(), entity); err != nil {
		t.Fatalf("seed task %s: %v", id, err)
	}
}

// TaskDAO.List 关键字通配符转义：关键字里的 % 不得被当作 LIKE 通配符。
func TestTaskDAO_List_KeywordWildcardEscaped(t *testing.T) {
	q := newTestQuery(t, ddlWfTask)
	d := NewTaskDAOWithQuery(q)
	now := time.Now()

	seedTask(t, d, "k1", "", "50% off", now)
	seedTask(t, d, "k2", "", "50X off", now)
	seedTask(t, d, "k3", "", "plain task", now)

	ctx := context.Background()

	// 含 % 的关键字不得匹配 "50X off"（未转义时 % 会当作通配符）
	tasks, total, err := d.List(ctx, &dto.TaskQuery{Keyword: "50%"})
	if err != nil {
		t.Fatalf("List with wildcard keyword: %v", err)
	}
	ids := taskIDSet(tasks)
	if ids["k2"] {
		t.Errorf("keyword %q matched %q: %% acted as wildcard", "50%", "50X off")
	}
	if total != int64(len(tasks)) {
		t.Errorf("total = %d, len(tasks) = %d", total, len(tasks))
	}

	// 普通子串关键字仍正常工作
	tasks, _, err = d.List(ctx, &dto.TaskQuery{Keyword: "50"})
	if err != nil {
		t.Fatalf("List with plain keyword: %v", err)
	}
	ids = taskIDSet(tasks)
	if !ids["k1"] || !ids["k2"] {
		t.Errorf("plain keyword %q should match k1 and k2, got %v", "50", ids)
	}
	if ids["k3"] {
		t.Errorf("plain keyword %q should not match k3", "50")
	}
}

func TestTaskDAO_List_InstanceIDs(t *testing.T) {
	q := newTestQuery(t, ddlWfTask)
	d := NewTaskDAOWithQuery(q)
	now := time.Now()

	seedTask(t, d, "t1", "inst-1", "n1", now)
	seedTask(t, d, "t2", "inst-2", "n2", now)
	seedTask(t, d, "t3", "inst-3", "n3", now)
	seedTask(t, d, "t4", "", "n4", now)

	ctx := context.Background()
	tests := []struct {
		name        string
		instanceIDs []string
		wantIDs     map[string]bool
	}{
		{"subset", []string{"inst-1", "inst-3"}, map[string]bool{"t1": true, "t3": true}},
		{"no match", []string{"inst-x"}, map[string]bool{}},
		{"empty means no filter", nil, map[string]bool{"t1": true, "t2": true, "t3": true, "t4": true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tasks, total, err := d.List(ctx, &dto.TaskQuery{InstanceIDs: tt.instanceIDs})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if total != int64(len(tt.wantIDs)) {
				t.Errorf("total = %d, want %d", total, len(tt.wantIDs))
			}
			got := taskIDSet(tasks)
			for id := range tt.wantIDs {
				if !got[id] {
					t.Errorf("missing task %q", id)
				}
			}
			for id := range got {
				if !tt.wantIDs[id] {
					t.Errorf("unexpected task %q", id)
				}
			}
		})
	}
}

// List ORDER BY 回退：未知排序字段回退 created_at。
func TestTaskDAO_List_OrderByFallback(t *testing.T) {
	q := newTestQuery(t, ddlWfTask)
	d := NewTaskDAOWithQuery(q)
	base := time.Now()

	// 乱序写入，验证排序来自 ORDER BY 而非插入顺序
	seedTask(t, d, "o3", "", "n3", base.Add(2*time.Hour))
	seedTask(t, d, "o1", "", "n1", base)
	seedTask(t, d, "o2", "", "n2", base.Add(1*time.Hour))

	ctx := context.Background()
	orderIDs := func(tasks []*model.WfTask) []string {
		ids := make([]string, 0, len(tasks))
		for _, tk := range tasks {
			ids = append(ids, tk.ID)
		}
		return ids
	}

	tests := []struct {
		name    string
		query   *dto.TaskQuery
		wantIDs []string
	}{
		{
			name:    "unknown field falls back to created_at asc",
			query:   &dto.TaskQuery{PageRequest: dto.PageRequest{OrderBy: "no_such_column"}},
			wantIDs: []string{"o1", "o2", "o3"},
		},
		{
			name:    "unknown field with desc",
			query:   &dto.TaskQuery{PageRequest: dto.PageRequest{OrderBy: "no_such_column", OrderDesc: true}},
			wantIDs: []string{"o3", "o2", "o1"},
		},
		{
			name:    "empty order by keeps default",
			query:   &dto.TaskQuery{},
			wantIDs: []string{"o1", "o2", "o3"},
		},
		{
			name:    "known field still honored",
			query:   &dto.TaskQuery{PageRequest: dto.PageRequest{OrderBy: "name", OrderDesc: true}},
			wantIDs: []string{"o3", "o2", "o1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tasks, _, err := d.List(ctx, tt.query)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			got := orderIDs(tasks)
			if len(got) != len(tt.wantIDs) {
				t.Fatalf("got %d tasks, want %d", len(got), len(tt.wantIDs))
			}
			for i := range tt.wantIDs {
				if got[i] != tt.wantIDs[i] {
					t.Errorf("order = %v, want %v", got, tt.wantIDs)
					break
				}
			}
		})
	}
}

// TaskDAO.Delete 事务删除任务与候选池。
func TestTaskDAO_Delete_CleansAssigneesAtomically(t *testing.T) {
	q := newTestQuery(t, ddlWfTask, ddlWfTaskAssignee)
	taskDAO := NewTaskDAOWithQuery(q)
	assigneeDAO := NewTaskAssigneeDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	seedTask(t, taskDAO, "task-1", "inst-1", "approve", now)
	assignees := []*model.WfTaskAssignee{
		{ID: "ca-1", TaskID: "task-1", EntityType: "person", EntityID: "u1", TenantID: "tenant1", CreatedAt: now},
		{ID: "ca-2", TaskID: "task-1", EntityType: "role", EntityID: "r1", TenantID: "tenant1", CreatedAt: now},
	}
	if err := assigneeDAO.CreateBatch(ctx, assignees); err != nil {
		t.Fatalf("seed assignees: %v", err)
	}

	if err := taskDAO.Delete(ctx, "task-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if tk, _ := taskDAO.Get(ctx, "task-1"); tk != nil {
		t.Error("task should be deleted")
	}
	rows, err := assigneeDAO.GetByTaskID(ctx, "tenant1", "task-1")
	if err != nil {
		t.Fatalf("GetByTaskID: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("assignees should be cleaned, got %d rows", len(rows))
	}
}

func TestTaskDAO_Delete_MissingTask_KeepsData(t *testing.T) {
	q := newTestQuery(t, ddlWfTask, ddlWfTaskAssignee)
	taskDAO := NewTaskDAOWithQuery(q)
	assigneeDAO := NewTaskAssigneeDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	// 候选行挂在别的任务上；删除不存在的任务不得影响任何数据
	seedTask(t, taskDAO, "task-1", "inst-1", "approve", now)
	if err := assigneeDAO.Create(ctx, &model.WfTaskAssignee{
		ID: "ca-1", TaskID: "task-1", EntityType: "person", EntityID: "u1", TenantID: "tenant1", CreatedAt: now,
	}); err != nil {
		t.Fatalf("seed assignee: %v", err)
	}

	if err := taskDAO.Delete(ctx, "missing-task"); err == nil {
		t.Fatal("expected error for missing task")
	}

	if tk, _ := taskDAO.Get(ctx, "task-1"); tk == nil {
		t.Error("existing task must not be touched")
	}
	rows, err := assigneeDAO.GetByTaskID(ctx, "tenant1", "task-1")
	if err != nil || len(rows) != 1 {
		t.Errorf("assignees must not be touched, rows=%d err=%v", len(rows), err)
	}
}

// List 候选维度过滤：person/role/department 三类候选实体等价命中；
// 已指派与状态不符的任务不得进入可认领集合。
func TestTaskDAO_List_CandidatePoolFilter(t *testing.T) {
	q := newTestQuery(t, ddlWfTask, ddlWfTaskAssignee)
	d := NewTaskDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	seedTask := func(taskID, status, assignee string) {
		t.Helper()
		var assigneePtr *string
		if assignee != "" {
			assigneePtr = &assignee
		}
		if err := q.WfTask.Create(&model.WfTask{
			ID: taskID, TaskDefKey: "n1", Name: "审批", TaskType: "user_task",
			Status: status, Assignee: assigneePtr,
			TenantID: "t1", CreatedBy: "system", CreatedAt: now,
		}); err != nil {
			t.Fatalf("seed task %s: %v", taskID, err)
		}
	}
	seedTask("task-person", "pending", "")
	seedTask("task-role", "pending", "")
	seedTask("task-dept", "pending", "")
	seedTask("task-taken", "pending", "u2")
	seedTask("task-done", "completed", "")

	pool := func(taskID, entityType, entityID string) {
		t.Helper()
		if err := q.WfTaskAssignee.Create(&model.WfTaskAssignee{
			ID: "pool-" + taskID, TaskID: taskID, EntityType: entityType, EntityID: entityID,
			TenantID: "t1", CreatedAt: now,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", taskID, err)
		}
	}
	pool("task-person", "person", "u1")
	pool("task-role", "role", "r1")
	pool("task-dept", "department", "d1")
	pool("task-taken", "person", "u1")
	pool("task-done", "person", "u1")

	list := func(candUser string, roleIDs, deptIDs []string) map[string]bool {
		t.Helper()
		tasks, _, err := d.List(ctx, &dto.TaskQuery{
			TenantID:         "t1",
			CandidateUser:    candUser,
			CandidateRoleIDs: roleIDs,
			CandidateDeptIDs: deptIDs,
			PageRequest:      dto.PageRequest{Page: 1, PageSize: 50, Status: []string{"pending"}},
		})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		set := make(map[string]bool)
		for _, task := range tasks {
			set[task.ID] = true
		}
		return set
	}

	got := list("u1", nil, nil)
	if !got["task-person"] {
		t.Error("person 候选应命中候选池内任务")
	}
	if got["task-taken"] {
		t.Error("已被他人签收的任务不得进入可认领集合")
	}
	if got["task-done"] {
		t.Error("已完结任务不得进入可认领集合")
	}

	if !list("", []string{"r1"}, nil)["task-role"] {
		t.Error("role 候选应按角色命中")
	}
	if !list("", nil, []string{"d1"})["task-dept"] {
		t.Error("department 候选应按部门命中")
	}
	if list("", nil, []string{"d2"})["task-dept"] {
		t.Error("其他部门成员不应命中")
	}

	got = list("u1", []string{"r1"}, []string{"d1"})
	if !got["task-person"] || !got["task-role"] || !got["task-dept"] {
		t.Errorf("三类候选应取并集, got %v", got)
	}
	if got["task-taken"] {
		t.Error("并集查询同样不得命中已指派任务")
	}
}
