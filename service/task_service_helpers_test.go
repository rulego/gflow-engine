package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/gflow-engine/utils"
)

// ---------------------------------------------------------------------------
// taskToHiTask tests
// ---------------------------------------------------------------------------

func TestTaskToHiTask_ZeroValue(t *testing.T) {
	task := &model.WfTask{}
	hi := taskToHiTask(task)
	if hi == nil {
		t.Fatal("taskToHiTask returned nil")
	}
	if hi.ID != "" {
		t.Errorf("expected empty ID, got %q", hi.ID)
	}
}

func TestTaskToHiTask_FullFields(t *testing.T) {
	now := time.Now()
	assignee := "user1"
	owner := "owner1"
	processInstID := "inst-1"
	parentID := "parent-1"
	desc := "desc"
	vars := `{"key":"value"}`
	comment := "approved"
	endReason := "approved"
	formKey := "form1"
	approvalRule := `{"type":"all"}`
	delegateFrom := "user2"
	delegateReason := "busy"
	endTime := now.Add(time.Hour)
	duration := int64(3600000)

	task := &model.WfTask{
		ID:                "task-1",
		ProcessInstanceID: &processInstID,
		ProcessID:         "proc-1",
		TaskDefKey:        "node1",
		TaskType:          "userTask",
		Name:              "审批任务",
		Description:       &desc,
		ParentID:          &parentID,
		Status:            string(enums.TaskStatusCompleted),
		Assignee:          &assignee,
		Owner:             &owner,
		DueDate:           &now,
		Priority:          80,
		FormKey:           &formKey,
		Variables:         &vars,
		ClaimedAt:         &now,
		ApprovalType:      "single",
		ApprovalRule:      &approvalRule,
		DelegateFrom:      &delegateFrom,
		DelegateReason:    &delegateReason,
		DelegateTime:      &now,
		EndedAt:           &endTime,
		Comment:           &comment,
		EndReason:         &endReason,
		Duration:          &duration,
		TenantID:          "tenant1",
		CreatedBy:         "system",
		CreatedAt:         now,
		UpdatedBy:         &assignee,
		UpdatedAt:         &now,
		SequenceOrder:     1,
	}

	hi := taskToHiTask(task)

	assertEqual(t, "ID", hi.ID, task.ID)
	assertEqual(t, "ProcessID", hi.ProcessID, task.ProcessID)
	assertEqual(t, "TaskType", hi.TaskType, task.TaskType)
	assertEqual(t, "Name", hi.Name, task.Name)
	assertEqual(t, "Status", hi.Status, task.Status)
	assertEqual(t, "Priority", hi.Priority, task.Priority)
	assertEqual(t, "TenantID", hi.TenantID, task.TenantID)
	assertEqual(t, "CreatedBy", hi.CreatedBy, task.CreatedBy)
	assertEqual(t, "ApprovalType", hi.ApprovalType, task.ApprovalType)
	assertEqual(t, "SequenceOrder", hi.SequenceOrder, task.SequenceOrder)

	assertPtrEqual(t, "ProcessInstanceID", hi.ProcessInstanceID, task.ProcessInstanceID)
	assertPtrEqual(t, "Assignee", hi.Assignee, task.Assignee)
	assertPtrEqual(t, "Owner", hi.Owner, task.Owner)
	assertPtrEqual(t, "Comment", hi.Comment, task.Comment)
	assertPtrEqual(t, "EndReason", hi.EndReason, task.EndReason)
	assertPtrEqual(t, "FormKey", hi.FormKey, task.FormKey)
	assertPtrEqual(t, "Variables", hi.Variables, task.Variables)
	assertPtrEqual(t, "ParentID", hi.ParentID, task.ParentID)
	assertPtrEqual(t, "ApprovalRule", hi.ApprovalRule, task.ApprovalRule)
	assertPtrEqual(t, "DelegateFrom", hi.DelegateFrom, task.DelegateFrom)
	assertPtrEqual(t, "DelegateReason", hi.DelegateReason, task.DelegateReason)

	if hi.TaskDefKey == nil || *hi.TaskDefKey != task.TaskDefKey {
		t.Errorf("TaskDefKey mismatch")
	}
}

func TestTaskToHiTask_PointerFieldsNil(t *testing.T) {
	task := &model.WfTask{
		ID:         "task-nil",
		TaskDefKey: "node1",
	}

	hi := taskToHiTask(task)

	if hi.ProcessInstanceID != nil {
		t.Error("ProcessInstanceID should be nil")
	}
	if hi.Assignee != nil {
		t.Error("Assignee should be nil")
	}
	if hi.EndedAt != nil {
		t.Error("EndedAt should be nil")
	}
}

// ---------------------------------------------------------------------------
// parseCountersignRule tests
// ---------------------------------------------------------------------------

func TestParseCountersignRule_Empty(t *testing.T) {
	s := &TaskServiceImpl{}
	rule, err := s.parseCountersignRule("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rule.Type != string(enums.CountersignTypeAll) {
		t.Errorf("Type = %q, want 'all'", rule.Type)
	}
	if rule.Value != 0 {
		t.Errorf("Value = %v, want 0", rule.Value)
	}
	if rule.IsSequential {
		t.Error("IsSequential should be false by default")
	}
}

func TestParseCountersignRule_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType string
		wantVal  float64
		wantSeq  bool
	}{
		{"all", `{"type":"all"}`, "all", 0, false},
		{"any", `{"type":"any"}`, "any", 0, false},
		{"majority", `{"type":"majority"}`, "majority", 0, false},
		{"percent", `{"type":"percent","value":75}`, "percent", 75, false},
		{"count", `{"type":"count","value":3}`, "count", 3, false},
		{"sequential", `{"type":"any","isSequential":true}`, "any", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &TaskServiceImpl{}
			rule, err := s.parseCountersignRule(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertEqual(t, "Type", rule.Type, tt.wantType)
			if rule.Value != tt.wantVal {
				t.Errorf("Value = %v, want %v", rule.Value, tt.wantVal)
			}
			if rule.IsSequential != tt.wantSeq {
				t.Errorf("IsSequential = %v, want %v", rule.IsSequential, tt.wantSeq)
			}
		})
	}
}

func TestParseCountersignRule_InvalidJSON(t *testing.T) {
	s := &TaskServiceImpl{}
	_, err := s.parseCountersignRule("not-json")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// ---------------------------------------------------------------------------
// utils.FormatTimePtr tests（经 task 服务装配路径使用的可空时间格式化）
// ---------------------------------------------------------------------------

func TestFormatTime_Nil(t *testing.T) {
	if result := utils.FormatTimePtr(nil); result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestFormatTime_Valid(t *testing.T) {
	tm := time.Date(2025, 6, 15, 14, 30, 0, 0, time.UTC)
	result := utils.FormatTimePtr(&tm)
	if result == nil {
		t.Fatal("expected non-nil")
	}
	assertEqual(t, "formatted time", *result, "2025-06-15 14:30:00")
}

// ---------------------------------------------------------------------------
// getApprovalRuleString tests
// ---------------------------------------------------------------------------

func TestGetApprovalRuleString_Nil(t *testing.T) {
	s := &TaskServiceImpl{}
	if r := s.getApprovalRuleString(nil); r != "" {
		t.Errorf("expected empty, got %q", r)
	}
}

func TestGetApprovalRuleString_Value(t *testing.T) {
	s := &TaskServiceImpl{}
	rule := `{"type":"all"}`
	assertEqual(t, "rule", s.getApprovalRuleString(&rule), rule)
}

// ---------------------------------------------------------------------------
// mergeVariables tests
// ---------------------------------------------------------------------------

func TestMergeVariables_NilNew(t *testing.T) {
	s := &TaskServiceImpl{}
	existing := `{"a":1}`
	result, err := s.mergeVariables(&existing, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != &existing {
		t.Error("should return same pointer when new is nil")
	}
}

func TestMergeVariables_NilExisting(t *testing.T) {
	s := &TaskServiceImpl{}
	newVars := map[string]interface{}{"b": 2}
	result, err := s.mergeVariables(nil, newVars)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil")
	}
	var parsed map[string]interface{}
	_ = json.Unmarshal([]byte(*result), &parsed)
	if parsed["b"] != float64(2) {
		t.Errorf("b = %v, want 2", parsed["b"])
	}
}

func TestMergeVariables_EmptyExisting(t *testing.T) {
	s := &TaskServiceImpl{}
	existing := ""
	newVars := map[string]interface{}{"key": "val"}
	result, err := s.mergeVariables(&existing, newVars)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var parsed map[string]interface{}
	_ = json.Unmarshal([]byte(*result), &parsed)
	assertEqual(t, "key", parsed["key"], "val")
}

func TestMergeVariables_Overwrite(t *testing.T) {
	s := &TaskServiceImpl{}
	existing := `{"a":1,"b":2}`
	newVars := map[string]interface{}{"b": 99, "c": 3}
	result, err := s.mergeVariables(&existing, newVars)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var parsed map[string]interface{}
	_ = json.Unmarshal([]byte(*result), &parsed)
	if parsed["a"] != float64(1) {
		t.Errorf("a = %v, want 1", parsed["a"])
	}
	if parsed["b"] != float64(99) {
		t.Errorf("b = %v, want 99", parsed["b"])
	}
	if parsed["c"] != float64(3) {
		t.Errorf("c = %v, want 3", parsed["c"])
	}
}

func TestMergeVariables_InvalidExistingJSON(t *testing.T) {
	s := &TaskServiceImpl{}
	existing := "not-json"
	_, err := s.mergeVariables(&existing, map[string]interface{}{"a": 1})
	if err == nil {
		t.Error("expected error for invalid existing JSON")
	}
}

// ---------------------------------------------------------------------------
// CheckCountersignSubTaskCompletion – unit tests using in-memory DAO
// ---------------------------------------------------------------------------

func TestCheckCountersignSubTaskCompletion_NoDAO(t *testing.T) {
	s := &TaskServiceImpl{
		taskDAO: nil, // Will panic on GetByParentID – expected
	}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from nil DAO")
		}
	}()
	_, _, _ = s.CheckCountersignSubTaskCompletion(context.Background(), "parent-1", "")
}

// any 规则=任意一人同意即通过：首人 reject 不能定局（其余人仍可能投同意），
// 全员投完仍无同意票才判拒绝。
func TestCheckCountersignSubTaskCompletion_AnyRule(t *testing.T) {
	q := secFixDB(t)
	ctx := context.Background()

	newSub := func(id, endReason string) *model.WfTask {
		st := &model.WfTask{
			ID: id, TaskDefKey: "approve", Name: "会签", TaskType: "user_task",
			Status:   string(enums.TaskStatusActive),
			TenantID: "t1", CreatedBy: "system", CreatedAt: time.Now(),
		}
		if endReason != "" {
			st.Status = string(enums.TaskStatusCompleted)
			st.EndReason = &endReason
		}
		return st
	}

	// 场景1：3 人 any，首人 reject → 不定局
	for _, st := range []*model.WfTask{
		newSub("any1-sub1", string(enums.ApprovalResultRejected)),
		newSub("any1-sub2", ""),
		newSub("any1-sub3", ""),
	} {
		st.ParentID = secFixStrPtr("any1-parent")
		require.NoError(t, q.WfTask.Create(st))
	}
	s := &TaskServiceImpl{taskDAO: dao.NewTaskDAOWithQuery(q), workflowEngine: &testEngineDouble{}}
	isCompleted, isApproved, err := s.CheckCountersignSubTaskCompletion(ctx, "any1-parent", `{"type":"any"}`)
	require.NoError(t, err)
	assertEqual(t, "any 首人 reject 后 isCompleted", isCompleted, false)
	assertEqual(t, "any 首人 reject 后 isApproved", isApproved, false)

	// 场景2：第二人 approve → 立即通过
	updated, err := s.taskDAO.Get(ctx, "any1-sub2")
	require.NoError(t, err)
	updated.Status = string(enums.TaskStatusCompleted)
	approved := string(enums.ApprovalResultApproved)
	updated.EndReason = &approved
	require.NoError(t, s.taskDAO.Update(ctx, updated))
	isCompleted, isApproved, err = s.CheckCountersignSubTaskCompletion(ctx, "any1-parent", `{"type":"any"}`)
	require.NoError(t, err)
	assertEqual(t, "any 有一人同意后 isCompleted", isCompleted, true)
	assertEqual(t, "any 有一人同意后 isApproved", isApproved, true)

	// 场景3：全员 reject → 完成且拒绝
	for _, id := range []string{"any2-sub1", "any2-sub2"} {
		st := newSub(id, string(enums.ApprovalResultRejected))
		st.ParentID = secFixStrPtr("any2-parent")
		require.NoError(t, q.WfTask.Create(st))
	}
	isCompleted, isApproved, err = s.CheckCountersignSubTaskCompletion(ctx, "any2-parent", `{"type":"any"}`)
	require.NoError(t, err)
	assertEqual(t, "any 全员拒绝后 isCompleted", isCompleted, true)
	assertEqual(t, "any 全员拒绝后 isApproved", isApproved, false)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func assertEqual(t *testing.T, name string, got, want interface{}) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func assertPtrEqual(t *testing.T, name string, got, want interface{}) {
	t.Helper()
	// Both nil
	if got == nil && want == nil {
		return
	}
	// One nil
	if got == nil || want == nil {
		t.Errorf("%s: got %v, want %v", name, got, want)
		return
	}
	// Compare underlying values via JSON (handles pointer comparison)
	g, _ := json.Marshal(got)
	w, _ := json.Marshal(want)
	if string(g) != string(w) {
		t.Errorf("%s: got %s, want %s", name, g, w)
	}
}
