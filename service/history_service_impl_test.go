package service

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/gflow-engine/model"
)

// ---------------------------------------------------------------------------
// HistoryServiceImpl conversion tests
// ---------------------------------------------------------------------------

func TestConvertToHistoricProcessInstance_NilBusinessKey(t *testing.T) {
	s := &HistoryServiceImpl{}
	instance := &model.WfInstance{
		ID:        "inst-1",
		ProcessID: "proc-1",
		Name:      "Test Process",
		TenantID:  "tenant1",
		CreatedBy: "user1",
		CreatedAt: time.Now(),
	}

	hi := s.convertToHistoricProcessInstance(context.Background(), instance, nil)
	if hi.ID != "inst-1" {
		t.Errorf("ID = %q, want 'inst-1'", hi.ID)
	}
	if hi.BusinessKey != "" {
		t.Errorf("BusinessKey = %q, want empty", hi.BusinessKey)
	}
	if hi.ProcessDefinitionID != "proc-1" {
		t.Errorf("ProcessDefinitionID = %q, want 'proc-1'", hi.ProcessDefinitionID)
	}
}

func TestConvertToHistoricProcessInstance_WithBusinessKey(t *testing.T) {
	s := &HistoryServiceImpl{}
	bk := "BIZ-001"
	instance := &model.WfInstance{
		ID:          "inst-2",
		ProcessID:   "proc-2",
		BusinessKey: &bk,
		Name:        "Leave Approval",
		TenantID:    "tenant1",
		CreatedBy:   "user1",
		CreatedAt:   time.Now(),
	}

	hi := s.convertToHistoricProcessInstance(context.Background(), instance, nil)
	if hi.BusinessKey != "BIZ-001" {
		t.Errorf("BusinessKey = %q, want 'BIZ-001'", hi.BusinessKey)
	}
}

func TestConvertToHistoricProcessInstance_WithDuration(t *testing.T) {
	s := &HistoryServiceImpl{}
	start := time.Now()
	end := start.Add(2 * time.Hour)
	instance := &model.WfInstance{
		ID:        "inst-3",
		ProcessID: "proc-3",
		Name:      "Test",
		TenantID:  "t1",
		CreatedBy: "u1",
		CreatedAt: start,
		EndedAt:   &end,
	}

	hi := s.convertToHistoricProcessInstance(context.Background(), instance, nil)
	if hi.EndTime == nil {
		t.Fatal("EndTime should not be nil")
	}
	if hi.DurationInMillis == nil {
		t.Fatal("DurationInMillis should not be nil")
	}
	expectedDuration := end.Sub(start).Milliseconds()
	if *hi.DurationInMillis != expectedDuration {
		t.Errorf("DurationInMillis = %d, want %d", *hi.DurationInMillis, expectedDuration)
	}
}

func TestConvertToHistoricProcessInstance_NoEnd(t *testing.T) {
	s := &HistoryServiceImpl{}
	instance := &model.WfInstance{
		ID:        "inst-4",
		ProcessID: "proc-4",
		Name:      "Running",
		TenantID:  "t1",
		CreatedBy: "u1",
		CreatedAt: time.Now(),
	}

	hi := s.convertToHistoricProcessInstance(context.Background(), instance, nil)
	if hi.EndTime != nil {
		t.Error("EndTime should be nil for running instance")
	}
	if hi.DurationInMillis != nil {
		t.Error("DurationInMillis should be nil for running instance")
	}
}

// ---------------------------------------------------------------------------
// convertToHistoricTaskInstance tests
// ---------------------------------------------------------------------------

func TestConvertToHistoricTaskInstance_AllNilPointers(t *testing.T) {
	s := &HistoryServiceImpl{}
	task := &model.WfTask{
		ID:         "task-1",
		ProcessID:  "proc-1",
		TaskDefKey: "node1",
		Name:       "Task 1",
		TenantID:   "t1",
		CreatedBy:  "system",
		CreatedAt:  time.Now(),
		Priority:   50,
	}

	hi := s.convertToHistoricTaskInstance(task)
	if hi.ID != "task-1" {
		t.Errorf("ID = %q", hi.ID)
	}
	if hi.ProcessInstanceID != "" {
		t.Errorf("ProcessInstanceID should be empty, got %q", hi.ProcessInstanceID)
	}
	if hi.Assignee != "" {
		t.Errorf("Assignee should be empty, got %q", hi.Assignee)
	}
	if hi.Owner != "" {
		t.Errorf("Owner should be empty, got %q", hi.Owner)
	}
	if hi.Description != "" {
		t.Errorf("Description should be empty, got %q", hi.Description)
	}
	if hi.FormKey != "" {
		t.Errorf("FormKey should be empty, got %q", hi.FormKey)
	}
	if hi.ParentTaskID != "" {
		t.Errorf("ParentTaskID should be empty, got %q", hi.ParentTaskID)
	}
}

func TestConvertToHistoricTaskInstance_AllFieldsSet(t *testing.T) {
	s := &HistoryServiceImpl{}
	now := time.Now()
	end := now.Add(time.Hour)
	assignee := "user1"
	owner := "owner1"
	processInstID := "inst-1"
	desc := "description"
	formKey := "form1"
	parentID := "parent-1"
	claimedAt := now.Add(10 * time.Minute)
	dueDate := now.Add(24 * time.Hour)
	duration := end.Sub(now).Milliseconds()

	task := &model.WfTask{
		ID:                "task-full",
		ProcessInstanceID: &processInstID,
		ProcessID:         "proc-1",
		TaskDefKey:        "node1",
		Name:              "Full Task",
		Description:       &desc,
		Assignee:          &assignee,
		Owner:             &owner,
		Status:            "completed",
		Priority:          80,
		FormKey:           &formKey,
		ParentID:          &parentID,
		ClaimedAt:         &claimedAt,
		DueDate:           &dueDate,
		EndedAt:           &end,
		TenantID:          "t1",
		CreatedBy:         "system",
		CreatedAt:         now,
	}

	hi := s.convertToHistoricTaskInstance(task)

	assertEqual(t, "ID", hi.ID, "task-full")
	assertEqual(t, "ProcessInstanceID", hi.ProcessInstanceID, "inst-1")
	assertEqual(t, "Assignee", hi.Assignee, "user1")
	assertEqual(t, "Owner", hi.Owner, "owner1")
	assertEqual(t, "Description", hi.Description, "description")
	assertEqual(t, "FormKey", hi.FormKey, "form1")
	assertEqual(t, "ParentTaskID", hi.ParentTaskID, "parent-1")
	assertEqual(t, "Priority", hi.Priority, 80)
	assertEqual(t, "TaskDefinitionKey", hi.TaskDefinitionKey, "node1")
	assertEqual(t, "TenantID", hi.TenantID, "t1")

	if hi.EndTime == nil {
		t.Fatal("EndTime should not be nil")
	}
	if hi.DurationInMillis == nil {
		t.Fatal("DurationInMillis should not be nil")
	}
	if *hi.DurationInMillis != duration {
		t.Errorf("DurationInMillis = %d, want %d", *hi.DurationInMillis, duration)
	}
	if hi.ClaimTime == nil {
		t.Fatal("ClaimTime should not be nil")
	}
	if hi.DueDate == nil {
		t.Fatal("DueDate should not be nil")
	}
}

func TestConvertToHistoricTaskInstance_InProgress(t *testing.T) {
	s := &HistoryServiceImpl{}
	task := &model.WfTask{
		ID:         "task-active",
		TaskDefKey: "node1",
		Name:       "Active Task",
		Status:     "active",
		TenantID:   "t1",
		CreatedAt:  time.Now(),
	}

	hi := s.convertToHistoricTaskInstance(task)
	if hi.EndTime != nil {
		t.Error("EndTime should be nil for active task")
	}
	if hi.DurationInMillis != nil {
		t.Error("DurationInMillis should be nil for active task")
	}
	if hi.ClaimTime != nil {
		t.Error("ClaimTime should be nil when not claimed")
	}
}

// ---------------------------------------------------------------------------
// HistoryService input validation tests
// ---------------------------------------------------------------------------

func TestHistoryService_GetHistoricProcessInstance_EmptyID(t *testing.T) {
	s := &HistoryServiceImpl{}
	_, err := s.GetHistoricProcessInstance(context.TODO(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty ID")
	}
}

func TestHistoryService_GetHistoricTaskInstance_EmptyID(t *testing.T) {
	s := &HistoryServiceImpl{}
	_, err := s.GetHistoricTaskInstance(context.TODO(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty ID")
	}
}

func TestHistoryService_DeleteHistoricProcessInstance_EmptyID(t *testing.T) {
	s := &HistoryServiceImpl{}
	err := s.DeleteHistoricProcessInstance(context.TODO(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty ID")
	}
}

func TestHistoryService_DeleteHistoricTaskInstance_EmptyID(t *testing.T) {
	s := &HistoryServiceImpl{}
	err := s.DeleteHistoricTaskInstance(context.TODO(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty ID")
	}
}
