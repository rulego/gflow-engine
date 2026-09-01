package enums

import (
	"strings"
	"testing"
)

func TestAllTaskStatusConstantsAreLowercase(t *testing.T) {
	all := GetAllTaskStatuses()
	for _, s := range all {
		if string(s) != strings.ToLower(string(s)) {
			t.Errorf("TaskStatus %q is not lowercase", s)
		}
	}
}

func TestTaskStatusCount(t *testing.T) {
	all := GetAllTaskStatuses()
	if len(all) != 11 {
		t.Errorf("expected 11 task statuses, got %d", len(all))
	}
}

func TestIsValidTaskStatus_All(t *testing.T) {
	for _, s := range GetAllTaskStatuses() {
		if !IsValidTaskStatus(s) {
			t.Errorf("IsValidTaskStatus(%q) = false, want true", s)
		}
	}
}

func TestIsValidTaskStatus_Invalid(t *testing.T) {
	for _, s := range []TaskStatus{"", "INVALID", "unknown", "transferred"} {
		if IsValidTaskStatus(s) {
			t.Errorf("IsValidTaskStatus(%q) = true, want false", s)
		}
	}
}

func TestGetAllProcessStatus(t *testing.T) {
	all := GetAllProcessStatus()
	if len(all) != 3 {
		t.Errorf("expected 3 process statuses, got %d", len(all))
	}
}

func TestIsValidProcessStatus(t *testing.T) {
	for _, s := range GetAllProcessStatus() {
		if !IsValidProcessStatus(s) {
			t.Errorf("IsValidProcessStatus(%q) = false", s)
		}
	}
	if !IsValidProcessStatus("draft") {
		t.Error("draft is defined as constant and should be valid")
	}
	if IsValidProcessStatus("") {
		t.Error("empty should be invalid")
	}
}

func TestGetAllInstanceStatuses(t *testing.T) {
	all := GetAllInstanceStatuses()
	if len(all) != 8 {
		t.Errorf("expected 8 instance statuses (incl deleted), got %d", len(all))
	}
}

func TestAllInstanceStatusesAreValid(t *testing.T) {
	for _, s := range GetAllInstanceStatuses() {
		if !IsValidInstanceStatus(s) {
			t.Errorf("IsValidInstanceStatus(%q) = false", s)
		}
	}
	// draft/cancelled 为合法状态；cancelled 视为终态
	for _, s := range []InstanceStatus{InstanceStatusDraft, InstanceStatusCancelled} {
		if !IsValidInstanceStatus(s) {
			t.Errorf("IsValidInstanceStatus(%q) should be true", s)
		}
	}
	for _, s := range []InstanceStatus{"", "unknown"} {
		if IsValidInstanceStatus(s) {
			t.Errorf("IsValidInstanceStatus(%q) should be false", s)
		}
	}
}

func TestCanTransitionInstanceStatus(t *testing.T) {
	tests := []struct {
		from, to InstanceStatus
		want     bool
	}{
		// active →
		{InstanceStatusActive, InstanceStatusCompleted, true},
		{InstanceStatusActive, InstanceStatusSuspended, true},
		{InstanceStatusActive, InstanceStatusTerminated, true},
		{InstanceStatusActive, InstanceStatusFailed, true},
		{InstanceStatusActive, InstanceStatusActive, false},
		{InstanceStatusActive, InstanceStatusDraft, false},
		// suspended →
		{InstanceStatusSuspended, InstanceStatusActive, true},
		{InstanceStatusSuspended, InstanceStatusTerminated, true},
		{InstanceStatusSuspended, InstanceStatusCompleted, false},
		{InstanceStatusSuspended, InstanceStatusFailed, false},
		// terminal states
		{InstanceStatusCompleted, InstanceStatusActive, false},
		{InstanceStatusTerminated, InstanceStatusActive, false},
		{InstanceStatusFailed, InstanceStatusActive, false},
		// draft：激活或丢弃
		{InstanceStatusDraft, InstanceStatusActive, true},
		{InstanceStatusDraft, InstanceStatusCancelled, true},
		{InstanceStatusDraft, InstanceStatusDeleted, true},
		{InstanceStatusDraft, InstanceStatusCompleted, false},
	}
	for _, tt := range tests {
		got := CanTransitionInstanceStatus(tt.from, tt.to)
		if got != tt.want {
			t.Errorf("CanTransition(%s→%s) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
}

func TestGetActiveInstanceStatuses(t *testing.T) {
	active := GetActiveInstanceStatuses()
	if len(active) != 2 {
		t.Errorf("expected 2 active statuses, got %d", len(active))
	}
	for _, s := range active {
		if s != InstanceStatusActive && s != InstanceStatusSuspended {
			t.Errorf("unexpected active status: %s", s)
		}
	}
}

func TestIsTerminalInstanceStatus(t *testing.T) {
	for _, s := range []InstanceStatus{InstanceStatusCompleted, InstanceStatusTerminated, InstanceStatusFailed} {
		if !IsTerminalInstanceStatus(s) {
			t.Errorf("IsTerminal(%s) = false", s)
		}
	}
	for _, s := range []InstanceStatus{InstanceStatusActive, InstanceStatusSuspended, InstanceStatusDraft} {
		if IsTerminalInstanceStatus(s) {
			t.Errorf("IsTerminal(%s) = true, want false", s)
		}
	}
}

func TestGetAllActionTypes(t *testing.T) {
	all := GetAllActionTypes()
	if len(all) != 12 {
		t.Errorf("expected 12 action types, got %d", len(all))
	}
}

func TestIsValidActionType(t *testing.T) {
	for _, a := range GetAllActionTypes() {
		if !IsValidActionType(a) {
			t.Errorf("IsValidActionType(%q) = false", a)
		}
	}
	if IsValidActionType("") {
		t.Error("empty should be invalid")
	}
}

func TestGetTaskActionTypes(t *testing.T) {
	task := GetTaskActionTypes()
	if len(task) != 7 {
		t.Errorf("expected 7 task action types, got %d", len(task))
	}
}

func TestGetInstanceActionTypes(t *testing.T) {
	inst := GetInstanceActionTypes()
	if len(inst) != 5 {
		t.Errorf("expected 5 instance action types, got %d", len(inst))
	}
}

func TestTaskAndInstanceActionsAreDisjoint(t *testing.T) {
	task := GetTaskActionTypes()
	inst := GetInstanceActionTypes()
	taskSet := map[ActionType]bool{}
	for _, a := range task {
		taskSet[a] = true
	}
	for _, a := range inst {
		if taskSet[a] {
			t.Errorf("action %q appears in both task and instance lists", a)
		}
	}
}

func TestGetAllApprovalResults(t *testing.T) {
	all := GetAllApprovalResults()
	if len(all) != 5 {
		t.Errorf("expected 5 approval results, got %d", len(all))
	}
}

func TestIsPositiveApprovalResult(t *testing.T) {
	if !IsPositiveApprovalResult(ApprovalResultApproved) {
		t.Error("approved should be positive")
	}
	for _, r := range []ApprovalResult{ApprovalResultRejected, ApprovalResultReturned, ApprovalResultPending} {
		if IsPositiveApprovalResult(r) {
			t.Errorf("IsPositive(%s) = true, want false", r)
		}
	}
}

func TestIsNegativeApprovalResult(t *testing.T) {
	for _, r := range []ApprovalResult{ApprovalResultRejected, ApprovalResultReturned} {
		if !IsNegativeApprovalResult(r) {
			t.Errorf("IsNegative(%s) = false", r)
		}
	}
	if IsNegativeApprovalResult(ApprovalResultApproved) {
		t.Error("approved should not be negative")
	}
}

func TestGetAllCountersignTypes(t *testing.T) {
	all := GetAllCountersignTypes()
	if len(all) != 7 {
		t.Errorf("expected 7 countersign types, got %d", len(all))
	}
}

func TestIsValidCountersignType(t *testing.T) {
	for _, c := range GetAllCountersignTypes() {
		if !IsValidCountersignType(c) {
			t.Errorf("IsValidCountersignType(%q) = false", c)
		}
	}
	// 仅空值非法（all/any/majority/percent/count 均为合法规则类型）
	for _, c := range []CountersignType{""} {
		if IsValidCountersignType(c) {
			t.Errorf("IsValidCountersignType(%q) should be false", c)
		}
	}
}

func TestGetAllAssigneeTypes(t *testing.T) {
	all := GetAllAssigneeTypes()
	if len(all) != 5 {
		t.Errorf("expected 5 assignee types, got %d", len(all))
	}
}

func TestGetAllCandidateTypes(t *testing.T) {
	all := GetAllCandidateTypes()
	if len(all) != 7 {
		t.Errorf("expected 7 candidate types, got %d", len(all))
	}
	for _, c := range all {
		if !IsValidCandidateType(c) {
			t.Errorf("IsValidCandidateType(%q) = false", c)
		}
	}
}

func TestGetAllReturnTypes(t *testing.T) {
	if len(GetAllReturnTypes()) != 3 {
		t.Errorf("expected 3 return types")
	}
}

func TestGetAllDelegateTypes(t *testing.T) {
	if len(GetAllDelegateTypes()) != 4 {
		t.Errorf("expected 4 delegate types")
	}
}

func TestGetAllNotificationTypes(t *testing.T) {
	if len(GetAllNotificationTypes()) != 8 {
		t.Errorf("expected 8 notification types")
	}
}

func TestGetAllSelfApprovalTypes(t *testing.T) {
	all := GetAllSelfApprovalTypes()
	if len(all) != 5 {
		t.Errorf("expected 5 self-approval types, got %d", len(all))
	}
	for _, s := range all {
		if !IsValidSelfApprovalType(s) {
			t.Errorf("IsValidSelfApprovalType(%q) = false", s)
		}
	}
	if IsValidSelfApprovalType("") {
		t.Error("empty should be invalid")
	}
}

// TestTaskStatusValues verifies all TaskStatus constants use lowercase.
func TestTaskStatusValues(t *testing.T) {
	statuses := []TaskStatus{
		TaskStatusCreated, TaskStatusAssigned, TaskStatusWaiting, TaskStatusPending,
		TaskStatusActive, TaskStatusDelegated, TaskStatusCompleted, TaskStatusReturned,
		TaskStatusWithdrawn, TaskStatusSuspended, TaskStatusTerminated,
	}
	for _, s := range statuses {
		if s != TaskStatus(strings.ToLower(string(s))) {
			t.Errorf("TaskStatus %q is not lowercase", s)
		}
	}
}

// TestProcessStatusValues verifies all ProcessStatus constants use lowercase.
func TestProcessStatusValues(t *testing.T) {
	statuses := GetAllProcessStatus()
	for _, s := range statuses {
		if s != ProcessStatus(strings.ToLower(string(s))) {
			t.Errorf("ProcessStatus %q is not lowercase", s)
		}
	}
}

// TestInstanceStatusValues verifies all InstanceStatus constants use lowercase.
func TestInstanceStatusValues(t *testing.T) {
	statuses := GetAllInstanceStatuses()
	for _, s := range statuses {
		if s != InstanceStatus(strings.ToLower(string(s))) {
			t.Errorf("InstanceStatus %q is not lowercase", s)
		}
	}
}

// TestNoTransferredStatus verifies "transferred" is not a valid TaskStatus.
func TestNoTransferredStatus(t *testing.T) {
	for _, s := range []TaskStatus{
		TaskStatusCreated, TaskStatusAssigned, TaskStatusWaiting, TaskStatusPending,
		TaskStatusActive, TaskStatusDelegated, TaskStatusCompleted, TaskStatusReturned,
		TaskStatusWithdrawn, TaskStatusSuspended, TaskStatusTerminated,
	} {
		if string(s) == "transferred" {
			t.Error("TaskStatus enum should not contain 'transferred'")
		}
	}
}

// TestIsValidTaskStatus verifies all 11 task statuses pass validation.
func TestIsValidTaskStatus(t *testing.T) {
	all := []TaskStatus{
		TaskStatusCreated, TaskStatusAssigned, TaskStatusWaiting, TaskStatusPending,
		TaskStatusActive, TaskStatusDelegated, TaskStatusCompleted, TaskStatusReturned,
		TaskStatusTerminated,
	}
	for _, s := range all {
		if !IsValidTaskStatus(s) {
			t.Errorf("IsValidTaskStatus(%q) should be true", s)
		}
	}
	// Invalid cases
	if IsValidTaskStatus("INVALID") {
		t.Error("IsValidTaskStatus should reject 'INVALID'")
	}
	if IsValidTaskStatus("transferred") {
		t.Error("IsValidTaskStatus should reject 'transferred'")
	}
}

// TestGetAllTaskStatusesCount verifies GetAllTaskStatuses returns all 11 statuses.
func TestGetAllTaskStatusesCount(t *testing.T) {
	all := GetAllTaskStatuses()
	if len(all) != 11 {
		t.Errorf("GetAllTaskStatuses should return 11 statuses, got %d", len(all))
	}
}

// TestIsValidInstanceStatus verifies all declared statuses pass validation.
func TestIsValidInstanceStatus(t *testing.T) {
	for _, s := range GetAllInstanceStatuses() {
		if !IsValidInstanceStatus(s) {
			t.Errorf("IsValidInstanceStatus(%q) should be true", s)
		}
	}
}

// TestApprovalTypeEnum verifies approval type values.
func TestApprovalTypeEnum(t *testing.T) {
	types := map[ApprovalType]bool{
		ApprovalTypeSingle:      true,
		ApprovalTypeOr:          true,
		ApprovalTypeSequential:  true,
		ApprovalTypeVote:        true,
		ApprovalTypeCountersign: true,
		ApprovalTypeSystem:      true,
		ApprovalTypeCC:          true,
	}
	for k := range types {
		if k != ApprovalType(strings.ToLower(string(k))) {
			t.Errorf("ApprovalType %q is not lowercase", k)
		}
	}
}
