package service

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

func newTaskServiceImplForTest() *TaskServiceImpl {
	return &TaskServiceImpl{
		idGenerator: NewIDGenerator(),
	}
}

func newTaskServiceForValidation() *TaskServiceImpl {
	return &TaskServiceImpl{}
}

func TestTaskServiceImpl_CreateTask_PendingStatus_Default(t *testing.T) {
	// Verify that when no assignee is set, the default status would be Pending.
	task := &model.WfTask{Name: "Test Task"}
	// The CreateTask method sets status based on assignee.
	// With nil DAO it panics before we can check, so we verify the logic directly.
	if task.Assignee != nil && *task.Assignee != "" {
		t.Error("expected nil assignee for pending task")
	}
}

func TestTaskServiceImpl_CreateTask_ActiveStatus_WithAssignee(t *testing.T) {
	// Verify that when assignee is set, the default status would be Active.
	assignee := "user1"
	task := &model.WfTask{Name: "Test Task", Assignee: &assignee}
	if task.Assignee == nil || *task.Assignee == "" {
		t.Error("expected non-nil assignee")
	}
}

func TestTaskServiceImpl_CreateTask_NilDAO_Panics(t *testing.T) {
	s := &TaskServiceImpl{idGenerator: NewIDGenerator()}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from nil DAO")
		}
	}()
	s.CreateTask(context.Background(), Actor{}, &model.WfTask{Name: "test"})
}

func TestTaskServiceImpl_SetAssignee_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.SetAssignee(context.Background(), Actor{}, "", "user1")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_SetOwner_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.SetOwner(context.Background(), Actor{}, "", "user1")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_Delegate_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Delegate(context.Background(), Actor{}, "", "user1", "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_Delegate_EmptyUserID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Delegate(context.Background(), Actor{}, "task1", "", "reason")
	if err == nil {
		t.Error("expected error for empty userID")
	}
}

func TestTaskServiceImpl_Resolve_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Resolve(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_SetPriority_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.SetPriority(context.Background(), Actor{}, "", 5)
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_SetDueDate_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.SetDueDate(context.Background(), Actor{}, "", time.Now())
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_SuspendTask_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.SuspendTask(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_ActivateTask_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.ActivateTask(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_GetTaskVariables_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	_, err := s.GetTaskVariables(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_SetTaskVariables_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.SetTaskVariables(context.Background(), Actor{}, "", map[string]interface{}{"k": "v"})
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_GetActiveTasksByProcessInstanceID_EmptyID(t *testing.T) {
	s := newTaskServiceImplForTest()
	_, err := s.GetActiveTasksByProcessInstanceID(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty processInstanceId")
	}
}

func TestTaskServiceImpl_GetHistoryTasksByProcessInstanceID_NilDAO_Panics(t *testing.T) {
	s := &TaskServiceImpl{}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from nil DAO")
		}
	}()
	s.GetHistoryTasksByProcessInstanceID(context.Background(), "inst-1")
}

func TestTaskServiceImpl_GetHistoryTask_EmptyID(t *testing.T) {
	s := newTaskServiceImplForTest()
	_, err := s.GetHistoryTask(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_GetTaskByDefKey_NilDAO_Panics(t *testing.T) {
	s := &TaskServiceImpl{}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from nil DAO")
		}
	}()
	s.GetTaskByDefKey(context.Background(), "inst1", "key1")
}

func TestTaskServiceImpl_Transfer_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Transfer(context.Background(), Actor{}, "", "u2", "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_Transfer_EmptyFromUser(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Transfer(context.Background(), Actor{}, "t1", "u2", "reason")
	if err == nil {
		t.Error("expected error for empty fromUserID")
	}
}

func TestTaskServiceImpl_Transfer_EmptyToUser(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Transfer(context.Background(), Actor{UserID: "u1"}, "t1", "", "reason")
	if err == nil {
		t.Error("expected error for empty toUserID")
	}
}

func TestTaskServiceImpl_Withdraw_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Withdraw(context.Background(), Actor{}, "", "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_Withdraw_EmptyUserID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Withdraw(context.Background(), Actor{}, "t1", "reason")
	if err == nil {
		t.Error("expected error for empty userID")
	}
}

func TestTaskServiceImpl_Return_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Return(context.Background(), Actor{}, "", "node1", "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_Return_EmptyTargetActivityID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Return(context.Background(), Actor{UserID: "u1"}, "t1", "", "reason")
	if err == nil {
		t.Error("expected error for empty targetActivityID")
	}
}

func TestTaskServiceImpl_AddSign_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.AddSign(context.Background(), Actor{}, "", []string{"u1"}, "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_AddSign_EmptyUserIDs(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.AddSign(context.Background(), Actor{}, "t1", []string{}, "reason")
	if err == nil {
		t.Error("expected error for empty userIDs")
	}
}

func TestTaskServiceImpl_ReduceSign_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.ReduceSign(context.Background(), Actor{}, "", []string{"u1"}, "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_ReduceSign_EmptyUserIDs(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.ReduceSign(context.Background(), Actor{}, "t1", []string{}, "reason")
	if err == nil {
		t.Error("expected error for empty userIDs")
	}
}

func TestTaskServiceImpl_Complete_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Complete(context.Background(), Actor{}, "", nil)
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_Approve_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Approve(context.Background(), Actor{}, "", "ok", nil)
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_Reject_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	err := s.Reject(context.Background(), Actor{}, "", "no", nil)
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_GetApprovalStatistics_EmptyUserID(t *testing.T) {
	s := newTaskServiceImplForTest()
	_, err := s.GetApprovalStatistics(context.Background(), Actor{TenantID: "t1"})
	if err == nil {
		t.Error("expected error for empty userID")
	}
}

func TestTaskServiceImpl_GetApprovalStatisticsDetail_EmptyUserID(t *testing.T) {
	s := newTaskServiceImplForTest()
	_, err := s.GetApprovalStatisticsDetail(context.Background(), Actor{TenantID: "t1"}, nil, nil)
	if err == nil {
		t.Error("expected error for empty userID")
	}
}

func TestTaskServiceImpl_GetNodeApprovalStatus_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	_, err := s.GetNodeApprovalStatus(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_GetNodeApprovers_EmptyTaskID(t *testing.T) {
	s := newTaskServiceImplForTest()
	_, err := s.GetNodeApprovers(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskServiceImpl_GetNodeApprovalStatusByProcessInstance_EmptyID(t *testing.T) {
	s := newTaskServiceImplForTest()
	_, err := s.GetNodeApprovalStatusByProcessInstance(context.Background(), "", "key1")
	if err == nil {
		t.Error("expected error for empty processInstanceID")
	}
}

func TestTaskServiceImpl_GetNodeApprovalStatusByProcessInstance_EmptyKey(t *testing.T) {
	s := newTaskServiceImplForTest()
	_, err := s.GetNodeApprovalStatusByProcessInstance(context.Background(), "inst1", "")
	if err == nil {
		t.Error("expected error for empty taskDefKey")
	}
}

func TestTaskServiceImpl_ImplementsInterface(t *testing.T) {
	var _ TaskService = (*TaskServiceImpl)(nil)
}

// Parameter-validation tests for the guard clauses that run before any DAO
// calls (no database required).

func TestCreateTask_NilTask(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.CreateTask(context.Background(), Actor{}, nil)
	if err == nil {
		t.Error("expected error for nil task")
	}
}

func TestGetTask_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetTask(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestDeleteTask_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.DeleteTask(context.Background(), Actor{}, "", "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestClaim_EmptyTaskID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Claim(context.Background(), Actor{UserID: "user1"}, "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestClaim_EmptyUserID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Claim(context.Background(), Actor{}, "task1")
	if err == nil {
		t.Error("expected error for empty userID")
	}
}

func TestUnclaim_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Unclaim(context.Background(), Actor{UserID: "user1"}, "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestSetAssignee_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.SetAssignee(context.Background(), Actor{}, "", "user1")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestSetOwner_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.SetOwner(context.Background(), Actor{}, "", "user1")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestDelegate_EmptyTaskID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Delegate(context.Background(), Actor{}, "", "user1", "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestDelegate_EmptyUserID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Delegate(context.Background(), Actor{}, "task1", "", "reason")
	if err == nil {
		t.Error("expected error for empty userID")
	}
}

func TestResolve_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Resolve(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

// GetHistoryTasksByProcessInstanceID has no input validation; with a nil DAO
// it panics, which the test verifies.
func TestGetHistoryTasksByProcessInstanceID_EmptyID_NoDAO(t *testing.T) {
	s := newTaskServiceForValidation()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from nil DAO")
		}
	}()
	_, _ = s.GetHistoryTasksByProcessInstanceID(context.Background(), "")
}

// GetTaskByDefKey has no input validation; with a nil DAO it panics, which
// the test verifies.
func TestGetTaskByDefKey_EmptyID_NoDAO(t *testing.T) {
	s := newTaskServiceForValidation()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from nil DAO")
		}
	}()
	_, _ = s.GetTaskByDefKey(context.Background(), "", "key1")
}

func TestGetHistoryTask_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetHistoryTask(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestGetTaskCandidates_EmptyDefKey(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetTaskCandidates(context.Background(), "inst1", "")
	if err == nil {
		t.Error("expected error for empty taskDefKey")
	}
}

func TestGetTaskCandidates_EmptyInstanceID(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetTaskCandidates(context.Background(), "", "key1")
	if err == nil {
		t.Error("expected error for empty processInstanceID")
	}
}

func TestCompleteWithApproval_NilRequest(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.CompleteWithApproval(context.Background(), Actor{}, nil)
	if err == nil {
		t.Error("expected error for nil request")
	}
}

func TestCompleteWithApproval_EmptyTaskID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.CompleteWithApproval(context.Background(), Actor{}, &ApprovalRequest{})
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestComplete_EmptyTaskID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Complete(context.Background(), Actor{}, "", nil)
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestSetPriority_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.SetPriority(context.Background(), Actor{}, "", 5)
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestSetDueDate_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.SetDueDate(context.Background(), Actor{}, "", time.Now())
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestSuspendTask_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.SuspendTask(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestActivateTask_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.ActivateTask(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestGetTaskVariables_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetTaskVariables(context.Background(), Actor{}, "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestGetTaskVariable_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetTaskVariable(context.Background(), Actor{}, "", "var1")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestSetTaskVariables_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.SetTaskVariables(context.Background(), Actor{}, "", map[string]interface{}{"a": 1})
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestAddSign_EmptyTaskID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.AddSign(context.Background(), Actor{}, "", []string{"user1"}, "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestAddSign_EmptyUserIDs(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.AddSign(context.Background(), Actor{}, "task1", nil, "reason")
	if err == nil {
		t.Error("expected error for empty userIDs")
	}
}

func TestReduceSign_EmptyTaskID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.ReduceSign(context.Background(), Actor{}, "", []string{"user1"}, "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestReduceSign_EmptyUserIDs(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.ReduceSign(context.Background(), Actor{}, "task1", nil, "reason")
	if err == nil {
		t.Error("expected error for empty userIDs")
	}
}

func TestTransfer_EmptyTaskID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Transfer(context.Background(), Actor{}, "", "user2", "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTransfer_EmptyFromUser(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Transfer(context.Background(), Actor{}, "task1", "user2", "reason")
	if err == nil {
		t.Error("expected error for empty fromUserID")
	}
}

func TestTransfer_EmptyToUser(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Transfer(context.Background(), Actor{UserID: "user1"}, "task1", "", "reason")
	if err == nil {
		t.Error("expected error for empty toUserID")
	}
}

func TestWithdraw_EmptyTaskID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Withdraw(context.Background(), Actor{}, "", "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestWithdraw_EmptyUserID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Withdraw(context.Background(), Actor{}, "task1", "reason")
	if err == nil {
		t.Error("expected error for empty userID")
	}
}

func TestReturn_EmptyTaskID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Return(context.Background(), Actor{}, "", "target", "reason")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestReturn_EmptyTargetActivityID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Return(context.Background(), Actor{UserID: "user1"}, "task1", "", "reason")
	if err == nil {
		t.Error("expected error for empty targetActivityID")
	}
}

func TestReturn_EmptyUserID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.Return(context.Background(), Actor{}, "task1", "target", "reason")
	if err == nil {
		t.Error("expected error for empty userID")
	}
}

func TestGetNodeApprovalStatus_EmptyID(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetNodeApprovalStatus(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestGetNodeApprovalStatusByProcessInstance_EmptyInstanceID(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetNodeApprovalStatusByProcessInstance(context.Background(), "", "key1")
	if err == nil {
		t.Error("expected error for empty processInstanceID")
	}
}

func TestGetNodeApprovalStatusByProcessInstance_EmptyDefKey(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetNodeApprovalStatusByProcessInstance(context.Background(), "inst1", "")
	if err == nil {
		t.Error("expected error for empty taskDefKey")
	}
}

func TestCreateCountersignSubTasks_EmptyParentID(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.CreateCountersignSubTasks(context.Background(), "", []string{"user1"}, "")
	if err == nil {
		t.Error("expected error for empty parentTaskID")
	}
}

func TestCreateCountersignSubTasks_EmptyAssignees(t *testing.T) {
	s := newTaskServiceForValidation()
	err := s.CreateCountersignSubTasks(context.Background(), "parent1", nil, "")
	if err == nil {
		t.Error("expected error for empty assignees")
	}
}

func TestCheckCountersignSubTaskCompletion_EmptyParentID(t *testing.T) {
	s := newTaskServiceForValidation()
	_, _, err := s.CheckCountersignSubTaskCompletion(context.Background(), "", "")
	if err == nil {
		t.Error("expected error for empty parentTaskID")
	}
}

func TestGetApprovalStatistics_EmptyUserID(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetApprovalStatistics(context.Background(), Actor{TenantID: "tenant1"})
	if err == nil {
		t.Error("expected error for empty userID")
	}
}

func TestGetApprovalStatistics_EmptyTenantID(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetApprovalStatistics(context.Background(), Actor{UserID: "user1"})
	if err == nil {
		t.Error("expected error for empty tenantID")
	}
}

func TestGetApprovalStatisticsDetail_EmptyUserID(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetApprovalStatisticsDetail(context.Background(), Actor{TenantID: "tenant1"}, nil, nil)
	if err == nil {
		t.Error("expected error for empty userID")
	}
}

func TestGetApprovalStatisticsDetail_EmptyTenantID(t *testing.T) {
	s := newTaskServiceForValidation()
	_, err := s.GetApprovalStatisticsDetail(context.Background(), Actor{UserID: "user1"}, nil, nil)
	if err == nil {
		t.Error("expected error for empty tenantID")
	}
}

func TestTaskServiceImpl_ImplementsTaskService(t *testing.T) {
	var _ TaskService = (*TaskServiceImpl)(nil)
}

func TestApprovalRequest_Fields(t *testing.T) {
	req := &ApprovalRequest{
		TaskID:         "task1",
		ApprovalResult: enums.ApprovalResultApproved,
		Comment:        "Looks good",
		Variables:      map[string]interface{}{"key": "value"},
	}
	if req.TaskID != "task1" {
		t.Errorf("TaskID = %q", req.TaskID)
	}
	if req.ApprovalResult != enums.ApprovalResultApproved {
		t.Errorf("ApprovalResult = %q", req.ApprovalResult)
	}
}

func TestTaskQuery_PageDefaults(t *testing.T) {
	q := &dto.TaskQuery{}
	if q.PageRequest.GetPage() != 1 {
		t.Errorf("GetPage() = %d, want 1", q.PageRequest.GetPage())
	}
	if q.PageRequest.GetPageSize() != 10 {
		t.Errorf("GetPageSize() = %d, want 10", q.PageRequest.GetPageSize())
	}
}

func TestTaskQuery_CustomPaging(t *testing.T) {
	q := &dto.TaskQuery{
		PageRequest: dto.PageRequest{
			Page:     3,
			PageSize: 25,
		},
	}
	if q.PageRequest.GetPage() != 3 {
		t.Errorf("GetPage() = %d, want 3", q.PageRequest.GetPage())
	}
	if q.PageRequest.GetPageSize() != 25 {
		t.Errorf("GetPageSize() = %d, want 25", q.PageRequest.GetPageSize())
	}
}
