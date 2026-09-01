package service

import (
	"context"
	"errors"
	"testing"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/enums"
)

func newProcessServiceImplForTest() *ProcessServiceImpl {
	return &ProcessServiceImpl{
		idGenerator: NewIDGenerator(),
	}
}

func newProcessServiceForValidation() *ProcessServiceImpl {
	return &ProcessServiceImpl{}
}

// expectPanic runs fn and fails the test if it does NOT panic.
func expectPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("%s: expected panic", name)
		}
	}()
	fn()
}

func TestProcessServiceImpl_Deploy_EmptyName(t *testing.T) {
	s := newProcessServiceImplForTest()
	_, err := s.Deploy(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, &model.WfProcess{
		ProcessKey: "key1",
		TenantID:   "t1",
	}, false)
	if err == nil {
		t.Error("expected error for empty name")
	}
}

func TestProcessServiceImpl_Deploy_EmptyProcessKey(t *testing.T) {
	s := newProcessServiceImplForTest()
	_, err := s.Deploy(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, &model.WfProcess{
		Name:     "Test",
		TenantID: "t1",
	}, false)
	if err == nil {
		t.Error("expected error for empty processKey")
	}
}

func TestProcessServiceImpl_Deploy_EmptyTenantID(t *testing.T) {
	s := newProcessServiceImplForTest()
	_, err := s.Deploy(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, &model.WfProcess{
		Name:       "Test",
		ProcessKey: "key1",
	}, false)
	if err == nil {
		t.Error("expected error for empty tenantID")
	}
}

func TestProcessServiceImpl_Deploy_NilDAO_Panics(t *testing.T) {
	s := &ProcessServiceImpl{}
	expectPanic(t, "Deploy", func() {
		s.Deploy(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, &model.WfProcess{
			Name:           "Test",
			ProcessKey:     "key1",
			TenantID:       "t1",
			DefinitionJSON: `{"ruleChain":{"id":"key1","name":"Test"}}`,
		}, false)
	})
}

func TestProcessServiceImpl_Deploy_Duplicate_NilDAO_Panics(t *testing.T) {
	s := &ProcessServiceImpl{}
	expectPanic(t, "Deploy(duplicate)", func() {
		s.Deploy(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, &model.WfProcess{
			Name:           "Test",
			ProcessKey:     "key1",
			TenantID:       "t1",
			DefinitionJSON: `{"ruleChain":{"id":"key1","name":"Test"}}`,
		}, true)
	})
}

// TestProcessServiceImpl_Create_InvalidDSL 校验 C1：畸形/空 DSL 在进入 DAO 前
// 即被拒绝（ErrValidation），不落库。
func TestProcessServiceImpl_Create_InvalidDSL(t *testing.T) {
	s := newProcessServiceImplForTest()
	for name, dsl := range map[string]string{
		"empty":       "",
		"not json":    "not-json",
		"broken json": `{"ruleChain":`,
	} {
		_, err := s.Create(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, &model.WfProcess{
			Name:           "Test",
			ProcessKey:     "key1",
			TenantID:       "t1",
			DefinitionJSON: dsl,
		}, true)
		if err == nil {
			t.Errorf("%s: expected error for invalid DSL", name)
			continue
		}
		if !errors.Is(err, ErrValidation) {
			t.Errorf("%s: expected ErrValidation, got %v", name, err)
		}
	}
}

func TestProcessServiceImpl_Create_EmptyName(t *testing.T) {
	s := newProcessServiceImplForTest()
	_, err := s.Create(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, &model.WfProcess{
		ProcessKey: "key1",
		TenantID:   "t1",
	}, false)
	if err == nil {
		t.Error("expected error for empty name")
	}
}

func TestProcessServiceImpl_Create_EmptyProcessKey(t *testing.T) {
	s := newProcessServiceImplForTest()
	_, err := s.Create(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, &model.WfProcess{
		Name:     "Test",
		TenantID: "t1",
	}, false)
	if err == nil {
		t.Error("expected error for empty processKey")
	}
}

func TestProcessServiceImpl_Create_EmptyTenantID(t *testing.T) {
	s := newProcessServiceImplForTest()
	_, err := s.Create(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, &model.WfProcess{
		Name:       "Test",
		ProcessKey: "key1",
	}, false)
	if err == nil {
		t.Error("expected error for empty tenantID")
	}
}

func TestProcessServiceImpl_Update_EmptyID(t *testing.T) {
	s := newProcessServiceImplForTest()
	err := s.Update(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, &model.WfProcess{Name: "Test"})
	if err == nil {
		t.Error("expected error for empty ID")
	}
}

func TestProcessServiceImpl_Update_NilDAO_Panics(t *testing.T) {
	s := &ProcessServiceImpl{}
	expectPanic(t, "Update", func() {
		s.Update(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, &model.WfProcess{ID: "proc-1", Name: "Test"})
	})
}

func TestProcessServiceImpl_Activate_EmptyID(t *testing.T) {
	s := newProcessServiceImplForTest()
	_, err := s.Activate(context.Background(), Actor{UserID: "tester", TenantID: "tenant1"}, "")
	if err == nil {
		t.Error("expected error for empty processID")
	}
}

func TestProcessServiceImpl_GetVersions_NilDAO_Panics(t *testing.T) {
	s := &ProcessServiceImpl{}
	expectPanic(t, "GetVersions", func() {
		s.GetVersions(context.Background(), "t1", "key1", 1, 10)
	})
}

func TestProcessServiceImpl_List_NilDAO_Panics(t *testing.T) {
	s := &ProcessServiceImpl{}
	expectPanic(t, "List", func() {
		s.List(context.Background(), SystemActor(), nil)
	})
}

func TestProcessServiceImpl_Delete_NilInstanceDAO_Panics(t *testing.T) {
	s := &ProcessServiceImpl{}
	expectPanic(t, "Delete", func() {
		s.Delete(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, "proc-1")
	})
}

func TestProcessServiceImpl_UpdateStatus_NilDAO_Panics(t *testing.T) {
	s := &ProcessServiceImpl{}
	expectPanic(t, "UpdateStatus", func() {
		s.UpdateStatus(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, "proc-1", "active")
	})
}

func TestProcessServiceImpl_UpdateStatusByKey_NilDAO_Panics(t *testing.T) {
	s := &ProcessServiceImpl{}
	expectPanic(t, "UpdateStatusByKey", func() {
		s.UpdateStatusByKey(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, "key1", "active")
	})
}

func TestProcessServiceImpl_Retire_NilDAO_Panics(t *testing.T) {
	s := &ProcessServiceImpl{}
	expectPanic(t, "Retire", func() {
		s.Retire(context.Background(), Actor{UserID: "tester", TenantID: "t1"}, "proc-1")
	})
}

func TestNewProcessService_NilEngine(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from nil engine")
		}
	}()
	NewProcessService(nil)
}

func TestProcessServiceImpl_ImplementsInterface(t *testing.T) {
	var _ ProcessService = (*ProcessServiceImpl)(nil)
}

// Parameter validation for the simple pass-through methods.

func TestProcessService_Get_EmptyID(t *testing.T) {
	s := newProcessServiceForValidation()
	_, err := s.Get(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty ID")
	}
}

func TestProcessService_GetByKey_EmptyKey(t *testing.T) {
	s := newProcessServiceForValidation()
	_, err := s.GetByKey(context.Background(), "tenant1", "")
	if err == nil {
		t.Error("expected error for empty processKey")
	}
}

func TestProcessService_GetByKeyAndVersion_EmptyKey(t *testing.T) {
	s := newProcessServiceForValidation()
	_, err := s.GetByKeyAndVersion(context.Background(), "tenant1", "", 1)
	if err == nil {
		t.Error("expected error for empty processKey")
	}
}

// version<=0（不传版本）按契约回退 GetLatestByKey 取最新版本，属合法输入，
// 不再报错——该路径需真实 DAO（查 version 最大行），归 DAO 层测试覆盖。

func TestProcessService_GetVersions_EmptyKey(t *testing.T) {
	s := newProcessServiceForValidation()
	_, _, err := s.GetVersions(context.Background(), "tenant1", "", 1, 10)
	if err == nil {
		t.Error("expected error for empty processKey")
	}
}

func TestProcessService_Delete_EmptyID(t *testing.T) {
	s := newProcessServiceForValidation()
	err := s.Delete(context.Background(), Actor{UserID: "tester", TenantID: "tenant1"}, "")
	if err == nil {
		t.Error("expected error for empty processID")
	}
}

func TestProcessService_Retire_EmptyID(t *testing.T) {
	s := newProcessServiceForValidation()
	err := s.Retire(context.Background(), Actor{UserID: "tester", TenantID: "tenant1"}, "")
	if err == nil {
		t.Error("expected error for empty processID")
	}
}

func TestProcessService_UpdateStatus_EmptyID(t *testing.T) {
	s := newProcessServiceForValidation()
	err := s.UpdateStatus(context.Background(), Actor{UserID: "tester", TenantID: "tenant1"}, "", enums.ProcessStatusActive)
	if err == nil {
		t.Error("expected error for empty processID")
	}
}

func TestProcessService_UpdateStatusByKey_EmptyKey(t *testing.T) {
	s := newProcessServiceForValidation()
	err := s.UpdateStatusByKey(context.Background(), Actor{UserID: "tester", TenantID: "tenant1"}, "", enums.ProcessStatusActive)
	if err == nil {
		t.Error("expected error for empty processKey")
	}
}

func TestProcessService_Update_ImplementsInterface(t *testing.T) {
	var _ ProcessService = (*ProcessServiceImpl)(nil)
}

func TestProcessStatus_AllValues(t *testing.T) {
	tests := []struct {
		status  enums.ProcessStatus
		isValid bool
	}{
		{enums.ProcessStatusActive, true},
		{enums.ProcessStatusRetired, true},
		{enums.ProcessStatusDraft, true}, // Create 落库为 draft，必须是合法状态
		{"invalid", false},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			got := enums.IsValidProcessStatus(tt.status)
			if got != tt.isValid {
				t.Errorf("IsValidProcessStatus(%q) = %v, want %v", tt.status, got, tt.isValid)
			}
		})
	}
}

func TestGetAllProcessStatus(t *testing.T) {
	statuses := enums.GetAllProcessStatus()
	if len(statuses) < 2 {
		t.Errorf("expected at least 2 statuses, got %d", len(statuses))
	}
}
