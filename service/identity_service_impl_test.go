package service

import (
	"context"
	"testing"
)

// ---------------------------------------------------------------------------
// IdentityServiceImpl tests (using mock data)
// ---------------------------------------------------------------------------

func TestNewIdentityService(t *testing.T) {
	svc := NewIdentityService()
	if svc == nil {
		t.Fatal("NewIdentityService returned nil")
	}
}

func TestIdentityService_GetUserIDsByRoleID(t *testing.T) {
	svc := NewIdentityService()
	ctx := context.Background()

	tests := []struct {
		name     string
		tenantID string
		roleID   string
		wantLen  int
		wantErr  bool
	}{
		{"valid role", "tenant1", "role001", 1, false},
		{"role002", "tenant1", "role002", 1, false},
		{"role003", "tenant1", "role003", 1, false},
		{"nonexistent role", "tenant1", "role999", 0, false},
		{"empty roleID", "tenant1", "", 0, true},
		{"wrong tenant", "tenant2", "role001", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids, err := svc.GetUserIDsByRoleID(ctx, tt.tenantID, tt.roleID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(ids) != tt.wantLen {
				t.Errorf("len(ids) = %d, want %d, ids = %v", len(ids), tt.wantLen, ids)
			}
		})
	}
}

func TestIdentityService_GetDepartmentManagerUserID(t *testing.T) {
	svc := NewIdentityService()
	ctx := context.Background()

	tests := []struct {
		name         string
		tenantID     string
		departmentID string
		wantManager  string
		wantErr      bool
	}{
		{"dept001 manager", "tenant1", "dept001", "manager001", false},
		{"dept002 manager", "tenant1", "dept002", "user004", false},
		{"nonexistent dept", "tenant1", "dept999", "", false},
		{"empty deptID", "tenant1", "", "", true},
		{"wrong tenant", "tenant2", "dept001", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			managerID, err := svc.GetDepartmentManagerUserID(ctx, tt.tenantID, tt.departmentID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if managerID != tt.wantManager {
				t.Errorf("managerID = %q, want %q", managerID, tt.wantManager)
			}
		})
	}
}

func TestIdentityService_GetUserIDsByGroupID(t *testing.T) {
	svc := NewIdentityService()
	ctx := context.Background()

	tests := []struct {
		name     string
		tenantID string
		groupID  string
		wantLen  int
		wantErr  bool
	}{
		{"dept001 users", "tenant1", "dept001", 4, false}, // user001, user002, user003, manager001
		{"dept002 users", "tenant1", "dept002", 2, false}, // user004, user005
		{"role001 users", "tenant1", "role001", 1, false},
		{"nonexistent group", "tenant1", "group999", 0, false},
		{"empty groupID", "tenant1", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids, err := svc.GetUserIDsByGroupID(ctx, tt.tenantID, tt.groupID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(ids) != tt.wantLen {
				t.Errorf("len(ids) = %d, want %d, ids = %v", len(ids), tt.wantLen, ids)
			}
		})
	}
}

func TestIdentityService_GetUserIDsByDepartmentID(t *testing.T) {
	svc := NewIdentityService()
	ctx := context.Background()

	tests := []struct {
		name     string
		tenantID string
		deptID   string
		wantLen  int
		wantErr  bool
	}{
		{"dept001", "tenant1", "dept001", 4, false}, // user001, user002, user003, manager001
		{"dept002", "tenant1", "dept002", 2, false}, // user004, user005
		{"nonexistent", "tenant1", "dept999", 0, false},
		{"empty deptID", "tenant1", "", 0, true},
		{"wrong tenant", "tenant2", "dept001", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids, err := svc.GetUserIDsByDepartmentID(ctx, tt.tenantID, tt.deptID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(ids) != tt.wantLen {
				t.Errorf("len(ids) = %d, want %d, ids = %v", len(ids), tt.wantLen, ids)
			}
		})
	}
}

func TestIdentityService_GetUserManagerID(t *testing.T) {
	svc := NewIdentityService()
	ctx := context.Background()

	tests := []struct {
		name        string
		tenantID    string
		userID      string
		wantManager string
		wantErr     bool
	}{
		{"user001 manager", "tenant1", "user001", "manager001", false},
		{"user002 manager", "tenant1", "user002", "manager001", false},
		{"manager001 manager", "tenant1", "manager001", "director001", false},
		{"no manager", "tenant1", "user004", "", false}, // user004 has no manager in test data
		{"nonexistent user", "tenant1", "user999", "", false},
		{"empty userID", "tenant1", "", "", true},
		{"wrong tenant", "tenant2", "user001", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			managerID, err := svc.GetUserManagerID(ctx, tt.tenantID, tt.userID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if managerID != tt.wantManager {
				t.Errorf("managerID = %q, want %q", managerID, tt.wantManager)
			}
		})
	}
}

func TestIdentityService_GetUserManagerHierarchy(t *testing.T) {
	svc := NewIdentityService()
	ctx := context.Background()

	tests := []struct {
		name     string
		tenantID string
		userID   string
		levels   int
		wantLen  int
		wantErr  bool
	}{
		{"user001 1 level", "tenant1", "user001", 1, 1, false},          // -> manager001
		{"user001 2 levels", "tenant1", "user001", 2, 2, false},         // -> manager001 -> director001
		{"user001 all levels", "tenant1", "user001", 0, 2, false},       // -> manager001 -> director001
		{"manager001 1 level", "tenant1", "manager001", 1, 1, false},    // -> director001
		{"manager001 all levels", "tenant1", "manager001", 0, 1, false}, // -> director001 -> no more
		{"no manager", "tenant1", "user004", 0, 0, false},
		{"empty userID", "tenant1", "", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids, err := svc.GetUserManagerHierarchy(ctx, tt.tenantID, tt.userID, tt.levels)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(ids) != tt.wantLen {
				t.Errorf("len(ids) = %d, want %d, ids = %v", len(ids), tt.wantLen, ids)
			}
		})
	}
}

func TestIdentityService_GetUserDepartmentID(t *testing.T) {
	svc := NewIdentityService()
	ctx := context.Background()

	tests := []struct {
		name     string
		tenantID string
		userID   string
		wantDept string
		wantErr  bool
	}{
		{"user001", "tenant1", "user001", "dept001", false},
		{"user004", "tenant1", "user004", "dept002", false},
		{"manager001", "tenant1", "manager001", "dept001", false},
		{"nonexistent user", "tenant1", "user999", "", false},
		{"empty userID", "tenant1", "", "", true},
		{"wrong tenant", "tenant2", "user001", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deptID, err := svc.GetUserDepartmentID(ctx, tt.tenantID, tt.userID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if deptID != tt.wantDept {
				t.Errorf("deptID = %q, want %q", deptID, tt.wantDept)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// IdentityServiceImpl mock extension tests
// ---------------------------------------------------------------------------

func TestIdentityService_AddMockUser(t *testing.T) {
	svc := NewIdentityService().(*IdentityServiceImpl)
	svc.AddMockUser(&User{
		ID:       "newuser",
		TenantID: "tenant1",
	})
	ctx := context.Background()
	deptID, err := svc.GetUserDepartmentID(ctx, "tenant1", "newuser")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deptID != "" {
		t.Errorf("expected empty dept for new user, got %q", deptID)
	}
}

func TestIdentityService_AddMockRoleUsers(t *testing.T) {
	svc := NewIdentityService().(*IdentityServiceImpl)
	svc.AddMockRoleUsers("newrole", []string{"user001", "user002"})
	ctx := context.Background()
	ids, err := svc.GetUserIDsByRoleID(ctx, "tenant1", "newrole")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 2 {
		t.Errorf("len = %d, want 2, ids = %v", len(ids), ids)
	}
}

func TestIdentityService_AddMockUserManager(t *testing.T) {
	svc := NewIdentityService().(*IdentityServiceImpl)
	svc.AddMockUserManager("user004", "manager001")
	ctx := context.Background()
	managerID, err := svc.GetUserManagerID(ctx, "tenant1", "user004")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if managerID != "manager001" {
		t.Errorf("managerID = %q, want 'manager001'", managerID)
	}
}

// ---------------------------------------------------------------------------
// GetUserFromCtx / SetUserToCtx tests (extended)
// ---------------------------------------------------------------------------

func TestGetUserFromCtx_NilContext(t *testing.T) {
	ident := GetUserFromCtx(context.Background())
	if ident != nil {
		t.Error("expected nil for empty context")
	}
}

func TestSetAndGetUserFromCtx_Extended(t *testing.T) {
	user := &Actor{
		UserName: "testuser-ext",
		UserID:   "uid-ext",
		TenantID: "tenant-ext",
	}
	ctx := SetUserToCtx(context.Background(), user)
	got := GetUserFromCtx(ctx)
	if got == nil {
		t.Fatal("expected non-nil")
	}
	if got.UserName != "testuser-ext" {
		t.Errorf("UserName = %q, want 'testuser-ext'", got.UserName)
	}
	if got.UserID != "uid-ext" {
		t.Errorf("UserId = %q, want 'uid-ext'", got.UserID)
	}
	if got.TenantID != "tenant-ext" {
		t.Errorf("TenantId = %q, want 'tenant-ext'", got.TenantID)
	}
}

func TestGetUserFromCtx_WrongTypeInContext(t *testing.T) {
	// 故意用裸 string key + 错误值类型：验证 GetUserFromCtx 对异质 key/值不 panic 且返回 nil
	//lint:ignore SA1012,SA1029 测试目标即错误用法
	ctx := context.WithValue(context.Background(), "currentUser", "not-an-identity")
	ident := GetUserFromCtx(ctx)
	if ident != nil {
		t.Error("expected nil for wrong type")
	}
}

// ---------------------------------------------------------------------------
// BaseService tests
// ---------------------------------------------------------------------------

func TestBaseService_GetUsernameFromCtx_Empty(t *testing.T) {
	svc := &BaseService{}
	if name := svc.GetUsernameFromCtx(context.Background()); name != "" {
		t.Errorf("expected empty, got %q", name)
	}
}

func TestBaseService_GetUsernameFromCtx_WithUser(t *testing.T) {
	svc := &BaseService{}
	user := &Actor{UserName: "alice", UserID: "u1", TenantID: "t1"}
	ctx := SetUserToCtx(context.Background(), user)
	if name := svc.GetUsernameFromCtx(ctx); name != "alice" {
		t.Errorf("name = %q, want 'alice'", name)
	}
}

func TestBaseService_GetUserIDFromCtx_Empty(t *testing.T) {
	svc := &BaseService{}
	if id := svc.GetUserIDFromCtx(context.Background()); id != "" {
		t.Errorf("expected empty, got %q", id)
	}
}

func TestBaseService_GetTenantIDFromCtx_Empty(t *testing.T) {
	svc := &BaseService{}
	if id := svc.GetTenantIDFromCtx(context.Background()); id != "" {
		t.Errorf("expected empty, got %q", id)
	}
}

func TestBaseService_GetUserFromCtx(t *testing.T) {
	svc := &BaseService{}
	user := &Actor{UserName: "bob", UserID: "u2", TenantID: "t2"}
	ctx := SetUserToCtx(context.Background(), user)
	got := svc.GetUserFromCtx(ctx)
	if got == nil || got.UserName != "bob" {
		t.Error("GetUserFromCtx mismatch")
	}
}
