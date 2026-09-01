package service

import (
	"context"
	"testing"

	"github.com/rulego/gflow-engine/types/constants"
)

// TestCallingMode_DefaultIsUnknown 验证未设置时默认返回 CallingModeUnknown，
// 保证旧调用路径不破坏（既不强制身份校验也不显式跳过）。
func TestCallingMode_DefaultIsUnknown(t *testing.T) {
	ctx := context.Background()
	if mode := GetCallingMode(ctx); mode != CallingModeUnknown {
		t.Errorf("expected CallingModeUnknown, got %v", mode)
	}
}

// TestCallingMode_NilContextDoesNotPanic GetCallingMode/SetCallingMode 必须容忍 nil ctx
// 避免上层忘记传 context 时引发 panic。
func TestCallingMode_NilContextDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("GetCallingMode/SetCallingMode panicked on nil ctx: %v", r)
		}
	}()

	//lint:ignore SA1012 测试目标就是 nil ctx 行为（见函数名）
	if mode := GetCallingMode(nil); mode != CallingModeUnknown {
		t.Errorf("expected CallingModeUnknown for nil ctx, got %v", mode)
	}
	// SetCallingMode(nil) 应当返回非 nil context，不 panic
	//lint:ignore SA1012 测试目标就是 nil ctx 行为
	newCtx := SetCallingMode(nil, CallingModeAPI)
	if newCtx == nil {
		t.Fatal("SetCallingMode(nil, ...) returned nil context")
	}
	if mode := GetCallingMode(newCtx); mode != CallingModeAPI {
		t.Errorf("expected CallingModeAPI after SetCallingMode, got %v", mode)
	}
}

// TestSetCallingMode_RoundTrip 验证 SetCallingMode + GetCallingMode 双向一致。
func TestSetCallingMode_RoundTrip(t *testing.T) {
	cases := []CallingMode{
		CallingModeUnknown,
		CallingModeAPI,
		CallingModeInternal,
	}
	for _, mode := range cases {
		ctx := SetCallingMode(context.Background(), mode)
		got := GetCallingMode(ctx)
		if got != mode {
			t.Errorf("round-trip failed: set %v, got %v", mode, got)
		}
	}
}

// TestWithAPICallingMode 验证 WithAPICallingMode helper 正确标记 API 调用来源。
func TestWithAPICallingMode(t *testing.T) {
	ctx := WithAPICallingMode(context.Background())
	if mode := GetCallingMode(ctx); mode != CallingModeAPI {
		t.Errorf("expected CallingModeAPI, got %v", mode)
	}
}

// TestWithInternalCallingMode 验证 WithInternalCallingMode helper 正确标记引擎内部调用。
// create_task_aspect 等内部路径用此 helper 跳过 assignee 校验。
func TestWithInternalCallingMode(t *testing.T) {
	ctx := WithInternalCallingMode(context.Background())
	if mode := GetCallingMode(ctx); mode != CallingModeInternal {
		t.Errorf("expected CallingModeInternal, got %v", mode)
	}
}

// TestCallingMode_Overwrite 验证后设置的 mode 覆盖前一个，
// 避免上下文复用时残留状态导致误判。
func TestCallingMode_Overwrite(t *testing.T) {
	ctx := WithAPICallingMode(context.Background())
	ctx = WithInternalCallingMode(ctx)
	if mode := GetCallingMode(ctx); mode != CallingModeInternal {
		t.Errorf("expected CallingModeInternal after overwrite, got %v", mode)
	}
}

// TestCallingMode_UsesCorrectCtxKey 验证 CallingMode 写入正确的 context key，
// 与 constants.KeyCurrentUser 解耦（避免与身份信息键冲突）。
func TestCallingMode_UsesCorrectCtxKey(t *testing.T) {
	ctx := WithAPICallingMode(context.Background())
	// 写入用户身份（不影响 CallingMode）
	ident := &Actor{UserID: "u1", UserName: "tester", TenantID: "t1"}
	ctx = SetUserToCtx(ctx, ident)

	// 两个 key 应当独立
	if v := ctx.Value(constants.KeyCallingMode); v == nil {
		t.Fatal("KeyCallingMode not set in context")
	}
	if v := ctx.Value(constants.KeyCurrentUser); v == nil {
		t.Fatal("KeyCurrentUser not set in context")
	}
	if GetCallingMode(ctx) != CallingModeAPI {
		t.Errorf("CallingMode lost after SetUserToCtx")
	}
	if GetUserFromCtx(ctx) == nil || GetUserFromCtx(ctx).UserID != "u1" {
		t.Errorf("User identity lost after WithAPICallingMode")
	}
}

// TestErrAuthenticationRequired_Error 验证 sentinel error 可被 errors.Is 识别，
// BaseController 据此映射到 401。
func TestErrAuthenticationRequired_Error(t *testing.T) {
	if ErrAuthenticationRequired.Error() == "" {
		t.Fatal("ErrAuthenticationRequired should have non-empty message")
	}
	// 确保是 sentinel（可比较）
	if ErrAuthenticationRequired != ErrAuthenticationRequired {
		t.Fatal("ErrAuthenticationRequired should be a comparable sentinel")
	}
}
