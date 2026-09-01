package service

import (
	"context"
	"errors"

	"github.com/rulego/gflow-engine/types/constants"
)

// CallingMode 标识当前任务操作的调用来源
// 用于区分 API 入口与引擎内部调用（aspect / 节点回调等），
// 决定是否需要强制身份校验。
type CallingMode int

const (
	// CallingModeUnknown 未标记调用模式（视为非 API 入口，不强制身份校验；
	// CompleteWithApproval 入口会把它升级为 API）
	CallingModeUnknown CallingMode = iota
	// CallingModeAPI 来自 HTTP 控制器入口，必须强制身份校验
	CallingModeAPI
	// CallingModeInternal 来自引擎 aspect / 节点回调等内部调用，跳过身份校验
	CallingModeInternal
)

// ErrAuthenticationRequired 当 API 入口缺少身份信息时返回。
// 防止 CompleteWithApproval 在 GetUserFromCtx==nil 时静默跳过 assignee
// 校验——任何忘写 BuildBpmContext 的入口都能借此完成任意任务。
var ErrAuthenticationRequired = errors.New("authentication required: calling mode is API but no user in context")

// GetCallingMode 从上下文读取调用模式
func GetCallingMode(ctx context.Context) CallingMode {
	if ctx == nil {
		return CallingModeUnknown
	}
	v := ctx.Value(constants.KeyCallingMode)
	if v == nil {
		return CallingModeUnknown
	}
	if mode, ok := v.(CallingMode); ok {
		return mode
	}
	return CallingModeUnknown
}

// SetCallingMode 向上下文写入调用模式
func SetCallingMode(ctx context.Context, mode CallingMode) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, constants.KeyCallingMode, mode)
}

// WithAPICallingMode 标记上下文为 API 入口（控制器调用时使用）
func WithAPICallingMode(ctx context.Context) context.Context {
	return SetCallingMode(ctx, CallingModeAPI)
}

// WithInternalCallingMode 标记上下文为引擎内部调用（aspect / 节点回调时使用）
func WithInternalCallingMode(ctx context.Context) context.Context {
	return SetCallingMode(ctx, CallingModeInternal)
}

// forceAPICallingModeForRealUser 公共完成类入口的第二重信号。
//
// CallingModeInternal 会跳过 assignee/租户强校验（引擎 aspect 推进依赖此语义），
// 而 WithInternalCallingMode 是导出 API——宿主若把内部标记过的 ctx 误用进用户请求
// 入口（如中间件串联时复用了带标记的 ctx），强校验就被整体旁路。这里在公共入口
// 做防御：内部 ctx 携带【真实用户身份】（UserID 非空且非 SystemActor）时降级为
// API 模式，恢复强校验；纯系统上下文（无用户或 SystemActor）保持内部模式不变
// （aspect 推进、定时巡检、跨服务级联都依赖该语义）。
//
// 仅在 service 包内的公共完成类入口（CompleteWithApproval/Complete/Approve/Reject）
// 调用；bindActor 之后调用。
func forceAPICallingModeForRealUser(ctx context.Context) context.Context {
	if GetCallingMode(ctx) != CallingModeInternal {
		return ctx
	}
	if u := GetUserFromCtx(ctx); u != nil && u.UserID != "" && !IsSystemActor(u) {
		return WithAPICallingMode(ctx)
	}
	return ctx
}
