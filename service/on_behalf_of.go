package service

import "context"

// ctxKeyOnBehalfOf 代审被代人标记的 context key。
type ctxKeyOnBehalfOf struct{}

// WithOnBehalfOf 标记本次调用的代审被代人。引擎内仅 ProxyAudit 入口写入，
// 宿主不应调用；completeWithApprovalInternal 的代审分支以 proxyAuditFromCtx
// 双信号（标记 + proxy 事件来源）判定，只挂标记不足以走代审通道。
func WithOnBehalfOf(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, ctxKeyOnBehalfOf{}, userID)
}

// OnBehalfOfFromCtx 读取代审被代人标记，缺省空串（非代审路径）。
func OnBehalfOfFromCtx(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyOnBehalfOf{}).(string); ok {
		return v
	}
	return ""
}

// proxyAuditFromCtx 判定当前调用是否走代审通道：被代人标记与 proxy 事件来源
// 双信号一致才认，防止宿主只挂标记就把普通 approve 变成代审（绕过流程开关与
// 宿主权限位）。
func proxyAuditFromCtx(ctx context.Context) bool {
	return OnBehalfOfFromCtx(ctx) != "" && EventSourceFromCtx(ctx) == EventSourceProxy
}
