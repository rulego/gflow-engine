package service

import (
	"context"
	"time"
)

// TaskEventType 任务事件类型
type TaskEventType string

const (
	TaskEventAssigned         TaskEventType = "assigned"         // 任务分配（userTask 创建/顺序流转下一人/加签）
	TaskEventForwarded        TaskEventType = "forwarded"        // 转办/委托
	TaskEventApproved         TaskEventType = "approved"         // 审批通过（通知发起人进度）
	TaskEventRejected         TaskEventType = "rejected"         // 审批驳回（terminate 与 jump 回退路径均派发）
	TaskEventTerminated       TaskEventType = "terminated"       // 流程终止（Source 区分来源）
	TaskEventCompleted        TaskEventType = "completed"        // 流程完成
	TaskEventClaimed          TaskEventType = "claimed"          // 候选任务被认领（通知其他候选成员）
	TaskEventUnclaimed        TaskEventType = "unclaimed"        // 任务取消认领（回到候选池）
	TaskEventCandidateCreated TaskEventType = "candidateCreated" // 候选任务创建（通知候选成员有新待认领）
	TaskEventSuspended        TaskEventType = "suspended"        // 实例挂起
	TaskEventActivated        TaskEventType = "activated"        // 实例恢复（含草稿激活）
	TaskEventWithdrawn        TaskEventType = "withdrawn"        // 发起人撤回实例
	TaskEventStarted          TaskEventType = "started"          // 流程实例发起
	TaskEventReturned         TaskEventType = "returned"         // 任务退回到指定节点
	TaskEventAddSign          TaskEventType = "addSign"          // 加签（Reason 携带原因）
	TaskEventReduceSign       TaskEventType = "reduceSign"       // 减签（Reason 携带原因）
	TaskEventResolved         TaskEventType = "resolved"         // 委派归还（任务回到原 owner）
)

// 事件来源（TaskEvent.Source），用于区分同名动作的触发方。
const (
	EventSourceAPI      = "api"      // 上层 API 直接调用
	EventSourceWithdraw = "withdraw" // 撤回内部级联（终止/事件链）
	EventSourceReject   = "reject"   // 驳回策略级联
	EventSourceInternal = "internal" // 引擎内部驱动（无外部操作人）
)

// ctxKeyEventSource 事件来源标记的 context key。
type ctxKeyEventSource struct{}

// WithEventSource 在 ctx 上标记事件来源，供派发点写入 TaskEvent.Source。
// 引擎内部级联调用（撤回终止、驳回终止）前包装，避免扩展服务接口签名。
func WithEventSource(ctx context.Context, source string) context.Context {
	return context.WithValue(ctx, ctxKeyEventSource{}, source)
}

// EventSourceFromCtx 读取事件来源标记，缺省 api。
func EventSourceFromCtx(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyEventSource{}).(string); ok && v != "" {
		return v
	}
	return EventSourceAPI
}

// TaskEvent 任务事件载荷
type TaskEvent struct {
	Type         TaskEventType
	EventID      string    // 事件唯一ID（DispatchTaskEvent 统一填充，上层幂等/追踪用）
	TaskID       string    // 任务ID
	TaskDefKey   string    // 任务定义节点 key（设计期稳定标识；实例级事件为空）
	ParentTaskID string    // 父任务ID（会签子任务 assigned 时携带；空表示无父任务）
	InstanceID   string    // 流程实例ID
	ProcessID    string    // 流程定义ID
	TenantID     string    // 租户ID
	TaskName     string    // 任务名称
	ProcessName  string    // 流程名称（listener 层按需填充）
	ToUsers      []string  // 接收通知的用户；assigned/candidateCreated 为被分配人/候选人群
	FromUser     string    // 触发操作的用户ID；系统驱动为空
	Reason       string    // 驳回/终止/撤回原因
	Source       string    // 事件来源（EventSource*）
	Timestamp    time.Time // 事件发生时间
}

// TaskEventListener 任务事件监听器函数类型
// 上层应用（如 gflow）通过 Builder.SetTaskEventListener 注册。
// listener 内部应做去重、写通知表、推 WebSocket 等操作。
// 所有事件均异步派发：listener panic 被 recover，慢 IO 不阻塞流程主流程。
type TaskEventListener func(ctx context.Context, evt TaskEvent)
