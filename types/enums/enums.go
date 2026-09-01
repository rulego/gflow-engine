package enums

type ProcessStatus string

const (
	// ProcessStatusActive 生效
	ProcessStatusActive ProcessStatus = "active"
	// ProcessStatusRetired 停用
	ProcessStatusRetired ProcessStatus = "retired"
	// ProcessStatusDraft 草稿
	ProcessStatusDraft ProcessStatus = "draft"
)

// GetAllProcessStatus 获取所有流程状态
func GetAllProcessStatus() []ProcessStatus {
	return []ProcessStatus{
		ProcessStatusActive,
		ProcessStatusRetired,
		ProcessStatusDraft,
	}
}

// IsValidProcessStatus 验证流程状态是否有效
func IsValidProcessStatus(status ProcessStatus) bool {
	for _, d := range GetAllProcessStatus() {
		if d == status {
			return true
		}
	}
	return false
}

// TaskStatus 任务状态枚举
type TaskStatus string

const (
	TaskStatusCreated  TaskStatus = "created"  // 已创建（数据库默认状态）
	TaskStatusAssigned TaskStatus = "assigned" // 已分配（签收/转办后）
	// TaskStatusWaiting 待激活（当前版本引擎不会产生该状态，保留）
	TaskStatusWaiting   TaskStatus = "waiting"
	TaskStatusPending   TaskStatus = "pending"   // 待领取
	TaskStatusActive    TaskStatus = "active"    // 处理中
	TaskStatusDelegated TaskStatus = "delegated" // 已委托
	TaskStatusCompleted TaskStatus = "completed" // 完成
	TaskStatusReturned  TaskStatus = "returned"  // 已退回
	// TaskStatusWithdrawn 已撤回：发起人撤回已提交的申请（Withdraw/WithdrawByInstance）
	TaskStatusWithdrawn  TaskStatus = "withdrawn"
	TaskStatusSuspended  TaskStatus = "suspended"  // 挂起
	TaskStatusTerminated TaskStatus = "terminated" // 终止
)

// InstanceStatus 流程实例状态枚举
type InstanceStatus string

const (
	// InstanceStatusDraft 草稿
	InstanceStatusDraft InstanceStatus = "draft"
	// InstanceStatusActive 运行中
	InstanceStatusActive InstanceStatus = "active"
	// InstanceStatusCompleted 已完成
	InstanceStatusCompleted InstanceStatus = "completed"
	// InstanceStatusSuspended 已挂起
	InstanceStatusSuspended InstanceStatus = "suspended"
	// InstanceStatusTerminated 已终止
	InstanceStatusTerminated InstanceStatus = "terminated"
	// InstanceStatusCancelled 已取消
	InstanceStatusCancelled InstanceStatus = "cancelled"
	// InstanceStatusFailed 执行失败状态
	InstanceStatusFailed InstanceStatus = "failed"
	// InstanceStatusDeleted 已删除：实例被 DeleteProcessInstance 删除后归档到
	// wf_hi_instance 时标记的状态（活表不落该值）
	InstanceStatusDeleted InstanceStatus = "deleted"
)

// EntityType wf_task_assignee.entity_type 候选实体类型
type EntityType string

const (
	// EntityTypePerson 个人（直接以用户 ID 指定候选）
	EntityTypePerson EntityType = "person"
	// EntityTypeRole 角色（经 IdentityService 展开为角色下用户）
	EntityTypeRole EntityType = "role"
	// EntityTypeDepartment 部门（经 IdentityService 展开为部门成员）
	EntityTypeDepartment EntityType = "department"
)

// ApproverStatus 节点审批人状态（dto.NodeApproverDTO.Status 取值）
type ApproverStatus string

const (
	// ApproverStatusApproved 已审批
	ApproverStatusApproved ApproverStatus = "approved"
	// ApproverStatusPending 待审批
	ApproverStatusPending ApproverStatus = "pending"
)

// EndReason 任务结束原因标记（wf_task.end_reason 的引擎内置取值，便于历史检索）
type EndReason string

const (
	// EndReasonCC 抄送任务标记
	EndReasonCC EndReason = "cc"
	// EndReasonClaimedByOther 同节点其他候选任务被他人认领而终止
	EndReasonClaimedByOther EndReason = "claimed_by_other"
	// EndReasonWithdrawn 发起人撤回
	EndReasonWithdrawn EndReason = "withdrawn"
)

// ActionType 操作类型枚举
type ActionType string

const (
	// ActionTypeStart 启动流程
	ActionTypeStart ActionType = "start"
	// ActionTypeApprove 审批通过
	ActionTypeApprove ActionType = "approve"
	// ActionTypeReject 审批拒绝
	ActionTypeReject ActionType = "reject"
	// ActionTypeReturn 退回
	ActionTypeReturn ActionType = "return"
	// ActionTypeDelegate 委托
	ActionTypeDelegate ActionType = "delegate"
	// ActionTypeAddSignBefore 前加签
	ActionTypeAddSignBefore ActionType = "add_sign_before"
	// ActionTypeAddSignAfter 后加签
	ActionTypeAddSignAfter ActionType = "add_sign_after"
	// ActionTypeCountersign 会签
	ActionTypeCountersign ActionType = "countersign"
	// ActionTypeSuspend 挂起
	ActionTypeSuspend ActionType = "suspend"
	// ActionTypeResume 恢复
	ActionTypeResume ActionType = "resume"
	// ActionTypeTerminate 终止
	ActionTypeTerminate ActionType = "terminate"
	// ActionTypeCancel 取消
	ActionTypeCancel ActionType = "cancel"
)

// ApprovalResult 审批结果枚举
type ApprovalResult string

const (
	// ApprovalResultApproved 同意
	ApprovalResultApproved ApprovalResult = "approved"
	// ApprovalResultRejected 拒绝
	ApprovalResultRejected ApprovalResult = "rejected"
	// ApprovalResultReturned 退回
	ApprovalResultReturned ApprovalResult = "returned"
	// ApprovalResultDelegated 委托
	ApprovalResultDelegated ApprovalResult = "delegated"
	// ApprovalResultPending 待处理
	ApprovalResultPending ApprovalResult = "pending"
)

// CountersignType 会签类型枚举
type CountersignType string

const (
	// CountersignTypeAll 全部同意（所有人都需要同意）
	CountersignTypeAll CountersignType = "all"
	// CountersignTypeAny 任意一人同意
	CountersignTypeAny CountersignType = "any"
	// CountersignTypeMajority 多数同意（超过50%）
	CountersignTypeMajority CountersignType = "majority"
	// CountersignTypePercent 按比例同意（自定义百分比）
	CountersignTypePercent CountersignType = "percent"
	// CountersignTypeCount 按数量同意（自定义数量）
	CountersignTypeCount CountersignType = "count"

	// CountersignTypeSequential 顺序会签
	CountersignTypeSequential CountersignType = "sequential"
	// CountersignTypeParallel 并行会签
	CountersignTypeParallel CountersignType = "parallel"
)

// AssigneeType 任务分配类型枚举
type AssigneeType string

const (
	// AssigneeTypeUser 用户
	AssigneeTypeUser AssigneeType = "user"
	// AssigneeTypeRole 角色
	AssigneeTypeRole AssigneeType = "role"
	// AssigneeTypeDepartment 部门
	AssigneeTypeDepartment AssigneeType = "department"
	// AssigneeTypeGroup 用户组
	AssigneeTypeGroup AssigneeType = "group"
	// AssigneeTypeExpression 表达式
	AssigneeTypeExpression AssigneeType = "expression"
)

// CandidateType 审批人员类型枚举
type CandidateType string

const (
	// CandidateTypeUser 指定成员
	CandidateTypeUser CandidateType = "user"
	// CandidateTypeRole 指定角色
	CandidateTypeRole CandidateType = "role"
	// CandidateTypeDirectManager 直接上级
	CandidateTypeDirectManager CandidateType = "direct_manager"
	// CandidateTypeInitiatorSelect 发起人自选
	CandidateTypeInitiatorSelect CandidateType = "initiator_select"
	// CandidateTypeInitiatorSelf 发起人自己
	CandidateTypeInitiatorSelf CandidateType = "initiator_self"
	// CandidateTypeMultiLevelManager 多级上级
	CandidateTypeMultiLevelManager CandidateType = "multi_level_manager"
	// CandidateTypeDept 指定部门（解析部门成员为候选组）
	CandidateTypeDept CandidateType = "dept"
)

// GetAllCandidateTypes 获取所有审批人员类型
func GetAllCandidateTypes() []CandidateType {
	return []CandidateType{
		CandidateTypeUser,
		CandidateTypeRole,
		CandidateTypeDirectManager,
		CandidateTypeInitiatorSelect,
		CandidateTypeInitiatorSelf,
		CandidateTypeMultiLevelManager,
		CandidateTypeDept,
	}
}

// IsValidCandidateType 验证审批人员类型是否有效
func IsValidCandidateType(cType CandidateType) bool {
	for _, c := range GetAllCandidateTypes() {
		if c == cType {
			return true
		}
	}
	return false
}

// ReturnType 退回类型枚举
type ReturnType string

const (
	// ReturnTypePrevious 退回上一步
	ReturnTypePrevious ReturnType = "previous"
	// ReturnTypeStart 退回发起人
	ReturnTypeStart ReturnType = "start"
	// ReturnTypeSpecific 退回指定节点
	ReturnTypeSpecific ReturnType = "specific"
)

// DelegateType 委托类型枚举
type DelegateType string

const (
	// DelegateTypeTask 任务委托
	DelegateTypeTask DelegateType = "task"
	// DelegateTypePermission 权限委托
	DelegateTypePermission DelegateType = "permission"
	// DelegateTypeTemporary 临时委托
	DelegateTypeTemporary DelegateType = "temporary"
	// DelegateTypePermanent 永久委托
	DelegateTypePermanent DelegateType = "permanent"
)

// NotificationType 通知类型枚举
type NotificationType string

const (
	// NotificationTypeTaskAssigned 任务分配通知
	NotificationTypeTaskAssigned NotificationType = "task_assigned"
	// NotificationTypeTaskCompleted 任务完成通知
	NotificationTypeTaskCompleted NotificationType = "task_completed"
	// NotificationTypeTaskOverdue 任务超时通知
	NotificationTypeTaskOverdue NotificationType = "task_overdue"
	// NotificationTypeProcessCompleted 流程完成通知
	NotificationTypeProcessCompleted NotificationType = "process_completed"
	// NotificationTypeProcessRejected 流程拒绝通知
	NotificationTypeProcessRejected NotificationType = "process_rejected"
	// NotificationTypeDelegated 委托通知
	NotificationTypeDelegated NotificationType = "delegated"
	// NotificationTypeCountersign 会签通知
	NotificationTypeCountersign NotificationType = "countersign"
	// NotificationTypeAddSign 加签通知
	NotificationTypeAddSign NotificationType = "add_sign"
)

// ApprovalType 任务审批（投票）类型
type ApprovalType string

const (
	ApprovalTypeSingle      ApprovalType = "single"      // single   单人审批
	ApprovalTypeOr          ApprovalType = "or"          // or       或签（多名审批人，满足任一通过即可）
	ApprovalTypeSequential  ApprovalType = "sequential"  // sequential 按顺序依次审批
	ApprovalTypeVote        ApprovalType = "vote"        // vote     票签（按阈值规则判定通过比例，规则见 dto.CountersignRule）
	ApprovalTypeCountersign ApprovalType = "countersign" // countersign 会签（全员通过）
	// ApprovalTypeSystem 引擎内部使用（系统自动任务）；userTask 节点不接受该配置值。
	ApprovalTypeSystem ApprovalType = "system"
	// ApprovalTypeCC 引擎内部使用（抄送任务）；userTask 节点不接受该配置值。
	ApprovalTypeCC ApprovalType = "cc"
)

// 枚举验证函数

// IsValidTaskStatus 验证任务状态是否有效
func IsValidTaskStatus(status TaskStatus) bool {
	switch status {
	case TaskStatusCreated, TaskStatusAssigned, TaskStatusWaiting, TaskStatusPending,
		TaskStatusActive, TaskStatusDelegated, TaskStatusCompleted, TaskStatusReturned,
		TaskStatusWithdrawn, TaskStatusSuspended, TaskStatusTerminated:
		return true
	default:
		return false
	}
}

// GetAllTaskStatuses 获取所有任务状态
func GetAllTaskStatuses() []TaskStatus {
	return []TaskStatus{
		TaskStatusCreated,
		TaskStatusAssigned,
		TaskStatusWaiting,
		TaskStatusPending,
		TaskStatusActive,
		TaskStatusDelegated,
		TaskStatusCompleted,
		TaskStatusReturned,
		TaskStatusWithdrawn,
		TaskStatusSuspended,
		TaskStatusTerminated,
	}
}

// GetAllInstanceStatuses 获取所有实例状态（含草稿与已取消，cancelled 视为终态）
func GetAllInstanceStatuses() []InstanceStatus {
	return []InstanceStatus{
		InstanceStatusDraft,
		InstanceStatusActive,
		InstanceStatusCompleted,
		InstanceStatusSuspended,
		InstanceStatusTerminated,
		InstanceStatusCancelled,
		InstanceStatusFailed,
		InstanceStatusDeleted,
	}
}

// IsValidInstanceStatus 验证实例状态是否有效
func IsValidInstanceStatus(status InstanceStatus) bool {
	for _, s := range GetAllInstanceStatuses() {
		if s == status {
			return true
		}
	}
	return false
}

// CanTransitionInstanceStatus 检查实例状态是否可以转换
func CanTransitionInstanceStatus(from, to InstanceStatus) bool {
	switch from {
	case InstanceStatusDraft:
		// 草稿：激活开始流转，或丢弃草稿（终止/删除归档）
		return to == InstanceStatusActive || to == InstanceStatusCancelled || to == InstanceStatusDeleted
	case InstanceStatusActive:
		return to == InstanceStatusCompleted || to == InstanceStatusSuspended || to == InstanceStatusTerminated || to == InstanceStatusCancelled || to == InstanceStatusFailed || to == InstanceStatusDeleted
	case InstanceStatusSuspended:
		return to == InstanceStatusActive || to == InstanceStatusTerminated
	case InstanceStatusCompleted, InstanceStatusTerminated, InstanceStatusCancelled, InstanceStatusFailed, InstanceStatusDeleted:
		return false // 终态，不能再转换
	default:
		return false
	}
}

// GetActiveInstanceStatuses 获取活跃的实例状态（非终态）
func GetActiveInstanceStatuses() []InstanceStatus {
	return []InstanceStatus{
		InstanceStatusActive,
		InstanceStatusSuspended,
	}
}

// IsTerminalInstanceStatus 检查是否为终态实例状态
// cancelled 也是终态（见 GetAllInstanceStatuses 注释）；漏判会让 ForceResume
// 之类按终态拒绝的路径放行已取消实例
func IsTerminalInstanceStatus(status InstanceStatus) bool {
	return status == InstanceStatusCompleted || status == InstanceStatusTerminated ||
		status == InstanceStatusCancelled || status == InstanceStatusFailed
}

// GetAllActionTypes 获取所有操作类型
func GetAllActionTypes() []ActionType {
	return []ActionType{
		ActionTypeStart, ActionTypeApprove, ActionTypeReject, ActionTypeReturn,
		ActionTypeDelegate, ActionTypeAddSignBefore, ActionTypeAddSignAfter,
		ActionTypeCountersign, ActionTypeSuspend, ActionTypeResume,
		ActionTypeTerminate, ActionTypeCancel,
	}
}

// IsValidActionType 验证操作类型是否有效
func IsValidActionType(actionType ActionType) bool {
	for _, a := range GetAllActionTypes() {
		if a == actionType {
			return true
		}
	}
	return false
}

// GetTaskActionTypes 获取任务相关的操作类型
func GetTaskActionTypes() []ActionType {
	return []ActionType{
		ActionTypeApprove, ActionTypeReject, ActionTypeReturn,
		ActionTypeDelegate, ActionTypeAddSignBefore, ActionTypeAddSignAfter,
		ActionTypeCountersign,
	}
}

// GetInstanceActionTypes 获取实例相关的操作类型
func GetInstanceActionTypes() []ActionType {
	return []ActionType{
		ActionTypeStart,
		ActionTypeSuspend,
		ActionTypeResume,
		ActionTypeTerminate,
		ActionTypeCancel,
	}
}

// GetAllApprovalResults 获取所有审批结果
func GetAllApprovalResults() []ApprovalResult {
	return []ApprovalResult{
		ApprovalResultApproved,
		ApprovalResultRejected,
		ApprovalResultReturned,
		ApprovalResultDelegated,
		ApprovalResultPending,
	}
}

// IsValidApprovalResult 验证审批结果是否有效
func IsValidApprovalResult(result ApprovalResult) bool {
	for _, r := range GetAllApprovalResults() {
		if r == result {
			return true
		}
	}
	return false
}

// IsPositiveApprovalResult 检查是否为正面审批结果
func IsPositiveApprovalResult(result ApprovalResult) bool {
	return result == ApprovalResultApproved
}

// IsNegativeApprovalResult 检查是否为负面审批结果
func IsNegativeApprovalResult(result ApprovalResult) bool {
	return result == ApprovalResultRejected || result == ApprovalResultReturned
}

// GetAllCountersignTypes 获取所有会签类型
func GetAllCountersignTypes() []CountersignType {
	// 会签规则阈值类型（all/any/majority/percent/count）与执行方式（sequential/parallel）同属一集
	return []CountersignType{
		CountersignTypeAll,
		CountersignTypeAny,
		CountersignTypeMajority,
		CountersignTypePercent,
		CountersignTypeCount,
		CountersignTypeSequential,
		CountersignTypeParallel,
	}
}

// IsValidCountersignType 验证会签类型是否有效
func IsValidCountersignType(cType CountersignType) bool {
	for _, c := range GetAllCountersignTypes() {
		if c == cType {
			return true
		}
	}
	return false
}

// GetAllAssigneeTypes 获取所有受理人类型
func GetAllAssigneeTypes() []AssigneeType {
	return []AssigneeType{
		AssigneeTypeUser,
		AssigneeTypeRole,
		AssigneeTypeDepartment,
		AssigneeTypeGroup,
		AssigneeTypeExpression,
	}
}

// IsValidAssigneeType 验证受理人类型是否有效
func IsValidAssigneeType(aType AssigneeType) bool {
	for _, a := range GetAllAssigneeTypes() {
		if a == aType {
			return true
		}
	}
	return false
}

// GetAllReturnTypes 获取所有退回类型
func GetAllReturnTypes() []ReturnType {
	return []ReturnType{
		ReturnTypePrevious,
		ReturnTypeStart,
		ReturnTypeSpecific,
	}
}

// IsValidReturnType 验证退回类型是否有效
func IsValidReturnType(rType ReturnType) bool {
	for _, r := range GetAllReturnTypes() {
		if r == rType {
			return true
		}
	}
	return false
}

// GetAllDelegateTypes 获取所有委托类型
func GetAllDelegateTypes() []DelegateType {
	return []DelegateType{
		DelegateTypeTask,
		DelegateTypePermission,
		DelegateTypeTemporary,
		DelegateTypePermanent,
	}
}

// IsValidDelegateType 验证委托类型是否有效
func IsValidDelegateType(dType DelegateType) bool {
	for _, d := range GetAllDelegateTypes() {
		if d == dType {
			return true
		}
	}
	return false
}

// GetAllNotificationTypes 获取所有通知类型
func GetAllNotificationTypes() []NotificationType {
	return []NotificationType{
		NotificationTypeTaskAssigned,
		NotificationTypeTaskCompleted,
		NotificationTypeTaskOverdue,
		NotificationTypeProcessCompleted,
		NotificationTypeProcessRejected,
		NotificationTypeDelegated,
		NotificationTypeCountersign,
		NotificationTypeAddSign,
	}
}

// IsValidNotificationType 验证通知类型是否有效
func IsValidNotificationType(nType NotificationType) bool {
	for _, n := range GetAllNotificationTypes() {
		if n == nType {
			return true
		}
	}
	return false
}

// SelfApprovalType 自审配置类型枚举
type SelfApprovalType string

const (
	// SelfApprovalTypeSkip 跳过自审，移除发起人
	SelfApprovalTypeSkip SelfApprovalType = "skip"
	// SelfApprovalTypeAutoApprove 名为自动通过，但当前实现与 allow 一致：
	// 仅保留发起人为审批人，不会产生任何自动通过标记。依赖自动通过语义的场景请勿使用。
	SelfApprovalTypeAutoApprove SelfApprovalType = "auto_approve"
	// SelfApprovalTypeDelegateToManager 委托给上级主管
	SelfApprovalTypeDelegateToManager SelfApprovalType = "delegate_to_manager"
	// SelfApprovalTypeDelegateToDepartmentManager 委托给部门负责人
	SelfApprovalTypeDelegateToDepartmentManager SelfApprovalType = "delegate_to_department_manager"
	// SelfApprovalTypeAllow 默认允许自审
	SelfApprovalTypeAllow SelfApprovalType = "allow"
)

// GetAllSelfApprovalTypes 获取所有自审配置类型
func GetAllSelfApprovalTypes() []SelfApprovalType {
	return []SelfApprovalType{
		SelfApprovalTypeSkip,
		SelfApprovalTypeAutoApprove,
		SelfApprovalTypeDelegateToManager,
		SelfApprovalTypeDelegateToDepartmentManager,
		SelfApprovalTypeAllow,
	}
}

// IsValidSelfApprovalType 验证自审配置类型是否有效
func IsValidSelfApprovalType(saType SelfApprovalType) bool {
	for _, sa := range GetAllSelfApprovalTypes() {
		if sa == saType {
			return true
		}
	}
	return false
}
