package dto

import (
	"time"
)

// CompleteTaskRequest 完成任务请求
// 用于完成任务操作。
//
// 表达"通过/拒绝"的约定：把审批结果写入 Variables 的 constants.VarsApproved
// 键（bool，true=通过），意见写入 constants.VarsComment 键；这两个键由引擎
// 消费并在完成后从流程变量中移除。需要更完整的审批语义（如退回、委派归还）
// 请改用 TaskService.CompleteWithApproval + dto.ApprovalRequest。
type CompleteTaskRequest struct {
	TaskID    string                 `json:"taskId"`    // 任务ID
	Variables map[string]interface{} `json:"variables"` // 任务变量（可携带 VarsApproved/VarsComment 审批意图键）
	Comment   string                 `json:"comment"`   // 完成备注（为空时以 Variables 中的 VarsComment 为准）
}

// DelegateTaskRequest 委托任务请求
// 用于委托任务操作
type DelegateTaskRequest struct {
	TaskID   string `json:"taskId"`   // 任务ID
	Assignee string `json:"assignee"` // 委托给的用户
	Reason   string `json:"reason"`   // 委托原因
}

// TaskQuery 任务查询条件
// 包含最常用的查询字段，用于 DAO 层查询
type TaskQuery struct {
	PageRequest
	//是否查询历史任务
	// form tag 必须显式声明：gin 的 ShouldBindQuery 只读 form tag，
	// 缺失时 ?instanceId= 等查询参数会被静默丢弃（过滤失效，跨实例数据混入）。
	QueryHistory bool `json:"queryHistory" form:"queryHistory"`
	// 基础查询条件
	TaskID         string   `json:"taskId" form:"taskId"`                 // 任务ID
	InstanceID     *string  `json:"instanceId" form:"instanceId"`         // 流程实例ID
	InstanceIDs    []string `json:"instanceIds" form:"instanceIds"`       // 流程实例ID列表（批量过滤）
	TaskDefKey     string   `json:"taskDefKey" form:"taskDefKey"`         // 节点定义Key
	Assignee       string   `json:"assignee" form:"assignee"`             // 办理人
	Owner          string   `json:"owner" form:"owner"`                   // 任务拥有者
	Name           string   `json:"name" form:"name"`                     // 任务名称
	Keyword        string   `json:"keyword" form:"keyword"`               // 关键字搜索（名称、描述等）
	TenantID       string   `json:"tenantId"`                             // 租户ID
	ParentIDIsNull bool     `json:"parentIdIsNull" form:"parentIdIsNull"` // 父任务ID为NULL
	ApprovalType   string   `json:"approvalType" form:"approvalType"`     // 任务审批类型
	// CandidateUser 候选用户：开启后查询匹配 assignee=Assignee 或（status 为 Pending 且该用户在候选人池）的任务。
	// 候选人池由 wf_task_assignee 维护（entity_type='person' AND entity_id=CandidateUser）。
	CandidateUser string `json:"candidateUser" form:"candidateUser"`
	// CandidateRoleIDs 候选角色：用户拥有的角色 ID 列表。
	// 配合 CandidateUser，扩展候选匹配到 entity_type='role' AND entity_id IN (CandidateRoleIDs)。
	// 为空时仅匹配 person 候选。
	CandidateRoleIDs []string `json:"candidateRoleIDs" form:"candidateRoleIDs"`

	// 时间范围查询条件
	CreatedAfter  *time.Time `json:"createdAfter" form:"createdAfter"`   // 创建时间起始
	CreatedBefore *time.Time `json:"createdBefore" form:"createdBefore"` // 创建时间截止
	EndedAfter    *time.Time `json:"endedAfter" form:"endedAfter"`       // 完成时间起始
	EndedBefore   *time.Time `json:"endedBefore" form:"endedBefore"`     // 完成时间截止
	DueDateBefore *time.Time `json:"dueDateBefore" form:"dueDateBefore"` // 到期时间早于（含 NULL 过滤；用于过期任务查询）
}

// NodeApproverDTO 节点审批人信息
type NodeApproverDTO struct {
	EntityID      string  `json:"entityId"`      // 实体ID（用户ID/组ID等）
	EntityType    string  `json:"entityType"`    // 实体类型：person/department/role
	EntityName    string  `json:"entityName"`    // 实体名称
	CandidateType string  `json:"candidateType"` // candidate / cc
	Action        string  `json:"action"`        // 动作：assign/add/remove
	SortOrder     int32   `json:"sortOrder"`     // 会签顺序
	Status        string  `json:"status"`        // 状态：pending/approved/rejected
	ApprovalTime  *string `json:"approvalTime"`  // 审批时间
	Comment       string  `json:"comment"`       // 审批意见
	CreatedAt     string  `json:"createdAt"`     // 创建时间
}

// NodeApprovalStatusDTO 节点审批状态信息
type NodeApprovalStatusDTO struct {
	TaskID            string             `json:"taskId"`            // 任务ID
	TaskDefKey        string             `json:"taskDefKey"`        // 任务定义Key
	TaskName          string             `json:"taskName"`          // 任务名称
	ProcessInstanceID string             `json:"processInstanceId"` // 流程实例ID
	ApprovedList      []*NodeApproverDTO `json:"approvedList"`      // 已审批人员列表
	PendingList       []*NodeApproverDTO `json:"pendingList"`       // 待审批人员列表
	DSLContent        string             `json:"dslContent"`        // 流程定义内容
	FormContent       string             `json:"formContent"`       // 表单定义内容
	TotalCount        int                `json:"totalCount"`        // 总审批人数
	ApprovedCount     int                `json:"approvedCount"`     // 已审批人数
	PendingCount      int                `json:"pendingCount"`      // 待审批人数
	ApprovalRule      string             `json:"approvalRule"`      // 审批规则
	IsCompleted       bool               `json:"isCompleted"`       // 是否完成
}

// CountersignRule 会签规则结构
type CountersignRule struct {
	Type         string  `json:"type"`         // 规则类型：all, any, majority, percent, count
	Value        float64 `json:"value"`        // 规则值：用于percent和count类型
	IsSequential bool    `json:"isSequential"` // 是否是顺序会签, 默认false false: 并行会签, true: 顺序会签
}
