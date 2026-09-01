package dto

import (
	"time"
)

// ProcessInstanceDTO 流程实例DTO
// 基于WfInstance模型，用于对外提供流程实例信息
type ProcessInstanceDTO struct {
	ID              string     `json:"id"`              // UUID 主键
	ProcessID       string     `json:"processId"`       // 流程定义ID
	BusinessKey     *string    `json:"businessKey"`     // 业务键,业务系统唯一编号
	Name            string     `json:"name"`            // 实例名称
	Status          string     `json:"status"`          // 生命周期状态：draft/active/completed/suspended/terminated/cancelled/failed（见 enums.InstanceStatus）
	Variables       *string    `json:"variables"`       // 流程变量
	CurrentActivity *string    `json:"currentActivity"` // 当前运行到的节点定义ID
	Priority        int32      `json:"priority"`        // 优先级（数值越大越优先）
	ParentID        *string    `json:"parentId"`        // 父流程实例ID（用于子流程）
	TenantID        string     `json:"tenantId"`        // 租户ID（SaaS 多租户）
	CreatedBy       string     `json:"createdBy"`       // 发起人用户ID
	CreatedAt       time.Time  `json:"createdAt"`       // 实例创建时间
	UpdatedBy       *string    `json:"updatedBy"`       // 最后更新人
	UpdatedAt       *time.Time `json:"updatedAt"`       // 最后更新时间
}

// StartProcessInstanceRequest 启动流程实例请求
// 用于启动新的流程实例
type StartProcessInstanceRequest struct {
	ProcessDefinitionID string                 `json:"processDefinitionId"` // 流程定义ID
	BusinessKey         *string                `json:"businessKey"`         // 业务键
	Name                string                 `json:"name"`                // 实例名称
	Variables           map[string]interface{} `json:"variables"`           // 流程变量
	TenantID            string                 `json:"tenantId"`            // 租户ID
	Priority            int32                  `json:"priority"`            // 优先级
	ParentID            string                 `json:"parentId"`            // 父流程实例ID
}

// ProcessInstanceResponse 流程实例响应结构
// Process instance response structure for API responses
type ProcessInstanceResponse struct {
	ID                  string                 `json:"id"`                  // 流程实例ID
	ProcessDefinitionID string                 `json:"processDefinitionId"` // 流程定义ID
	BusinessKey         *string                `json:"businessKey"`         // 业务键
	Name                string                 `json:"name"`                // 流程实例名称
	Status              string                 `json:"status"`              // 状态
	StartTime           time.Time              `json:"startTime"`           // 开始时间
	EndTime             *time.Time             `json:"endTime"`             // 结束时间
	TenantID            string                 `json:"tenantId"`            // 租户ID
	StartedBy           string                 `json:"startedBy"`           // 启动人ID
	Priority            int32                  `json:"priority"`            // 优先级
	ParentID            *string                `json:"parentId"`            // 父流程实例ID
	Variables           map[string]interface{} `json:"variables"`           // 流程变量
}

// ProcessInstanceQueryDTO 流程实例查询条件DTO
// 用于流程实例的查询操作
type ProcessInstanceQueryDTO struct {
	// 分页参数
	PageRequest
	// QueryHistory 是否查询历史实例：false（默认）查运行时表 wf_instance，
	// true 查归档表 wf_hi_instance
	QueryHistory bool   `json:"queryHistory" form:"queryHistory"`
	InstanceID   string `json:"processInstanceId" form:"processInstanceId"`       // 流程实例ID
	ProcessID    string `json:"processDefinitionId" form:"processDefinitionId"`   // 流程定义ID
	ProcessKey   string `json:"processDefinitionKey" form:"processDefinitionKey"` // 流程定义Key
	BusinessKey  string `json:"businessKey" form:"businessKey"`                   // 业务键
	// 发起人与创建时间窗：按发起人用户ID过滤、按实例创建时间过滤区间
	StartUserID   string     `json:"startUserId" form:"startUserId"`
	CreatedAfter  *time.Time `json:"createdAfter" form:"createdAfter"`
	CreatedBefore *time.Time `json:"createdBefore" form:"createdBefore"`
}

// ProcessInstanceOperationRequest 流程实例操作请求
// 用于流程实例的操作（挂起、激活、终止等）
type ProcessInstanceOperationRequest struct {
	ProcessInstanceID string                 `json:"processInstanceId"` // 流程实例ID
	Operation         string                 `json:"operation"`         // 操作类型
	Reason            string                 `json:"reason"`            // 操作原因
	Variables         map[string]interface{} `json:"variables"`         // 变量更新
}

// ProcessVariableDTO 流程变量DTO
// 用于流程变量的传输
type ProcessVariableDTO struct {
	Name              string      `json:"name"`              // 变量名
	Value             interface{} `json:"value"`             // 变量值
	Type              string      `json:"type"`              // 变量类型
	ProcessInstanceID string      `json:"processInstanceId"` // 流程实例ID
	TaskID            string      `json:"taskId"`            // 任务ID（可选）
	Scope             string      `json:"scope"`             // 作用域：global=全局，local=局部
}

type InstanceDetailResponse struct {
	InstanceID     string `json:"instanceId"`
	InstanceStatus string `json:"instanceStatus"`
	// 实例基础信息：详情页头部展示申请人/发起时间/流程名，打印模板取申请编号与申请人
	Name        string    `json:"name"`        // 实例名称（申请标题）
	ProcessName string    `json:"processName"` // 流程定义名称
	StartUserID string    `json:"startUserId"` // 发起人用户ID（前端解析为姓名）
	StartTime   time.Time `json:"startTime"`   // 发起时间
	// DefinitionJSON 流程定义 DSL 原文（JSON 字符串）；FormSchemaJSON 表单 schema
	// （JSON 字符串）。两者均可能较大，由前端解析渲染。
	DefinitionJSON string `json:"definitionJson"`
	FormSchemaJSON string `json:"formSchemaJson"`
	// 已经执行和待执行任务（审批时间线，按节点分组；加签等场景下含 SubExecutions 子树）
	Executions []ExecutionInfo        `json:"executions"`
	Variables  map[string]interface{} `json:"variables"`

	// CurrentUserActivityTask 当前操作人视角下的当前活动任务（无则为零值）
	CurrentUserActivityTask CurrentUserActivityTask `json:"currentUserActivityTask"`
	// ActionPermissions 节点动作权限映射：key 为动作名（如 return/addSign），
	// value 为是否允许（false 表示设计器显式禁用该按钮）
	ActionPermissions map[string]interface{} `json:"actionPermissions"`
}

type CurrentUserActivityTask struct {
	TaskID     string `json:"currentUserTaskId"` // 任务ID（JSON 键与 Go 字段名不同，前端固定用 currentUserTaskId）
	TaskDefKey string `json:"taskDefKey"`
	TaskName   string `json:"taskName"`

	FormPermissions map[string]string `json:"formPermissions"` // 表单字段权限：字段名 → r(只读)/w(可写)/h(隐藏)；审批提交的变量按此过滤
	// Variables 任务创建时的变量快照（AI 兜底待办携带 _ai 原始输出，
	// 供审批详情面板高亮展示；普通审批任务与实例变量基本一致）。
	Variables map[string]interface{} `json:"variables,omitempty"`
}

// ExecutionInfo represents the approval history of a task.
type ExecutionInfo struct {
	TaskID     string `json:"taskId"`
	TaskDefKey string `json:"taskDefKey"`
	TaskName   string `json:"taskName"`
	TaskType   string `json:"taskType"`
	Status     string `json:"status"`
	// 受托人
	Assignee *string `json:"assignee"`
	// EndedAt/ClaimedAt 时间字符串，格式 "2006-01-02 15:04:05"，未发生时为空串
	EndedAt   string `json:"endedAt"`
	ClaimedAt string `json:"claimedAt"`
	// EndReason 任务结束原因：approved 通过 / rejected 拒绝 / returned 退回 /
	// delegated 委派 / withdrawn 撤回 等；未结束时为 nil
	EndReason    *string `json:"endReason"`
	ApprovalType string  `json:"approvalType"`
	Comment      *string `json:"comment"`
	// SubExecutions 子任务明细：加签（countersign 父子结构）场景下挂该任务的
	// 会签子任务，普通审批任务为空
	SubExecutions []ExecutionInfo `json:"subExecutions"`
	// IsCandidate 候选待认领任务（无 assignee + pending，等候选成员 claim）
	IsCandidate bool `json:"isCandidate"`
}
