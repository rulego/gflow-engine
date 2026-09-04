package service

import (
	"context"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// ApprovalRequest 审批请求。
// 操作人由 CompleteWithApproval 的显式 actor 参数传入；CallingModeAPI 下引擎以
// actor.UserID 校验 assignee 权限。引擎不含用户体系、不做认证，身份真实性由宿主保证。
type ApprovalRequest struct {
	TaskID string `json:"taskId"` // 任务ID
	// ApprovalResult 审批结果，取 enums.ApprovalResult 全集：
	//   approved 同意 / rejected 拒绝 / returned 退回 / delegated 委托 / pending 待处理。
	// 留空时视为普通完成：任务置 Completed 并推进流程，不写 EndReason；
	// approved/rejected 之外的值请确认节点 RejectStrategy 对该结果的处置是否符合预期。
	ApprovalResult enums.ApprovalResult   `json:"approvalResult"`
	Comment        string                 `json:"comment"`   // 审批意见
	Variables      map[string]interface{} `json:"variables"` // 流程变量（API 模式按表单字段权限过滤）
}

// TaskService 任务服务接口
// 提供任务管理的核心功能，包括任务的查询、声明、完成、委派、审批、评论等操作
type TaskService interface {
	// ========== 基础任务管理 ==========

	// CreateTask 创建新任务
	CreateTask(ctx context.Context, actor Actor, task *model.WfTask) (string, error)

	// GetTask 根据任务ID获取任务详情
	GetTask(ctx context.Context, actor Actor, taskID string) (*model.WfTask, error)

	// GetTaskList 获取任务列表
	GetTaskList(ctx context.Context, actor Actor, query *dto.TaskQuery) ([]*model.WfTask, int64, error)

	// GetApprovalStatistics 获取基础审批统计。
	// 返回 map 固定键位：todoCount 待办、activeCount 进行中、doneCount 已办（已扣除 CC）、
	// ccCount 抄送、applicationCount 我发起的申请、totalCount 前四项之和、
	// overdueCount 已超期、todayArrived 今日新增、weekCompleted 本周完成、
	// monthApproved/monthRejected 本月通过/驳回、avgDurationMonth 本月平均时长（毫秒）。
	GetApprovalStatistics(ctx context.Context, actor Actor) (map[string]interface{}, error)

	// GetApprovalStatisticsDetail 获取明细审批统计（支持时间范围筛选、完成趋势、流程分类分布与效率指标）。
	// 返回 map 固定键位：todoCount/doneCount（已扣 CC）/ccCount/myApplicationCount/totalCount 计数，
	// todayCompleted/weekCompleted/monthCompleted 完成数，trendData 完成趋势
	// （[]map[string]interface{}，元素形如 {date, count}）、typeDistribution 分类分布
	// （元素形如 {type, count}），avgDuration/minDuration/maxDuration 平均/最短/最长时长（毫秒）、
	// overtimeCount 超期数、rejectedCount 驳回数、rejectionRate 驳回率（百分比 0~100）。
	// 注意与 GetApprovalStatistics 的键名不完全一致（此处为 myApplicationCount/monthCompleted）。
	GetApprovalStatisticsDetail(ctx context.Context, actor Actor, startDate, endDate *time.Time) (map[string]interface{}, error)

	// DeleteTask 删除任务
	DeleteTask(ctx context.Context, actor Actor, taskID, reason string) error

	// ========== 任务分配管理 ==========

	// Claim 认领任务（把无主候选任务分配给操作人 actor）。
	// 权限：候选池非空时须命中候选记录（person/role/dept 展开），否则 ErrPermissionDenied；
	// 候选池为空表示任务未限定候选人，直接放行。校验路径 fail-closed：DAO/identity 故障拒绝认领。
	// 幂等：操作人重复认领自己已认领的任务返回 nil；已被他人认领或非 Pending 状态返回 ErrTaskNotClaimable。
	Claim(ctx context.Context, actor Actor, taskID string) error

	// Unclaim 取消声明任务（释放任务分配，与 Claim 对称）。
	// 委派中的任务（有 Owner）会归还给原审批人而非扔回候选池。
	Unclaim(ctx context.Context, actor Actor, taskID string) error

	// SetAssignee 设置任务分配人。管理操作：须管理员（SuperAdmin）或系统身份，
	// 普通用户无权调用（否则可劫持他人任务或解除分配使任务回池）。
	SetAssignee(ctx context.Context, actor Actor, taskID, userID string) error

	// SetOwner 设置任务所有者。管理操作：须管理员（SuperAdmin）或系统身份，
	// Owner 驱动委派归还路径，普通用户无权改写。
	SetOwner(ctx context.Context, actor Actor, taskID, userID string) error

	// Delegate 委派任务给其他用户：assignee 转为目标用户，原 assignee 记为 Owner。
	// 被委派人 approve/reject 后任务归还 Owner 复审（见 CompleteWithApproval 的委派归还路径），
	// 不会直接完成流转。设计器显式禁用 delegate 时拒绝。
	Delegate(ctx context.Context, actor Actor, taskID, userID, reason string) error

	// Resolve 解决委派的任务（委派任务完成后返回给原分配人）。仅被委派人（assignee）、
	// 原办理人（Owner）或管理员/系统身份可调用。
	Resolve(ctx context.Context, actor Actor, taskID string) error

	// GetHistoryTasksByProcessInstanceID 获取实例的全部已归档任务（wf_hi_task）
	GetHistoryTasksByProcessInstanceID(ctx context.Context, processInstanceID string) ([]*model.WfTask, error)

	// GetHistoryTask 根据任务ID获取历史任务详情
	GetHistoryTask(ctx context.Context, taskID string) (*model.WfTask, error)

	// GetTaskByDefKey 按实例+节点定义Key取当前任务（同 key 多行时取第一条）
	GetTaskByDefKey(ctx context.Context, processInstanceID, taskDefKey string) (*model.WfTask, error)

	// GetTaskCandidates 获取任务候选人
	GetTaskCandidates(ctx context.Context, processInstanceID, taskDefKey string) ([]*dto.NodeApproverDTO, error)

	// AddCandidates 批量写入任务候选人（存原始角色/部门/person 引用，查询时展开）。
	// 每条 entityID 一条 wf_task_assignee 记录。
	AddCandidates(ctx context.Context, actor Actor, taskID, entityType string, entityIDs []string) error

	// RemoveCandidates 移除任务候选人（按 entityType + entityIDs 删除对应 wf_task_assignee 记录）。
	RemoveCandidates(ctx context.Context, actor Actor, taskID, entityType string, entityIDs []string) error

	// ========== 任务完成和审批 ==========

	// Complete 完成任务并推进流程。
	//
	// 审批协议：variables 中的约定键会被解释为审批意图，完成时从流程变量中移除——
	//   - constants.VarsApproved（"approved"，bool）：true 视为通过、false 视为拒绝；
	//   - constants.VarsComment（"comment"，string）：审批意见。
	// 若只是普通业务变量，避免使用这两个键名；需要显式区分通过/拒绝请改用
	// CompleteWithApproval / Approve / Reject。
	Complete(ctx context.Context, actor Actor, taskID string, variables map[string]interface{}) error

	// CompleteWithApproval 完成任务（带审批结果的统一审批入口，Approve/Reject 均走此方法）。
	// API 调用模式（见 CallingMode）下强制身份校验：无身份返回 ErrAuthenticationRequired，
	// 操作人非 assignee 返回 ErrPermissionDenied；存在未决加签子任务返回 ErrTaskNotClaimable。
	// variables 会合并进任务变量并下传后续节点（API 路径按表单字段权限过滤）。
	CompleteWithApproval(ctx context.Context, actor Actor, request *ApprovalRequest) error

	// Approve 审批通过任务（等价于 ApprovalResult=approved 的 CompleteWithApproval）
	Approve(ctx context.Context, actor Actor, taskID, comment string, variables map[string]interface{}) error

	// Reject 审批拒绝任务（等价于 ApprovalResult=rejected 的 CompleteWithApproval）
	Reject(ctx context.Context, actor Actor, taskID, comment string, variables map[string]interface{}) error

	// ========== 任务属性管理 ==========

	// SetPriority 设置任务优先级
	SetPriority(ctx context.Context, actor Actor, taskID string, priority int) error

	// SetDueDate 设置任务到期时间
	SetDueDate(ctx context.Context, actor Actor, taskID string, dueDate time.Time) error

	// SuspendTask 挂起任务
	SuspendTask(ctx context.Context, actor Actor, taskID string) error

	// ActivateTask 激活任务
	ActivateTask(ctx context.Context, actor Actor, taskID string) error

	// ========== 变量管理 ==========

	// GetTaskVariables 获取任务变量
	GetTaskVariables(ctx context.Context, actor Actor, taskID string) (map[string]interface{}, error)

	// GetTaskVariable 获取指定任务变量
	GetTaskVariable(ctx context.Context, actor Actor, taskID, variableName string) (interface{}, error)

	// SetTaskVariables 设置任务变量
	SetTaskVariables(ctx context.Context, actor Actor, taskID string, variables map[string]interface{}) error

	// SetTaskVariable 设置指定任务变量
	SetTaskVariable(ctx context.Context, actor Actor, taskID, variableName string, value interface{}) error

	// RemoveTaskVariable 删除任务变量
	RemoveTaskVariable(ctx context.Context, actor Actor, taskID, variableName string) error

	// ========== 中国特色审批功能 ==========

	// AddSign 加签（添加额外的审批人）
	AddSign(ctx context.Context, actor Actor, taskID string, userIDs []string, reason string) error

	// ReduceSign 减签（移除部分审批人）
	ReduceSign(ctx context.Context, actor Actor, taskID string, userIDs []string, reason string) error

	// Transfer 转办（将任务转给其他人处理）
	Transfer(ctx context.Context, actor Actor, taskID, toUserID, reason string) error

	// Reassign 管理员强制改派任务（跳过 assignee/候选人校验，保留 active 校验与 reassign 权限闸门）。
	// 落库方式对标 Transfer：写 task.Variables + 发 TaskEvent，不写 history 表。
	Reassign(ctx context.Context, actor Actor, taskID, newAssignee, reason string) (string, error)

	// GetOverdueTasks 全局视角查询已过 dueDate 的 active 任务（管理员监控，不带 assignee 过滤）。
	GetOverdueTasks(ctx context.Context, actor Actor, query *dto.TaskQuery) ([]*model.WfTask, int64, error)

	// ScanOverdueTasks 跨租户扫描已过 dueDate 的 active/pending 任务（平台级巡检）。
	// 供宿主应用跑定时逾期提醒/升级策略，避免各宿主绕过引擎直接扫 wf_task 表。
	ScanOverdueTasks(ctx context.Context, limit int) ([]*model.WfTask, error)

	// GetClaimableInstanceIDs 批量判断哪些实例存在"当前用户可认领"的任务
	// （无 assignee 且用户在候选人池），供待办列表页一次性标记 needsClaim，
	// 替代逐实例逐任务调 GetTaskCandidates 的 N+1 用法。
	GetClaimableInstanceIDs(ctx context.Context, actor Actor, instanceIDs []string) ([]string, error)

	// AddTaskComment 为任务添加审批意见（任务在办/已归档均可评论）。
	// 评论人以显式参数 actor 为准（与审批动作的显式操作人口径一致）。返回评论ID。
	AddTaskComment(ctx context.Context, actor Actor, taskID, content string) (string, error)

	// GetTaskComments 按时间正序获取任务全部评论。
	// actor 含租户时做任务归属校验（跨租户按不存在处理）。
	GetTaskComments(ctx context.Context, actor Actor, taskID string) ([]*model.WfTaskComment, error)

	// GetBacklogByProcess 按流程定义聚合 active 任务数，倒序取 top 10（管理员积压看板）。
	GetBacklogByProcess(ctx context.Context, actor Actor) ([]*BacklogItem, error)

	// Withdraw 撤回（申请人撤回已提交的申请）
	Withdraw(ctx context.Context, actor Actor, taskID, reason string) error

	// WithdrawByInstance 按流程实例撤回（发起人视角入口）。
	// 发起人通常不是当前审批节点的办理人，没有 currentUserActivityTask，无法走 task 维度 Withdraw。
	// 此方法按 instanceID 取当前 active 任务，复用 withdrawInternal（含 StartUserID 校验 + 终止实例）。
	WithdrawByInstance(ctx context.Context, actor Actor, instanceID, reason string) error

	// Return 退回（将任务退回到指定节点）
	Return(ctx context.Context, actor Actor, taskID, targetActivityID, reason string) error

	// ========== 节点审批人管理 ==========

	// GetNodeApprovalStatus 获取节点审批状态信息
	GetNodeApprovalStatus(ctx context.Context, taskID string) (*dto.NodeApprovalStatusDTO, error)

	// GetNodeApprovers 获取节点审批人列表
	GetNodeApprovers(ctx context.Context, taskID string) ([]*dto.NodeApproverDTO, error)

	// GetNodeApprovalStatusByProcessInstance 根据流程实例和任务定义Key获取节点审批状态
	GetNodeApprovalStatusByProcessInstance(ctx context.Context, processInstanceID, taskDefKey string) (*dto.NodeApprovalStatusDTO, error)
}

// TaskServiceInternal 引擎内部机制使用的任务服务接口，宿主不应调用。
// 在 TaskService 的基础上暴露会签/加签子任务管理与节点任务归档等引擎内部机制——
// 它们由 userTaskNode 等工作流组件在流程推进过程中回调，语义与调用时机强绑定
// 引擎内部状态机，宿主直接调用会破坏流转正确性（如误归档活动节点任务）。
// TaskServiceImpl 同时满足 TaskService 与本接口。
type TaskServiceInternal interface {
	TaskService

	// CreateCountersignSubTasks 创建会签子任务
	//   - parentTaskID: 父任务ID
	//   - assignees: 会签人员列表
	//   - approvalRule: 会签规则
	CreateCountersignSubTasks(ctx context.Context, parentTaskID string, assignees []string, approvalRule string) error

	// CheckCountersignSubTaskCompletion 检查会签子任务完成状态
	// 返回值：
	//   - bool: 是否完成
	//   - bool: 是否通过（仅在完成时有效）
	//   - error: 错误信息
	CheckCountersignSubTaskCompletion(ctx context.Context, parentTaskID, approvalRule string) (bool, bool, error)

	// SupersedeNodeTasks 把某实例某节点（taskDefKey）的全部任务归档到 wf_hi_task
	// 并从 wf_task 删除，返回被归档的任务数。
	//
	// 用途：驳回回跳（rejectToPrev / rejectToStarter / rejectToNode）重新进入目标
	// userTask 节点前，清理该节点上一轮遗留的 Completed 任务。否则重入时
	// getExistingTasks 会返回这些旧任务，checkTasksCompletion 立即判定"已完成"，
	// evaluateApproval 看到历史 approved → TellSuccess，导致目标节点被"静默自动通过"，
	// 驳回语义被绕过。
	//
	// 旧任务归档到 wf_hi_task 保留审计；删出 wf_task 后重入时 getExistingTasks 为空，
	// 节点会重新创建任务、重新走审批。仅作用于 (instanceID, taskDefKey)，不波及其它节点。
	// 正常推进（含 sequential）不走 jump 路径，不受影响。
	SupersedeNodeTasks(ctx context.Context, instanceID, taskDefKey, reason string) (int, error)
}
