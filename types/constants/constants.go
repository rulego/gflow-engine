/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package constants

// ContextKey 类型化 context key：context 以 interface{} 为 key，
// 裸 string 键易与其他包冲突（staticcheck SA1029）
type ContextKey string

const (
	// KeyCurrentUser 当前用户（ctx 携带 *service.Actor；也作 metadata 键读取）
	KeyCurrentUser ContextKey = "current_user"
	// KeyTaskID 任务ID
	KeyTaskID = "task_id"
	// KeyInstanceID 实例ID
	KeyInstanceID = "instance_id"
	// KeyProcessID 流程ID
	KeyProcessID = "process_id"
	// KeyProcessKey 流程定义Key
	KeyProcessKey = "process_key"
	// KeyBusinessKey 业务Key
	KeyBusinessKey = "business_key"
	// KeyOwner 拥有者
	KeyOwner = "owner"
	// KeyAssignee 受理人
	KeyAssignee = "assignee"
	// KeyTenantID 租户ID
	KeyTenantID = "tenant_id"
	// KeyDescription 描述
	KeyDescription = "description"
	// KeyCallingMode 调用模式标记（API / Internal），CompleteWithApproval
	// 据此决定是否强制 assignee 身份校验
	KeyCallingMode ContextKey = "calling_mode"
	// KeyDelayOffsetMs 延迟节点恢复偏移（毫秒）。delay 节点 OnMsg 读取此键
	// 跳过已等待的时间，避免恢复后从头重新计时。参照 rulego delay_node.go。
	KeyDelayOffsetMs = "_delayOffsetMs"
	// KeySequentialAssignees 顺序审批待创建审批人缓存键：userTask 节点把审批人
	// 列表写入任务变量，后续 OnMsg 按同一顺序创建下一个任务（多份缓存取最长一份）。
	KeySequentialAssignees = "_sequentialAssignees"
	// KeyEndExecLock end 节点去重锁值。并发审批（fork-join 恢复 / 或签）会让
	// end 节点被并发 OnMsg 多次；TaskCreator 用 TryLock 抢到锁的那次执行把
	// 锁值写入 metadata，After 据此识别"本次是首次执行"，其余重复执行跳过
	// 任务创建与 CompleteProcessInstance 等副作用。
	KeyEndExecLock = "end_exec_lock"
)

const (
	// VarsApproved 审批结果约定键：写入任务变量后由 Complete 消费——
	// true 按通过推进、false 按拒绝处理，完成时该键从流程变量中移除
	VarsApproved = "approved"
	// VarsComment 审批意见约定键：与 VarsApproved 一样在完成后被移除并落入审批记录
	VarsComment = "comment"
)
const (
	// EndReasonPrefixRejected 拒绝终止写入 end_reason 的前缀
	EndReasonPrefixRejected = "审批拒绝"
	// EndReasonPrefixWithdrawn 发起人撤回写入 end_reason 的前缀
	EndReasonPrefixWithdrawn = "申请人撤回"
)
const (
	TaskTypeUserTask = "userTask"
	TaskTypeDelay    = "delay"
	TaskTypeCCTask   = "ccTask"
)

// DSL 节点类型（DefinitionJSON metadata.nodes[].type 取值，与 rulego 注册的
// 节点类型一致）。userTask/delay 与任务类型（TaskType*）同值，别名为单一来源。
const (
	NodeTypeEnd           = "end"
	NodeTypeSwitch        = "switch"
	NodeTypeJsSwitch      = "jsSwitch"
	NodeTypeMsgTypeSwitch = "msgTypeSwitch"
	NodeTypeInclusive     = "inclusive"
	// NodeTypeRouteGateway 遗留路由网关：从未注册，装载期由
	// WfProcess.MigrateRouteGateway 迁移为 switch
	NodeTypeRouteGateway = "routeGateway"
	NodeTypeStart        = "start"
	NodeTypeServiceTask  = "serviceTask"
	NodeTypeUserTask     = TaskTypeUserTask
	NodeTypeAIAgent      = "aiAgent"
	NodeTypeDelay        = TaskTypeDelay
	NodeTypeSubProcess   = "subProcess"
)

const (
	UserSystem = "system"
)

// 优先级：数值越大越优先（0~100，须通过 utils.ValidatePriority 校验）
const (
	PriorityLow    = 10
	PriorityNormal = 50
	PriorityHigh   = 80
	PriorityUrgent = 100
)

// TimeFormatLayout 时间格式化格式
const TimeFormatLayout = "2006-01-02 15:04:05"
