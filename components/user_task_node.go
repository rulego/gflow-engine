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

package components

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
)

// UserTaskNodeType 用户任务节点类型（与 DSL 节点类型/任务类型同值，统一取自 types/constants）
const UserTaskNodeType = constants.NodeTypeUserTask

// UserTaskNodeConfiguration 用户任务节点配置结构（节点 configuration 字段）
// additionalInfo 中的 formPermissions/ActionPermissions 仅前端显示控制；
// rejectStrategy/rejectTargetNode 为运行时驳回配置（configuration 未配置时回退读取 additionalInfo）。
type UserTaskNodeConfiguration struct {
	// 任务显示名（可选）：覆盖节点 name 作为任务名称，
	// 未配置时回退节点 name（additionalInfo.description 同理回退）。
	TaskName string `json:"taskName"`
	// 任务描述（可选）：覆盖 additionalInfo.description。
	TaskDescription string `json:"taskDescription"`
	// 表单标识（可选）：透传到 wf_task.form_key，前端按此渲染任务表单。
	FormKey string `json:"formKey"`

	// 审批人员类型
	// 可选：user（指定成员）/role（指定角色）/dept（指定部门，待认领）/direct_manager（直接上级）/initiator_select（发起人自选）/initiator_self（发起人自己）/multi_level_manager（多级上级）
	CandidateType string `json:"candidateType"`
	// 审批人员类型配置，按 CandidateType 取用以下键：
	// userIds []string       - user 型：指定成员ID列表
	// roleIds []string       - role 型：指定角色ID列表（运行时经 IdentityService 展开）
	// departmentIds []string - dept 型：指定部门ID列表（产生待认领任务，须先认领再审批）
	// levels int             - multi_level_manager 型：逐级上级审批层级数（默认1）
	// selected string         - initiator_select 型：审批人表达式模板（如 ${msg.selectedUsers}），
	//   运行时以流程变量求值得到审批人ID列表
	CandidateConfig map[string]interface{} `json:"candidateConfig"`

	// 审批配置
	// 审批类型：single(单人，缺省)/or(或签)/sequential(顺序依次审批)/countersign(会签)/vote(票签)
	ApprovalType string `json:"approvalType"`
	// 多人审批的通过规则 JSON（对应 dto.CountersignRule）：
	//   {"type":"all","value":0,"isSequential":false}
	// type 缺省为 all；可选 all 全部通过 / any 任一通过 / majority 过半 /
	// percent 按百分比通过（value 取 0~100）/ count 固定通过人数（value 为票数）。
	// isSequential=true 表示子任务按创建顺序逐个激活。仅 countersign/vote 等多人
	// 会签场景消费该字段，single 单人审批忽略。
	ApprovalRule string `json:"approvalRule"`

	// 自审配置：审批人与提交人为同一人时的处理方式
	// 可选：""(缺省，不做自审过滤)/allow(发起人自己审批)/skip(自动跳过)/delegate_to_manager(转交直接上级)/delegate_to_department_manager(转交部门负责人)
	SelfApprovalType string `json:"selfApprovalType"`

	// 任务到期时间（可选）。注意这是节点级静态配置：该节点的所有任务实例共用同一个
	// 到期时刻，不支持流程变量。
	// 支持格式：RFC3339 / "2006-01-02 15:04:05" / "2006-01-02"
	// 解析失败时仅告警，任务 DueDate 保持为空
	DueDate string `json:"dueDate"`

	// 超时处理策略（可选）：到期时间改为相对每个任务创建时刻的时长，到期后由宿主
	// 逾期巡检（overdue sweeper）按 Action 处理。配置后优先于静态 DueDate。
	TimeoutPolicy *TimeoutPolicy `json:"timeoutPolicy,omitempty"`

	// 驳回策略
	// 可选：
	//   - ""（默认）/ "terminate" : 终止实例
	//   - "rejectToStarter"        : 跳回开始节点
	//   - "rejectToPrev"           : 跳到上一个 userTask 节点
	//   - "rejectToNode"           : 跳到 RejectTargetNode 指定的节点
	// 跳转失败时会调用 fallbackRejection：若节点存在 Reject/Failure 出边则走该出边，否则 terminate
	RejectStrategy string `json:"rejectStrategy"`
	// 退回目标节点ID（仅 rejectToNode 生效）：须填链定义中存在的节点ID，填错时按
	// fallbackRejection 兜底处理
	RejectTargetNode string `json:"rejectTargetNode"`

	// RejectType 当前版本未生效：无论配置什么都忽略。拒绝来源的差异化控制统一由
	// RejectStrategy 表达；保留 JSON 字段仅为存量 DSL 反序列化兼容。
	RejectType string `json:"rejectType"`
}

// TimeoutPolicy userTask 超时处理策略。
// DueInMinutes 相对任务创建时刻的时长；Action 由宿主逾期巡检执行：
//   - remind      到期提醒办理人（默认，缺省/未知动作同此，仅提醒不代办）
//   - autoApprove 到期自动通过（以 system 身份记录审批意见并推进流程）
//   - autoReject  到期自动拒绝
type TimeoutPolicy struct {
	DueInMinutes int    `json:"dueInMinutes"`
	Action       string `json:"action"`
}

// resolveDueDate 计算任务到期时间：timeoutPolicy.dueInMinutes（>0）优先，
// 否则用 Init 解析的静态 dueDate。
func (n *UserTaskNode) resolveDueDate() *time.Time {
	if p := n.Config.TimeoutPolicy; p != nil && p.DueInMinutes > 0 {
		t := time.Now().Add(time.Duration(p.DueInMinutes) * time.Minute)
		return &t
	}
	return n.dueDate
}

// UserTaskNode 用户任务节点
type UserTaskNode struct {
	Config          UserTaskNodeConfiguration
	TaskService     service.TaskServiceInternal
	IdentityService service.IdentityService        // 由上层应用注入，按角色/部门/主管解析审批人
	RuntimeService  service.RuntimeServiceInternal // reject 时主动 terminate / jump 节点
	CurrentNodeDef  types.RuleNode

	TaskEventListener service.TaskEventListener // 任务事件监听器

	TaskName        string // 任务名称
	TaskDescription string // 任务描述
	// 预编译的模板
	initiatorSelectedTemplate el.Template // 发起人自选审批人模板
	// 解析后的任务到期时间（来自 Config.DueDate，解析失败为 nil）
	dueDate *time.Time
}

// Type 返回节点类型
func (n *UserTaskNode) Type() string {
	return UserTaskNodeType
}

// New 创建新的用户任务节点实例
func (n *UserTaskNode) New() types.Node {
	return &UserTaskNode{
		Config: UserTaskNodeConfiguration{
			ApprovalType: string(enums.ApprovalTypeSingle),
		},
		TaskService:       n.TaskService,
		IdentityService:   n.IdentityService,
		RuntimeService:    n.RuntimeService,
		TaskEventListener: n.TaskEventListener,
	}
}

// Init 初始化节点
func (n *UserTaskNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &n.Config)
	if err != nil {
		return err
	}
	// 保存当前节点信息，用于流程实例节点保存 CurrentActivity Name
	n.CurrentNodeDef = base.NodeUtils.GetSelfDefinition(configuration)
	// 任务基础配置，从节点名称和扩展字段获取
	// configuration.taskName / taskDescription 优先，未配置时回退节点 name / additionalInfo.description
	n.TaskName = strings.TrimSpace(n.Config.TaskName)
	if n.TaskName == "" {
		n.TaskName = n.CurrentNodeDef.Name
	}
	n.TaskDescription = strings.TrimSpace(n.Config.TaskDescription)
	if n.TaskDescription == "" {
		if description, ok := n.CurrentNodeDef.GetAdditionalInfo("description"); ok {
			// additionalInfo 的值类型由 DSL 决定（可能是 map/number/[]interface{}），
			// 直接 .(string) 在非字符串时会 panic；用 str.ToString 安全转换。
			n.TaskDescription = str.ToString(description)
		}
	}
	// rejectStrategy / rejectTargetNode 未在节点 configuration 配置时，从 additionalInfo 读取
	// （设计器把这两项写在 additionalInfo 里）
	if n.Config.RejectStrategy == "" {
		if v, ok := n.CurrentNodeDef.GetAdditionalInfo("rejectStrategy"); ok {
			if s, ok := v.(string); ok {
				n.Config.RejectStrategy = s
			}
		}
	}
	if n.Config.RejectTargetNode == "" {
		if v, ok := n.CurrentNodeDef.GetAdditionalInfo("rejectTargetNode"); ok {
			if s, ok := v.(string); ok {
				n.Config.RejectTargetNode = s
			}
		}
	}
	// 驳回策略校验：未知取值在初始化期告警，运行时按 terminate 处理
	if s := strings.TrimSpace(n.Config.RejectStrategy); !isValidUserTaskRejectStrategy(s) {
		logrus.Warnf("userTask node %s has unknown rejectStrategy %q; will terminate as fallback at runtime", n.GetSelfId(), n.Config.RejectStrategy)
	}
	// 自审互斥校验：initiator_self 的唯一审批人是发起人，skip 会把名单清空，
	// 任务创建必然失败（"no assignees found"）。初始化期告警；设计器发布校验会硬拦。
	if strings.TrimSpace(n.Config.CandidateType) == string(enums.CandidateTypeInitiatorSelf) &&
		strings.TrimSpace(n.Config.SelfApprovalType) == string(enums.SelfApprovalTypeSkip) {
		logrus.Warnf("userTask node %s: initiator_self + selfApprovalType=skip leaves no assignee; task creation will fail at runtime", n.GetSelfId())
	}
	// 解析到期时间：失败仅告警，DueDate 保持为空
	if n.Config.DueDate != "" {
		if due := parseDueDate(n.Config.DueDate); due != nil {
			n.dueDate = due
		} else {
			logrus.Warnf("userTask node %s has invalid dueDate %q, ignored", n.GetSelfId(), n.Config.DueDate)
		}
	}
	// 预编译表达式模板，提高运行时性能
	if err := n.compileTemplates(); err != nil {
		return fmt.Errorf("failed to compile templates: %w", err)
	}

	return nil
}

// OnMsg 处理消息
// 核心逻辑：
// 1. 如果该节点没创建任务，则创建任务
// 2. 每个用户审批后，也会进来OnMsg，只有全部都审批完，才能调用 ctx.TellSuccess 否则调用ctx.DoOnEnd 结束
func (n *UserTaskNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	defer recoverNodePanic(ctx, msg, UserTaskNodeType, n.GetSelfId())
	// 从消息中获取流程实例信息
	processInstanceID := n.getProcessInstanceID(msg)
	if processInstanceID == "" {
		ctx.TellFailure(msg, fmt.Errorf("process instance ID not found"))
		return
	}

	// 查询与创建在同一把锁内完成：同一实例同一节点的并发重入
	// （多个审批人同时操作、引擎重试）会在"查无任务"判定上竞争，导致重复建任务。
	// 锁在闭包内 defer 释放：DB 调用 panic 时不会把该实例+节点的 key 永久锁死
	created := false
	existingTasks, err := func() ([]*model.WfTask, error) {
		unlock := taskOpMutex.Lock(processInstanceID + "/" + n.GetSelfId())
		defer unlock()
		tasks, err := n.getExistingTasks(ctx.GetContext(), processInstanceID)
		if err != nil {
			return nil, fmt.Errorf("failed to check existing tasks: %w", err)
		}
		if len(tasks) == 0 {
			if err := n.createUserTasks(ctx, processInstanceID, msg); err != nil {
				return nil, fmt.Errorf("failed to create user tasks: %w", err)
			}
			created = true
			return nil, nil
		}
		return tasks, nil
	}()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// 如果没有任务，说明刚创建了新任务
	if created {
		// 任务创建后，流程暂停等待用户操作，调用DoOnEnd结束当前节点执行
		logrus.Debugf("User tasks created for node %s, waiting for completion", n.GetSelfId())
		ctx.DoOnEnd(msg, nil, "")
		return
	}

	// 检查所有任务是否都已完成
	allCompleted, err := n.checkTasksCompletion(ctx, msg, existingTasks)
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("failed to check tasks completion: %w", err))
		return
	}

	// 顺序审批：如果尚未完成且当前没有激活任务，则按顺序创建下一个任务
	if enums.ApprovalType(n.Config.ApprovalType) == enums.ApprovalTypeSequential && !allCompleted {
		// 拒绝即停：在创建下一个顺序任务之前，先扫描已完成的任务里是否有被拒绝的。
		// 顺序审批任一拒绝即触发驳回策略；会签的拒绝判定在下方统一处理
		//（顺序审批的子任务按需创建，必须在此早退）。
		for _, t := range existingTasks {
			if t.Status == string(enums.TaskStatusCompleted) && t.EndReason != nil &&
				*t.EndReason == string(enums.ApprovalResultRejected) {
				newMsg := msg.Copy()
				if newMsg.Metadata == nil {
					newMsg.Metadata = types.NewMetadata()
				}
				logrus.Debugf("Sequential rejected for node %s, invoking reject strategy", n.GetSelfId())
				n.handleRejection(ctx, newMsg, processInstanceID)
				return
			}
		}

		// 优先从已完成任务的 variables 读取 _sequentialAssignees 缓存
		assignees := sequentialAssigneesFrom(existingTasks)
		// 缓存缺失时重新解析
		if len(assignees) == 0 {
			resolved, rerr := n.resolveAssignees(ctx.GetContext(), metaValue(msg, constants.KeyTenantID), metaValue(msg, constants.KeyOwner), extractVariables(ctx, msg))
			if rerr == nil && len(resolved) > 0 {
				assignees = resolved
			}
		}
		if len(assignees) > 0 {
			completed := 0
			active := 0
			for _, t := range existingTasks {
				if t.Status == string(enums.TaskStatusCompleted) {
					completed++
				} else if t.Status == string(enums.TaskStatusActive) {
					active++
				}
			}
			if active == 0 && completed < len(assignees) {
				processID := n.getProcessID(msg)
				tenantID := metaValue(msg, constants.KeyTenantID)
				// 锁内重新确认状态后创建，防并发完成同时推进产生重复任务；
				// defer 释放防 panic 锁死（与 OnMsg 同一写法）
				var createErr error
				advanced := false
				func() {
					unlock := taskOpMutex.Lock(processInstanceID + "/" + n.GetSelfId())
					defer unlock()
					tasks, terr := n.getExistingTasks(ctx.GetContext(), processInstanceID)
					if terr != nil {
						createErr = terr
						return
					}
					c, a := 0, 0
					for _, t := range tasks {
						if t.Status == string(enums.TaskStatusCompleted) {
							c++
						} else if t.Status == string(enums.TaskStatusActive) {
							a++
						}
					}
					if a == 0 && c < len(assignees) {
						taskVars := extractVariables(ctx, msg)
						taskVars[constants.KeySequentialAssignees] = assignees
						// 每个后续子任务独立求到期时间：timeoutPolicy 相对各自创建时刻，
						// 与首个任务（createUserTasks）口径一致
						createErr = n.createSingleTask(ctx, processInstanceID, processID, tenantID, assignees[c], taskVars, n.resolveDueDate())
						advanced = createErr == nil
					}
				}()
				if createErr != nil {
					ctx.TellFailure(msg, fmt.Errorf("failed to create next sequential task: %w", createErr))
					return
				}
				if advanced {
					logrus.Debugf("Sequential next task created for node %s", n.GetSelfId())
					ctx.DoOnEnd(msg, nil, "")
					return
				}
			}
		}
	}

	// 检查审批结果：统计approved和rejected的任务数
	approvedCount := 0
	rejectedCount := 0
	for _, task := range existingTasks {
		if task.Status == string(enums.TaskStatusCompleted) && task.EndReason != nil {
			if *task.EndReason == string(enums.ApprovalResultApproved) {
				approvedCount++
			} else if *task.EndReason == string(enums.ApprovalResultRejected) {
				rejectedCount++
			}
		}
	}

	// 对于会签类型，如果已经有拒绝的任务，立即终止流程（一票否决）
	if n.Config.ApprovalType == string(enums.ApprovalTypeCountersign) && rejectedCount > 0 {
		newMsg := msg.Copy()
		if newMsg.Metadata == nil {
			newMsg.Metadata = types.NewMetadata()
		}
		logrus.Debugf("Countersign rejected for node %s, invoking reject strategy", n.GetSelfId())
		n.handleRejection(ctx, newMsg, processInstanceID)
		return
	}
	// 顺序审批的拒绝处理已在 sequential-advance 分支顶部完成（早退路径）。

	if allCompleted {
		// 所有任务都已完成，根据审批结果决定流程走向
		newMsg := msg.Copy()
		if newMsg.Metadata == nil {
			newMsg.Metadata = types.NewMetadata()
		}

		// 判断整体审批结果：按类型计算
		approved := n.evaluateApproval(existingTasks, approvedCount, rejectedCount)

		if approved {
			logrus.Debugf("All tasks completed and approved for node %s", n.GetSelfId())
			ctx.TellSuccess(newMsg)
		} else {
			logrus.Debugf("All tasks completed but rejected for node %s, invoking reject strategy", n.GetSelfId())
			n.handleRejection(ctx, newMsg, processInstanceID)
		}
	} else {
		// 还有任务未完成，继续等待
		logrus.Debugf("Tasks still pending for node %s, waiting for completion", n.GetSelfId())
		ctx.DoOnEnd(msg, nil, "")
	}
}

// getExistingTasks 获取当前节点的现有任务（含已完成终态任务）。
// 顺序审批的推进逻辑依赖终态任务计算进度、读取 _sequentialAssignees 缓存，
// 因此不能过滤 Completed 任务。驳回回跳场景的终态任务清理由 jumpToNode
// 里的 SupersedeNodeTasks 完成。
func (n *UserTaskNode) getExistingTasks(ctx context.Context, processInstanceID string) ([]*model.WfTask, error) {
	query := &dto.TaskQuery{
		InstanceID:     &processInstanceID,
		TaskDefKey:     n.GetSelfId(),
		ParentIDIsNull: true,
	}

	tasks, _, err := n.TaskService.GetTaskList(ctx, service.ActorFromCtx(ctx), query)
	return tasks, err
}

// checkTasksCompletion 检查任务完成状态
func (n *UserTaskNode) checkTasksCompletion(ctx types.RuleContext, msg types.RuleMsg, tasks []*model.WfTask) (bool, error) {
	completedTasks := 0
	for _, task := range tasks {
		if task.Status == string(enums.TaskStatusCompleted) {
			completedTasks++
		}
	}

	switch enums.ApprovalType(n.Config.ApprovalType) {
	case enums.ApprovalTypeSingle:
		// 单人审批：只要完成就按审批结果
		allCompleted := completedTasks == len(tasks)
		return allCompleted, nil

	case enums.ApprovalTypeOr:
		// 或签：任何一个完成就完成
		anyCompleted := completedTasks > 0
		return anyCompleted, nil

	case enums.ApprovalTypeSequential:
		// 顺序审批每次只有一个 active 子任务，期望总数取 _sequentialAssignees 缓存
		// 长度；缓存缺失时退化为 len(tasks)。
		expectedTotal := len(sequentialAssigneesFrom(tasks))
		if expectedTotal == 0 {
			return completedTasks == len(tasks), nil
		}
		return completedTasks >= expectedTotal, nil

	case enums.ApprovalTypeCountersign, enums.ApprovalTypeVote:
		// 会签/票签：委托 service 按 ApprovalRule 规则(all/any/majority/percent/count)判定。
		// vote 复用会签父+子结构，达到阈值或注定拒绝即完成（剩余子任务终止）。
		parentID := ""
		for _, t := range tasks {
			if t.ParentID == nil || *t.ParentID == "" {
				parentID = t.ID
				break
			}
		}
		if parentID == "" {
			return false, nil
		}
		isCompleted, _, err := n.TaskService.CheckCountersignSubTaskCompletion(ctx.GetContext(), parentID, n.Config.ApprovalRule)
		if err != nil {
			return false, err
		}
		return isCompleted, nil

	default:
		allCompleted := completedTasks == len(tasks)
		return allCompleted, nil
	}
}

// sequentialAssigneesFrom 从任务 variables 提取 KeySequentialAssignees 缓存。
// createUserTasks 在创建第一个顺序审批任务时写入审批人列表，
// 后续按需创建的子任务沿用同一份缓存；多份缓存取最长的一份。
// 找不到缓存返回 nil。
func sequentialAssigneesFrom(tasks []*model.WfTask) []string {
	var best []string
	for _, t := range tasks {
		if t == nil || t.Variables == nil || *t.Variables == "" {
			continue
		}
		vars, err := service.ParseVariablesJSON(t.Variables)
		if err != nil {
			continue
		}
		cached, ok := vars[constants.KeySequentialAssignees]
		if !ok {
			continue
		}
		arr, ok := cached.([]interface{})
		if !ok {
			continue
		}
		if len(arr) > len(best) {
			best = make([]string, 0, len(arr))
			for _, v := range arr {
				if s, ok := v.(string); ok {
					best = append(best, s)
				}
			}
		}
	}
	return best
}

// evaluateApproval 计算整体审批是否通过
// 根据不同审批类型与规则，判断是否通过
func (n *UserTaskNode) evaluateApproval(tasks []*model.WfTask, approvedCount, rejectedCount int) bool {
	switch enums.ApprovalType(n.Config.ApprovalType) {
	case enums.ApprovalTypeSingle:
		return rejectedCount == 0 && approvedCount > 0
	case enums.ApprovalTypeOr:
		return rejectedCount == 0 && approvedCount > 0
	case enums.ApprovalTypeSequential:
		// 顺序审批：若任何一步拒绝则失败；全部通过则成功
		for _, t := range tasks {
			if t.Status == string(enums.TaskStatusCompleted) && t.EndReason != nil {
				if *t.EndReason == string(enums.ApprovalResultRejected) {
					return false
				}
			}
		}
		return approvedCount > 0 && rejectedCount == 0
	case enums.ApprovalTypeCountersign:
		// 会签在 OnMsg 上面已有一票否决处理（遇拒绝立即失败）
		return rejectedCount == 0 && approvedCount > 0
	case enums.ApprovalTypeVote:
		// 票签阈值判定在 complete 时由 service 层 checkCountersign 完成（父任务 complete + ExecuteNext）。
		// 此为 OnMsg 兜底粗判（与会签一致），精确阈值不在此重复。
		return rejectedCount == 0 && approvedCount > 0
	default:
		return rejectedCount == 0 && approvedCount > 0
	}
}

// dueDateLayouts 任务到期时间支持的格式
var dueDateLayouts = []string{time.RFC3339, constants.TimeFormatLayout, "2006-01-02"}

// parseDueDate 解析到期时间配置，格式见 dueDateLayouts；空值或解析失败返回 nil。
func parseDueDate(value string) *time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	for _, layout := range dueDateLayouts {
		if t, err := time.Parse(layout, value); err == nil {
			return &t
		}
	}
	return nil
}

// getProcessInstanceID 从消息 metadata 获取流程实例 ID
func (n *UserTaskNode) getProcessInstanceID(msg types.RuleMsg) string {
	if msg.Metadata != nil {
		if pid := metaValue(msg, constants.KeyInstanceID); pid != "" {
			return pid
		}
	}
	return ""
}

// getProcessID 从消息 metadata 获取流程定义 ID
func (n *UserTaskNode) getProcessID(msg types.RuleMsg) string {
	if msg.Metadata != nil {
		if pid := metaValue(msg, constants.KeyProcessID); pid != "" {
			return pid
		}
	}
	return ""
}

// GetSelfId 获取当前节点ID
func (n *UserTaskNode) GetSelfId() string {
	return selfID(n.CurrentNodeDef, UserTaskNodeType)
}

// GetSelfName 获取当前节点Name
func (n *UserTaskNode) GetSelfName() string {
	return selfName(n.CurrentNodeDef)
}

// Destroy 无需清理的资源，空实现满足 types.Node 接口
func (n *UserTaskNode) Destroy() {}

// compileTemplates 预编译所有表达式模板，提高运行时性能
func (n *UserTaskNode) compileTemplates() error {
	// 发起人自选：提前编译 selected 表达式
	if strings.ToLower(strings.TrimSpace(n.Config.CandidateType)) == string(enums.CandidateTypeInitiatorSelect) {
		if sel, ok := n.Config.CandidateConfig["selected"]; ok {
			s := fmt.Sprintf("%v", sel)
			if s != "" {
				tpl, terr := el.NewTemplate(s)
				if terr != nil {
					// 坏模板必须让 Init 失败：静默跳过会把错误推迟到运行期，
					// 表现为误导性的 "no assignees found for task"
					return fmt.Errorf("failed to compile initiator selected template %q: %w", s, terr)
				}
				n.initiatorSelectedTemplate = tpl
			}
		}
	}
	return nil
}
