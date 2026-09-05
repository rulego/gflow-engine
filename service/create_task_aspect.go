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

package service

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/gflow-engine/utils/lock"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/el"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
)

var (
	// Compile-time check TaskCreator implements types.BeforeAspect.
	_ types.BeforeAspect = (*TaskCreator)(nil)
	// Compile-time check TaskCreator implements types.AfterAspect.
	_ types.AfterAspect = (*TaskCreator)(nil)
)

type TaskCreator struct {
	instanceDAO    *dao.InstanceDAO
	workflowEngine WorkflowEngine
}

// Order 切面执行顺序，值越高执行越晚。900 使其排在绝大多数切面之后。
func (aspect *TaskCreator) Order() int {
	return 900
}

// New 每条规则链使用独立的切面实例。
func (aspect *TaskCreator) New() types.Aspect {
	return &TaskCreator{
		workflowEngine: aspect.workflowEngine,
		instanceDAO:    aspect.instanceDAO,
	}
}

// Type 切面类型标识。
func (aspect *TaskCreator) Type() string {
	return "task_creator"
}

// endNodeDedupLock end 节点按实例去重的进程内键锁。LocalLock 非重入、带 TTL，
// 这里只用于 TryLock 抢占（抢不到即放弃），不存在重入/死锁问题；TTL 兜底防止
// 持锁方崩溃后锁永不释放（实例完成后 end 不应再执行，TTL 到期释放无副作用）。
var endNodeDedupLock = lock.DefaultKeyLock

// endDedupTTL end 去重锁 TTL：持锁方崩溃后兜底释放；须覆盖 CompleteProcessInstance
// 与 onCompleted 规则链（可能含 LLM 调用）的执行时长，锁先过期会让对端副本重复执行 end。
const endDedupTTL = 2 * time.Minute

// endDedupLocker 取 end 去重锁实现：宿主注入了分布式锁（多副本部署）则跨副本
// 生效，否则退回进程内 LocalLock（单机部署）。
func (aspect *TaskCreator) endDedupLocker() lock.Locker {
	if aspect.workflowEngine != nil {
		if l := aspect.workflowEngine.GetLocker(); l != nil {
			return l
		}
	}
	return endNodeDedupLock
}

func endNodeLockKey(instanceID string) string {
	return "bpm:end-dedup:" + instanceID
}

// PointCut 确定此切面应用于哪些节点：TaskCreator 应用于所有节点；BPM 上下文
// 检查（metadata 中是否含 instance_id）放在 Before/After 内执行，便于在非 BPM
// 上下文（如编辑器测试、外部直接调用）跳过时输出 Debug 日志。
//
// 排除 fork/join 这类控制节点：它们的语义是"分发/合并"，本身不产生业务任务。
// join 节点会被每个父分支各 OnMsg 一次（第一次 TellCollect 返回 false 不放行，
// 第二次收齐才 TellSuccess），如果 aspect 给它们创建 wf_task，会产生永远停在
// Pending 的僵尸记录——因为第一次 OnMsg 不会触发 aspect.After 的 Complete。
func (aspect *TaskCreator) PointCut(ctx types.RuleContext, msg types.RuleMsg, relationType string) bool {
	nodeType := ctx.Self().Type()
	if nodeType == "fork" || nodeType == "join" {
		return false
	}
	// ccTask 节点自建逐用户抄送任务(approval_type=cc/status=completed/end_reason=cc)，
	// 若 aspect 再建一条 assignee=nil 的任务会产生审批类型错配(single)的幽灵记录，
	// 污染统计与审计。ccTask 的任务语义由节点自身负责，aspect 不介入。
	if nodeType == constants.TaskTypeCCTask {
		return false
	}
	return true
}

// Before 在节点处理前执行：非用户任务节点抢 end 去重锁、更新 currentActivity、
// 创建 wf_task（用户任务的任务创建由节点自身负责）。
func (aspect *TaskCreator) Before(ctx types.RuleContext, msg types.RuleMsg, relationType string) types.RuleMsg {
	// 非 BPM 上下文执行（如规则链测试运行、外部直接调用）时，metadata 中没有
	// instance_id，跳过当前活动节点更新与任务创建，避免用空 ID 触发无意义的 UPDATE
	instanceId := msg.GetMetadata().GetValue(constants.KeyInstanceID)
	if instanceId == "" {
		logrus.Debugf("TaskCreator.Before skipped: no instance_id in metadata, nodeId: %s", ctx.GetSelfId())
		return msg
	}
	// end 节点并发去重：并发 approve / fork-join 多分支恢复会让 end 被多次 OnMsg。
	// TryLock 按"首次执行"抢占：抢到者写入锁值并在 After 中执行完成实例等副作用；
	// 未抢到的重复执行直接返回（不建任务、不更新 current activity），保证
	// end 任务只创建一条、CompleteProcessInstance / onCompleted 规则链只触发一次。
	if ctx.Self().Type() == types.NodeTypeEnd {
		lockVal, ok, err := aspect.endDedupLocker().TryLock(ctx.GetContext(), endNodeLockKey(instanceId), endDedupTTL)
		if err != nil {
			// 锁服务异常时保守放行（宁可重复执行也不能卡死流程）
			logrus.WithError(err).Warnf("end-node dedup TryLock error, allow execution, instanceId: %s", instanceId)
		} else if ok {
			msg.GetMetadata().PutValue(constants.KeyEndExecLock, lockVal)
		} else {
			logrus.WithField("instanceId", instanceId).
				Info("TaskCreator.Before: duplicate end-node execution suppressed")
			return msg
		}
	}
	// 如果非用户任务则创建任务（用户任务在其自身节点创建任务）
	if ctx.Self().Type() != constants.TaskTypeUserTask {
		data := msg.GetData()
		var name, desc string
		var nodeConfig types.Configuration
		if chainCtx, ok := ctx.RuleChain().(types.ChainCtx); ok {
			if ruleNode, ok := chainCtx.Definition().GetNode(ctx.GetSelfId()); ok {
				name = ruleNode.Name
				nodeConfig = ruleNode.Configuration
				if v, ok := ruleNode.GetAdditionalInfo(constants.KeyDescription); ok {
					desc = cast.ToString(v)
				}
			}
		}
		err := aspect.setCurrentActivity(ctx.GetContext(), instanceId, ctx.GetSelfId())
		if err != nil {
			logrus.WithError(err).Errorf("update current activity error,instanceId: %s ,nodeId: %s", instanceId, ctx.GetSelfId())
		}
		// 恢复路径：metadata 里已有 task_id（由 RestoreProcessInstance 注入），
		// 说明该节点对应的 wf_task 已经存在（上次执行到一半被重启打断）。
		// 此时必须跳过 CreateTask，否则会产生重复 wf_task，且旧 task 永远停在 Pending。
		existingTaskId := msg.GetMetadata().GetValue(constants.KeyTaskID)
		if existingTaskId != "" {
			return msg
		}
		processId := msg.GetMetadata().GetValue(constants.KeyProcessID)
		// 记录任务
		createdAt := time.Now()
		task := &model.WfTask{
			ProcessInstanceID: &instanceId,
			ProcessID:         processId,
			TaskDefKey:        ctx.GetSelfId(),
			TaskType:          ctx.Self().Type(),
			Name:              name,
			Description:       &desc,
			Assignee:          nil,
			CreatedAt:         createdAt,
			CreatedBy:         constants.UserSystem,
			Status:            string(enums.TaskStatusPending),
			Variables:         &data,
			TenantID:          msg.GetMetadata().GetValue(constants.KeyTenantID),
		}
		// delay 任务建行时落到期时间：模板表达式只有此刻能求值，
		// 超期检测与救援据此判定
		if dueDate := delayTaskDueDate(ctx, msg, ctx.Self().Type(), nodeConfig, createdAt); dueDate != nil {
			task.DueDate = dueDate
		}
		// 节点自动创建任务：系统动作，操作人取 SystemActor
		_, err = aspect.workflowEngine.GetTaskService().CreateTask(ctx.GetContext(), SystemActor(), task)
		if err != nil {
			logrus.WithError(err).Errorf("create Task error,instanceId: %s ,nodeId: %s", instanceId, ctx.GetSelfId())
		}
		msg.GetMetadata().PutValue(constants.KeyTaskID, task.ID)
	}

	return msg
}

// After 在节点处理后执行：完成任务；end 节点额外完成流程实例并触发
// onCompleted 规则链（经 KeyEndExecLock 去重，见 Before）。
func (aspect *TaskCreator) After(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) types.RuleMsg {
	// 非 BPM 上下文执行时跳过任务完成与流程实例完成逻辑（与 Before 对称）
	instanceId := msg.GetMetadata().GetValue(constants.KeyInstanceID)
	if instanceId == "" {
		logrus.Debugf("TaskCreator.After skipped: no instance_id in metadata, nodeId: %s", ctx.GetSelfId())
		return msg
	}
	// 与 Before 的 end 去重配对：只有抢到锁的首次执行才走后续收尾（完成 end 任务、
	// 完成实例、触发 onCompleted 规则链）；重复执行没有锁值标记，直接跳过。
	if ctx.Self().Type() == types.NodeTypeEnd &&
		msg.GetMetadata().GetValue(constants.KeyEndExecLock) == "" {
		return msg
	}
	taskId := msg.GetMetadata().GetValue(constants.KeyTaskID)
	// 内部回调的显式操作人：优先沿用链执行 ctx 携带的身份（含租户，供租户校验/审计）；
	// ctx 无身份时按系统动作处理，并从链元数据补齐租户（租户校验依赖 TenantID）。
	internalActor := ActorFromCtx(ctx.GetContext())
	if internalActor.TenantID == "" {
		internalActor.TenantID = msg.GetMetadata().GetValue(constants.KeyTenantID)
	}
	// 非用户任务：将节点任务置为完成（用户任务由审批人显式完成后才走到 After）
	if ctx.Self().Type() != constants.TaskTypeUserTask {
		var variables map[string]interface{}
		if jsonData, err := msg.GetJsonData(); err == nil {
			if v, ok := jsonData.(map[string]interface{}); ok {
				variables = v
			}
		}
		// 更新任务完成
		// 引擎 aspect 调用 Complete 属于内部路径（节点自动推进），显式标记 CallingModeInternal
		// 避免误判为越权。
		internalCtx := WithInternalCallingMode(ctx.GetContext())
		if err := aspect.workflowEngine.GetTaskService().Complete(internalCtx, internalActor, taskId, variables); err != nil {
			logrus.WithError(err).Errorf("complete task error, instanceId: %s, nodeId: %s", instanceId, ctx.GetSelfId())
		}
	}

	// 如果 relationType 等于 "Failure"：
	//   - 节点声明了 Failure 出边 = 设计者显式定义了失败分支（如 subProcess 子实例
	//     被终止 → 父流程经 Failure 边续跑下游），消息已沿边路由，【不】终止实例；
	//   - 无 Failure 出边 = 意外失败，消息将丢失、实例会永久卡死 → 终止实例留痕。
	if relationType == types.Failure && !aspect.nodeHasFailureEdge(ctx, msg) {
		aspect.handleProcessInstanceFailure(ctx, msg, err)
	}
	if ctx.Self().Type() == types.NodeTypeEnd {
		lockVal := msg.GetMetadata().GetValue(constants.KeyEndExecLock)
		defer func() {
			if err := aspect.endDedupLocker().Unlock(context.Background(), endNodeLockKey(instanceId), lockVal); err != nil {
				logrus.WithError(err).Debugf("end-node dedup unlock failed, instanceId: %s", instanceId)
			}
		}()
		internalCtx := WithInternalCallingMode(ctx.GetContext())
		// end 节点归档实例：操作人沿用链执行 ctx 身份（租户校验依赖其 TenantID）
		processErr := aspect.workflowEngine.GetRuntimeService().CompleteProcessInstance(internalCtx, internalActor, instanceId, "")
		if processErr != nil {
			logrus.WithError(processErr).Errorf("complete process instance error,instanceId: %s ,nodeId: %s", instanceId, ctx.GetSelfId())
		}
		// 审批流程完成后，触发关联的规则链
		aspect.triggerOnCompleted(ctx, msg, instanceId)
	}
	return msg
}

func (aspect *TaskCreator) setCurrentActivity(ctx context.Context, processInstanceID, nodeID string) error {
	now := time.Now()
	var instance = &model.WfInstance{
		ID:              processInstanceID,
		UpdatedAt:       &now,
		CurrentActivity: &nodeID,
	}
	return aspect.instanceDAO.Update(ctx, instance)
}

// handleProcessInstanceFailure 处理流程实例失败
func (aspect *TaskCreator) handleProcessInstanceFailure(ctx types.RuleContext, msg types.RuleMsg, err error) {
	instanceId := msg.GetMetadata().GetValue(constants.KeyInstanceID)
	if instanceId == "" {
		return
	}

	// 构建失败原因
	reason := "Process execution failed"
	if err != nil {
		reason = err.Error()
	}

	// 走完整终止链路（级联终止任务+归档+事件）：只改实例状态会把活跃任务留在
	// 候选人待办里（后续操作全被实例终态守卫拒绝），实例行也不归档
	internalCtx := WithInternalCallingMode(ctx.GetContext())
	actor := ActorFromCtx(internalCtx)
	if actor.TenantID == "" {
		actor.TenantID = msg.GetMetadata().GetValue(constants.KeyTenantID)
	}
	if terminateErr := aspect.workflowEngine.GetRuntimeService().TerminateProcessInstance(internalCtx, actor, instanceId, reason); terminateErr != nil {
		logrus.WithError(terminateErr).Errorf("Failed to terminate process instance: %s", instanceId)
	} else {
		logrus.Infof("Process instance terminated due to failure: %s, reason: %s", instanceId, reason)
	}
}

// triggerOnCompleted 审批流程完成后，触发关联的规则链
func (aspect *TaskCreator) triggerOnCompleted(ctx types.RuleContext, msg types.RuleMsg, instanceId string) {
	executor := aspect.workflowEngine.GetRuleChainExecutor()
	if executor == nil {
		return
	}
	processId := msg.GetMetadata().GetValue(constants.KeyProcessID)
	if processId == "" {
		return
	}
	process, err := aspect.workflowEngine.GetProcessService().Get(ctx.GetContext(), processId)
	if err != nil || process == nil || process.Ext == nil || *process.Ext == "" {
		return
	}

	var ext struct {
		OnCompleted *struct {
			ChainId string `json:"chainId"`
			Mode    string `json:"mode"`
		} `json:"onCompleted"`
	}
	if err := json.Unmarshal([]byte(*process.Ext), &ext); err != nil || ext.OnCompleted == nil || ext.OnCompleted.ChainId == "" {
		return
	}

	chainId := ext.OnCompleted.ChainId

	if ext.OnCompleted.Mode == "sync" {
		if err := executor.Execute(chainId, msg); err != nil {
			logrus.WithError(err).Errorf("trigger onCompleted rule chain error, chainId: %s, instanceId: %s", chainId, instanceId)
		}
	} else {
		executor.ExecuteAsync(chainId, msg)
	}
}

// nodeHasFailureEdge 判断当前失败节点在流程定义里是否声明了 Failure 出边。
// 有出边 = 设计者显式定义了失败分支（消息由 rulego 沿边路由到下游），实例继续；
// 无出边 = 意外失败，由调用方终止实例避免卡死。
// 定义读取失败时按"无出边"处理（fail-closed：宁可终止留痕，不可静默卡死）。
func (aspect *TaskCreator) nodeHasFailureEdge(ctx types.RuleContext, msg types.RuleMsg) bool {
	processId := msg.GetMetadata().GetValue(constants.KeyProcessID)
	if processId == "" {
		return false
	}
	process, err := aspect.workflowEngine.GetProcessService().Get(ctx.GetContext(), processId)
	if err != nil || process == nil {
		return false
	}
	selfId := ctx.GetSelfId()
	var doc struct {
		Metadata struct {
			Connections []struct {
				FromId string `json:"fromId"`
				ToId   string `json:"toId"`
				Type   string `json:"type"`
			} `json:"connections"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal([]byte(process.DefinitionJSON), &doc); err != nil {
		return false
	}
	for _, conn := range doc.Metadata.Connections {
		if conn.FromId == selfId && conn.Type == types.Failure {
			return true
		}
	}
	return false
}

// delayTaskDueDate 计算 delay 任务行的到期时间（base + 时长）；非 delay 节点
// 或求不出时长时返回 nil，DueDate 留空。时长来源与 delay 节点自身口径一致：
// delayMs 优先（纯数字直取，模板表达式用消息上下文求值），兼容已废弃的
// periodInSeconds（×1000）。
func delayTaskDueDate(ctx types.RuleContext, msg types.RuleMsg, nodeType string, nodeConfig types.Configuration, base time.Time) *time.Time {
	if nodeType != constants.NodeTypeDelay {
		return nil
	}
	if ms, ok := delayDurationMs(ctx, msg, nodeConfig); ok {
		due := base.Add(time.Duration(ms) * time.Millisecond)
		return &due
	}
	logrus.Debugf("delay task due date unresolved, nodeId: %s", ctx.GetSelfId())
	return nil
}

// delayDurationMs 从节点配置解析延迟毫秒数：delayMs 纯数字直接取，模板表达式
// 按节点消息上下文求值后取整；均不可用时回退 periodInSeconds。解析失败返回 false。
func delayDurationMs(ctx types.RuleContext, msg types.RuleMsg, nodeConfig types.Configuration) (int64, bool) {
	delayMs := strings.TrimSpace(cast.ToString(nodeConfig["delayMs"]))
	if delayMs != "" {
		if v, err := strconv.ParseInt(delayMs, 10, 64); err == nil {
			return v, true
		}
		tmpl, err := el.NewTemplate(delayMs)
		if err != nil {
			return 0, false
		}
		rendered := strings.TrimSpace(tmpl.ExecuteAsString(ctx.GetEnv(msg, true)))
		if v, err := strconv.ParseInt(rendered, 10, 64); err == nil {
			return v, true
		}
		return 0, false
	}
	if seconds := cast.ToInt(nodeConfig["periodInSeconds"]); seconds > 0 {
		return int64(seconds) * 1000, true
	}
	return 0, false
}
