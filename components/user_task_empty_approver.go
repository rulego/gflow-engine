// This file implements the emptyApproverPolicy fallback for userTask nodes:
// when approver resolution yields no members, the node no longer fails the
// instance but routes by node-level policy (tenant admin / auto approve / park).

package components

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
)

// 审批人为空时的节点级兜底策略（emptyApproverPolicy）。
const (
	// EmptyApproverPolicyTenantAdmin 转交租户管理员（缺省）：经 TenantAdminResolver
	// SPI 解析管理员作为兜底审批人，剔除发起人防止自批。
	EmptyApproverPolicyTenantAdmin = "tenant_admin"
	// EmptyApproverPolicyAutoApprove 自动通过：任务以发起人名义照建，锁外由系统
	// 身份按通过完成并推进流程，审批意见注明自动通过缘由。
	EmptyApproverPolicyAutoApprove = "auto_approve"
	// EmptyApproverPolicyPark 挂起待指派：任务照建进入待认领状态，等管理员
	// 直接指派（SetAssignee）或补候选池后认领。
	EmptyApproverPolicyPark = "park"
)

// parkedCandidatePlaceholder park 且无组实体、管理员解析也不可用时的占位候选。
// 不对应任何真实用户，展开为空即无人可认领——仅用于封住"空池任务可被任意
// 同租户用户认领"的口子（见 createClaimTask 的候选写入失败回滚注释）。
const parkedCandidatePlaceholder = "__parked__"

// isValidEmptyApproverPolicy 与 Validate/Init 共用的取值合法性判定（空串由
// Normalize 落缺省，这里一并放行便于装配期先后顺序无关）。
func isValidEmptyApproverPolicy(v string) bool {
	switch v {
	case "", EmptyApproverPolicyTenantAdmin, EmptyApproverPolicyAutoApprove, EmptyApproverPolicyPark:
		return true
	}
	return false
}

// handleEmptyApprovers 审批人解析为空的分流入口（createUserTasks 空成员分支）。
// 仅接管"成员集合为空"：身份服务查询报错（组织数据/配置损坏）仍走原失败路径，
// 两类问题的处置方式不同，不在同一分支混判。
func (n *UserTaskNode) handleEmptyApprovers(ctx types.RuleContext, processInstanceID, processID, tenantID, owner string, variables map[string]interface{}, dueDate *time.Time) error {
	switch n.Config.EmptyApproverPolicy {
	case EmptyApproverPolicyAutoApprove:
		if owner != "" {
			applyFallbackVars(variables, EmptyApproverPolicyAutoApprove, approverSummary(&n.Config.Approver), "审批人为空，自动通过")
			return n.createAutoApproveTask(ctx, processInstanceID, processID, tenantID, owner, variables, dueDate, "审批人为空，自动通过")
		}
		// 无发起人（系统驱动消息）无法构造"以谁的名义自动通过"，降级挂起
		logrus.WithField("node", n.GetSelfId()).Warn("emptyApproverPolicy=auto_approve without owner; degrade to park")
		return n.parkEmptyApproverTask(ctx, processInstanceID, processID, tenantID, variables, dueDate)
	case EmptyApproverPolicyPark:
		return n.parkEmptyApproverTask(ctx, processInstanceID, processID, tenantID, variables, dueDate)
	default:
		rawAdmins := n.resolveTenantAdminIDs(ctx.GetContext(), tenantID)
		admins := excludeID(rawAdmins, owner)
		switch {
		case len(admins) == 1:
			applyFallbackVars(variables, EmptyApproverPolicyTenantAdmin, approverSummary(&n.Config.Approver), "审批人为空，已转交租户管理员")
			return n.createSingleTask(ctx, processInstanceID, processID, tenantID, admins[0], variables, dueDate, "审批人为空，已转交租户管理员")
		case len(admins) > 1:
			// 多管理员：建代认领任务，先认领先办，与角色候选任务的语义一致
			applyFallbackVars(variables, EmptyApproverPolicyTenantAdmin, approverSummary(&n.Config.Approver), "审批人为空，已转交租户管理员")
			return n.createClaimTaskWithCandidates(ctx, processInstanceID, processID, tenantID, admins, variables, dueDate, "审批人为空，已转交租户管理员")
		case len(rawAdmins) > 0:
			// 唯一管理员就是发起人：转交即自批，改走自动通过
			applyFallbackVars(variables, EmptyApproverPolicyAutoApprove, approverSummary(&n.Config.Approver), "审批人为空且管理员即发起人，自动通过")
			return n.createAutoApproveTask(ctx, processInstanceID, processID, tenantID, owner, variables, dueDate, "审批人为空且管理员即发起人，自动通过")
		default:
			// 管理员不存在/全部停用，或宿主未实现 TenantAdminResolver：按挂起降级，
			// 不失败——兜底链的终点是"有人能处理"，park 至少保住流程数据
			logrus.WithField("node", n.GetSelfId()).Warn("no tenant admin resolvable for empty approvers; degrade to park")
			return n.parkEmptyApproverTask(ctx, processInstanceID, processID, tenantID, variables, dueDate)
		}
	}
}

// createAutoApproveTask 自动通过路径的任务构造：单任务直接以发起人受理；
// 会签/票签的完成判定依赖父+子任务结构（零子任务报 ErrNoSubTasks），以发起人
// 充当唯一子任务，完成动作由 autoApproveEmptyApproverTasks 在锁外执行，
// 阈值判定/父任务收尾复用既有机制。
func (n *UserTaskNode) createAutoApproveTask(ctx types.RuleContext, processInstanceID, processID, tenantID, owner string, variables map[string]interface{}, dueDate *time.Time, reason string) error {
	if ct := enums.ApprovalType(n.Config.ApproveMode); ct == enums.ApprovalTypeCountersign || ct == enums.ApprovalTypeVote {
		// 合成单票结构必须以 1 票即过的规则收尾：节点原票签阈值（如 count=3）
		// 在单票下永远凑不齐，会把"自动通过"判成"自动拒绝"
		return n.createCountersignTasks(ctx, processInstanceID, processID, tenantID, []string{owner}, variables, dueDate, autoApproveCountersignRule)
	}
	return n.createSingleTask(ctx, processInstanceID, processID, tenantID, owner, variables, dueDate, reason)
}

// autoApproveCountersignRule 兜底自动通过合成结构专用的会签规则（any：一人同意即通过）
const autoApproveCountersignRule = `{"type":"any"}`

// parkEmptyApproverTask 挂起待指派：任务照建（Pending），等待管理员直接指派或补候选池。
// 不变式：落库任务的候选池必须非空——空池任务会被任意同租户用户认领（越权）。
func (n *UserTaskNode) parkEmptyApproverTask(ctx types.RuleContext, processInstanceID, processID, tenantID string, variables map[string]interface{}, dueDate *time.Time) error {
	applyFallbackVars(variables, EmptyApproverPolicyPark, approverSummary(&n.Config.Approver), "审批人为空，任务挂起待指派")
	// 组池节点：候选实体照写，成员展开为空即天然无人可认领。
	// 组 ID 全空（手写 DSL 可绕过部署期校验）按无组实体处理，
	// 否则任务零候选落库、空池可被任意同租户用户认领。
	if groups := approverPoolGroups(&n.Config.Approver); len(groups) > 0 {
		return n.createClaimTask(ctx, processInstanceID, processID, tenantID, variables, dueDate, groups)
	}
	// 无组实体的节点（发起人自选解析为空等）：落管理员 person 候选，管理员可直接
	// 认领处理；SPI 不可用时落占位候选封住空池
	candidates := n.resolveTenantAdminIDs(ctx.GetContext(), tenantID)
	if len(candidates) == 0 {
		candidates = []string{parkedCandidatePlaceholder}
	}
	return n.createClaimTaskWithCandidates(ctx, processInstanceID, processID, tenantID, candidates, variables, dueDate, "审批人为空，任务挂起待指派")
}

// hasNonEmptyID 判断候选 ID 列表是否含有效 ID（空串剔除后非空）
func hasNonEmptyID(ids []string) bool {
	for _, id := range ids {
		if id != "" {
			return true
		}
	}
	return false
}

// resolveTenantAdminIDs 经宿主注入的 TenantAdminResolver 解析租户管理员。
// SPI 未实现或查询失败返回 nil（调用方按降级处理），不向上抛错——兜底路径
// 自身不能再成为实例失败的新来源。
func (n *UserTaskNode) resolveTenantAdminIDs(ctx context.Context, tenantID string) []string {
	if n.IdentityService == nil || tenantID == "" {
		return nil
	}
	resolver, ok := n.IdentityService.(service.TenantAdminResolver)
	if !ok {
		return nil
	}
	ids, err := resolver.GetTenantAdminUserIDs(ctx, tenantID)
	if err != nil {
		logrus.WithError(err).WithField("tenantID", tenantID).Warn("resolve tenant admins failed; empty-approver fallback degrades")
		return nil
	}
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// createClaimTaskWithCandidates 按显式候选列表建代认领任务（兜底路径专用：
// 候选来自管理员解析或占位符，而非节点 approver 配置的组实体）。
// AddCandidates 失败时回滚已创建任务，与 createClaimTask 的防越权口径一致。
func (n *UserTaskNode) createClaimTaskWithCandidates(ctx types.RuleContext, processInstanceID, processID, tenantID string, candidateIDs []string, variables map[string]interface{}, dueDate *time.Time, reason string) error {
	desc := n.TaskDescription
	vars := serializeVariables(variables)
	task := &model.WfTask{
		ProcessInstanceID: &processInstanceID,
		ProcessID:         processID,
		TaskDefKey:        n.GetSelfId(),
		TaskType:          UserTaskNodeType,
		Name:              n.TaskName,
		Description:       &desc,
		Status:            string(enums.TaskStatusPending),
		Variables:         &vars,
		ApprovalType:      n.Config.ApproveMode,
		ApprovalRule:      &n.approvalRule,
		TenantID:          tenantID,
		FormKey:           formKeyPtr(n.Config.FormKey),
		CreatedBy:         constants.UserSystem,
		CreatedAt:         time.Now(),
		DueDate:           dueDate,
	}

	taskID, err := n.TaskService.CreateTask(ctx.GetContext(), service.SystemActor(), task)
	if err != nil {
		return err
	}

	filtered := make([]string, 0, len(candidateIDs))
	for _, id := range candidateIDs {
		if id != "" {
			filtered = append(filtered, id)
		}
	}
	if len(filtered) > 0 {
		if cErr := n.TaskService.AddCandidates(ctx.GetContext(), systemActorForTenant(tenantID), taskID, string(enums.EntityTypePerson), filtered); cErr != nil {
			_ = n.TaskService.DeleteTask(ctx.GetContext(), service.SystemActor(), taskID, "candidate write failed")
			return fmt.Errorf("failed to add fallback candidates for task %s: %w", taskID, cErr)
		}
		// person 候选的成员即候选本身，适配 notifyCandidateCreated 的展开器形态；
		// 占位候选 __parked__ 只用于封池，不对应真实用户，不进通知收件人
		notifyIDs := make([]string, 0, len(filtered))
		for _, id := range filtered {
			if id != parkedCandidatePlaceholder {
				notifyIDs = append(notifyIDs, id)
			}
		}
		if len(notifyIDs) > 0 {
			n.notifyCandidateCreated(ctx, taskID, processInstanceID, processID, tenantID, notifyIDs,
				func(_ context.Context, _ string, id string) ([]string, error) { return []string{id}, nil },
				reason)
		}
	}
	return nil
}

// autoApproveEmptyApproverTasks emptyApproverPolicy=auto_approve（以及 tenant_admin
// 下"唯一管理员即发起人"转自动通过）的锁外自动完成：完成动作经
// AfterCommit→ExecuteNext 重入本节点，必须与 autoApproveOwnerTasks 一样在创建锁
// 释放后调用（taskOpMutex 非重入）。幂等：已完成的任务被状态过滤，重入不再命中。
// tenant_admin 正常路径下创建的是管理员任务/代认领任务，不会出现发起人受理的
// active 任务，此钩子对其天然无操作。完成失败时任务留在待办，退化为手动审批。
func (n *UserTaskNode) autoApproveEmptyApproverTasks(ctx types.RuleContext, msg types.RuleMsg, processInstanceID string) {
	if p := n.Config.EmptyApproverPolicy; p != EmptyApproverPolicyAutoApprove && p != EmptyApproverPolicyTenantAdmin {
		return
	}
	if n.TaskService == nil {
		return
	}
	owner := metaValue(msg, constants.KeyOwner)
	if owner == "" {
		return
	}
	// 含会签/票签子任务（无 ParentIDIsNull 过滤），与 autoApproveOwnerTasks 同口径
	query := &dto.TaskQuery{
		InstanceID: &processInstanceID,
		TaskDefKey: n.GetSelfId(),
	}
	tasks, _, err := n.TaskService.GetTaskList(ctx.GetContext(), service.ActorFromCtx(ctx.GetContext()), query)
	if err != nil {
		logrus.WithError(err).Warnf("empty-approver auto approve: query tasks of node %s failed, skip", n.GetSelfId())
		return
	}
	for _, t := range tasks {
		if t.Status != string(enums.TaskStatusActive) || t.Assignee == nil || *t.Assignee != owner {
			continue
		}
		// 只完成带兜底标记的任务（applyFallbackVars 写入 fallback_policy）。
		// tenant_admin 是缺省策略，本钩子会在每次建任务后触发：常规流转里
		// "审批人恰好是发起人"（selfApproval=none）的任务必须留给人工审批，
		// 不能被这里误伤成自动通过。
		reason := fallbackReasonFromVars(t.Variables)
		if reason == "" {
			continue
		}
		comment := reason + "（emptyApproverPolicy 兜底自动通过）"
		req := &service.ApprovalRequest{
			TaskID:         t.ID,
			ApprovalResult: enums.ApprovalResultApproved,
			Comment:        comment,
			Variables:      map[string]interface{}{},
		}
		if err := n.TaskService.CompleteWithApproval(service.WithInternalCallingMode(ctx.GetContext()), service.SystemActor(), req); err != nil {
			logrus.WithError(err).WithField("taskId", t.ID).
				Warn("empty-approver auto approve failed; task left active for manual approval")
		} else {
			logrus.WithFields(logrus.Fields{"taskId": t.ID, "node": n.GetSelfId()}).
				Info("empty-approver task auto approved")
		}
	}
}

// fallbackReasonFromVars 从任务变量读取兜底缘由（applyFallbackVars 写入），
// 自动完成的审批意见据此留痕。解析失败返回空串走通用文案。
func fallbackReasonFromVars(raw *string) string {
	if raw == nil || *raw == "" {
		return ""
	}
	vars, err := service.ParseVariablesJSON(raw)
	if err != nil {
		return ""
	}
	if r, ok := vars["fallback_reason"].(string); ok {
		return r
	}
	return ""
}

// applyFallbackVars 把兜底留痕写入任务变量（对标 reassign_* 五件套的留痕方式），
// 详情面板与通知据此展示"为什么这单到了我手上"。
func applyFallbackVars(variables map[string]interface{}, policy, from, reason string) {
	if variables == nil {
		return
	}
	variables["fallback_policy"] = policy
	variables["fallback_from"] = from
	variables["fallback_reason"] = reason
	variables["fallback_time"] = time.Now()
}

// approverSummary 生成原审批人配置的摘要（fallback_from 取值），
// 形如 role:r001,r002，供详情面板展示"原审批人是谁"。
func approverSummary(c *ApproverConfig) string {
	ids := c.UserIds
	switch enums.CandidateType(c.Type) {
	case enums.CandidateTypeRole:
		ids = c.RoleIds
	case enums.CandidateTypeDept:
		ids = c.DeptIds
	}
	if len(ids) == 0 {
		return c.Type
	}
	return fmt.Sprintf("%s:%s", c.Type, strings.Join(ids, ","))
}

// excludeID 从候选中剔除指定用户（兜底审批人=发起人时防止自批）。
func excludeID(ids []string, exclude string) []string {
	if exclude == "" || len(ids) == 0 {
		return ids
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != "" && id != exclude {
			out = append(out, id)
		}
	}
	return out
}
