package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"

	"github.com/rulego/gflow-engine/model"
)

// GetProcessInstanceDetail 获取流程实例详情（时间线、流程定义、当前用户任务与权限）
// 入参：actor 查询视角的用户（操作人）；processInstanceID 流程实例ID
// 出参：dto.InstanceDetailResponse
func (s *RuntimeServiceImpl) GetProcessInstanceDetail(ctx context.Context, actor Actor, processInstanceID string) (*dto.InstanceDetailResponse, error) {
	ctx = bindActor(ctx, actor)
	currentUserId := actor.UserID
	// 1. 获取流程实例
	instance, err := s.GetProcessInstance(ctx, actor, processInstanceID)
	if err != nil {
		return nil, err
	}
	if instance == nil {
		return nil, fmt.Errorf("instance not found")
	}

	if err := ensureTenantAccess(ctx, "process instance", instance.TenantID); err != nil {
		return nil, err
	}

	// 2. 查询该实例下任务列表并组装审批时间线
	// 可见性校验需要覆盖历史 assignee 与 CC 抄送归属，故终态实例查历史、活态实例同时查运行时+历史
	taskQuery := &dto.TaskQuery{InstanceID: &processInstanceID}
	if instance.Status == string(enums.InstanceStatusActive) || instance.Status == string(enums.InstanceStatusSuspended) {
		taskQuery.QueryHistory = false
	} else {
		taskQuery.QueryHistory = true
	}
	tasks, _, err := s.workflowEngine.GetTaskService().GetTaskList(ctx, actor, taskQuery)
	if err != nil {
		return nil, err
	}

	// 活态实例主查询只读运行时表，可见性判定需要补查历史 task（已 completed 的前序节点 assignee + CC 抄送归属）
	visibilityTasks := tasks
	if !taskQuery.QueryHistory {
		histQuery := &dto.TaskQuery{InstanceID: &processInstanceID, QueryHistory: true}
		if histTasks, _, histErr := s.workflowEngine.GetTaskService().GetTaskList(ctx, actor, histQuery); histErr == nil {
			visibilityTasks = append(visibilityTasks, histTasks...)
		}
	}

	// 可见性校验：当前用户必须是 发起人 / 该实例任一 task 的 assignee（含历史）/ CC 抄送归属，否则 IDOR 越权拒绝。
	// actor.UserID 为空即拒绝（fail-closed）：宿主漏传操作人时不得因缺身份而放行。
	// 工作流管理员（Actor.SuperAdmin）放行——管理侧需要查看全部实例。
	if u := GetUserFromCtx(ctx); u != nil && u.SuperAdmin {
		// 管理员放行
	} else if currentUserId == "" {
		return nil, fmt.Errorf("instance %s detail requires an actor with user id: %w", processInstanceID, ErrAuthenticationRequired)
	} else if !isInstanceVisibleToUser(instance, visibilityTasks, currentUserId) && !s.isPendingCandidateVisible(ctx, visibilityTasks, currentUserId) {
		return nil, fmt.Errorf("current user %s has no view permission on instance %s: %w", currentUserId, processInstanceID, ErrPermissionDenied)
	}
	var approvalList []dto.ExecutionInfo
	// 回退/撤回会把 returned/terminated 任务归档进历史表并从运行时表删除，
	// 活态实例只读运行时表就看不到这些痕迹，故并入历史条目（visibilityTasks
	// 已是两表全集），再按创建时间排序还原节点先后。
	timelineTasks := tasks
	if !taskQuery.QueryHistory {
		seen := make(map[string]bool, len(visibilityTasks))
		for _, t := range tasks {
			seen[t.ID] = true
		}
		for _, t := range visibilityTasks {
			if !seen[t.ID] {
				timelineTasks = append(timelineTasks, t)
				seen[t.ID] = true
			}
		}
		sort.SliceStable(timelineTasks, func(i, j int) bool {
			return timelineTasks[i].CreatedAt.Before(timelineTasks[j].CreatedAt)
		})
	}
	for _, task := range timelineTasks {
		if task.ParentID == nil || *task.ParentID == "" {
			approvalList = append(approvalList, Task2ExecutionInfo(task, timelineTasks))
		}
	}

	// 3. 当前用户在该实例上的待审批任务 - 查找 active/suspended/pending 状态的任务
	var currentUserTask *model.WfTask
	var currentUserTaskStatus string // 记录找到的任务状态，用于权限判断
	if currentUserId != "" {
		// 优先查找 active 任务
		for _, t := range tasks {
			if t.Assignee != nil && *t.Assignee == currentUserId && t.Status == string(enums.TaskStatusActive) {
				currentUserTask = t
				currentUserTaskStatus = string(enums.TaskStatusActive)
				break
			}
		}
		// 其次查找 suspended 任务（挂起的任务可唤醒）
		if currentUserTask == nil {
			for _, t := range tasks {
				if t.Assignee != nil && *t.Assignee == currentUserId && t.Status == string(enums.TaskStatusSuspended) {
					currentUserTask = t
					currentUserTaskStatus = string(enums.TaskStatusSuspended)
					break
				}
			}
		}
		// 最后查找 pending 任务（候选组待认领）：仅选中当前用户是候选成员的任务。
		// 同节点候选结果调用内缓存，避免对同一 defKey 重复展开角色/部门
		if currentUserTask == nil {
			candMemo := map[string]bool{}
			for _, t := range tasks {
				if t.Status != string(enums.TaskStatusPending) {
					continue
				}
				ok, cached := candMemo[t.TaskDefKey]
				if !cached {
					ok = s.isUserCandidate(ctx, t, currentUserId)
					candMemo[t.TaskDefKey] = ok
				}
				if ok {
					currentUserTask = t
					currentUserTaskStatus = string(enums.TaskStatusPending)
					break
				}
			}
		}
	}

	// 4. 流程定义与节点配置
	var definitionJSON string
	var formSchemaJSON string
	var processName string                                  // 流程定义名称：详情头部/打印模板展示
	var actionPermissions = map[string]interface{}{}        // 审批人节点（userTask）的设计器动作权限
	var starterActionPermissions = map[string]interface{}{} // 发起人动作权限（流程级 ruleChain.additionalInfo.actionPermissions）
	var formPermissions = map[string]string{}
	if instance.ProcessID != "" {
		procDef, err := s.workflowEngine.GetProcessService().Get(ctx, instance.ProcessID)
		if err == nil && procDef != nil {
			definitionJSON = procDef.DefinitionJSON
			processName = procDef.Name

			// 从 definitionJson 中提取 form schema
			var defData map[string]interface{}
			if err := json.Unmarshal([]byte(definitionJSON), &defData); err == nil {
				if form, ok := defData["form"]; ok {
					if formBytes, err := json.Marshal(form); err == nil {
						formSchemaJSON = string(formBytes)
					}
				}
			}
			if formSchemaJSON == "" {
				formSchemaJSON = "{}"
			}

			// 设计器配置分两层：
			// - ruleChain.additionalInfo.actionPermissions：流程级发起人动作（withdraw/resubmit/urge/suspend/terminate）
			//   —— 即流程级"高级设置"，不挂在 startTask 节点上
			// - userTask（审批人节点）的 additionalInfo.actionPermissions：审批人视角动作（addComment/transfer/return/terminate/uploadAttachment/awaken）
			if rc, err := procDef.ToRuleChain(); err == nil {
				// 流程级发起人动作权限
				if ap, ok := rc.RuleChain.GetAdditionalInfo("actionPermissions"); ok {
					if v, ok := ap.(map[string]interface{}); ok {
						starterActionPermissions = v
					}
				}

				if currentUserTask != nil {
					// 审批人节点 actionPermissions：复用 service 层校验同一解析路径，
					// 保证详情按钮显隐与 service 强制校验判定一致。
					actionPermissions = resolveNodeActionPermissions(ctx, s.workflowEngine, processInstanceID, currentUserTask.TaskDefKey)

					if node, ok := rc.GetNode(currentUserTask.TaskDefKey); ok {
						if ap, ok := node.GetAdditionalInfo("formPermissions"); ok {
							// additionalInfo 经 JSON 反序列化为 map[string]interface{}，字段值需逐键断言为 string 取出。
							if v, ok := ap.(map[string]interface{}); ok {
								for k, val := range v {
									if s, ok := val.(string); ok {
										formPermissions[k] = s
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// 5. 变量（来自任务）
	var variables map[string]interface{}
	sourceTask := currentUserTask
	if sourceTask == nil && len(tasks) > 0 {
		sourceTask = tasks[len(tasks)-1]
	}
	if sourceTask != nil {
		// 解析失败保持 nil（响应里变量为空），不阻断详情装配
		if v, err := ParseVariablesJSON(sourceTask.Variables); err == nil {
			variables = v
		}
	}

	// 6. 组装响应。ActionPermissions 在第 7 步按"状态 × 设计器"二维合并后填入
	resp := &dto.InstanceDetailResponse{
		InstanceID:        instance.ID,
		InstanceStatus:    instance.Status,
		Name:              instance.Name,
		ProcessName:       processName,
		StartUserID:       instance.StartUserID,
		StartTime:         instance.CreatedAt,
		DefinitionJSON:    definitionJSON,
		FormSchemaJSON:    formSchemaJSON,
		Executions:        approvalList,
		Variables:         variables,
		ActionPermissions: map[string]interface{}{},
	}

	// 7. 基于"状态 × 设计器"二维计算动作权限
	// 设计器不可控的动作（核心审批 + 任务生命周期），按状态强制开放：
	//   approve / reject / claim / unclaim / activate / delete / suspend
	// 设计器可控的动作（默认开/关由前端 DEFAULT_ACTION_PERMISSIONS 决定）：
	//   addComment / withdraw / awaken / resubmit / transfer / return / urge / terminate / uploadAttachment
	// 合并语义：
	//   - 状态强制键：状态命中即开放，忽略 designer
	//   - 设计器可控键：状态命中 AND designer 未显式 false 才开放
	//   - designer 显式开启的可选键（transfer/return/urge/uploadAttachment）：仅 active 任务才有意义
	// 注意：发起人和审批人权限可叠加（同一用户可能既是发起人又是审批人），前端通过 pageType 区分显示
	isActiveTask := currentUserTask != nil && currentUserTaskStatus == string(enums.TaskStatusActive)

	if instance.StartUserID == currentUserId {
		// 发起人动作的设计器开关来自流程级 additionalInfo.actionPermissions（starterActionPermissions）
		switch instance.Status {
		case string(enums.InstanceStatusDraft):
			resp.ActionPermissions["activate"] = true // 草稿→发起审批（实例级，必备）
			resp.ActionPermissions["delete"] = true   // 草稿→可删除（实例级，必备）
		case string(enums.InstanceStatusActive):
			// suspend / withdraw 受流程级配置控制
			if !designerDisabled(starterActionPermissions, "suspend") {
				resp.ActionPermissions["suspend"] = true
			}
			if !designerDisabled(starterActionPermissions, "withdraw") {
				resp.ActionPermissions["withdraw"] = true
			}
			// urge：发起人催办（active 实例时可催办审批人）
			if !designerDisabled(starterActionPermissions, "urge") {
				resp.ActionPermissions["urge"] = true
			}
		case string(enums.InstanceStatusSuspended):
			resp.ActionPermissions["activate"] = true // 已挂起→可恢复（实例级，必备）
			if !designerDisabled(starterActionPermissions, "terminate") {
				resp.ActionPermissions["terminate"] = true
			}
		case string(enums.InstanceStatusFailed):
			resp.ActionPermissions["activate"] = true // 失败→可恢复（实例级，必备）
			if !designerDisabled(starterActionPermissions, "resubmit") {
				resp.ActionPermissions["resubmit"] = true
			}
		case string(enums.InstanceStatusTerminated):
			if !designerDisabled(starterActionPermissions, "resubmit") {
				resp.ActionPermissions["resubmit"] = true
			}
			// completed / cancelled: 终态，无操作权限
		}
	}

	if currentUserTask != nil {
		switch currentUserTaskStatus {
		case string(enums.TaskStatusActive):
			// approve / reject 是核心审批动作，后端强制开放，设计器不可控制
			resp.ActionPermissions["approve"] = true
			resp.ActionPermissions["reject"] = true
			// addComment 受 designer 控制（默认开）
			if !designerDisabled(actionPermissions, "addComment") {
				resp.ActionPermissions["addComment"] = true
			}
			// unclaim：当前用户已签收的 active 任务可释放
			if currentUserTask.Assignee != nil &&
				*currentUserTask.Assignee == currentUserId &&
				currentUserTask.ClaimedAt != nil {
				resp.ActionPermissions["unclaim"] = true
			}
		case string(enums.TaskStatusPending):
			resp.ActionPermissions["claim"] = true // 待领取→必须能签收（设计器不可控制）
		case string(enums.TaskStatusSuspended):
			if !designerDisabled(actionPermissions, "awaken") {
				resp.ActionPermissions["awaken"] = true
			}
		}

		// designer 显式开启的可选动作（transfer / return / delegate / addSign / reduceSign / urge / uploadAttachment）
		// 仅在 active 任务上有意义
		if isActiveTask {
			for _, k := range []string{"transfer", "return", "delegate", "addSign", "reduceSign", "urge", "uploadAttachment"} {
				if designerEnabled(actionPermissions, k) {
					resp.ActionPermissions[k] = true
				}
			}
		}

		resp.CurrentUserActivityTask = dto.CurrentUserActivityTask{
			TaskID:          currentUserTask.ID,
			TaskDefKey:      currentUserTask.TaskDefKey,
			TaskName:        currentUserTask.Name,
			FormPermissions: formPermissions,
			Variables:       parseTaskVariables(currentUserTask.Variables),
		}
	}

	// 8. reject 策略透传：rejectStrategy / rejectTargetNode 是行为配置（控制拒绝后走向），
	// 不是按钮显隐；只要 designer 配了就透传给前端
	if v, ok := actionPermissions["rejectStrategy"]; ok && v != "" {
		resp.ActionPermissions["rejectStrategy"] = v
	}
	if v, ok := actionPermissions["rejectTargetNode"]; ok && v != "" {
		resp.ActionPermissions["rejectTargetNode"] = v
	}

	return resp, nil
}

// isInstanceVisibleToUser 判定当前用户对该实例是否有可见性。
// 可见条件（满足任一）：发起人、该实例任一 task 的 assignee（含历史）、CC 抄送归属。
// 待认领候选成员不在 task.assignee 上，由调用方经 isPendingCandidateVisible 补充判定。
// currentUserId 由调用方保证非空（空 UserID 在 GetProcessInstanceDetail 已被拒绝）。
func isInstanceVisibleToUser(instance *model.WfInstance, tasks []*model.WfTask, currentUserId string) bool {
	if instance.StartUserID == currentUserId {
		return true
	}
	for _, t := range tasks {
		if t == nil {
			continue
		}
		if t.Assignee != nil && *t.Assignee == currentUserId {
			return true
		}
	}
	return false
}

// isPendingCandidateVisible 待认领任务的候选成员对实例详情可见：候选任务无 assignee，
// 不满足 isInstanceVisibleToUser 的任何条件，但成员须能看到详情才能签收。
func (s *RuntimeServiceImpl) isPendingCandidateVisible(ctx context.Context, tasks []*model.WfTask, currentUserId string) bool {
	candMemo := map[string]bool{}
	for _, t := range tasks {
		if t == nil || t.Status != string(enums.TaskStatusPending) {
			continue
		}
		ok, cached := candMemo[t.TaskDefKey]
		if !cached {
			ok = s.isUserCandidate(ctx, t, currentUserId)
			candMemo[t.TaskDefKey] = ok
		}
		if ok {
			return true
		}
	}
	return false
}

// designerDisabled 判断设计器是否在 actionPermissions 中显式禁用了某个动作。
// 缺省（未配置）视为未禁用；只有显式 false 才视为禁用。
func designerDisabled(actionPermissions map[string]interface{}, key string) bool {
	v, ok := actionPermissions[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	return ok && !b
}

// designerEnabled 判断设计器是否在 actionPermissions 中显式开启了某个动作。
// 用于"默认关"的可选动作（transfer / return / urge / uploadAttachment）。
func designerEnabled(actionPermissions map[string]interface{}, key string) bool {
	v, ok := actionPermissions[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}

// Task2ExecutionInfo 把任务行装配为审批时间线视图。tasks 须包含同实例（至少是
// 同节点父子关系内的）全部任务，才能正确拼出 SubExecutions 会签子任务子树。
func Task2ExecutionInfo(task *model.WfTask, tasks []*model.WfTask) dto.ExecutionInfo {
	executionInfo := dto.ExecutionInfo{
		TaskID:        task.ID,
		TaskDefKey:    task.TaskDefKey,
		TaskName:      task.Name,
		TaskType:      task.TaskType,
		Status:        task.Status,
		EndReason:     task.EndReason,
		Assignee:      task.Assignee,
		Comment:       task.Comment,
		SubExecutions: GetSubTasks(task.ID, tasks),
	}
	executionInfo.IsCandidate = (task.Assignee == nil || *task.Assignee == "") && task.Status == string(enums.TaskStatusPending)
	if task.EndedAt != nil {
		executionInfo.EndedAt = task.EndedAt.Format(constants.TimeFormatLayout)
	}
	if task.ClaimedAt != nil {
		executionInfo.ClaimedAt = task.ClaimedAt.Format(constants.TimeFormatLayout)
	}
	return executionInfo
}

// GetSubTasks 取 parentTaskId 的全部直接子任务（加签产生的会签子任务），并递归装配其自身子树。
func GetSubTasks(parentTaskId string, tasks []*model.WfTask) []dto.ExecutionInfo {
	var assignees []dto.ExecutionInfo
	for _, item := range tasks {
		if item.ParentID != nil && *item.ParentID == parentTaskId {
			info := Task2ExecutionInfo(item, tasks)
			assignees = append(assignees, info)
		}
	}
	return assignees
}

// parseTaskVariables 解析任务变量快照（JSON 字符串 → map）；
// 空值/解析失败返回 nil（AI 兜底待办的 _ai 快照经此暴露给审批详情）。
func parseTaskVariables(raw *string) map[string]interface{} {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(*raw), &m); err != nil || m == nil {
		return nil
	}
	return m
}
