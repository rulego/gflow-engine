// This file centralizes actionPermissions parsing + the service-layer action
// gates and Return target validation for TaskServiceImpl.
//
// 节点配置解析抽成 resolveNodeActionPermissions，既供 GetProcessInstanceDetail
// 装配（前端按钮显隐），也供五个 service 入口
// （transfer/delegate/return/addSign/reduceSign）强制校验设计器显式 false。

package service

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
)

// resolveNodeActionPermissions 返回某节点 additionalInfo.actionPermissions。
// lenient 版：任一加载/解析步骤失败都返回空 map，仅供 UI 装配等展示场景
// （GetProcessInstanceDetail 详情按钮显隐）；安全校验走 resolveNodeActionPermissionsStrict。
//
// 服务未注入（嵌入式半装配场景）同样返回空 map 降级，不允许 panic。
func resolveNodeActionPermissions(ctx context.Context, engine WorkflowEngine, instanceID, taskDefKey string) map[string]interface{} {
	if instanceID == "" || taskDefKey == "" || engine == nil {
		return map[string]interface{}{}
	}
	runtimeService := engine.GetRuntimeService()
	if runtimeService == nil {
		return map[string]interface{}{}
	}
	instance, err := runtimeService.GetProcessInstance(ctx, ActorFromCtx(ctx), instanceID)
	if err != nil || instance == nil {
		return map[string]interface{}{}
	}
	if instance.ProcessID == "" {
		return map[string]interface{}{}
	}
	processService := engine.GetProcessService()
	if processService == nil {
		return map[string]interface{}{}
	}
	procDef, err := processService.Get(ctx, instance.ProcessID)
	if err != nil || procDef == nil {
		return map[string]interface{}{}
	}
	rc, err := procDef.ToRuleChain()
	if err != nil || rc == nil {
		return map[string]interface{}{}
	}
	node, ok := rc.GetNode(taskDefKey)
	if !ok {
		return map[string]interface{}{}
	}
	ap, ok := node.GetAdditionalInfo("actionPermissions")
	if !ok {
		return map[string]interface{}{}
	}
	v, ok := ap.(map[string]interface{})
	if !ok {
		return map[string]interface{}{}
	}
	return v
}

// resolveProcessActionPermissions 读取流程级 actionPermissions
// （ruleChain.additionalInfo，即设计器「高级设置」面板写入的发起人/审批人开关）。
// 附带返回解析后的链定义，供收回路径守卫复用，避免二次解析。
func resolveProcessActionPermissions(ctx context.Context, engine WorkflowEngine, processID string) (map[string]interface{}, *types.RuleChain, error) {
	if engine == nil || processID == "" {
		return nil, nil, fmt.Errorf("resolve process action permissions: invalid engine/process")
	}
	processService := engine.GetProcessService()
	if processService == nil {
		return nil, nil, fmt.Errorf("resolve process action permissions: process service not injected")
	}
	procDef, err := processService.Get(ctx, processID)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve process action permissions: get process: %w", err)
	}
	if procDef == nil {
		return nil, nil, fmt.Errorf("resolve process action permissions: process not found")
	}
	rc, err := procDef.ToRuleChain()
	if err != nil {
		return nil, nil, fmt.Errorf("resolve process action permissions: parse rule chain: %w", err)
	}
	if rc == nil {
		return nil, nil, fmt.Errorf("resolve process action permissions: rule chain is nil")
	}
	if ap, ok := rc.RuleChain.GetAdditionalInfo("actionPermissions"); ok {
		if v, ok := ap.(map[string]interface{}); ok {
			return v, rc, nil
		}
		return nil, nil, fmt.Errorf("resolve process action permissions: invalid actionPermissions type")
	}
	return map[string]interface{}{}, rc, nil
}

// resolveNodeActionPermissionsStrict 与上方的 lenient 版同源，但解析失败返回 error
// 而非空 map。仅供 requireActionEnabled 做 fail-closed 校验：解析不出来说明无法判断
// 设计器是否显式禁用了某动作，此时拒绝执行，避免解析失败绕过设计器的显式 disable。
//
// 返回值语义：
//   - (nil, nil)：节点解析成功但未配置 actionPermissions 键 → 无显式禁用，放行；
//   - (map, nil)：解析出 actionPermissions；
//   - (nil, err)：实例/定义不存在、服务未注入、规则链解析失败、配置类型错误等。
func resolveNodeActionPermissionsStrict(ctx context.Context, engine WorkflowEngine, instanceID, taskDefKey string) (map[string]interface{}, error) {
	if instanceID == "" || taskDefKey == "" || engine == nil {
		return nil, fmt.Errorf("resolve action permissions: invalid instance/defKey/engine")
	}
	runtimeService := engine.GetRuntimeService()
	if runtimeService == nil {
		return nil, fmt.Errorf("resolve action permissions: runtime service not injected")
	}
	instance, err := runtimeService.GetProcessInstance(ctx, ActorFromCtx(ctx), instanceID)
	if err != nil {
		return nil, fmt.Errorf("resolve action permissions: get instance: %w", err)
	}
	if instance == nil {
		return nil, fmt.Errorf("resolve action permissions: instance not found")
	}
	if instance.ProcessID == "" {
		return nil, fmt.Errorf("resolve action permissions: instance has no process")
	}
	processService := engine.GetProcessService()
	if processService == nil {
		return nil, fmt.Errorf("resolve action permissions: process service not injected")
	}
	procDef, err := processService.Get(ctx, instance.ProcessID)
	if err != nil {
		return nil, fmt.Errorf("resolve action permissions: get process: %w", err)
	}
	if procDef == nil {
		return nil, fmt.Errorf("resolve action permissions: process not found")
	}
	rc, err := procDef.ToRuleChain()
	if err != nil {
		return nil, fmt.Errorf("resolve action permissions: parse rule chain: %w", err)
	}
	if rc == nil {
		return nil, fmt.Errorf("resolve action permissions: rule chain is nil")
	}
	node, ok := rc.GetNode(taskDefKey)
	if !ok {
		return nil, fmt.Errorf("resolve action permissions: node %s not found", taskDefKey)
	}
	ap, ok := node.GetAdditionalInfo("actionPermissions")
	if !ok {
		return nil, nil
	}
	v, ok := ap.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("resolve action permissions: actionPermissions has wrong type")
	}
	return v, nil
}

// resolveNodeFormPermissions 返回某节点 additionalInfo.formPermissions（字段级权限 r/w/h）。
// 必须传入事务 scope：本函数在 WithInstanceTx 回调内调用，若走全局连接，
// SQLite 等单写锁数据库会与外层事务互等形成死锁。
func resolveNodeFormPermissions(ctx context.Context, scope *InstanceScope, instanceID, taskDefKey string) map[string]interface{} {
	if scope == nil || instanceID == "" || taskDefKey == "" {
		return map[string]interface{}{}
	}
	instance, err := scope.Instances().Get(ctx, instanceID)
	if err != nil || instance == nil || instance.ProcessID == "" {
		return map[string]interface{}{}
	}
	procDef, err := scope.Processes().Get(ctx, instance.ProcessID)
	if err != nil || procDef == nil {
		return map[string]interface{}{}
	}
	rc, err := procDef.ToRuleChain()
	if err != nil || rc == nil {
		return map[string]interface{}{}
	}
	node, ok := rc.GetNode(taskDefKey)
	if !ok {
		return map[string]interface{}{}
	}
	fp, ok := node.GetAdditionalInfo("formPermissions")
	if !ok {
		return map[string]interface{}{}
	}
	v, ok := fp.(map[string]interface{})
	if !ok {
		return map[string]interface{}{}
	}
	return v
}

// requireActionEnabled 校验流程设计器是否在 actionPermissions 中显式禁用了某动作。
// 解析失败一律 fail-closed 拒绝：解析不出即无法证明设计器未禁用，放行等于绕过显式 disable。
func (s *TaskServiceImpl) requireActionEnabled(ctx context.Context, task *model.WfTask, actionKey string) error {
	if task == nil || task.ProcessInstanceID == nil || *task.ProcessInstanceID == "" {
		return nil
	}
	ap, err := resolveNodeActionPermissionsStrict(ctx, s.workflowEngine, *task.ProcessInstanceID, task.TaskDefKey)
	if err != nil {
		return fmt.Errorf("cannot resolve action permissions for action %q: %w", actionKey, ErrPermissionDenied)
	}
	if designerDisabled(ap, actionKey) {
		return fmt.Errorf("action %q disabled by designer on node %s: %w",
			actionKey, task.TaskDefKey, ErrPermissionDenied)
	}
	return nil
}

// requireProcessActionEnabled 流程级动作开关：设计器把撤回等实例级动作的开关写在
// ruleChain.additionalInfo.actionPermissions，节点级配置里不存在，节点级校验拦不住。
// 须在实例行锁事务外调用——流程定义读取走默认连接，锁内调用属于 tx 逃逸（单写库
// 与外层事务互等）。解析失败记录根因后拒绝（fail-closed）。
func (s *TaskServiceImpl) requireProcessActionEnabled(ctx context.Context, processID, actionKey string) error {
	ap, _, err := resolveProcessActionPermissions(ctx, s.workflowEngine, processID)
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"process_id": processID,
			"action":     actionKey,
		}).Warn("resolve process action permissions failed")
		return fmt.Errorf("cannot resolve action permissions for action %q: %w", actionKey, ErrPermissionDenied)
	}
	if designerDisabled(ap, actionKey) {
		return fmt.Errorf("action %q disabled by designer: %w", actionKey, ErrPermissionDenied)
	}
	return nil
}

// filterVariablesByFormPermissions 按节点 formPermissions 过滤审批人提交的变量：
// 只读(r)/隐藏(h)字段不允许审批人覆盖；可写(w/缺省)字段放行。无配置时不限制。
func (s *TaskServiceImpl) filterVariablesByFormPermissions(ctx context.Context, scope *InstanceScope, task *model.WfTask, vars map[string]interface{}) map[string]interface{} {
	if len(vars) == 0 || task == nil || task.ProcessInstanceID == nil || *task.ProcessInstanceID == "" {
		return vars
	}
	fp := resolveNodeFormPermissions(ctx, scope, *task.ProcessInstanceID, task.TaskDefKey)
	if len(fp) == 0 {
		return vars
	}
	filtered := make(map[string]interface{}, len(vars))
	for k, v := range vars {
		if perm, ok := fp[k].(string); ok && (perm == "r" || perm == "h") {
			continue
		}
		filtered[k] = v
	}
	return filtered
}

// hasCompletedTaskAtNode 判断实例中某节点是否存在已完成的 userTask 任务。
// 只查运行表：活跃实例的终态任务在实例归档前留在运行表，而 Return 仅对
// active 实例的任务可用，历史表查询在这里不可达也无必要。
func (s *TaskServiceImpl) hasCompletedTaskAtNode(ctx context.Context, scope *InstanceScope, instanceID, taskDefKey string) (bool, error) {
	wt := scope.Tx().WfTask
	var task model.WfTask
	err := wt.WithContext(ctx).
		Where(wt.ProcessInstanceID.Eq(instanceID)).
		Where(wt.TaskDefKey.Eq(taskDefKey)).
		Where(wt.TaskType.Eq(constants.TaskTypeUserTask)).
		Where(wt.Status.Eq(string(enums.TaskStatusCompleted))).
		Limit(1).
		Scan(&task)
	if err != nil {
		return false, fmt.Errorf("failed to query completed tasks at node %s: %w", taskDefKey, err)
	}
	return task.ID != "", nil
}

// requireReturnTarget 校验 Return 目标与操作人：
//   - 操作人须为当前任务受理人或候选人（scope 内判定）；
//   - 目标须为图内 userTask 节点、当前节点的拓扑上游且非当前节点自身，重执行
//     区域（target→self）不得穿过并行网关——与驳回回跳的部署期/运行期守卫同口径
//     （重入 fork 会向各分支重复派发任务，重入 join 会因兄弟分支消息不再到来而
//     永久等待）；
//   - 目标在本实例中已有完成的 userTask 任务（只退回已经运行过的节点）。
func (s *TaskServiceImpl) requireReturnTarget(ctx context.Context, scope *InstanceScope, graph *ChainGraph, task *model.WfTask, targetActivityID, userID string) error {
	if task == nil || task.ProcessInstanceID == nil {
		return nil
	}
	instanceID := *task.ProcessInstanceID
	selfDefKey := task.TaskDefKey

	// 1. 操作人须为当前任务受理人或候选人
	if !s.isReturnOperator(ctx, scope, task, userID) {
		return fmt.Errorf("return by non-assignee/non-candidate %s: %w", userID, ErrPermissionDenied)
	}

	// 2. 目标须为图内 userTask 节点
	if targetActivityID == "" || !graph.HasNode(targetActivityID) {
		return fmt.Errorf("return target %q is not in the process definition: %w", targetActivityID, ErrValidation)
	}
	if graph.NodeTypes[targetActivityID] != constants.NodeTypeUserTask {
		return fmt.Errorf("return target %q is not a user task node: %w", targetActivityID, ErrValidation)
	}

	// 3. 目标须为当前节点的上游且非当前节点自身
	if targetActivityID == selfDefKey {
		return fmt.Errorf("return target %q is the current node itself: %w", targetActivityID, ErrValidation)
	}
	if !graph.UpstreamReachable(selfDefKey, targetActivityID) {
		return fmt.Errorf("return target %q is not upstream of current node %q: %w", targetActivityID, selfDefKey, ErrValidation)
	}

	// 4. 重执行区域不得穿过并行网关
	if graph.RollbackRegionForked(targetActivityID, selfDefKey) {
		return fmt.Errorf("return path from %q to %q crosses a parallel gateway: %w", selfDefKey, targetActivityID, ErrValidation)
	}

	// 5. 目标在本实例中已有完成的任务
	exists, err := s.hasCompletedTaskAtNode(ctx, scope, instanceID, targetActivityID)
	if err != nil {
		logrus.WithError(err).WithField("instanceId", instanceID).
			Warn("failed to check completed tasks at return target")
		return fmt.Errorf("failed to resolve return target: %w", ErrPermissionDenied)
	}
	if !exists {
		return fmt.Errorf("return target %q has no completed task to return to: %w", targetActivityID, ErrValidation)
	}
	return nil
}

// returnTargetGraph 加载任务所属流程的规则链并构建拓扑视图，供回退目标校验与
// 重执行区域计算使用。定义缺失/解析失败返回错误（fail-closed）——无法证明目标
// 合法性时放行等于绕过流程约束。须在进入实例事务之前调用：事务内走非 tx 连接
// 读流程表，单写锁数据库会与外层事务互等。
func (s *TaskServiceImpl) returnTargetGraph(ctx context.Context, task *model.WfTask) (*ChainGraph, error) {
	const failReason = "cannot resolve process graph for return"
	if task == nil || task.ProcessID == "" || s.workflowEngine == nil {
		return nil, fmt.Errorf("%s: %w", failReason, ErrPermissionDenied)
	}
	procDef, err := s.workflowEngine.GetProcessService().Get(ctx, task.ProcessID)
	if err != nil || procDef == nil {
		return nil, fmt.Errorf("%s: %w", failReason, ErrPermissionDenied)
	}
	rc, err := procDef.ToRuleChain()
	if err != nil || rc == nil {
		return nil, fmt.Errorf("%s: %w", failReason, ErrPermissionDenied)
	}
	return buildChainGraph(rc), nil
}

// returnRegionNodes 计算回退需要清理任务的重执行区域：从 target 正向可达、且能
// 到达 self 的 userTask 节点（含 target 与 self 自身），与驳回回跳的
// rejectResetNodes（components 层）同口径。区域外节点（如另一条并行分支）不在
// 回流路径上，其任务——尤其是仍在办理中的——必须保持原状，否则汇合点永远凑不齐。
// graph 为 nil 时退化为只返回两者自身。输出按节点 ID 排序保证确定性。
func returnRegionNodes(g *ChainGraph, target, self string) []string {
	seen := make(map[string]bool, 4)
	out := make([]string, 0, 4)
	add := func(id, nodeType string) {
		if id == "" || seen[id] || nodeType != constants.NodeTypeUserTask {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	if g == nil {
		add(target, constants.NodeTypeUserTask)
		add(self, constants.NodeTypeUserTask)
		return out
	}
	reachableFromTarget := map[string]bool{target: true}
	queue := []string{target}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range g.Forward[cur] {
			if !reachableFromTarget[next] {
				reachableFromTarget[next] = true
				queue = append(queue, next)
			}
		}
	}
	reachesSelf := map[string]bool{self: true}
	queue = []string{self}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, prev := range g.Backward[cur] {
			if !reachesSelf[prev] {
				reachesSelf[prev] = true
				queue = append(queue, prev)
			}
		}
	}
	add(target, g.NodeTypes[target])
	add(self, g.NodeTypes[self])
	for id := range reachableFromTarget {
		if reachesSelf[id] {
			add(id, g.NodeTypes[id])
		}
	}
	sort.Strings(out)
	return out
}

// buildReturnableNodes 装配当前办理人的可回退目标节点：从本实例已有完成任务的
// userTask 节点中，过滤出当前节点的拓扑上游、非当前节点自身、且重执行区域不穿
// 并行网关的节点，按最近完成在前排序。与 requireReturnTarget 写路径校验同口径，
// 前端回退弹窗据此直出选项，不再从 executions 自行推断。仅当前节点存在可办
// 任务且 return 按钮可见时调用。
func buildReturnableNodes(chain *types.RuleChain, tasks []*model.WfTask, selfDefKey string) []dto.ReturnableNode {
	if chain == nil || selfDefKey == "" {
		return nil
	}
	g := buildChainGraph(chain)
	// 每节点取最近一次完成记录（按 defKey 去重，名称取最近一轮的任务名）
	type nodeLatest struct {
		name    string
		endedAt time.Time
	}
	latest := make(map[string]*nodeLatest, len(tasks))
	for _, t := range tasks {
		if t == nil || t.TaskType != constants.TaskTypeUserTask || t.TaskDefKey == "" ||
			t.TaskDefKey == selfDefKey || t.Status != string(enums.TaskStatusCompleted) {
			continue
		}
		ended := time.Time{}
		if t.EndedAt != nil {
			ended = *t.EndedAt
		}
		cur, ok := latest[t.TaskDefKey]
		if !ok || ended.After(cur.endedAt) {
			latest[t.TaskDefKey] = &nodeLatest{name: t.Name, endedAt: ended}
		}
	}
	keys := make([]string, 0, len(latest))
	for k := range latest {
		keys = append(keys, k)
	}
	// 最近完成在前；同一时刻按 ID 兜底排序保证输出稳定
	sort.Slice(keys, func(i, j int) bool {
		if !latest[keys[i]].endedAt.Equal(latest[keys[j]].endedAt) {
			return latest[keys[i]].endedAt.After(latest[keys[j]].endedAt)
		}
		return keys[i] < keys[j]
	})
	out := make([]dto.ReturnableNode, 0, len(keys))
	for _, k := range keys {
		if !g.HasNode(k) || g.NodeTypes[k] != constants.NodeTypeUserTask {
			continue
		}
		if !g.UpstreamReachable(selfDefKey, k) {
			continue
		}
		if g.RollbackRegionForked(k, selfDefKey) {
			continue
		}
		out = append(out, dto.ReturnableNode{Key: k, Name: latest[k].name})
	}
	return out
}

// isReturnOperator 判断 userID 是否为当前任务的受理人或候选人。
// 候选池经 scope（tx-bound）读取——requireReturnTarget 在 WithInstanceTx 回调内执行，
// 若走默认 DAO（s.taskAssigneeDAO）会 tx 逃逸，SQLite 等单写锁数据库与外层事务互等死锁。
func (s *TaskServiceImpl) isReturnOperator(ctx context.Context, scope *InstanceScope, task *model.WfTask, userID string) bool {
	if userID == "" {
		return false
	}
	if task.Assignee != nil && *task.Assignee == userID {
		return true
	}
	if task.ProcessInstanceID == nil {
		return false
	}
	return s.isUserCandidateInScope(ctx, scope, task, userID)
}

// isUserCandidateInScope 在事务内展开候选池判断 userID 是否为候选人。
// role/department 经 identity 展开；identity 缺失或展开失败按非候选处理（fail-closed，
// 返回 false → 拒绝 return）。
func (s *TaskServiceImpl) isUserCandidateInScope(ctx context.Context, scope *InstanceScope, task *model.WfTask, userID string) bool {
	if scope == nil || task == nil || task.ProcessInstanceID == nil {
		return false
	}
	rows, err := scope.TaskAssignees().GetByInstanceAndDefKey(ctx, task.TenantID, *task.ProcessInstanceID, task.TaskDefKey)
	if err != nil {
		logrus.WithError(err).WithField("taskDefKey", task.TaskDefKey).
			Warn("failed to query task candidates in tx for return operator check; treating as non-candidate")
		return false
	}
	identity := s.workflowEngine.GetIdentityService()
	for _, c := range rows {
		if c == nil {
			continue
		}
		for _, m := range expandCandidateMembers(ctx, identity, task.TenantID, c.EntityType, c.EntityID) {
			if m == userID {
				return true
			}
		}
	}
	return false
}
