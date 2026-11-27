package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
)

// StartSubProcessInstance 启动子流程实例（call activity 语义）：
// child.ParentID=parentInstanceID（创建时写入）；记 parentNodeID 供子完成回调恢复父流程。
func (s *RuntimeServiceImpl) StartSubProcessInstance(ctx context.Context, parentInstanceID, parentNodeID, childProcessDefID string, variables map[string]interface{}) (string, error) {
	parent, err := s.instanceDAO.Get(ctx, parentInstanceID)
	if err != nil {
		return "", fmt.Errorf("failed to get parent instance: %w", err)
	}
	if parent == nil {
		return "", fmt.Errorf("%w: parent instance %s", ErrInstanceNotFound, parentInstanceID)
	}
	// 递归深度 + 环检测：防止环状子流程（A→B→A）或嵌套过深打挂引擎
	if err := s.checkSubProcessDepth(ctx, parentInstanceID, childProcessDefID); err != nil {
		return "", err
	}
	childDef, err := s.processDAO.Get(ctx, childProcessDefID)
	if err != nil {
		return "", fmt.Errorf("failed to get child process def: %w", err)
	}
	if childDef == nil {
		return "", fmt.Errorf("%w: child process def %s", ErrNotFound, childProcessDefID)
	}
	// 防御性租户比对：子流程定义必须与父实例同租户，阻断跨租户子流程注入。
	if childDef.TenantID != parent.TenantID {
		return "", fmt.Errorf("subProcess child def %s belongs to tenant %q, parent tenant %q: %w",
			childProcessDefID, childDef.TenantID, parent.TenantID, ErrPermissionDenied)
	}
	// CreatedBy/StartUserID 现均存发起人"用户 ID"（见 startInstanceCore 注释），父实例并不保存
	// 展示名；子流程发起人只继承用户 ID，UserName 留空由展示层按 ID 批量查名。
	initiator := Actor{UserID: parent.StartUserID, TenantID: parent.TenantID}
	// 子流程继承父流程业务变量:subProcess 节点目前传 nil variables,子实例会丢失父数据,
	// 子链内的条件节点拿不到父变量。调用方未显式传变量时,从父实例 variables(启动业务变量)
	// 加载,让子链能读到父数据。
	if len(variables) == 0 {
		if pv, err := ParseVariablesJSON(parent.Variables); err == nil && len(pv) > 0 {
			variables = pv
		}
	}
	childID, engine, msg, err := s.startInstanceCore(ctx, childDef, initiator, "", variables, false, parentInstanceID)
	if err != nil {
		return "", err
	}
	// 先存映射（同步），再异步驱动——避免子异步完成早于映射写入的竞态；
	// 异步驱动同时避免子流程同步完成重入父 OnMsg。
	s.subProcessParentNodes.Store(childID, parentNodeID)
	if engine != nil {
		// OnMsg panic 兜底：终止子实例，避免子实例滞留。
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logrus.Errorf("subProcess child %s OnMsg panicked: %v", childID, r)
					// 用独立 context（调用方 ctx 可能已取消）以内部模式终止子实例。
					actx := WithInternalCallingMode(context.Background())
					reason := fmt.Sprintf("subProcess child panicked: %v", r)
					if terr := s.TerminateProcessInstance(actx, SystemActor(), childID, reason); terr != nil {
						logrus.WithError(terr).Errorf("failed to terminate panicked subProcess child %s", childID)
					}
				}
			}()
			// 持双门闩（见 acquireDriveGates）；独立 ctx：调用方取消不应中断取闩
			defer s.acquireDriveGates(context.Background(), childID)()
			engine.OnMsg(msg)
		}()
	}
	return childID, nil
}

// maxSubProcessDepth 子流程嵌套深度上限，防止环状/过深递归打挂引擎。
const maxSubProcessDepth = 10

// checkSubProcessDepth 沿父实例链回溯，检测环状递归（childProcessDefID 已在祖先链中）或嵌套过深。
func (s *RuntimeServiceImpl) checkSubProcessDepth(ctx context.Context, parentInstanceID, childProcessDefID string) error {
	depth := 0
	curID := parentInstanceID
	for curID != "" {
		depth++
		if depth > maxSubProcessDepth {
			return fmt.Errorf("subProcess 嵌套深度超过上限 %d", maxSubProcessDepth)
		}
		inst, err := s.instanceDAO.Get(ctx, curID)
		if err != nil || inst == nil {
			break
		}
		// 祖先链中已有该流程定义的实例 → 环状递归（A→B→A 或 A→A 自环）
		if inst.ProcessID == childProcessDefID {
			return fmt.Errorf("subProcess 递归环检测到：流程 %s 已在祖先链中", childProcessDefID)
		}
		if inst.ParentID == nil || *inst.ParentID == "" {
			break
		}
		curID = *inst.ParentID
	}
	return nil
}

// deriveSubProcessParentNode 从 DB 重推导父流程的 subProcess 节点ID（持久化回退，重启后内存映射丢失时用）。
// 链路：子流程定义.ruleChain.id → 父实例.流程定义 → 父链里 targetId 匹配的 subProcess 节点。
func (s *RuntimeServiceImpl) deriveSubProcessParentNode(ctx context.Context, parentInstanceID, childProcessDefID string) (string, bool) {
	childDef, err := s.processDAO.Get(ctx, childProcessDefID)
	if err != nil || childDef == nil {
		return "", false
	}
	childAlias := extractRuleChainID(childDef.DefinitionJSON)
	if childAlias == "" {
		return "", false
	}
	parent, err := s.instanceDAO.Get(ctx, parentInstanceID)
	if err != nil || parent == nil {
		return "", false
	}
	parentDef, err := s.processDAO.Get(ctx, parent.ProcessID)
	if err != nil || parentDef == nil {
		return "", false
	}
	return findSubProcessNodeByTarget(parentDef.DefinitionJSON, childAlias)
}

// findSubProcessNodeByTarget 在流程定义里找 type=subProcess 且 configuration.targetId==target 的节点ID。
func findSubProcessNodeByTarget(def, target string) (string, bool) {
	var doc struct {
		Metadata struct {
			Nodes []struct {
				ID   string `json:"id"`
				Type string `json:"type"`
				Cfg  struct {
					TargetId string `json:"targetId"`
				} `json:"configuration"`
			} `json:"nodes"`
		} `json:"metadata"`
	}
	if json.Unmarshal([]byte(def), &doc) != nil {
		return "", false
	}
	for _, n := range doc.Metadata.Nodes {
		if n.Type == constants.NodeTypeSubProcess && n.Cfg.TargetId == target {
			return n.ID, true
		}
	}
	return "", false
}

// ResolveSubProcessTarget 把 subProcess targetId（子链 ruleChain.id）解析为子流程定义ID。
// 内存映射在收到部署请求的副本上由 PreloadChain 注册；多实例部署时发起与部署
// 可能落在不同副本，未命中回落 DB 按 processKey 查 active 定义，回填映射并预载
// 链自愈，此后该副本与部署侧同口径。
func (s *RuntimeServiceImpl) ResolveSubProcessTarget(ctx context.Context, tenantID, ruleChainID string) (string, bool) {
	key := tenantID + "|" + ruleChainID
	if v, ok := s.subProcessTargets.Load(key); ok {
		return v.(string), true
	}
	if tenantID == "" || ruleChainID == "" {
		return "", false
	}
	proc, err := s.processDAO.GetByKey(ctx, tenantID, ruleChainID)
	if err != nil || proc == nil || proc.Status != string(enums.ProcessStatusActive) {
		return "", false
	}
	// 自愈：回填映射并把子链预载进本副本租户池（幂等），后续 TellFlow 直接命中
	if err := s.PreloadChain(tenantID, proc.ID, proc.DefinitionJSON); err != nil {
		logrus.WithError(err).WithField("processId", proc.ID).
			Warn("subProcess target fallback preload failed; resolution still valid")
	}
	return proc.ID, true
}

// SubProcessChildState 查询父实例下子流程状态：active（在跑）/ completed（已归档完成）。
// 查询失败返回 err：调用方（subProcess 节点）必须走 Failure 边，
// 不允许在未知状态下重复启动子实例。
func (s *RuntimeServiceImpl) SubProcessChildState(ctx context.Context, parentInstanceID string) (active, completed bool, err error) {
	if active, err = s.instanceDAO.HasActiveByParentID(ctx, parentInstanceID); err != nil {
		return false, false, fmt.Errorf("failed to query active subProcess children: %w", err)
	}
	if completed, err = s.hiInstanceDAO.HasByParentID(ctx, parentInstanceID); err != nil {
		return false, false, fmt.Errorf("failed to query completed subProcess children: %w", err)
	}
	return active, completed, nil
}

// SubProcessChildTerminated 子流程是否有被终止(reject terminate)的归档子实例。
// 供父 subProcess 节点区分"子正常完成走 Success" vs "子被终止走 Failure 边"。
// 查询失败返回 err，由调用方决定失败处置。
func (s *RuntimeServiceImpl) SubProcessChildTerminated(ctx context.Context, parentInstanceID string) (bool, error) {
	terminated, err := s.hiInstanceDAO.HasTerminatedByParentID(ctx, parentInstanceID)
	if err != nil {
		return false, fmt.Errorf("failed to query terminated subProcess children: %w", err)
	}
	return terminated, nil
}

// collectChildOutputVariables 汇总子实例最终业务变量供回传父实例：instance.Variables
// 为底（含自动化节点产出回写），已完成任务变量按 EndedAt 升序叠加（后结束覆盖
// 先结束），最后剥保留键。作废的在途任务不参与——审批人未提交的草稿值不是最终
// 业务变量。保留键口径=_ 前缀（引擎命名空间，业务变量不得用 _ 开头）加审批提交
// 剥离同款（代审/兜底/加签/转办/reassign_）。纯内存不触 DAO。
// 继承自父且未被子侧修改的键原值返回，合并回父等价无操作（零感知）。
func collectChildOutputVariables(instance *model.WfInstance, tasks []*model.WfTask) map[string]interface{} {
	merged, err := ParseVariablesJSON(instance.Variables)
	if err != nil {
		logrus.WithError(err).WithField("instanceId", instance.ID).
			Warn("parse child instance variables for parent writeback failed; start from empty")
		merged = make(map[string]interface{})
	}
	completed := make([]*model.WfTask, 0, len(tasks))
	for _, t := range tasks {
		if t != nil && t.Status == string(enums.TaskStatusCompleted) {
			completed = append(completed, t)
		}
	}
	sort.SliceStable(completed, func(i, j int) bool {
		ei, ej := completed[i].EndedAt, completed[j].EndedAt
		if ei != nil && ej != nil {
			return ei.Before(*ej)
		}
		return ei != nil && ej == nil // 无结束时间的排最后
	})
	for _, task := range completed {
		tv, err := ParseVariablesJSON(task.Variables)
		if err != nil {
			logrus.WithError(err).WithField("taskId", task.ID).
				Warn("parse child task variables for parent writeback failed; skipped")
			continue
		}
		for k, v := range tv {
			merged[k] = v
		}
	}
	stripEngineReservedVars(merged)
	for k := range merged {
		if strings.HasPrefix(k, "_") {
			delete(merged, k)
		}
	}
	return merged
}

// mergeVariablesIntoInstance 把 vars 逐键覆盖进实例 variables，并删除 removeKeys
// 列出的键（同一短事务内完成；成功回传清残留 _childEndReason 用）。
// 子→主回传与失败元数据写入共用：调用方已提交子实例事务，本函数在父行开新锁
// 事务，避免父子实例行锁嵌套死锁；父实例已终态（并发窗口）或读写失败时返回
// 错误，调用方降级告警，不回滚子实例已完成的事实。
func (s *RuntimeServiceImpl) mergeVariablesIntoInstance(ctx context.Context, instanceID string, vars map[string]interface{}, removeKeys ...string) error {
	if len(vars) == 0 && len(removeKeys) == 0 {
		return nil
	}
	return WithInstanceTx(ctx, s.instanceDAO.Underlying(), instanceID, func(scope *InstanceScope) error {
		tx := scope.Tx()
		instance, err := tx.WfInstance.WithContext(ctx).Where(tx.WfInstance.ID.Eq(instanceID)).First()
		if err != nil {
			return fmt.Errorf("failed to get parent instance for variable merge: %w", err)
		}
		existing, err := ParseVariablesJSON(instance.Variables)
		if err != nil {
			return fmt.Errorf("failed to parse parent instance variables: %w", err)
		}
		for k, v := range vars {
			existing[k] = v
		}
		for _, k := range removeKeys {
			delete(existing, k)
		}
		return s.setProcessInstanceVariablesInTx(ctx, tx, instanceID, existing)
	})
}

// resumeParentAfterChildTerminated 子实例终止后恢复父流程(子→父失败传播)。
// 子已归档到 hi_instance(ParentID/ProcessID 保留),据此推导父 subProcess 节点并 ExecuteNext 重入。
func (s *RuntimeServiceImpl) resumeParentAfterChildTerminated(ctx context.Context, childInstanceID string) {
	hi, err := s.hiInstanceDAO.GetIncludingDeleted(ctx, childInstanceID)
	if err != nil || hi == nil || hi.ParentID == nil || *hi.ParentID == "" {
		return
	}
	parentNodeID, ok := "", false
	if v, mok := s.subProcessParentNodes.LoadAndDelete(childInstanceID); mok {
		parentNodeID, ok = v.(string), true
	} else {
		parentNodeID, ok = s.deriveSubProcessParentNode(ctx, *hi.ParentID, hi.ProcessID)
	}
	if !ok {
		return
	}
	// 失败路径只回传元数据：end_reason 写入父保留键 _childEndReason，父 Failure
	// 分支据此区分审批拒绝/节点错误/撤回/终止；业务结果键不回传（失败的结果
	// 不是父流程要消费的数据）。ExecuteNext 仍传 nil。
	if hi.EndReason != nil && *hi.EndReason != "" {
		if err := s.mergeVariablesIntoInstance(ctx, *hi.ParentID, map[string]interface{}{
			constants.VarsChildEndReason: *hi.EndReason,
		}); err != nil {
			logrus.WithError(err).WithField("parentId", *hi.ParentID).
				Warn("write child end reason into parent variables failed")
		}
	}
	if err := s.ExecuteNext(ctx, *hi.ParentID, parentNodeID, nil); err != nil {
		logrus.WithError(err).Warn("resume parent after subProcess child terminated failed")
	}
}
