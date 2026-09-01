package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/types/constants"
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
	// CreatedBy 现存储发起人用户 ID（见 startInstanceCore 注释），子流程发起人沿用父实例身份。
	initiator := Actor{UserID: parent.StartUserID, UserName: parent.CreatedBy, TenantID: parent.TenantID}
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
		// panic 不能打挂宿主进程；失败要留痕，否则子流程静默不启动无从排查
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logrus.Errorf("subProcess child %s OnMsg panicked: %v", childID, r)
				}
			}()
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
func (s *RuntimeServiceImpl) ResolveSubProcessTarget(tenantID, ruleChainID string) (string, bool) {
	if v, ok := s.subProcessTargets.Load(tenantID + "|" + ruleChainID); ok {
		return v.(string), true
	}
	return "", false
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

// resumeParentAfterChildTerminated 子实例终止后恢复父流程(子→父失败传播)。
// 子已归档到 hi_instance(ParentID/ProcessID 保留),据此推导父 subProcess 节点并 ExecuteNext 重入。
func (s *RuntimeServiceImpl) resumeParentAfterChildTerminated(ctx context.Context, childInstanceID string) {
	hi, err := s.hiInstanceDAO.Get(ctx, childInstanceID)
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
	if err := s.ExecuteNext(ctx, *hi.ParentID, parentNodeID, nil); err != nil {
		logrus.WithError(err).Warn("resume parent after subProcess child terminated failed")
	}
}
