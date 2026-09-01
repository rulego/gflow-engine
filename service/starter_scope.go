package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/rulego/api/types"
)

// 发起人范围（流程级 ruleChain.additionalInfo.starterScope）。
// 由设计器 PromoterDrawer 配置；未配置视为 all（存量流程全员可发起，行为不变）。
// 部门类型（dept）预留：IdentityService 目前只有主部门查询，待补部门子树展开后启用。
const (
	ScopeTypeAll  = "all"  // 全员
	ScopeTypeUser = "user" // 指定成员
	ScopeTypeRole = "role" // 指定角色
)

// StarterScope 发起人范围配置。
type StarterScope struct {
	Type    string   `json:"type"`
	UserIDs []string `json:"userIds,omitempty"`
	RoleIDs []string `json:"roleIds,omitempty"`
}

// ParseStarterScope 从流程定义链上读取发起人范围。
// 缺失、格式异常或未知类型一律回退 all：范围是设计者数据而非外部输入，
// 损坏时不应把流程变成"无人可发起"。空 type 同样按 all 处理。
func ParseStarterScope(chain *types.RuleChain) StarterScope {
	if chain == nil || chain.RuleChain.AdditionalInfo == nil {
		return StarterScope{Type: ScopeTypeAll}
	}
	raw, ok := chain.RuleChain.AdditionalInfo["starterScope"]
	if !ok {
		return StarterScope{Type: ScopeTypeAll}
	}
	// additionalInfo 来自 JSON 反序列化，重新序列化再解一次最稳妥（容忍任意中间形态）
	var scope StarterScope
	if b, err := json.Marshal(raw); err == nil {
		_ = json.Unmarshal(b, &scope)
	}
	switch scope.Type {
	case ScopeTypeUser, ScopeTypeRole:
		return scope
	default:
		return StarterScope{Type: ScopeTypeAll}
	}
}

// MatchStarterScope 判断用户是否在发起范围内。
// role 类型需要调用方传入用户角色 ID（经 IdentityService 解析）；解析失败/未注入
// 传 nil 时按"无角色"处理（fail-closed：无法证明在范围内即拒绝）。
func MatchStarterScope(scope StarterScope, userID string, userRoleIDs []string) bool {
	switch scope.Type {
	case ScopeTypeUser:
		for _, id := range scope.UserIDs {
			if id != "" && id == userID {
				return true
			}
		}
		return false
	case ScopeTypeRole:
		if len(scope.RoleIDs) == 0 || len(userRoleIDs) == 0 {
			return false
		}
		for _, want := range scope.RoleIDs {
			for _, got := range userRoleIDs {
				if want != "" && want == got {
					return true
				}
			}
		}
		return false
	default: // all / 未知类型
		return true
	}
}

// checkStarterScope 发起人范围强校验：所有用户发起入口（页面/API/导入后直发）
// 都经 StartProcessInstanceByID，此处拦截后无绕过路径。subProcess 子实例不经
// 此方法（runtime_subprocess 直接 startInstanceCore），不受范围限制。
func (s *RuntimeServiceImpl) checkStarterScope(ctx context.Context, processDef *model.WfProcess, initiator Actor) error {
	chain, err := processDef.ToRuleChain()
	if err != nil {
		// 结构性问题交给后续链装载报错，这里不拦
		return nil
	}
	scope := ParseStarterScope(chain)
	if scope.Type == ScopeTypeAll {
		return nil
	}
	var userRoleIDs []string
	if scope.Type == ScopeTypeRole {
		userRoleIDs = identityRoleIDs(ctx, s.workflowEngine.GetIdentityService(), initiator.TenantID, initiator.UserID)
	}
	if MatchStarterScope(scope, initiator.UserID, userRoleIDs) {
		return nil
	}
	return fmt.Errorf("initiator %s is not in the starter scope of process %s: %w",
		initiator.UserID, processDef.ProcessKey, ErrPermissionDenied)
}
