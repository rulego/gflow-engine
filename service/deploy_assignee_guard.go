// 部署期校验直派审批人（approver.type=user 的 userIds）存在且启用。
// 直派不经候选池展开，也不命中停用兜底链——落在已删除用户名下的任务
// 无人可办，实例就此卡死。宿主未实现 ActiveUserChecker 时整体跳过。

package service

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"

	"github.com/sirupsen/logrus"
)

// deployDirectAssignees 汇集链内全部直派审批人（userTask 节点 approver.type=user
// 且 userIds 非空），返回 节点ID → userIds。节点配置是弱类型 map：与运行期
// 解析（components 的 ApproverConfig）同键，这里只读不依赖组件结构体。
func deployDirectAssignees(chain *types.RuleChain) map[string][]string {
	out := map[string][]string{}
	if chain == nil {
		return out
	}
	for _, node := range chain.Metadata.Nodes {
		if node.Type != constants.NodeTypeUserTask {
			continue
		}
		cfg, ok := map[string]interface{}(node.Configuration)["approver"].(map[string]interface{})
		if !ok {
			continue
		}
		if t, _ := cfg["type"].(string); t != "user" {
			continue
		}
		rawIDs, _ := cfg["userIds"].([]interface{})
		var ids []string
		for _, v := range rawIDs {
			if s, ok := v.(string); ok && s != "" {
				ids = append(ids, s)
			}
		}
		if len(ids) > 0 {
			out[node.Id] = ids
		}
	}
	return out
}

// ValidateDirectAssignees 部署期直派审批人存在/启用校验。identity 未实现
// ActiveUserChecker 时跳过；身份服务查询失败时降级放行并告警——部署不被身份
// 服务抖动卡死，运行期还有停用兜底链与空审批人兜底两道网；存在幽灵/停用
// 直派时返回 ErrValidation，错误信息定位节点与具体 userId。
func ValidateDirectAssignees(ctx context.Context, identity IdentityService, tenantID string, chain *types.RuleChain) error {
	if identity == nil || tenantID == "" {
		return nil
	}
	checker, ok := identity.(ActiveUserChecker)
	if !ok {
		return nil
	}
	direct := deployDirectAssignees(chain)
	if len(direct) == 0 {
		return nil
	}
	uniqSet := make(map[string]bool)
	for _, ids := range direct {
		for _, id := range ids {
			uniqSet[id] = true
		}
	}
	uniq := make([]string, 0, len(uniqSet))
	for id := range uniqSet {
		uniq = append(uniq, id)
	}
	sort.Strings(uniq)
	active, err := checker.AreActiveUsers(ctx, tenantID, uniq)
	if err != nil {
		logrus.WithError(err).WithField("tenantID", tenantID).
			Warn("direct assignee check skipped: identity service query failed")
		return nil
	}
	var issues []string
	nodeIDs := make([]string, 0, len(direct))
	for nodeID := range direct {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	for _, nodeID := range nodeIDs {
		for _, id := range direct[nodeID] {
			if !active[id] {
				issues = append(issues, fmt.Sprintf("node %s 直派审批人 %q 不存在或已停用", nodeID, id))
			}
		}
	}
	if len(issues) > 0 {
		return fmt.Errorf("invalid direct assignees: %w: %s", ErrValidation, strings.Join(issues, "; "))
	}
	return nil
}
