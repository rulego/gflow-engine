package service

// Tests for forecast.go: switch/condition branch prediction in BuildUpcomingNodes.
// A deterministic switch (cases evaluable from instance variables) must be traversed
// along the matched branch; anything not cleanly evaluable keeps the old truncation.

import (
	"testing"

	"github.com/rulego/gflow-engine/types/constants"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/rulego/api/types"
)

func forecastChain() *types.RuleChain {
	node := func(id, typ string, cfg map[string]interface{}) *types.RuleNode {
		return &types.RuleNode{Id: id, Type: typ, Configuration: cfg}
	}
	return &types.RuleChain{
		Metadata: types.RuleMetadata{
			Nodes: []*types.RuleNode{
				node("n_mgr", "userTask", map[string]interface{}{}),
				node("n_sw", "switch", map[string]interface{}{
					"cases": []interface{}{
						map[string]interface{}{"case": "msg.days > 10", "then": "b_backup"},
						map[string]interface{}{"case": "msg.days > 5", "then": "b_gm"},
					},
				}),
				node("n_gm", "userTask", map[string]interface{}{}),
				node("n_backup", "userTask", map[string]interface{}{}),
				node("gflow_end", "end", map[string]interface{}{}),
			},
			Connections: []types.NodeConnection{
				{FromId: "n_mgr", ToId: "n_sw", Type: types.Success},
				{FromId: "n_sw", ToId: "n_backup", Type: "b_backup"},
				{FromId: "n_sw", ToId: "n_gm", Type: "b_gm"},
				{FromId: "n_sw", ToId: "gflow_end", Type: "b_end"},
				{FromId: "n_gm", ToId: "gflow_end", Type: types.Success},
				{FromId: "n_backup", ToId: "gflow_end", Type: types.Success},
			},
		},
	}
}

func forecastResolver() func(map[string]interface{}, string, string, map[string]interface{}) *ApproverPreview {
	return func(cfg map[string]interface{}, tenantID, owner string, _ map[string]interface{}) *ApproverPreview {
		return &ApproverPreview{ApproverType: "user", Assignees: []string{"u1"}}
	}
}

func upcomingIDs(nodes []dto.UpcomingNode) []string {
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.NodeID)
	}
	return ids
}

// 活跃节点后继是 switch 且变量可确定性求值：沿命中分支预测（首真命中，12 天应走
// 第一条 >10 分支而非 >5），未命中走 Default。
func TestBuildUpcomingNodes_SwitchDeterministicPrediction(t *testing.T) {
	chain := forecastChain()
	// 原指向 end 的兜底边改走 Default 且指向 backup（b_backup 保留验证首真命中）
	chain.Metadata.Connections[3].ToId = "n_backup"
	chain.Metadata.Connections[3].Type = types.DefaultRelationType

	SetApproverPreviewResolver(forecastResolver())
	defer SetApproverPreviewResolver(nil)

	// days=8：>10 不成立、>5 成立 → 命中 b_gm
	up := BuildUpcomingNodes(chain, []string{"n_mgr"}, "t1", "starter", map[string]interface{}{"days": 8})
	require.Equal(t, []string{"n_gm"}, upcomingIDs(up), "8 天应预测 gm 节点")

	// days=12：首条 >10 即命中 → backup（验证按 cases 顺序首真命中）
	up = BuildUpcomingNodes(chain, []string{"n_mgr"}, "t1", "starter", map[string]interface{}{"days": 12})
	require.Equal(t, []string{"n_backup"}, upcomingIDs(up), "12 天应按首真命中预测 backup")

	// days=3：两案均不成立 → Default → backup
	up = BuildUpcomingNodes(chain, []string{"n_mgr"}, "t1", "starter", map[string]interface{}{"days": 3})
	require.Equal(t, []string{"n_backup"}, upcomingIDs(up), "未命中应走 Default 分支")
}

// 变量缺失导致表达式求值出错、或分支型节点不可确定性求值（inclusive）时，预测退回截断。
func TestBuildUpcomingNodes_NonEvaluableTruncates(t *testing.T) {
	chain := forecastChain()
	SetApproverPreviewResolver(forecastResolver())
	defer SetApproverPreviewResolver(nil)

	// days 为字符串：比较求值出错 → 截断，不给出任何预测（保守：预测宁可缺席不给错）
	up := BuildUpcomingNodes(chain, []string{"n_mgr"}, "t1", "starter", map[string]interface{}{"days": "abc"})
	require.Empty(t, upcomingIDs(up), "求值不干净应维持截断")

	// inclusive 分支节点不在确定性求值范围 → 截断
	chain.Metadata.Nodes[1].Type = constants.NodeTypeInclusive
	up = BuildUpcomingNodes(chain, []string{"n_mgr"}, "t1", "starter", map[string]interface{}{"days": 8})
	require.Empty(t, upcomingIDs(up), "inclusive 应维持截断")
}
