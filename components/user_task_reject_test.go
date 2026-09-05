package components

// Tests for reject-jump reset region computation: the region between the jump
// target and the rejecting node must cover every userTask on the re-execution
// path (middle nodes would otherwise keep their completed tasks and be silently
// auto-passed on re-entry), while parallel branches outside the region must be
// left untouched (terminating their in-flight tasks would starve the join).

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func graphFromConns(nodeTypes map[string]string, conns [][2]string) *nodeGraph {
	g := &nodeGraph{
		nodeTypes: nodeTypes,
		forward:   make(map[string][]string),
		backward:  make(map[string][]string),
	}
	for _, c := range conns {
		g.forward[c[0]] = append(g.forward[c[0]], c[1])
		g.backward[c[1]] = append(g.backward[c[1]], c[0])
	}
	return g
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// 线性链 start→A→B→C：从 C 驳回回跳 A，A/B/C 三个审批节点全部重审
func TestRejectResetNodes_LinearChain(t *testing.T) {
	g := graphFromConns(
		map[string]string{"start": "startTask", "A": "userTask", "B": "userTask", "C": "userTask", "end": "end"},
		[][2]string{{"start", "A"}, {"A", "B"}, {"B", "C"}, {"C", "end"}},
	)
	got := rejectResetNodes(g, "A", "C")
	require.Len(t, got, 3)
	require.True(t, contains(got, "A"), "目标节点必须清理")
	require.True(t, contains(got, "B"), "中间节点旧完成态必须清理，否则重入被静默自动通过")
	require.True(t, contains(got, "C"), "驳回节点自身必须清理，防回流死循环")
}

// 驳回点下游的并行分支不在回流路径上，保持原状
func TestRejectResetNodes_ParallelBranchOutsideRegion(t *testing.T) {
	g := graphFromConns(
		map[string]string{
			"start": "startTask", "A": "userTask", "B": "userTask", "C": "userTask",
			"D": "userTask", "join": "join", "end": "end",
		},
		[][2]string{
			{"start", "A"}, {"A", "B"}, {"B", "C"}, {"C", "join"},
			{"join", "D"}, {"D", "end"},
		},
	)
	got := rejectResetNodes(g, "A", "C")
	require.Len(t, got, 3)
	require.False(t, contains(got, "D"), "驳回点下游分支不在回流路径，不得清理")
}

// startTask 与 end 等非 userTask 节点不参与清理
func TestRejectResetNodes_OnlyUserTasks(t *testing.T) {
	g := graphFromConns(
		map[string]string{"start": "startTask", "A": "userTask", "end": "end"},
		[][2]string{{"start", "A"}, {"A", "end"}},
	)
	got := rejectResetNodes(g, "start", "A")
	require.Len(t, got, 1)
	require.Equal(t, "A", got[0])
}

// 定义缺失时返回空（调用方按空区域跳过清理，不 panic）
func TestRejectResetNodes_NilGraph(t *testing.T) {
	require.Empty(t, rejectResetNodes(nil, "A", "C"))
}

// 会签一票否决只适用于全员通过规则；any/majority 等阈值规则首票拒绝不定局
func TestCountersignRequiresUnanimity(t *testing.T) {
	require.True(t, countersignRequiresUnanimity(""), "空规则按默认全员通过")
	require.True(t, countersignRequiresUnanimity(`{}`))
	require.True(t, countersignRequiresUnanimity(`{"type":"all"}`))
	require.False(t, countersignRequiresUnanimity(`{"type":"any"}`), "any 规则首票拒绝不定局")
	require.False(t, countersignRequiresUnanimity(`{"type":"majority"}`))
	require.True(t, countersignRequiresUnanimity(`not-json`), "解析失败按默认全员通过，与 complete 路径同口径")
}
