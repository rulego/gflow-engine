package service

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
)

// buildChainJSON 拼一个最小可解析的扁平格式 RuleChain JSON。
// nodes 是 (id, type, asyncIfAiAgent) 三元组；connections 是 (from, to) 对。
// asyncIfAiAgent 仅对 type=="aiAgent" 生效。
func buildChainJSON(t *testing.T, nodes [][3]interface{}, conns [][2]string) string {
	t.Helper()
	var b strings.Builder
	b.WriteString(`{"ruleChain":{"id":"t","name":"t","root":true},"metadata":{"firstNodeIndex":0,"nodes":[`)
	for i, n := range nodes {
		if i > 0 {
			b.WriteByte(',')
		}
		id, _ := n[0].(string)
		ty, _ := n[1].(string)
		async, _ := n[2].(bool)
		cfg := "{}"
		if ty == "aiAgent" && async {
			cfg = `{"async":true}`
		}
		fmt.Fprintf(&b, `{"id":%q,"type":%q,"name":%q,"configuration":%s}`, id, ty, id, cfg)
	}
	b.WriteString(`],"connections":[`)
	for i, c := range conns {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"fromId":%q,"toId":%q,"type":"Success"}`, c[0], c[1])
	}
	b.WriteString(`]}}`)
	return b.String()
}

func mustParseGraph(t *testing.T, defJSON string) *forkGraph {
	t.Helper()
	g, err := parseForkGraph(&model.WfProcess{DefinitionJSON: defJSON})
	require.NoError(t, err, "parseForkGraph")
	require.NotNil(t, g)
	return g
}

// newGraph 直接构造一个 graph（绕开 JSON 解析），用于不需要 configuration 解析的测试。
func newGraph(nodes map[string]string, edges [][2]string, asyncAgents map[string]bool) *forkGraph {
	g := &forkGraph{
		nodeType:    nodes,
		parents:     make(map[string]map[string]bool),
		children:    make(map[string]map[string]bool),
		asyncAgents: asyncAgents,
	}
	if g.asyncAgents == nil {
		g.asyncAgents = map[string]bool{}
	}
	for _, e := range edges {
		from, to := e[0], e[1]
		if g.children[from] == nil {
			g.children[from] = map[string]bool{}
		}
		g.children[from][to] = true
		if g.parents[to] == nil {
			g.parents[to] = map[string]bool{}
		}
		g.parents[to][from] = true
	}
	return g
}

func strPtr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// parseForkGraph
// ---------------------------------------------------------------------------

func TestParseForkGraph_FlatFormat(t *testing.T) {
	def := buildChainJSON(t,
		[][3]interface{}{{"fork1", "fork", false}, {"a", "userTask", false}, {"b", "userTask", false}, {"join1", "join", false}},
		[][2]string{{"fork1", "a"}, {"fork1", "b"}, {"a", "join1"}, {"b", "join1"}},
	)
	g := mustParseGraph(t, def)
	assert.Equal(t, "fork", g.nodeType["fork1"])
	assert.Equal(t, "userTask", g.nodeType["a"])
	assert.True(t, g.children["fork1"]["a"])
	assert.True(t, g.children["fork1"]["b"])
	assert.True(t, g.parents["a"]["fork1"])
}

func TestParseForkGraph_DesignerEnvelope(t *testing.T) {
	// envelope 格式：顶层有 form/flow/ruleChain/metadata 四键，由 ToRuleChain 解构。
	def := `{
		"form": {"fields": []},
		"flow": {"canvas": {}},
		"ruleChain": {"id":"t","name":"t","root":true},
		"metadata": {"firstNodeIndex":0,
			"nodes": [
				{"id":"fork1","type":"fork","name":"f"},
				{"id":"a","type":"userTask","name":"a","configuration":{}}
			],
			"connections":[{"fromId":"fork1","toId":"a","type":"Success"}]
		}
	}`
	g := mustParseGraph(t, def)
	assert.Equal(t, "fork", g.nodeType["fork1"])
	assert.Equal(t, "userTask", g.nodeType["a"])
	assert.True(t, g.children["fork1"]["a"])
}

func TestParseForkGraph_AsyncAiAgentRecorded(t *testing.T) {
	def := buildChainJSON(t,
		[][3]interface{}{
			{"fork1", "fork", false},
			{"sync_a", "aiAgent", false},
			{"async_a", "aiAgent", true},
		},
		[][2]string{{"fork1", "sync_a"}, {"fork1", "async_a"}},
	)
	g := mustParseGraph(t, def)
	assert.False(t, g.asyncAgents["sync_a"], "sync aiAgent should not be flagged async")
	assert.True(t, g.asyncAgents["async_a"], "async aiAgent should be flagged")
}

func TestParseForkGraph_InvalidJSON(t *testing.T) {
	_, err := parseForkGraph(&model.WfProcess{DefinitionJSON: `{not json`})
	require.Error(t, err)
}

func TestParseForkGraph_EmptyDefinition(t *testing.T) {
	_, err := parseForkGraph(&model.WfProcess{DefinitionJSON: ""})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// isAiAgentAsync
// ---------------------------------------------------------------------------

func TestIsAiAgentAsync(t *testing.T) {
	cases := []struct {
		name string
		cfg  types.Configuration
		want bool
	}{
		{"nil cfg", nil, false},
		{"missing key", types.Configuration{}, false},
		{"bool true", types.Configuration{"async": true}, true},
		{"bool false", types.Configuration{"async": false}, false},
		{"string true", types.Configuration{"async": "true"}, true},
		{"string True", types.Configuration{"async": "True"}, true},
		{"string TRUE", types.Configuration{"async": "TRUE"}, true},
		{"string false", types.Configuration{"async": "false"}, false},
		{"other type", types.Configuration{"async": 1}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, isAiAgentAsync(c.cfg))
		})
	}
}

// ---------------------------------------------------------------------------
// isSuspendNode
// ---------------------------------------------------------------------------

func TestIsSuspendNode(t *testing.T) {
	g := &forkGraph{
		nodeType: map[string]string{
			"u":      "userTask",
			"aSync":  "aiAgent",
			"aAsync": "aiAgent",
			"d":      "delay",
			"s":      "jsTransform",
			"f":      "fork",
			"j":      "join",
		},
		asyncAgents: map[string]bool{"aAsync": true},
	}
	assert.True(t, g.isSuspendNode("u"), "userTask is suspend")
	assert.True(t, g.isSuspendNode("aSync"), "sync aiAgent is suspend")
	assert.False(t, g.isSuspendNode("aAsync"), "async aiAgent is NOT suspend")
	assert.True(t, g.isSuspendNode("d"), "delay is suspend")
	assert.False(t, g.isSuspendNode("s"), "jsTransform is not suspend")
	assert.False(t, g.isSuspendNode("f"), "fork is not suspend")
	assert.False(t, g.isSuspendNode("j"), "join is not suspend")
	assert.False(t, g.isSuspendNode("missing"), "missing node returns false")
}

// ---------------------------------------------------------------------------
// findForkAncestor
// ---------------------------------------------------------------------------

func TestFindForkAncestor_Linear_NoFork(t *testing.T) {
	def := buildChainJSON(t,
		[][3]interface{}{{"start", "start", false}, {"a", "userTask", false}, {"end", "end", false}},
		[][2]string{{"start", "a"}, {"a", "end"}},
	)
	g := mustParseGraph(t, def)
	assert.Equal(t, "", g.findForkAncestor("a"), "linear chain has no fork ancestor")
	assert.Equal(t, "", g.findForkAncestor("start"), "start node has no fork ancestor")
}

func TestFindForkAncestor_DirectParent(t *testing.T) {
	def := buildChainJSON(t,
		[][3]interface{}{
			{"start", "start", false},
			{"fork1", "fork", false},
			{"a", "userTask", false},
			{"b", "userTask", false},
			{"join1", "join", false},
			{"end", "end", false},
		},
		[][2]string{
			{"start", "fork1"}, {"fork1", "a"}, {"fork1", "b"},
			{"a", "join1"}, {"b", "join1"}, {"join1", "end"},
		},
	)
	g := mustParseGraph(t, def)
	assert.Equal(t, "fork1", g.findForkAncestor("a"))
	assert.Equal(t, "fork1", g.findForkAncestor("b"))
	assert.Equal(t, "fork1", g.findForkAncestor("join1"))
	assert.Equal(t, "", g.findForkAncestor("fork1"), "fork1 itself excluded")
	assert.Equal(t, "", g.findForkAncestor("start"), "start has no fork ancestor")
}

func TestFindForkAncestor_DeeplyNested(t *testing.T) {
	// fork1 → script → task：task 的 fork 祖先在两层之上。
	def := buildChainJSON(t,
		[][3]interface{}{
			{"fork1", "fork", false},
			{"script", "jsTransform", false},
			{"task", "userTask", false},
		},
		[][2]string{{"fork1", "script"}, {"script", "task"}},
	)
	g := mustParseGraph(t, def)
	assert.Equal(t, "fork1", g.findForkAncestor("task"), "BFS should find fork through intermediate nodes")
}

func TestFindForkAncestor_EmptyStart(t *testing.T) {
	g := &forkGraph{}
	assert.Equal(t, "", g.findForkAncestor(""))
}

// ---------------------------------------------------------------------------
// findForkAncestor：inclusive（包容分支）也应被识别为分支网关
// ---------------------------------------------------------------------------

func TestFindForkAncestor_InclusiveAsBranchingNode(t *testing.T) {
	// inclusive → [task_a, task_b] → join：inclusive 应被识别为分支祖先
	def := buildChainJSON(t,
		[][3]interface{}{
			{"inc1", "inclusive", false},
			{"a", "userTask", false},
			{"b", "userTask", false},
			{"join1", "join", false},
		},
		[][2]string{{"inc1", "a"}, {"inc1", "b"}, {"a", "join1"}, {"b", "join1"}},
	)
	g := mustParseGraph(t, def)
	assert.Equal(t, "inc1", g.findForkAncestor("a"),
		"inclusive should be detected as branching ancestor")
	assert.Equal(t, "inc1", g.findForkAncestor("b"))
	assert.Equal(t, "inc1", g.findForkAncestor("join1"))
	assert.Equal(t, "", g.findForkAncestor("inc1"), "inclusive itself excluded")
}

func TestFindForkAncestor_InclusiveDeepDescendant(t *testing.T) {
	// inclusive → script → task：task 的分支祖先在两层之上
	def := buildChainJSON(t,
		[][3]interface{}{
			{"inc1", "inclusive", false},
			{"script", "jsTransform", false},
			{"task", "userTask", false},
		},
		[][2]string{{"inc1", "script"}, {"script", "task"}},
	)
	g := mustParseGraph(t, def)
	assert.Equal(t, "inc1", g.findForkAncestor("task"),
		"BFS should find inclusive through intermediate nodes")
}

func TestFindForkAncestor_SwitchNotBranching(t *testing.T) {
	// switch 是排他路由，不应被识别为分支网关
	def := buildChainJSON(t,
		[][3]interface{}{
			{"sw1", "switch", false},
			{"a", "userTask", false},
		},
		[][2]string{{"sw1", "a"}},
	)
	g := mustParseGraph(t, def)
	assert.Equal(t, "", g.findForkAncestor("a"),
		"switch is exclusive routing, not a branching node")
}

// ---------------------------------------------------------------------------
// hasNestedFork：inclusive 嵌套也应被检测到
// ---------------------------------------------------------------------------

func TestHasNestedFork_InclusiveInsideFork(t *testing.T) {
	// fork → inclusive → task：fork 内嵌 inclusive 应被检测
	def := buildChainJSON(t,
		[][3]interface{}{
			{"outer", "fork", false},
			{"inner", "inclusive", false},
			{"a", "userTask", false},
		},
		[][2]string{{"outer", "inner"}, {"inner", "a"}},
	)
	g := mustParseGraph(t, def)
	assert.True(t, g.hasNestedFork("outer"),
		"inclusive inside fork should be detected as nested branching")
}

func TestHasNestedFork_SequentialForkBlocksNotNested(t *testing.T) {
	// fork1 → [a,b] → join1 → fork2 → [c,d] → join2：两个并行块顺序串联，
	// 不是「fork 分支内再 fork」。扫描必须止于 join1，否则 fork2 被误判成
	// 嵌套 fork，审批提交时硬报 ErrUnsupportedForkTopology、实例卡 active
	//（与 findFirstSuspendNode/findAllSuspendNodes/branchHasAnyTask 同一边界）。
	def := buildChainJSON(t,
		[][3]interface{}{
			{"fork1", "fork", false},
			{"a", "userTask", false},
			{"b", "userTask", false},
			{"join1", "join", false},
			{"fork2", "fork", false},
			{"c", "userTask", false},
			{"d", "userTask", false},
			{"join2", "join", false},
		},
		[][2]string{
			{"fork1", "a"}, {"fork1", "b"},
			{"a", "join1"}, {"b", "join1"},
			{"join1", "fork2"},
			{"fork2", "c"}, {"fork2", "d"},
			{"c", "join2"}, {"d", "join2"},
		},
	)
	g := mustParseGraph(t, def)
	assert.False(t, g.hasNestedFork("fork1"),
		"sequential fork blocks (fork→join→fork) are supported topology, not nested fork")
}

func TestHasNestedFork_ForkInsideInclusive(t *testing.T) {
	// inclusive → fork → task：inclusive 内嵌 fork 也应被检测
	def := buildChainJSON(t,
		[][3]interface{}{
			{"outer", "inclusive", false},
			{"inner", "fork", false},
			{"a", "userTask", false},
		},
		[][2]string{{"outer", "inner"}, {"inner", "a"}},
	)
	g := mustParseGraph(t, def)
	assert.True(t, g.hasNestedFork("outer"),
		"fork inside inclusive should be detected as nested branching")
}

func TestIsBranchingNode(t *testing.T) {
	assert.True(t, isBranchingNode("fork"))
	assert.True(t, isBranchingNode("inclusive"))
	assert.False(t, isBranchingNode("switch"))
	assert.False(t, isBranchingNode("userTask"))
	assert.False(t, isBranchingNode("join"))
	assert.False(t, isBranchingNode(""))
}

// ---------------------------------------------------------------------------
// hasNestedFork
// ---------------------------------------------------------------------------

func TestHasNestedFork_NoNesting(t *testing.T) {
	def := buildChainJSON(t,
		[][3]interface{}{
			{"fork1", "fork", false},
			{"a", "userTask", false},
			{"b", "userTask", false},
			{"join1", "join", false},
		},
		[][2]string{{"fork1", "a"}, {"fork1", "b"}, {"a", "join1"}, {"b", "join1"}},
	)
	g := mustParseGraph(t, def)
	assert.False(t, g.hasNestedFork("fork1"))
}

func TestHasNestedFork_DirectChildFork(t *testing.T) {
	def := buildChainJSON(t,
		[][3]interface{}{
			{"outer", "fork", false},
			{"inner", "fork", false},
			{"a", "userTask", false},
		},
		[][2]string{{"outer", "inner"}, {"inner", "a"}},
	)
	g := mustParseGraph(t, def)
	assert.True(t, g.hasNestedFork("outer"))
}

func TestHasNestedFork_DeepNested(t *testing.T) {
	// outer → script → inner → task
	def := buildChainJSON(t,
		[][3]interface{}{
			{"outer", "fork", false},
			{"script", "jsTransform", false},
			{"inner", "fork", false},
			{"a", "userTask", false},
		},
		[][2]string{{"outer", "script"}, {"script", "inner"}, {"inner", "a"}},
	)
	g := mustParseGraph(t, def)
	assert.True(t, g.hasNestedFork("outer"), "deeply nested fork should be detected")
}

func TestHasNestedFork_EmptyForkID(t *testing.T) {
	g := &forkGraph{}
	assert.False(t, g.hasNestedFork(""))
}

// ---------------------------------------------------------------------------
// findFirstSuspendNode
// ---------------------------------------------------------------------------

func TestFindFirstSuspendNode_PicksHighestUserTask(t *testing.T) {
	// fork → script → task_a1 → task_a2 → join：BFS 应返回最靠近 fork 的 task_a1
	def := buildChainJSON(t,
		[][3]interface{}{
			{"fork", "fork", false},
			{"script", "jsTransform", false},
			{"task_a1", "userTask", false},
			{"task_a2", "userTask", false},
			{"join", "join", false},
		},
		[][2]string{{"fork", "script"}, {"script", "task_a1"}, {"task_a1", "task_a2"}, {"task_a2", "join"}},
	)
	g := mustParseGraph(t, def)
	assert.Equal(t, "task_a1", g.findFirstSuspendNode("script"),
		"should return the first (closest-to-fork) userTask, not the deepest")
}

func TestFindFirstSuspendNode_AsyncAiAgentSkipped(t *testing.T) {
	// 关键回归：async=true 的 aiAgent 不应被当作 suspend node。
	// 否则 multi-node 恢复会重发 LLM 请求（重复扣费）。
	def := buildChainJSON(t,
		[][3]interface{}{
			{"fork", "fork", false},
			{"async_agent", "aiAgent", true}, // async — should be SKIPPED
			{"sync_agent", "aiAgent", false}, // sync — should be PICKED
			{"real_task", "userTask", false}, // backup suspend after async agent
		},
		[][2]string{
			{"fork", "async_agent"}, {"async_agent", "real_task"},
			{"fork", "sync_agent"},
		},
	)
	g := mustParseGraph(t, def)
	assert.Equal(t, "sync_agent", g.findFirstSuspendNode("sync_agent"),
		"sync aiAgent itself is suspend")
	assert.Equal(t, "real_task", g.findFirstSuspendNode("async_agent"),
		"async aiAgent must be skipped; BFS continues to find real_task")
}

func TestFindFirstSuspendNode_NoSuspendInBranch(t *testing.T) {
	// 分支里只有同步节点（jsTransform）
	def := buildChainJSON(t,
		[][3]interface{}{
			{"fork", "fork", false},
			{"script1", "jsTransform", false},
			{"script2", "jsTransform", false},
		},
		[][2]string{{"fork", "script1"}, {"script1", "script2"}},
	)
	g := mustParseGraph(t, def)
	assert.Equal(t, "", g.findFirstSuspendNode("script1"),
		"branch with only sync nodes returns empty")
}

func TestFindFirstSuspendNode_DelayIsSuspend(t *testing.T) {
	def := buildChainJSON(t,
		[][3]interface{}{
			{"fork", "fork", false},
			{"d", "delay", false},
		},
		[][2]string{{"fork", "d"}},
	)
	g := mustParseGraph(t, def)
	assert.Equal(t, "d", g.findFirstSuspendNode("d"))
}

func TestFindFirstSuspendNode_EmptyRoot(t *testing.T) {
	g := &forkGraph{}
	assert.Equal(t, "", g.findFirstSuspendNode(""))
}

// ---------------------------------------------------------------------------
// allRowsCompleted
// ---------------------------------------------------------------------------

func TestAllRowsCompleted(t *testing.T) {
	completed := string(enums.ApprovalResultApproved)

	cases := []struct {
		name string
		rows []*model.WfTask
		want bool
	}{
		{"empty", nil, false},
		{"one completed", []*model.WfTask{
			{Status: string(enums.TaskStatusCompleted), EndReason: &completed},
		}, true},
		{"one active", []*model.WfTask{
			{Status: string(enums.TaskStatusActive)},
		}, false},
		{"mix completed and active", []*model.WfTask{
			{Status: string(enums.TaskStatusCompleted), EndReason: &completed},
			{Status: string(enums.TaskStatusActive)},
		}, false},
		{"all completed (countersign)", []*model.WfTask{
			{Status: string(enums.TaskStatusCompleted), EndReason: &completed},
			{Status: string(enums.TaskStatusCompleted), EndReason: &completed},
			{Status: string(enums.TaskStatusCompleted), EndReason: &completed},
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, allRowsCompleted(c.rows))
		})
	}
}

// ---------------------------------------------------------------------------
// sortedKeys
// ---------------------------------------------------------------------------

func TestSortedKeys(t *testing.T) {
	in := map[string]bool{"c": true, "a": true, "b": false}
	got := sortedKeys(in)
	assert.Equal(t, []string{"a", "b", "c"}, got, "should be sorted alphabetically")
}

func TestSortedKeys_Empty(t *testing.T) {
	assert.Equal(t, []string{}, sortedKeys(nil))
}

// ---------------------------------------------------------------------------
// InvalidateForkGraphCache
// ---------------------------------------------------------------------------

func TestInvalidateForkGraphCache(t *testing.T) {
	forkGraphCache.Store("proc_test_invalidate", &forkGraph{})
	forkGraphCache.Store("proc_keep", &forkGraph{})

	InvalidateForkGraphCache("proc_test_invalidate")

	_, ok1 := forkGraphCache.Load("proc_test_invalidate")
	_, ok2 := forkGraphCache.Load("proc_keep")
	assert.False(t, ok1, "target entry should be removed")
	assert.True(t, ok2, "unrelated entries should remain")

	// Cleanup
	InvalidateForkGraphCache("proc_keep")
}

func TestInvalidateForkGraphCache_EmptyID(t *testing.T) {
	// Should be a no-op, not panic.
	InvalidateForkGraphCache("")
}

// ---------------------------------------------------------------------------
// buildBranchResumeMsg — delay offset injection
// ---------------------------------------------------------------------------

func TestBuildBranchResumeMsg_InjectsDelayOffsetForDelayTask(t *testing.T) {
	// 关键回归：delay 类型的 task 必须注入 _delayOffsetMs，
	// 否则 delay 节点恢复时会从头重新计时（用户已经等过的时间被吃掉）。
	//
	// buildBranchResumeMsg 不注入 task_id：一旦注入，task_id 会通过 msg.Copy()
	// 传播到 join → end，让 end 节点误判"task 已存在"跳过任务创建。
	// task_id 由各节点自行管理；代价是 multi-node 恢复时可能多创建 1 条
	// delay wf_task 记录（已被 completed），是可接受的折中。
	inst := &model.WfInstance{
		ID:        "inst-1",
		ProcessID: "proc-1",
		TenantID:  "tenant-1",
		CreatedBy: "owner-1",
		Variables: strPtr(`{"x":1}`),
	}
	delayTask := &model.WfTask{
		ID:         "delay-task",
		TaskDefKey: "delay_node",
		TaskType:   constants.TaskTypeDelay,
		Status:     string(enums.TaskStatusCompleted),
		CreatedAt:  time.Now().Add(-5 * time.Second),
		Variables:  strPtr(`{"d":1}`),
	}
	s := &RuntimeServiceImpl{}
	msg := s.buildBranchResumeMsg(inst, "delay_node", []*model.WfTask{delayTask})

	offset := msg.Metadata.GetValue(constants.KeyDelayOffsetMs)
	assert.NotEmpty(t, offset, "_delayOffsetMs must be set for delay task")
	assert.Empty(t, msg.Metadata.GetValue(constants.KeyTaskID),
		"task_id must NOT be injected to avoid downstream TaskCreator confusion")
	assert.Equal(t, "tenant-1", msg.Metadata.GetValue(constants.KeyTenantID))
	assert.Equal(t, "inst-1", msg.Metadata.GetValue(constants.KeyInstanceID))
}

func TestBuildBranchResumeMsg_NoDelayOffsetForUserTask(t *testing.T) {
	inst := &model.WfInstance{
		ID:        "inst-1",
		ProcessID: "proc-1",
		TenantID:  "tenant-1",
	}
	userTask := &model.WfTask{
		ID:         "user-task",
		TaskDefKey: "task_node",
		TaskType:   constants.TaskTypeUserTask,
		Status:     string(enums.TaskStatusCompleted),
		Variables:  strPtr(`{"u":1}`),
	}
	s := &RuntimeServiceImpl{}
	msg := s.buildBranchResumeMsg(inst, "task_node", []*model.WfTask{userTask})

	assert.Empty(t, msg.Metadata.GetValue(constants.KeyDelayOffsetMs),
		"_delayOffsetMs must NOT be set for non-delay task")
}

func TestBuildBranchResumeMsg_FallsBackToInstanceVars(t *testing.T) {
	// rows 为空（分支还没创建 task）→ 用 instance.Variables 兜底
	inst := &model.WfInstance{
		ID:        "inst-1",
		ProcessID: "proc-1",
		TenantID:  "tenant-1",
		Variables: strPtr(`{"from_instance":true}`),
	}
	s := &RuntimeServiceImpl{}
	msg := s.buildBranchResumeMsg(inst, "node_x", nil)

	assert.Equal(t, `{"from_instance":true}`, msg.GetData(),
		"should fall back to instance.Variables when no task rows")
	assert.Empty(t, msg.Metadata.GetValue(constants.KeyTaskID),
		"task_id should be empty when no rows")
}

func TestBuildBranchResumeMsg_EmptyVarsFallback(t *testing.T) {
	// 完全没变量：rows 空 + instance vars 空
	inst := &model.WfInstance{ID: "i", ProcessID: "p", TenantID: "t"}
	s := &RuntimeServiceImpl{}
	msg := s.buildBranchResumeMsg(inst, "n", nil)
	assert.Equal(t, "{}", msg.GetData(), "should default to empty JSON object")
}

func TestBuildBranchResumeMsg_BusinessKeyPropagated(t *testing.T) {
	inst := &model.WfInstance{
		ID:          "i",
		ProcessID:   "p",
		TenantID:    "t",
		BusinessKey: strPtr("biz-123"),
	}
	s := &RuntimeServiceImpl{}
	msg := s.buildBranchResumeMsg(inst, "n", nil)
	assert.Equal(t, "biz-123", msg.Metadata.GetValue(constants.KeyBusinessKey),
		"BusinessKey should propagate to resume msg")
}

// ---------------------------------------------------------------------------
// analyzeForkResume — graph-level checks via direct graph construction.
//
// 注意：analyzeForkResume 通过 s.taskDAO.GetByProcessInstanceID 读 DB，taskDAO
// 是 *dao.TaskDAO 具体类型不能 mock。这里只测图算法分支（不需要读 task），
// task 状态相关的分支（skip / multi）由 e2e 覆盖（见 service/e2e/parallel_resume_test.go）。
// ---------------------------------------------------------------------------

// callAnalyzeForkResumeNoTask 调用 analyzeForkResume，但断言它会因为
// "找不到 task" 路径走 single fallback。用于测嵌套 fork / 无暂停节点 hard fail。
// 走到 loadForkGraph 需要 processDAO 存在，但缓存已预填时 processDAO 不会被调
// （loadForkGraph 缓存命中直接返回）。
// taskDAO 传 nil，让 DAO 检查在缓存命中后短路。
//
// analyzeForkResume 的早期分支：
//   if s.processDAO == nil || s.taskDAO == nil { return single }
// 会短路，因此直接构造 graph + 调用其私有图算法方法来间接验证。
//
// 这些验证已移到 TestAnalyzeForkResume_*_GraphOnly。

func TestAnalyzeForkResume_NestedFork_HardFailViaGraphAlgorithms(t *testing.T) {
	// 间接验证：analyzeForkResume 内部用 graph.hasNestedFork 做 hard fail。
	// 这里直接测算法行为，等价于 analyzeForkResume 路径里的同一行检查。
	graph := newGraph(
		map[string]string{"outer": "fork", "inner": "fork", "a": "userTask", "b": "userTask"},
		[][2]string{{"outer", "inner"}, {"outer", "a"}, {"inner", "b"}},
		nil,
	)
	assert.True(t, graph.hasNestedFork("outer"),
		"nested fork detection: this is what analyzeForkResume uses to hard-fail")
}

func TestAnalyzeForkResume_NoSuspendInBranch_HardFailViaGraphAlgorithms(t *testing.T) {
	// 间接验证：analyzeForkResume 内部用 findFirstSuspendNode 做 hard fail。
	// 这里直接测算法行为。
	graph := newGraph(
		map[string]string{"fork1": "fork", "a": "userTask", "script": "jsTransform", "join1": "join"},
		[][2]string{{"fork1", "a"}, {"fork1", "script"}, {"a", "join1"}, {"script", "join1"}},
		nil,
	)
	assert.Equal(t, "", graph.findFirstSuspendNode("script"),
		"branch with only sync nodes returns empty → analyzeForkResume hard-fails")
}

func TestAnalyzeForkResume_AsyncAiAgent_NotTreatedAsSuspend(t *testing.T) {
	graph := newGraph(
		map[string]string{"fork1": "fork", "async_a": "aiAgent", "sync_b": "userTask"},
		[][2]string{{"fork1", "async_a"}, {"fork1", "sync_b"}},
		map[string]bool{"async_a": true},
	)
	assert.False(t, graph.isSuspendNode("async_a"),
		"async aiAgent must be excluded from suspend detection")
	assert.True(t, graph.isSuspendNode("sync_b"))
}

// ---------------------------------------------------------------------------
// 串联 userTask 分支的算法函数：findBranchRoot / findAllSuspendNodes
// ---------------------------------------------------------------------------

func TestFindAllSuspendNodes_OrderIsBFS(t *testing.T) {
	// task_a1 → task_a2 → join：BFS 顺序应该是 task_a1 在前，task_a2 在后
	graph := newGraph(
		map[string]string{"fork1": "fork", "task_a1": "userTask", "task_a2": "userTask", "task_b": "userTask", "join1": "join"},
		[][2]string{
			{"fork1", "task_a1"}, {"fork1", "task_b"},
			{"task_a1", "task_a2"}, {"task_a2", "join1"}, {"task_b", "join1"},
		},
		nil,
	)
	suspends := graph.findAllSuspendNodes("task_a1")
	assert.Equal(t, []string{"task_a1", "task_a2"}, suspends,
		"BFS order from task_a1: closest-to-fork first")
}

func TestFindAllSuspendNodes_ReturnsNilForSyncOnlyBranch(t *testing.T) {
	// sync_b 是纯同步分支，没有 suspend 节点
	graph := newGraph(
		map[string]string{"fork1": "fork", "task_a": "userTask", "sync_b": "jsTransform", "join1": "join"},
		[][2]string{{"fork1", "task_a"}, {"fork1", "sync_b"}, {"task_a", "join1"}, {"sync_b", "join1"}},
		nil,
	)
	assert.Nil(t, graph.findAllSuspendNodes("sync_b"),
		"sync-only branch returns nil from findAllSuspendNodes")
}

func TestFindAllSuspendNodes_EmptyRoot(t *testing.T) {
	graph := newGraph(map[string]string{}, nil, nil)
	assert.Nil(t, graph.findAllSuspendNodes(""))
}

func TestFindBranchRoot_DirectChildOfFork(t *testing.T) {
	// startNodeId 就是 fork 的直接子节点
	graph := newGraph(
		map[string]string{"fork1": "fork", "task_a": "userTask", "join1": "join"},
		[][2]string{{"fork1", "task_a"}, {"task_a", "join1"}},
		nil,
	)
	assert.Equal(t, "task_a", graph.findBranchRoot("task_a", "fork1"),
		"direct child of fork: branch root is itself")
}

func TestFindBranchRoot_DeepDescendant(t *testing.T) {
	// task_a1 → task_a2 → join，startNodeId=task_a2，forkID=fork1
	// task_a1 是 fork1 的直接子节点，所以 task_a2 的 branch root 是 task_a1
	graph := newGraph(
		map[string]string{"fork1": "fork", "task_a1": "userTask", "task_a2": "userTask", "task_b": "userTask", "join1": "join"},
		[][2]string{
			{"fork1", "task_a1"}, {"fork1", "task_b"},
			{"task_a1", "task_a2"}, {"task_a2", "join1"}, {"task_b", "join1"},
		},
		nil,
	)
	assert.Equal(t, "task_a1", graph.findBranchRoot("task_a2", "fork1"),
		"deep descendant: branch root is the direct child of fork on the path")
}

func TestFindBranchRoot_NotInForkSubtree(t *testing.T) {
	// startNodeId 不在 fork 子树里
	graph := newGraph(
		map[string]string{"fork1": "fork", "task_a": "userTask", "external": "userTask"},
		[][2]string{{"fork1", "task_a"}},
		nil,
	)
	assert.Equal(t, "", graph.findBranchRoot("external", "fork1"),
		"startNodeId not in fork subtree: empty result")
}

func TestFindBranchRoot_EmptyInputs(t *testing.T) {
	graph := newGraph(map[string]string{}, nil, nil)
	assert.Equal(t, "", graph.findBranchRoot("", "fork1"))
	assert.Equal(t, "", graph.findBranchRoot("task_a", ""))
	assert.Equal(t, "", graph.findBranchRoot("fork1", "fork1"))
}

// ---------------------------------------------------------------------------
// 混合分支（suspend + sync-only）图算法正确性（间接验证 analyzeForkResume 行为）
//
// analyzeForkResume 是 *RuntimeServiceImpl 的方法，依赖 processDAO/taskDAO，
// 单元测试不便直接调。但内部用的图算法（findFirstSuspendNode / findAllSuspendNodes）
// 可以直接测。这里通过图算法的返回值间接验证：
//   - 纯同步分支：findFirstSuspendNode 返回 ""（→ analyzeForkResume 走 skip 分支）
//   - 含 suspend 的分支：findFirstSuspendNode 返回 suspend id（→ 进入 reqs）
// ---------------------------------------------------------------------------

func TestAnalyzeForkResume_MixedBranches_SyncBranchReturnsEmpty(t *testing.T) {
	// fork → [task_a (userTask), sync_b (jsTransform)] → join
	// sync-only 分支被跳过，只有含 suspend 的分支进入 multi-node 恢复
	graph := newGraph(
		map[string]string{"fork1": "fork", "task_a": "userTask", "sync_b": "jsTransform", "join1": "join"},
		[][2]string{{"fork1", "task_a"}, {"fork1", "sync_b"}, {"task_a", "join1"}, {"sync_b", "join1"}},
		nil,
	)
	// task_a 分支
	assert.Equal(t, "task_a", graph.findFirstSuspendNode("task_a"),
		"task_a branch has suspend: used in multi-node reqs")
	// sync_b 分支
	assert.Equal(t, "", graph.findFirstSuspendNode("sync_b"),
		"sync_b branch has no suspend: skipped")
}

func TestAnalyzeForkResume_L2_AllSyncBranches_NoSuspendFound(t *testing.T) {
	// fork → [sync_a, sync_b] → join
	// 两条分支都是纯同步（这种情况不会调到 ExecuteNext，但算法应当返回 single 路径）
	graph := newGraph(
		map[string]string{"fork1": "fork", "sync_a": "jsTransform", "sync_b": "log", "join1": "join"},
		[][2]string{{"fork1", "sync_a"}, {"fork1", "sync_b"}, {"sync_a", "join1"}, {"sync_b", "join1"}},
		nil,
	)
	assert.Equal(t, "", graph.findFirstSuspendNode("sync_a"))
	assert.Equal(t, "", graph.findFirstSuspendNode("sync_b"))
	// 两条分支都没 suspend → analyzeForkResume 走 "all sync" 兜底，返回 forkResumeSingle
}

// ---------------------------------------------------------------------------
// 分支内多个串联 userTask 的算法验证
//
// 验证 analyzeForkResume 在不同 approve 顺序下的决策（间接通过图算法）：
//   - approve task_a1 时：startNodeId (task_a1) != last suspend (task_a2)
//     → 走 forkResumeSingle（驱动 chain 让 task_a2 被创建）
//   - approve task_a2 时：startNodeId == last suspend
//     → 检查所有兄弟分支，决定 multi/skip
// ---------------------------------------------------------------------------

func TestAnalyzeForkResume_SerialUserTasks_a1NotLastSuspend(t *testing.T) {
	// fork → [task_a1 → task_a2, task_b] → join
	graph := newGraph(
		map[string]string{"fork1": "fork", "task_a1": "userTask", "task_a2": "userTask", "task_b": "userTask", "join1": "join"},
		[][2]string{
			{"fork1", "task_a1"}, {"fork1", "task_b"},
			{"task_a1", "task_a2"}, {"task_a2", "join1"}, {"task_b", "join1"},
		},
		nil,
	)
	// task_a1 的 branch root 是它自己
	branchRoot := graph.findBranchRoot("task_a1", "fork1")
	assert.Equal(t, "task_a1", branchRoot)

	// task_a1 分支里所有 suspend
	allSuspends := graph.findAllSuspendNodes(branchRoot)
	assert.Equal(t, []string{"task_a1", "task_a2"}, allSuspends)

	// task_a1 不是最后一个 suspend → analyzeForkResume 应当走 forkResumeSingle
	// 来驱动 chain 创建 task_a2
	lastSuspend := allSuspends[len(allSuspends)-1]
	assert.NotEqual(t, "task_a1", lastSuspend,
		"task_a1 is NOT the last suspend → algorithm drives chain via single-node path")
	assert.Equal(t, "task_a2", lastSuspend)
}

func TestAnalyzeForkResume_SerialUserTasks_a2IsLastSuspend(t *testing.T) {
	// 同上拓扑，但 startNodeId = task_a2
	graph := newGraph(
		map[string]string{"fork1": "fork", "task_a1": "userTask", "task_a2": "userTask", "task_b": "userTask", "join1": "join"},
		[][2]string{
			{"fork1", "task_a1"}, {"fork1", "task_b"},
			{"task_a1", "task_a2"}, {"task_a2", "join1"}, {"task_b", "join1"},
		},
		nil,
	)
	branchRoot := graph.findBranchRoot("task_a2", "fork1")
	assert.Equal(t, "task_a1", branchRoot,
		"task_a2's branch root is task_a1 (the direct child of fork on its path)")

	allSuspends := graph.findAllSuspendNodes(branchRoot)
	lastSuspend := allSuspends[len(allSuspends)-1]
	assert.Equal(t, "task_a2", lastSuspend,
		"task_a2 IS the last suspend → algorithm proceeds to check siblings")
}

// ---------------------------------------------------------------------------
// buildBranchResumeMsg 不注入 task_id
//
// 注入的 task_id 会通过 userTask.TellSuccess(msg.Copy()) 传播到下游 → join → end，
// 让 end 节点的 TaskCreator.Before 误以为"task 已存在"跳过 CreateTask，
// 导致 wf_hi_task 缺少 end 记录。因此不注入 task_id，
// 让下游节点（sendEmail、end 等）正常 CreateTask。
// ---------------------------------------------------------------------------

func TestBuildBranchResumeMsg_NoTaskIDMetadata(t *testing.T) {
	// 关键不变量：buildBranchResumeMsg 的 msg.metadata 不包含 task_id
	// （防止下游节点 TaskCreator 跳过 CreateTask）。
	inst := &model.WfInstance{
		ID:        "inst-1",
		ProcessID: "proc-1",
		TenantID:  "tenant-1",
		CreatedBy: "starter",
		Variables: strPtr("{}"),
	}
	taskID := "task-1"
	taskRows := []*model.WfTask{
		{
			ID:         taskID,
			TaskDefKey: "task_a",
			TaskType:   constants.TaskTypeUserTask,
			Variables:  strPtr(`{"a_unique": "AAA"}`),
			CreatedAt:  time.Now(),
		},
	}

	s := &RuntimeServiceImpl{}
	msg := s.buildBranchResumeMsg(inst, "task_a", taskRows)

	assert.Empty(t, msg.GetMetadata().GetValue(constants.KeyTaskID),
		"buildBranchResumeMsg must NOT inject task_id (would propagate to end and break TaskCreator)")
	assert.Equal(t, "inst-1", msg.GetMetadata().GetValue(constants.KeyInstanceID))
	assert.Equal(t, "proc-1", msg.GetMetadata().GetValue(constants.KeyProcessID))
	assert.Contains(t, msg.GetData(), "a_unique",
		"each branch's msg carries its own task.Variables")
}

// ---------------------------------------------------------------------------
// inclusive（包容分支）算法路径验证
//
// inclusive 节点和 fork 一样会同时激活多条分支，需要 join 汇聚。
// findForkAncestor 通过 isBranchingNode 同时匹配 "fork" 和 "inclusive"，
// 否则 inclusive 分支里的 userTask approve 后会走 single-node 路径，
// join 的 TellCollect 丢失父上下文 → 卡死。
// ---------------------------------------------------------------------------

func TestInclusive_FindForkAncestor_Detected(t *testing.T) {
	// inclusive → [task_a, task_b] → join：task_a 的分支祖先是 inclusive
	graph := newGraph(
		map[string]string{"inc1": "inclusive", "task_a": "userTask", "task_b": "userTask", "join1": "join"},
		[][2]string{{"inc1", "task_a"}, {"inc1", "task_b"}, {"task_a", "join1"}, {"task_b", "join1"}},
		nil,
	)
	assert.Equal(t, "inc1", graph.findForkAncestor("task_a"),
		"inclusive should be detected as branching ancestor for task_a")
	assert.Equal(t, "inc1", graph.findForkAncestor("task_b"),
		"inclusive should be detected as branching ancestor for task_b")
	assert.Equal(t, "inc1", graph.findForkAncestor("join1"),
		"join is also under inclusive's subtree")
}

func TestInclusive_HasNestedFork_InclusiveInsideFork(t *testing.T) {
	// fork → [inc1 → [task], ...] → join：fork 内嵌 inclusive 应被检测
	graph := newGraph(
		map[string]string{"fork1": "fork", "inc1": "inclusive", "task_a": "userTask", "task_b": "userTask", "join1": "join"},
		[][2]string{{"fork1", "inc1"}, {"fork1", "task_b"}, {"inc1", "task_a"}, {"task_a", "join1"}, {"task_b", "join1"}},
		nil,
	)
	assert.True(t, graph.hasNestedFork("fork1"),
		"inclusive inside fork should be detected as nested branching")
}

func TestInclusive_HasNestedFork_ForkInsideInclusive(t *testing.T) {
	// inclusive → [fork → [task], ...] → join：inclusive 内嵌 fork 应被检测
	graph := newGraph(
		map[string]string{"inc1": "inclusive", "fork1": "fork", "task_a": "userTask", "task_b": "userTask", "join1": "join"},
		[][2]string{{"inc1", "fork1"}, {"inc1", "task_b"}, {"fork1", "task_a"}, {"task_a", "join1"}, {"task_b", "join1"}},
		nil,
	)
	assert.True(t, graph.hasNestedFork("inc1"),
		"fork inside inclusive should be detected as nested branching")
}

func TestInclusive_FindBranchRoot_Works(t *testing.T) {
	// inclusive → [task_a1 → task_a2, task_b] → join：task_a2 的 branch root 是 task_a1
	graph := newGraph(
		map[string]string{"inc1": "inclusive", "task_a1": "userTask", "task_a2": "userTask", "task_b": "userTask", "join1": "join"},
		[][2]string{
			{"inc1", "task_a1"}, {"inc1", "task_b"},
			{"task_a1", "task_a2"}, {"task_a2", "join1"}, {"task_b", "join1"},
		},
		nil,
	)
	assert.Equal(t, "task_a1", graph.findBranchRoot("task_a2", "inc1"),
		"findBranchRoot works the same for inclusive as for fork")
	assert.Equal(t, "task_b", graph.findBranchRoot("task_b", "inc1"),
		"direct child of inclusive is its own branch root")
}

func TestInclusive_AllSuspendNodes_Detected(t *testing.T) {
	// inclusive → [task_a1 → task_a2, task_b] → join
	graph := newGraph(
		map[string]string{"inc1": "inclusive", "task_a1": "userTask", "task_a2": "userTask", "task_b": "userTask", "join1": "join"},
		[][2]string{
			{"inc1", "task_a1"}, {"inc1", "task_b"},
			{"task_a1", "task_a2"}, {"task_a2", "join1"}, {"task_b", "join1"},
		},
		nil,
	)
	assert.Equal(t, []string{"task_a1", "task_a2"}, graph.findAllSuspendNodes("task_a1"),
		"findAllSuspendNodes works for inclusive branches")
	assert.Equal(t, []string{"task_b"}, graph.findAllSuspendNodes("task_b"),
		"single-suspend branch under inclusive")
}

func TestSuspendScan_StopsAtJoinBoundary(t *testing.T) {
	// fork → [a, b] → join → tail(userTask)：join 之后的 suspend 节点不属于任何分支。
	// 分支扫描必须在 join 处截断：若把 tail 算进分支，「本分支最后一个 suspend」
	// 会被解析成 tail，两路分支完成后 join 收不齐消息，实例永远卡在 active。

	graph := newGraph(
		map[string]string{"fork1": "fork", "a": "userTask", "b": "userTask", "join1": "join", "tail": "userTask"},
		[][2]string{
			{"fork1", "a"}, {"fork1", "b"},
			{"a", "join1"}, {"b", "join1"},
			{"join1", "tail"},
		},
		nil,
	)
	assert.Equal(t, []string{"a"}, graph.findAllSuspendNodes("a"),
		"branch scan must stop at join; post-join tail is not part of branch a")
	assert.Equal(t, []string{"b"}, graph.findAllSuspendNodes("b"))
	assert.Equal(t, "a", graph.findFirstSuspendNode("a"))
}

func TestSuspendScan_SyncBranch_DoesNotCrossJoin(t *testing.T) {
	// 混合分支：sync 分支越过 join 会把 tail(userTask) 误当成分支出口
	graph := newGraph(
		map[string]string{"fork1": "fork", "sync": "httpCall", "b": "userTask", "join1": "join", "tail": "userTask"},
		[][2]string{
			{"fork1", "sync"}, {"fork1", "b"},
			{"sync", "join1"}, {"b", "join1"},
			{"join1", "tail"},
		},
		nil,
	)
	assert.Empty(t, graph.findFirstSuspendNode("sync"),
		"sync-only branch must stay sync-only; must not pick up post-join tail")
	assert.Empty(t, graph.findAllSuspendNodes("sync"))
}
