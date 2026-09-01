package components

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/stretchr/testify/require"
)

// Tests for automation_call_node.go

// fakeAutomationExecutor 实现 service.RuleChainExecutor，可配置 Execute 行为并计数。
type fakeAutomationExecutor struct {
	executeCalls atomic.Int32
	lastChainId  atomic.Value
	executeErr   error
}

func (f *fakeAutomationExecutor) Execute(chainId string, msg types.RuleMsg) error {
	f.executeCalls.Add(1)
	f.lastChainId.Store(chainId)
	return f.executeErr
}

func (f *fakeAutomationExecutor) ExecuteAsync(chainId string, msg types.RuleMsg) {
	_ = f.Execute(chainId, msg)
}

func (f *fakeAutomationExecutor) ExecuteAndCollect(chainId string, msg types.RuleMsg, timeout time.Duration) (types.RuleMsg, error) {
	return types.RuleMsg{}, fmt.Errorf("not implemented")
}

const automationTestTimeout = 3 * time.Second

var automationRegisterOnce sync.Once

// registerAutomationForTest 注册 automation 节点原型（全局 registry 只注册一次）。
func registerAutomationForTest(t *testing.T) {
	t.Helper()
	automationRegisterOnce.Do(func() {
		if err := rulego.Registry.Register(&AutomationCallNode{}); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				require.NoError(t, err)
			}
		}
	})
}

// buildAutomationEngine 构造一条仅含 automation 节点的引擎。
func buildAutomationEngine(t *testing.T, chainID, configJSON string) types.RuleEngine {
	t.Helper()
	def := `{
		"ruleChain": {"id": "` + chainID + `", "name": "main", "root": true},
		"metadata": {
			"firstNodeIndex": 0,
			"nodes": [{"id": "auto_node", "type": "automation", "name": "自动化", "configuration": ` + configJSON + `}],
			"connections": []
		}
	}`
	engine, err := rulego.New(chainID, []byte(def), rulego.WithConfig(rulego.NewConfig()))
	require.NoError(t, err)
	return engine
}

// runAutomationChain 驱动引擎并等待 OnEnd，返回 (relationType, err)。
// TellSuccess → ("Success", nil)；TellFailure → err 携带失败原因。
func runAutomationChain(t *testing.T, engine types.RuleEngine, msg types.RuleMsg) (string, error) {
	t.Helper()
	done := make(chan struct{})
	var rel string
	var runErr error
	engine.OnMsg(msg, types.WithOnEnd(func(_ types.RuleContext, _ types.RuleMsg, e error, r string) {
		rel, runErr = r, e
		close(done)
	}))
	select {
	case <-done:
	case <-time.After(automationTestTimeout):
		t.Fatal("timeout waiting for chain completion")
	}
	return rel, runErr
}

func TestAutomationCallNode_Type(t *testing.T) {
	if (&AutomationCallNode{}).Type() != "automation" {
		t.Errorf("Type = %q, want 'automation'", (&AutomationCallNode{}).Type())
	}
}

// Executor 未注入（嵌入式半装配场景）：节点失败，不静默放行
func TestAutomationCallNode_NoExecutor_Fails(t *testing.T) {
	registerAutomationForTest(t)
	SetAutomationExecutor(nil)
	engine := buildAutomationEngine(t, "test:auto:noexec", `{"targetId":"chain_x"}`)
	_, err := runAutomationChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
	require.ErrorContains(t, err, "executor not configured")
}

// targetId 为空：节点失败（发布校验应拦在前，此处验证运行期兜底）
func TestAutomationCallNode_EmptyTargetId_Fails(t *testing.T) {
	registerAutomationForTest(t)
	exec := &fakeAutomationExecutor{}
	SetAutomationExecutor(exec)
	engine := buildAutomationEngine(t, "test:auto:empty", `{}`)
	_, err := runAutomationChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
	require.ErrorContains(t, err, "targetId is empty")
	require.EqualValues(t, 0, exec.executeCalls.Load())
}

// 触发成功：TellSuccess 且执行器收到目标链 ID（fire-and-forget 语义）
func TestAutomationCallNode_ExecuteSuccess(t *testing.T) {
	registerAutomationForTest(t)
	exec := &fakeAutomationExecutor{}
	SetAutomationExecutor(exec)
	engine := buildAutomationEngine(t, "test:auto:ok", `{"targetId":"chain_ok"}`)

	rel, err := runAutomationChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
	require.NoError(t, err)
	require.Equal(t, "Success", rel)
	require.EqualValues(t, 1, exec.executeCalls.Load())
	require.Equal(t, "chain_ok", exec.lastChainId.Load())
}

// 触发失败（目标链不存在/已下线）：失败沿链路可见，不会静默吞掉
func TestAutomationCallNode_ExecuteError_Fails(t *testing.T) {
	registerAutomationForTest(t)
	exec := &fakeAutomationExecutor{executeErr: fmt.Errorf("chain not found")}
	SetAutomationExecutor(exec)
	engine := buildAutomationEngine(t, "test:auto:err", `{"targetId":"chain_gone"}`)

	_, err := runAutomationChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
	require.ErrorContains(t, err, "chain not found")
}
