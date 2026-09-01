package components

import (
	"testing"
	"time"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/action"
	"github.com/stretchr/testify/require"
)

// Tests for service_task_node.go

func TestServiceTaskNode_Type(t *testing.T) {
	node := &ServiceTaskNode{}
	if node.Type() != ServiceTaskNodeType {
		t.Errorf("Type = %q, want %q", node.Type(), ServiceTaskNodeType)
	}
}

func TestServiceTaskNodeType_Constant(t *testing.T) {
	if ServiceTaskNodeType != "serviceTask" {
		t.Errorf("ServiceTaskNodeType = %q, want 'serviceTask'", ServiceTaskNodeType)
	}
}

// New 出来的实例默认 FunctionName 必须为空，避免沿用上游 "test" 默认值。
func TestServiceTaskNode_New_DefaultFunctionNameEmpty(t *testing.T) {
	node := (&ServiceTaskNode{}).New()
	stn, ok := node.(*ServiceTaskNode)
	if !ok {
		t.Fatalf("New() returned %T, want *ServiceTaskNode", node)
	}
	if stn.Config.FunctionName != "" {
		t.Errorf("default FunctionName = %q, want empty string", stn.Config.FunctionName)
	}
}

func TestServiceTaskNode_New_ImplementsNode(t *testing.T) {
	var _ types.Node = (&ServiceTaskNode{}).New()
}

// 经 Services.Register 注册的函数必须能被 action.Functions.Get 查到，
// 这是 ServiceTaskNode 运行时的真实查找路径。
func TestServiceTaskNode_RegisterViaServices_ReachableByFunctionsRegistry(t *testing.T) {
	const fnName = "test:reachable:by:functions"
	defer action.Functions.UnRegister(fnName)

	called := false
	Services.Register(
		ServiceFuncDef{Name: fnName, Label: "可达性测试", Fields: []ServiceFuncField{{Name: "x"}}},
		func(ctx types.RuleContext, msg types.RuleMsg) { called = true },
	)

	fn, ok := action.Functions.Get(fnName)
	if !ok || fn == nil {
		t.Fatal("function registered via Services.Register not found in action.Functions")
	}
	if def, ok := Services.Get(fnName); !ok || def.Label != "可达性测试" {
		t.Errorf("Services.Get(%q) = %+v ok=%v, want label '可达性测试'", fnName, def, ok)
	}
	_ = called
}

// 集成测试：用真实 rulego 链验证 Services.Register 注册的函数能被 serviceTask 节点调用。

// 验证注册的函数被调用且 param 被透传进 msg.Data。
func TestServiceTaskNode_Integration_DynamicFunctionInvocation(t *testing.T) {
	const (
		chainId  = "test:integration:serviceTask"
		funcName = "test:integration:greet"
	)
	action.Functions.UnRegister(funcName)
	defer action.Functions.UnRegister(funcName)

	received := make(chan string, 1)
	Services.Register(
		ServiceFuncDef{
			Name: funcName, Label: "问候",
			Fields: []ServiceFuncField{{Name: "who", Label: "对象", Type: "string"}},
		},
		func(ctx types.RuleContext, msg types.RuleMsg) {
			received <- string(msg.GetData())
			ctx.TellSuccess(msg)
		},
	)
	_, ok := action.Functions.Get(funcName)
	require.True(t, ok)

	config := rulego.NewConfig()
	chainDef := `{
		"ruleChain": {"id": "` + chainId + `", "name": "serviceTask集成", "root": true},
		"metadata": {
			"firstNodeIndex": 0,
			"nodes": [{
				"id": "s1", "type": "serviceTask", "name": "问候节点",
				"configuration": {
					"functionName": "` + funcName + `",
					"param": "{\"who\":\"world\"}"
				}
			}],
			"connections": []
		}
	}`
	_ = rulego.Registry.Register(&ServiceTaskNode{}) // 幂等

	engine, err := rulego.New(chainId, []byte(chainDef), rulego.WithConfig(config))
	require.NoError(t, err)
	require.NotNil(t, engine)

	msg := types.NewMsg(0, "test", types.JSON, types.NewMetadata(), `{}`)
	done := make(chan struct{})
	engine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, m types.RuleMsg, err error, rel string) {
		defer close(done)
		require.NoError(t, err)
	}))

	select {
	case got := <-received:
		require.Contains(t, got, "world")
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: function not invoked")
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: chain not finished")
	}
}

// 验证未注册的函数名走 TellFailure。
func TestServiceTaskNode_Integration_UnregisteredFunctionFails(t *testing.T) {
	const (
		chainId  = "test:integration:serviceTask:notfound"
		funcName = "test:does:not:exist"
	)
	action.Functions.UnRegister(funcName)
	defer action.Functions.UnRegister(funcName)

	config := rulego.NewConfig()
	chainDef := `{
		"ruleChain": {"id": "` + chainId + `", "name": "未注册函数", "root": true},
		"metadata": {
			"firstNodeIndex": 0,
			"nodes": [{
				"id": "s1", "type": "serviceTask", "name": "缺失函数",
				"configuration": {"functionName": "` + funcName + `"}
			}],
			"connections": []
		}
	}`
	_ = rulego.Registry.Register(&ServiceTaskNode{})
	engine, err := rulego.New(chainId, []byte(chainDef), rulego.WithConfig(config))
	require.NoError(t, err)
	require.NotNil(t, engine)

	msg := types.NewMsg(0, "test", types.JSON, types.NewMetadata(), `{}`)
	done := make(chan struct{})
	var endErr error
	var endRel string
	engine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, m types.RuleMsg, err error, rel string) {
		endErr = err
		endRel = rel
		close(done)
	}))

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: chain not finished")
	}
	require.Equal(t, types.Failure, endRel)
	if endErr != nil {
		require.Contains(t, endErr.Error(), funcName)
	}
}

// panic 兜底验证：宿主注册的服务函数 panic 不能击穿进程，
// 必须被 recoverNodePanic 转为 Failure 边（recoverNodePanic 在 ServiceTaskNode.OnMsg）。
func TestServiceTaskNode_Panic_RoutedToFailure(t *testing.T) {
	const (
		chainId  = "test:panic:serviceTask"
		funcName = "test:panic:boom"
	)
	action.Functions.UnRegister(funcName)
	defer action.Functions.UnRegister(funcName)

	Services.Register(
		ServiceFuncDef{Name: funcName, Label: "炸裂", Fields: []ServiceFuncField{{Name: "x"}}},
		func(ctx types.RuleContext, msg types.RuleMsg) {
			panic("host function exploded")
		},
	)

	config := rulego.NewConfig()
	if err := registerNode(&ServiceTaskNode{}); err != nil {
		t.Fatalf("register serviceTask node: %v", err)
	}
	chainDef := `{
		"ruleChain": {"id": "` + chainId + `", "name": "panic兜底", "root": true},
		"metadata": {
			"firstNodeIndex": 0,
			"nodes": [{
				"id": "s1", "type": "serviceTask", "name": "panic节点",
				"configuration": {"functionName": "` + funcName + `"}
			}],
			"connections": []
		}
	}`
	engine, err := rulego.New(chainId, []byte(chainDef), rulego.WithConfig(config))
	require.NoError(t, err)

	msg := types.NewMsg(0, "test", types.JSON, types.NewMetadata(), `{}`)
	done := make(chan struct{})
	var rel string
	var endErr error
	engine.OnMsg(msg, types.WithOnEnd(func(_ types.RuleContext, _ types.RuleMsg, err error, relation string) {
		rel, endErr = relation, err
		close(done)
	}))

	select {
	case <-done:
		// 走到这里说明进程未被 panic 击穿
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for chain end; panic likely not recovered")
	}
	require.Equal(t, "Failure", rel, "panicking host function must route to Failure")
	require.Error(t, endErr)
	require.Contains(t, endErr.Error(), "panicked")
}

// metaValue 的 nil metadata 防御：rulego 的 GetMetadata 原样返回 m.Metadata（可为 nil），
// 组件统一经 metaValue 读取，nil 时必须返回空串而不是 panic。
func TestMetaValue_NilMetadata(t *testing.T) {
	msg := types.NewMsg(0, "test", types.JSON, nil, `{}`)
	msg.Metadata = nil
	if got := metaValue(msg, constants.KeyInstanceID); got != "" {
		t.Errorf("metaValue on nil metadata = %q, want empty", got)
	}
	md := types.NewMetadata()
	md.PutValue(constants.KeyInstanceID, "inst-1")
	msg.Metadata = md
	if got := metaValue(msg, constants.KeyInstanceID); got != "inst-1" {
		t.Errorf("metaValue = %q, want 'inst-1'", got)
	}
}
