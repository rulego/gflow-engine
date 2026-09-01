package components

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var startProcessRegisterOnce sync.Once

// fakeStartProcessRuntime 仅实现节点用到的 StartProcessInstanceByKey，
// 其余经接口内嵌保持 nil（本节点不会调用）。
type fakeStartProcessRuntime struct {
	service.RuntimeServiceInternal

	mu         sync.Mutex
	instanceID string
	err        error
	startCalls []map[string]interface{}
}

func (f *fakeStartProcessRuntime) StartProcessInstanceByKey(
	_ context.Context, actor service.Actor, processDefinitionKey, businessKey string,
	variables map[string]interface{}, _ ...service.StartOption) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startCalls = append(f.startCalls, map[string]interface{}{
		"actor":       actor,
		"processKey":  processDefinitionKey,
		"businessKey": businessKey,
		"variables":   variables,
	})
	if f.err != nil {
		return "", f.err
	}
	return f.instanceID, nil
}

func (f *fakeStartProcessRuntime) calls() []map[string]interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]interface{}, len(f.startCalls))
	copy(out, f.startCalls)
	return out
}

// 共享 fake：registry 原型只注册一次并绑定首个实例，各用例经 reset 配置期望值。
var sharedStartProcessFake = &fakeStartProcessRuntime{}

func resetStartProcessFake(instanceID string, err error) *fakeStartProcessRuntime {
	sharedStartProcessFake.mu.Lock()
	defer sharedStartProcessFake.mu.Unlock()
	sharedStartProcessFake.instanceID = instanceID
	sharedStartProcessFake.err = err
	sharedStartProcessFake.startCalls = nil
	return sharedStartProcessFake
}

// registerStartProcessForTest 注册带 fake 依赖的节点原型（全局 registry 幂等）。
func registerStartProcessForTest(t *testing.T) *fakeStartProcessRuntime {
	t.Helper()
	startProcessRegisterOnce.Do(func() {
		if err := rulego.Registry.Register(&StartProcessNode{RuntimeService: sharedStartProcessFake}); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				require.NoError(t, err)
			}
		}
	})
	return resetStartProcessFake("inst_default", nil)
}

func buildStartProcessEngine(t *testing.T, chainID, configJSON string) types.RuleEngine {
	t.Helper()
	def := `{
		"ruleChain": {"id": "` + chainID + `", "name": "main", "root": true},
		"metadata": {
			"firstNodeIndex": 0,
			"nodes": [{"id": "sp", "type": "startProcess", "name": "发起审批", "configuration": ` + configJSON + `}],
			"connections": []
		}
	}`
	engine, err := rulego.New(chainID, []byte(def), rulego.WithConfig(rulego.NewConfig()))
	require.NoError(t, err)
	return engine
}

func tenantMsg(data string) types.RuleMsg {
	msg := types.NewMsgWithJsonData(data)
	msg.GetMetadata().PutValue(constants.KeyTenantID, "tenant_a")
	return msg
}

func TestStartProcessNode_Type(t *testing.T) {
	node := &StartProcessNode{}
	assert.Equal(t, "startProcess", node.Type())
	assert.Equal(t, "bpm", node.Category())
}

func TestStartProcessNode_InitValidation(t *testing.T) {
	node := &StartProcessNode{}
	err := node.Init(rulego.NewConfig(), types.Configuration{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "processKey")

	err = node.Init(rulego.NewConfig(), types.Configuration{"processKey": "leave"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "initiator")

	err = node.Init(rulego.NewConfig(), types.Configuration{
		"processKey": "leave", "initiator": "u1",
		"variables": `{"month":"${msg.month}"}`,
	})
	require.NoError(t, err)

	// 新版编辑器（map 键值行）直接写 JSON 对象
	err = node.Init(rulego.NewConfig(), types.Configuration{
		"processKey": "leave", "initiator": "u1",
		"variables": map[string]interface{}{"month": "${msg.month}", "count": float64(12)},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"month": "${msg.month}", "count": float64(12)}, node.Config.Variables)

	// 旧字符串形态但不是合法 JSON 对象 → 明确报错
	err = node.Init(rulego.NewConfig(), types.Configuration{
		"processKey": "leave", "initiator": "u1",
		"variables": "{oops",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "variables")
}

func TestStartProcessNode_VariablesAsObject(t *testing.T) {
	fake := registerStartProcessForTest(t)

	// 新 DSL：variables 直接是 JSON 对象（编辑器 map 控件 / 纯配置表单写出）
	cfg := `{
		"processKey": "monthly_report",
		"initiator": "u_001",
		"variables": {"month": "${msg.month}", "count": 12, "fixed": "静态值"}
	}`
	engine := buildStartProcessEngine(t, "t_sp_obj_vars", cfg)
	msg := tenantMsg(`{"month":"2026-09"}`)

	_, rel, err := runChain(t, engine, msg)
	require.NoError(t, err)
	assert.Equal(t, types.Success, rel)

	vars := fake.calls()[0]["variables"].(map[string]interface{})
	assert.Equal(t, "2026-09", vars["month"])   // ${} 渲染
	assert.Equal(t, float64(12), vars["count"]) // 非字符串原样透传
	assert.Equal(t, "静态值", vars["fixed"])       // 无占位保持
}

func TestStartProcessNode_Success(t *testing.T) {
	fake := registerStartProcessForTest(t)
	fake.instanceID = "inst_100"

	cfg := `{
		"processKey": "monthly_report",
		"initiator": "${msg.initiatorId}",
		"businessKey": "mr-${msg.month}",
		"variables": "{\"month\":\"${msg.month}\",\"count\":12}"
	}`
	engine := buildStartProcessEngine(t, "t_sp_success", cfg)
	msg := tenantMsg(`{"initiatorId":"u_001","month":"2026-08"}`)

	endMsg, rel, err := runChain(t, engine, msg)
	require.NoError(t, err)
	assert.Equal(t, types.Success, rel)

	calls := fake.calls()
	require.Len(t, calls, 1)
	c := calls[0]
	actor := c["actor"].(service.Actor)
	assert.Equal(t, "u_001", actor.UserID)
	assert.Equal(t, "tenant_a", actor.TenantID)
	assert.Equal(t, "monthly_report", c["processKey"])
	assert.Equal(t, "mr-2026-08", c["businessKey"])
	assert.Equal(t, "2026-08", c["variables"].(map[string]interface{})["month"])

	assert.Equal(t, "inst_100", endMsg.GetMetadata().GetValue(KeyProcessInstanceID))
	var data map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(endMsg.GetData()), &data))
	assert.Equal(t, "inst_100", data[KeyProcessInstanceID])
	assert.Equal(t, "u_001", data["initiatorId"]) // 原消息字段保留
}

func TestStartProcessNode_VariablesDefaultFromMsgData(t *testing.T) {
	fake := registerStartProcessForTest(t)

	engine := buildStartProcessEngine(t, "t_sp_default_vars",
		`{"processKey":"leave","initiator":"u_001"}`)
	msg := tenantMsg(`{"days":3,"reason":"年假"}`)

	_, rel, err := runChain(t, engine, msg)
	require.NoError(t, err)
	assert.Equal(t, types.Success, rel)

	c := fake.calls()[0]
	vars := c["variables"].(map[string]interface{})
	assert.Equal(t, float64(3), vars["days"])
	assert.Equal(t, "年假", vars["reason"])
}

func TestStartProcessNode_MissingTenant(t *testing.T) {
	fake := registerStartProcessForTest(t)

	engine := buildStartProcessEngine(t, "t_sp_no_tenant",
		`{"processKey":"leave","initiator":"u_001"}`)
	msg := types.NewMsgWithJsonData(`{}`) // 无 tenant_id

	_, rel, err := runChain(t, engine, msg)
	assert.Equal(t, types.Failure, rel)
	assert.Error(t, err)
	assert.Empty(t, fake.calls())
}

func TestStartProcessNode_RuntimeError(t *testing.T) {
	fake := registerStartProcessForTest(t)
	fake.err = errors.New("process definition not found")

	engine := buildStartProcessEngine(t, "t_sp_err",
		`{"processKey":"ghost","initiator":"u_001"}`)
	msg := tenantMsg(`{}`)

	_, rel, err := runChain(t, engine, msg)
	assert.Equal(t, types.Failure, rel)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ghost")
}
