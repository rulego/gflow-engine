/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package components

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/stretchr/testify/require"
)

// 测试桩

// fakeAgentExecutor 实现 service.RuleChainExecutor，按用例配置 ExecuteAndCollect 的返回。
type fakeAgentExecutor struct {
	executeCalls atomic.Int32
	collectCalls atomic.Int32
	collectFn    func(chainId string, msg types.RuleMsg, timeout time.Duration) (types.RuleMsg, error)
}

func (f *fakeAgentExecutor) Execute(chainId string, msg types.RuleMsg) error {
	f.executeCalls.Add(1)
	return nil
}

func (f *fakeAgentExecutor) ExecuteAsync(chainId string, msg types.RuleMsg) {
	_ = f.Execute(chainId, msg)
}

func (f *fakeAgentExecutor) ExecuteAndCollect(chainId string, msg types.RuleMsg, timeout time.Duration) (types.RuleMsg, error) {
	f.collectCalls.Add(1)
	if f.collectFn != nil {
		return f.collectFn(chainId, msg, timeout)
	}
	return types.RuleMsg{}, fmt.Errorf("agent not found: %s", chainId)
}

var _ service.RuleChainExecutor = (*fakeAgentExecutor)(nil)

// fakeTaskService 实现 CreateTask + GetTaskList：
// 前者捕获 handleFailure/handleUnresolved 创建的兜底任务，后者供人工重入守卫查询。
type fakeTaskService struct {
	service.TaskServiceInternal // nil；其余方法不会被被测路径调用
	mu                          sync.Mutex
	created                     []*model.WfTask
	tasks                       []*model.WfTask // 守卫查询返回的存量任务
}

func (f *fakeTaskService) CreateTask(_ context.Context, _ service.Actor, task *model.WfTask) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, task)
	return task.ID, nil
}

func (f *fakeTaskService) GetTaskList(_ context.Context, _ service.Actor, _ *dto.TaskQuery) ([]*model.WfTask, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*model.WfTask
	out = append(out, f.tasks...)
	return out, int64(len(out)), nil
}

func (f *fakeTaskService) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = nil
	f.tasks = nil
}

var (
	testTaskSvc    = &fakeTaskService{}
	aiRegisterOnce sync.Once
	aiTestTimeout  = 5 * time.Second
)

// registerAIAgentForTest 注册带 fakeTaskService 的 AIAgentNode 原型（全局 registry 只注册一次）。
// Executor 由 New() 从 globalAutomationExecutor 读取，用例在实例化前 SetAutomationExecutor 切换。
func registerAIAgentForTest(t *testing.T) {
	t.Helper()
	aiRegisterOnce.Do(func() {
		if err := rulego.Registry.Register(&AIAgentNode{TaskService: testTaskSvc}); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				require.NoError(t, err)
			}
		}
	})
}

// useFakeExec 设置全局执行器并注册节点，返回 fake 供用例配置。
// 同时清空 fake 任务服务的存量数据，避免人工守卫被上一用例残留任务劫持。
func useFakeExec(t *testing.T) *fakeAgentExecutor {
	t.Helper()
	testTaskSvc.reset()
	exec := &fakeAgentExecutor{}
	SetAutomationExecutor(exec)
	registerAIAgentForTest(t)
	return exec
}

// runChain 驱动引擎并等待 OnEnd，返回 (endMsg, relationType, err)。
func runChain(t *testing.T, engine types.RuleEngine, msg types.RuleMsg) (types.RuleMsg, string, error) {
	t.Helper()
	done := make(chan struct{})
	var endMsg types.RuleMsg
	var rel string
	var runErr error
	engine.OnMsg(msg, types.WithOnEnd(func(_ types.RuleContext, m types.RuleMsg, e error, r string) {
		endMsg, rel, runErr = m, r, e
		close(done)
	}))
	select {
	case <-done:
	case <-time.After(aiTestTimeout):
		t.Fatal("timeout waiting for chain completion")
	}
	return endMsg, rel, runErr
}

// buildAIEngine 构造一条仅含 aiAgent 节点的主流程引擎。
func buildAIEngine(t *testing.T, chainID, configJSON string) types.RuleEngine {
	t.Helper()
	def := `{
		"ruleChain": {"id": "` + chainID + `", "name": "main", "root": true},
		"metadata": {
			"firstNodeIndex": 0,
			"nodes": [{"id": "ai_node", "type": "aiAgent", "name": "AI", "configuration": ` + configJSON + `}],
			"connections": []
		}
	}`
	engine, err := rulego.New(chainID, []byte(def), rulego.WithConfig(rulego.NewConfig()))
	require.NoError(t, err)
	return engine
}

// newAIInstanceMsg 构造带 instance_id 的主流程消息（人工重入守卫依赖实例上下文）。
func newAIInstanceMsg() types.RuleMsg {
	meta := types.NewMetadata()
	meta.PutValue(constants.KeyInstanceID, "inst-001")
	return types.NewMsg(0, "t", types.JSON, meta, `{}`)
}

// newAgentOutput 构造智能体输出消息。
func newAgentOutput(data string) types.RuleMsg {
	return types.NewMsg(0, "agent", types.JSON, types.NewMetadata(), data)
}

// 基础单元测试

func TestAIAgentNode_Type(t *testing.T) {
	node := &AIAgentNode{}
	require.Equal(t, AIAgentNodeType, node.Type())
	require.Equal(t, "aiAgent", AIAgentNodeType)
}

func TestAIAgentNode_New(t *testing.T) {
	node := &AIAgentNode{}
	newNode := node.New()
	require.NotNil(t, newNode)
	require.IsType(t, &AIAgentNode{}, newNode)
}

func TestAIAgentNode_Init_MissingAgentID(t *testing.T) {
	node := &AIAgentNode{}
	err := node.Init(types.Config{}, types.Configuration{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "agentId is required")
}

func TestAIAgentNode_Init_Defaults(t *testing.T) {
	node := &AIAgentNode{}
	err := node.Init(types.Config{}, types.Configuration{"agentId": "test_agent"})
	require.NoError(t, err)
	require.Equal(t, 120, node.Config.TimeoutSec)
	require.Nil(t, node.Config.Decision)
	// 缺省=平铺（与 httpCall 节点同默认）
	require.True(t, node.flattenOutput())
}

// 未知 decision 取值只告警不报错（运行时按默认处理）。
func TestAIAgentNode_Init_UnknownDecisionValues(t *testing.T) {
	node := &AIAgentNode{}
	err := node.Init(types.Config{}, types.Configuration{
		"agentId": "test_agent",
		"decision": map[string]interface{}{
			"rejectStrategy": "bogus",
			"unresolved":     "bogus",
		},
	})
	require.NoError(t, err)
}

// ExtractDecision 裁决标记解析
func TestExtractDecision(t *testing.T) {
	cases := []struct {
		name string
		out  string
		want AIDecision
	}{
		{"bare pass", "AI_DECISION: PASS", AIDecisionPass},
		{"bare reject", "AI_DECISION: REJECT", AIDecisionReject},
		{"case insensitive", "ai_decision: pass", AIDecisionPass},
		{"trailing punctuation", "AI_DECISION: PASS。", AIDecisionPass},
		{"json body then marker", `{"approved":true,"reason":"ok"}` + "\nAI_DECISION: PASS", AIDecisionPass},
		{"fenced json then marker", "```json\n{\"approved\":false}\n```\nAI_DECISION: REJECT", AIDecisionReject},
		{"prose then marker", "分析：金额超限，缺少违约条款。\nAI_DECISION: REJECT", AIDecisionReject},
		{"loose line trailing text", "AI_DECISION: REJECT，理由：金额超限", AIDecisionReject},
		{"chinese prefix", "裁决：通过", AIDecisionPass},
		{"chinese value with prefix", "AI_DECISION: 拒绝", AIDecisionReject},
		{"approve synonym", "AI_DECISION: APPROVED", AIDecisionPass},
		{"last marker wins", "AI_DECISION: PASS\n补充说明\nAI_DECISION: REJECT", AIDecisionReject},
		{"no marker plain text", "好的，这份合同没有问题。", AIDecisionUnresolved},
		{"no marker json only", `{"approved":true}`, AIDecisionUnresolved},
		// 协议复述防护：复述行含两极词，不得当作裁决
		{"protocol echo inline", "格式严格为 AI_DECISION: PASS 或 AI_DECISION: REJECT（PASS=同意放行，REJECT=拒绝）。", AIDecisionUnresolved},
		{"protocol echo at line start", "AI_DECISION: PASS 或 AI_DECISION: REJECT", AIDecisionUnresolved},
		{"echo then real marker", "AI_DECISION: PASS 或 AI_DECISION: REJECT\nAI_DECISION: REJECT", AIDecisionReject},
		{"bare chinese line not counted", "通过", AIDecisionUnresolved},
		{"empty", "", AIDecisionUnresolved},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, ExtractDecision(c.out))
		})
	}
}

// 集成测试（fake executor，不调真实 AI）

// 同步 + 裁决 PASS：标记放行，metadata 写 aiDecision=PASS，输出隔离进 _ai，映射字段提升。
func TestAIAgentNode_SyncDecisionPass(t *testing.T) {
	exec := useFakeExec(t)
	exec.collectFn = func(_ string, _ types.RuleMsg, _ time.Duration) (types.RuleMsg, error) {
		return newAgentOutput("```json\n{\"approved\":true,\"reason\":\"合规\"}\n```\nAI_DECISION: PASS"), nil
	}
	cfg := `{
		"agentId": "fake_agent", "async": false, "timeoutSec": 5,
		"decision": {"rejectStrategy": "terminate", "unresolved": "human"},
		"outputMappings": [{"from": "approved", "to": "aiApproved"}, {"from": "reason", "to": "aiReason"}]
	}`
	engine := buildAIEngine(t, "decision_pass_flow", cfg)

	endMsg, rel, err := runChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{"contractName":"c1"}`))
	require.NoError(t, err)
	require.Equal(t, types.Success, rel)
	require.Equal(t, "PASS", endMsg.GetMetadata().GetValue(MetaKeyAIDecision))
	require.Contains(t, endMsg.GetData(), "_ai", "full output must be namespaced under _ai")
	require.Contains(t, endMsg.GetData(), "aiApproved", "mapped field must be lifted")
	require.Contains(t, endMsg.GetData(), "contractName", "form fields must survive")
}

// 同步 + 裁决 REJECT → handleReject；RuntimeService 未注入时降级 TellFailure → Failure。
func TestAIAgentNode_SyncDecisionReject(t *testing.T) {
	exec := useFakeExec(t)
	exec.collectFn = func(_ string, _ types.RuleMsg, _ time.Duration) (types.RuleMsg, error) {
		return newAgentOutput("AI_DECISION: REJECT"), nil
	}
	cfg := `{
		"agentId": "fake_agent", "async": false, "timeoutSec": 5,
		"decision": {"rejectStrategy": "terminate"}
	}`
	engine := buildAIEngine(t, "decision_reject_flow", cfg)

	_, rel, err := runChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
	require.Equal(t, types.Failure, rel)
	require.Error(t, err)
}

// 未裁决 + 默认策略 human + 有兜底人 → 创建 userTask 待办并以 DoOnEnd 收尾（不挂链）。
func TestAIAgentNode_UnresolvedHuman(t *testing.T) {
	exec := useFakeExec(t)
	exec.collectFn = func(_ string, _ types.RuleMsg, _ time.Duration) (types.RuleMsg, error) {
		return newAgentOutput("这份合同看起来没什么问题。"), nil
	}
	cfg := `{
		"agentId": "fake_agent", "async": false, "timeoutSec": 5,
		"decision": {"rejectStrategy": "terminate", "unresolved": "human"},
		"failureHandler": ["u1", "u2"]
	}`
	engine := buildAIEngine(t, "unresolved_human_flow", cfg)

	meta := types.NewMetadata()
	meta.PutValue(constants.KeyTenantID, "tenantA")
	meta.PutValue(constants.KeyProcessID, "proc-001")
	_, rel, _ := runChain(t, engine, types.NewMsg(0, "t", types.JSON, meta, `{}`))
	require.Equal(t, "", rel, "unresolved→human ends via DoOnEnd, not Success/Failure")

	testTaskSvc.mu.Lock()
	tasks := append([]*model.WfTask(nil), testTaskSvc.created...)
	testTaskSvc.mu.Unlock()
	require.Len(t, tasks, 2, "one todo per failure handler")
	for _, tk := range tasks {
		require.Equal(t, UserTaskNodeType, tk.TaskType)
		require.Equal(t, "ai_node", tk.TaskDefKey)
		require.Equal(t, "active", tk.Status)
		require.Equal(t, "tenantA", tk.TenantID)
		require.Contains(t, tk.Name, "AI智能体未判定")
		require.NotEmpty(t, tk.Assignee)
	}
}

// 未裁决 + 策略 pass → 放行并标记 UNRESOLVED。
func TestAIAgentNode_UnresolvedPass(t *testing.T) {
	exec := useFakeExec(t)
	exec.collectFn = func(_ string, _ types.RuleMsg, _ time.Duration) (types.RuleMsg, error) {
		return newAgentOutput("我认为可以。"), nil
	}
	cfg := `{
		"agentId": "fake_agent", "async": false, "timeoutSec": 5,
		"decision": {"rejectStrategy": "terminate", "unresolved": "pass"}
	}`
	engine := buildAIEngine(t, "unresolved_pass_flow", cfg)

	endMsg, rel, err := runChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
	require.NoError(t, err)
	require.Equal(t, types.Success, rel)
	require.Equal(t, "UNRESOLVED", endMsg.GetMetadata().GetValue(MetaKeyAIDecision))
}

// 未裁决 + 策略 reject → 走拒绝策略（RuntimeService 缺失时 TellFailure）。
func TestAIAgentNode_UnresolvedReject(t *testing.T) {
	exec := useFakeExec(t)
	exec.collectFn = func(_ string, _ types.RuleMsg, _ time.Duration) (types.RuleMsg, error) {
		return newAgentOutput("看不懂要求"), nil
	}
	cfg := `{
		"agentId": "fake_agent", "async": false, "timeoutSec": 5,
		"decision": {"rejectStrategy": "terminate", "unresolved": "reject"}
	}`
	engine := buildAIEngine(t, "unresolved_reject_flow", cfg)

	_, rel, err := runChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
	require.Equal(t, types.Failure, rel)
	require.Error(t, err)
}

// 未裁决 + 默认 human 但未配兜底人 → 放行并标记（不静默挂链）。
func TestAIAgentNode_UnresolvedHumanNoHandler(t *testing.T) {
	exec := useFakeExec(t)
	exec.collectFn = func(_ string, _ types.RuleMsg, _ time.Duration) (types.RuleMsg, error) {
		return newAgentOutput("随便说点什么"), nil
	}
	cfg := `{
		"agentId": "fake_agent", "async": false, "timeoutSec": 5,
		"decision": {"rejectStrategy": "terminate"}
	}`
	engine := buildAIEngine(t, "unresolved_no_handler_flow", cfg)

	endMsg, rel, err := runChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
	require.NoError(t, err)
	require.Equal(t, types.Success, rel)
	require.Equal(t, "UNRESOLVED", endMsg.GetMetadata().GetValue(MetaKeyAIDecision))
}

// executor 返回 not-found 错误，无 failureHandler → TellFailure。
func TestAIAgentNode_NotFound(t *testing.T) {
	useFakeExec(t) // collectFn=nil → 返回 "agent not found"
	cfg := `{"agentId": "missing_agent", "async": false, "timeoutSec": 5}`
	engine := buildAIEngine(t, "not_found_flow", cfg)

	_, rel, err := runChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
	require.Equal(t, types.Failure, rel)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

// 同步模式 executor 返回超时错误 → handleFailure → 无 failureHandler 时 TellFailure。
func TestAIAgentNode_SyncTimeout(t *testing.T) {
	exec := useFakeExec(t)
	exec.collectFn = func(_ string, _ types.RuleMsg, _ time.Duration) (types.RuleMsg, error) {
		return types.RuleMsg{}, fmt.Errorf("agent chain timed out after 1s")
	}
	cfg := `{"agentId": "slow_agent", "async": false, "timeoutSec": 1}`
	engine := buildAIEngine(t, "sync_timeout_flow", cfg)

	_, rel, err := runChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
	require.Equal(t, types.Failure, rel)
	require.Error(t, err)
}

// 异步模式：fire-and-forget，executor.Execute 被调用一次，主流程立即 Success。
func TestAIAgentNode_AsyncFireAndForget(t *testing.T) {
	exec := useFakeExec(t)
	cfg := `{"agentId": "fake_agent", "async": true, "timeoutSec": 5}`
	engine := buildAIEngine(t, "async_flow", cfg)

	_, rel, err := runChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
	require.NoError(t, err)
	require.Equal(t, types.Success, rel)
	require.Equal(t, int32(1), exec.executeCalls.Load(), "async Execute should be called exactly once")
}

// 失败兜底：failureHandler 非空时创建的 WfTask 必须带租户、createdAt、正确 processID、Status=active。
func TestAIAgentNode_FailureHandler_HasTenant(t *testing.T) {
	exec := useFakeExec(t)
	exec.collectFn = func(_ string, _ types.RuleMsg, _ time.Duration) (types.RuleMsg, error) {
		return types.RuleMsg{}, fmt.Errorf("agent api error")
	}
	cfg := `{
		"agentId": "fake_agent", "async": false, "timeoutSec": 5,
		"failureHandler": ["u1", "u2"]
	}`
	engine := buildAIEngine(t, "failure_handler_flow", cfg)

	meta := types.NewMetadata()
	meta.PutValue(constants.KeyTenantID, "tenantA")
	meta.PutValue(constants.KeyProcessID, "proc-001")
	msg := types.NewMsg(0, "t", types.JSON, meta, `{}`)
	_, rel, _ := runChain(t, engine, msg)
	require.Equal(t, "", rel, "handleFailure ends via DoOnEnd, not Success/Failure relation")

	testTaskSvc.mu.Lock()
	tasks := testTaskSvc.created
	testTaskSvc.mu.Unlock()
	require.Len(t, tasks, 2, "should create one task per failure handler")
	for _, tk := range tasks {
		require.Equal(t, "tenantA", tk.TenantID, "task must carry tenant")
		require.Equal(t, "proc-001", tk.ProcessID, "task must use process_id, not process_key")
		require.Equal(t, "active", tk.Status)
		require.Equal(t, UserTaskNodeType, tk.TaskType)
		require.False(t, tk.CreatedAt.IsZero(), "task must have CreatedAt")
		require.NotEmpty(t, tk.Assignee)
		require.Contains(t, tk.Name, "AI智能体调用失败")
	}
}

// 人工兜底重入守卫

func mkFallbackTask(status, endReason string) *model.WfTask {
	er := endReason
	return &model.WfTask{
		TaskType:   UserTaskNodeType,
		TaskDefKey: "ai_node",
		Status:     status,
		EndReason:  &er,
		Assignee:   aiStrPtr("u1"),
	}
}

func aiStrPtr(s string) *string { return &s }

// 守卫：已有 active 人工待办 → 不调 AI，DoOnEnd 等待。
func TestAIAgentNode_GuardActiveTaskWaits(t *testing.T) {
	exec := useFakeExec(t)
	testTaskSvc.mu.Lock()
	testTaskSvc.tasks = []*model.WfTask{mkFallbackTask(string(enums.TaskStatusActive), "")}
	testTaskSvc.mu.Unlock()

	engine := buildAIEngine(t, "guard_active_flow", `{"agentId": "fake_agent", "timeoutSec": 5}`)
	_, rel, _ := runChain(t, engine, newAIInstanceMsg())
	require.Equal(t, "", rel, "guard waits via DoOnEnd")
	require.Equal(t, int32(0), exec.collectCalls.Load(), "AI must not be called when fallback task active")
}

// 守卫：人工已同意 → 直接放行走下一节点，不再调 AI。
func TestAIAgentNode_GuardApprovedSkipsAI(t *testing.T) {
	exec := useFakeExec(t)
	approved := string(enums.ApprovalResultApproved)
	testTaskSvc.mu.Lock()
	testTaskSvc.tasks = []*model.WfTask{
		mkFallbackTask(string(enums.TaskStatusCompleted), approved),
	}
	testTaskSvc.mu.Unlock()

	engine := buildAIEngine(t, "guard_approved_flow", `{"agentId": "fake_agent", "timeoutSec": 5}`)
	endMsg, rel, err := runChain(t, engine, newAIInstanceMsg())
	require.NoError(t, err)
	require.Equal(t, types.Success, rel)
	require.Equal(t, "HUMAN_PASS", endMsg.GetMetadata().GetValue(MetaKeyAIDecision))
	require.Equal(t, int32(0), exec.collectCalls.Load(), "AI must not be re-called after human approval")
}

// 守卫：人工已拒绝 → 走拒绝策略（RuntimeService 缺失 → TellFailure）。
func TestAIAgentNode_GuardRejected(t *testing.T) {
	exec := useFakeExec(t)
	rejected := string(enums.ApprovalResultRejected)
	testTaskSvc.mu.Lock()
	testTaskSvc.tasks = []*model.WfTask{
		mkFallbackTask(string(enums.TaskStatusCompleted), rejected),
	}
	testTaskSvc.mu.Unlock()

	engine := buildAIEngine(t, "guard_rejected_flow", `{"agentId": "fake_agent", "timeoutSec": 5, "decision": {"rejectStrategy": "terminate"}}`)
	_, rel, err := runChain(t, engine, newAIInstanceMsg())
	require.Equal(t, types.Failure, rel)
	require.Error(t, err)
	require.Equal(t, int32(0), exec.collectCalls.Load(), "AI must not be re-called after human rejection")
}

// 守卫：TaskCreator 切面产生的 aiAgent 型自动任务不参与判定，正常调用 AI。
func TestAIAgentNode_GuardIgnoresAutoTasks(t *testing.T) {
	exec := useFakeExec(t)
	testTaskSvc.mu.Lock()
	testTaskSvc.tasks = []*model.WfTask{{
		TaskType:   AIAgentNodeType,
		TaskDefKey: "ai_node",
		Status:     string(enums.TaskStatusCompleted),
	}}
	testTaskSvc.mu.Unlock()
	exec.collectFn = func(_ string, _ types.RuleMsg, _ time.Duration) (types.RuleMsg, error) {
		return newAgentOutput("AI_DECISION: PASS"), nil
	}

	engine := buildAIEngine(t, "guard_auto_flow", `{"agentId": "fake_agent", "timeoutSec": 5, "decision": {}}`)
	endMsg, rel, err := runChain(t, engine, newAIInstanceMsg())
	require.NoError(t, err)
	require.Equal(t, types.Success, rel)
	require.Equal(t, "PASS", endMsg.GetMetadata().GetValue(MetaKeyAIDecision))
	require.Equal(t, int32(1), exec.collectCalls.Load())
}

// 裁决协议注入：decision 启用时 user 消息末尾带协议；未启用时不带。
func TestAIAgentNode_ProtocolInjection(t *testing.T) {
	captured := make(chan string, 1)
	exec := useFakeExec(t)
	exec.collectFn = func(_ string, m types.RuleMsg, _ time.Duration) (types.RuleMsg, error) {
		captured <- m.GetData()
		return newAgentOutput("AI_DECISION: PASS"), nil
	}

	t.Run("decision enabled injects protocol", func(t *testing.T) {
		engine := buildAIEngine(t, "proto_on_flow", `{"agentId": "a", "decision": {}}`)
		runChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
		select {
		case data := <-captured:
			require.Contains(t, data, "AI_DECISION")
		default:
			t.Fatal("agent message not captured")
		}
	})

	t.Run("decision disabled no protocol", func(t *testing.T) {
		engine := buildAIEngine(t, "proto_off_flow", `{"agentId": "a"}`)
		runChain(t, engine, types.NewMsg(0, "t", types.JSON, types.NewMetadata(), `{}`))
		select {
		case data := <-captured:
			require.NotContains(t, data, "AI_DECISION")
		default:
			t.Fatal("agent message not captured")
		}
	})
}

// 上下文来源组装：5 个来源逐个开启验证（其余章节不得出现），全关时为占位消息。
func TestAIAgentNode_AssembleContextSources(t *testing.T) {
	srcMsg := func() types.RuleMsg {
		meta := types.NewMetadata()
		meta.PutValue(constants.KeyOwner, "owner-user")
		meta.PutValue(constants.KeyProcessKey, "pk-ctx")
		meta.PutValue(constants.KeyInstanceID, "inst-ctx")
		meta.PutValue(constants.KeyBusinessKey, "BK-1")
		meta.PutValue(constants.KeyTenantID, "t1")
		return types.NewMsg(0, "t", types.JSON, meta,
			`{"contractName":"采购合同X","amount":5000,"attachments":[{"name":"invoice.pdf","url":"http://x/f.pdf"}]}`)
	}

	cases := []struct {
		name    string
		sources string // contextSources JSON 片段
		want    []string
		notWant []string
	}{
		{"formData", `"formData":true`,
			[]string{"## 表单数据", "采购合同X", "5000"}, nil},
		{"attachments", `"attachments":true`,
			[]string{"## 附件", "invoice.pdf"},
			[]string{"## 表单数据", "采购合同X"}},
		{"processInfo", `"processInfo":true`,
			[]string{"## 流程信息", "pk-ctx", "inst-ctx", "BK-1"},
			[]string{"## 表单数据"}},
		{"prevComments", `"prevComments":true`,
			[]string{"## 前序审批意见", "预算充足，同意", "初审"},
			[]string{"## 表单数据"}},
		{"initiator", `"initiator":true`,
			[]string{"## 发起人", "owner-user"},
			[]string{"## 表单数据"}},
		{"all off", `"formData":false,"attachments":false,"processInfo":false,"prevComments":false,"initiator":false`,
			nil, []string{"## 表单数据", "## 附件", "## 流程信息", "## 前序审批意见", "## 发起人", "AI_DECISION"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			exec := useFakeExec(t)
			// prevComments 数据源：一条已完成 userTask 的意见快照
			//（useFakeExec 会清空存量任务，须逐用例预置）
			cmt := "预算充足，同意"
			testTaskSvc.mu.Lock()
			testTaskSvc.tasks = []*model.WfTask{{
				TaskType:   UserTaskNodeType,
				TaskDefKey: "pre_node",
				Name:       "初审",
				Status:     string(enums.TaskStatusCompleted),
				Comment:    &cmt,
			}}
			testTaskSvc.mu.Unlock()
			captured := ""
			exec.collectFn = func(_ string, m types.RuleMsg, _ time.Duration) (types.RuleMsg, error) {
				captured = m.GetData()
				return newAgentOutput("AI_DECISION: PASS"), nil
			}
			cfg := fmt.Sprintf(`{"agentId":"a","timeoutSec":5,"inputAssembly":{"contextSources":{%s}}}`, c.sources)
			engine := buildAIEngine(t, "ctx_src_"+c.name, cfg)
			runChain(t, engine, srcMsg())
			require.NotEmpty(t, captured, "agent message not captured")
			for _, w := range c.want {
				require.Contains(t, captured, w)
			}
			for _, nw := range c.notWant {
				require.NotContains(t, captured, nw)
			}
			if c.name == "all off" {
				require.Contains(t, captured, "请处理", "全关时应注入占位消息")
			}
		})
	}
}
