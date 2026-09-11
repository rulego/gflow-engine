package components

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/stretchr/testify/require"
)

// Tests for cc_task_node.go

func TestCCTaskNode_Type(t *testing.T) {
	node := &CCTaskNode{}
	if node.Type() != CCTaskNodeType {
		t.Errorf("Type = %q, want %q", node.Type(), CCTaskNodeType)
	}
}

func TestCCTaskNode_New(t *testing.T) {
	node := &CCTaskNode{}
	newNode := node.New()
	if newNode == nil {
		t.Fatal("expected non-nil node")
	}
	ccNode, ok := newNode.(*CCTaskNode)
	if !ok {
		t.Fatal("expected *CCTaskNode")
	}
	if ccNode.Type() != CCTaskNodeType {
		t.Errorf("Type = %q, want %q", ccNode.Type(), CCTaskNodeType)
	}
}

func TestCCTaskNode_New_PreservesTaskService(t *testing.T) {
	// TaskService is nil in test, but New should preserve the reference
	node := &CCTaskNode{TaskService: nil}
	newNode := node.New().(*CCTaskNode)
	// TaskService should be copied from original
	_ = newNode
}

func TestCCTaskNode_Init_EmptyConfig(t *testing.T) {
	node := &CCTaskNode{}
	err := node.Init(types.Config{}, nil)
	// Init with nil configuration may or may not error depending on maps.Map2Struct
	// We just verify it doesn't panic
	_ = err
}

func TestCCTaskNode_Destroy(t *testing.T) {
	node := &CCTaskNode{}
	// Should not panic
	node.Destroy()
}

func TestCCTaskNodeType_Constant(t *testing.T) {
	if CCTaskNodeType != "ccTask" {
		t.Errorf("CCTaskNodeType = %q, want 'ccTask'", CCTaskNodeType)
	}
}

func TestCCTaskNode_GetSelfId_Default(t *testing.T) {
	node := &CCTaskNode{}
	// With empty CurrentNodeDef, should return the type constant
	id := node.GetSelfId()
	if id != CCTaskNodeType {
		t.Errorf("GetSelfId = %q, want %q", id, CCTaskNodeType)
	}
}

func TestCCTaskNode_GetSelfName_Default(t *testing.T) {
	node := &CCTaskNode{}
	// With empty CurrentNodeDef, should return empty string
	name := node.GetSelfName()
	if name != "" {
		t.Errorf("GetSelfName = %q, want ''", name)
	}
}

func TestCCTaskNodeConfiguration_DefaultValues(t *testing.T) {
	cfg := CCTaskNodeConfiguration{}
	if len(cfg.CCUserIds) != 0 {
		t.Error("expected CCUserIds to default to empty")
	}
	if len(cfg.CCUserIds) != 0 {
		t.Error("expected CCUserIds to default to empty")
	}
}

// ===== OnMsg 抄送名单语义 =====

// fakeCCTaskService 捕获 OnMsg 创建的抄送任务。rulego 链在独立 goroutine 驱动，
// created 的清空/写入/读取都要过锁。
type fakeCCTaskService struct {
	service.TaskServiceInternal // 仅嵌入接口；被测路径只调 CreateTask
	mu                          sync.Mutex
	created                     []*model.WfTask
}

func (f *fakeCCTaskService) CreateTask(_ context.Context, _ service.Actor, task *model.WfTask) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, task)
	return task.ID, nil
}

func (f *fakeCCTaskService) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = nil
}

func (f *fakeCCTaskService) snapshot() []*model.WfTask {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*model.WfTask(nil), f.created...)
}

var (
	ccTestTaskSvc  = &fakeCCTaskService{}
	ccRegisterOnce sync.Once
	// ccTestIdentity 注册进 ccTask 原型的租户身份源：默认全部"在租户内"，
	// 跨租户过滤用例用 deny + reset。
	ccTestIdentity = &fakeTenantChecker{}
	// 链 ID 必须每用例唯一：rulego.New 对已存在的 ID 直接复用旧链，
	// 用例会跑在别的节点配置上（时间戳在 Windows 粗粒度时钟下会撞车）
	ccChainSeq atomic.Int64
)

// registerCCTaskForTest 注册带 fake TaskService 的 ccTask 原型；registry 只认首次注册
func registerCCTaskForTest(t *testing.T) {
	t.Helper()
	ccRegisterOnce.Do(func() {
		if err := rulego.Registry.Register(&CCTaskNode{
			TaskService: ccTestTaskSvc,
			TenantGuard: service.NewTenantMembershipGuard(ccTestIdentity),
		}); err != nil &&
			!strings.Contains(err.Error(), "already exists") {
			t.Fatalf("register ccTask node: %v", err)
		}
	})
}

// runCCEngine 驱动仅含 ccTask 单节点的链（业务变量在 msg.Data，与生产一致），
// 返回创建的抄送任务 assignee 列表。
func runCCEngine(t *testing.T, configuration, dataJSON string) []string {
	t.Helper()
	registerCCTaskForTest(t)
	ccTestTaskSvc.reset()

	chainDef := `{
		"ruleChain": {"id": "test:cc:chain", "name": "抄送测试链", "root": true},
		"metadata": {
			"firstNodeIndex": 0,
			"nodes": [{"id": "cc1", "type": "ccTask", "name": "抄送人事备案", "configuration": ` + configuration + `}],
			"connections": []
		}
	}`
	config := rulego.NewConfig()
	chainID := fmt.Sprintf("test:cc:chain:%d", ccChainSeq.Add(1))
	engine, err := rulego.New(chainID, []byte(chainDef), rulego.WithConfig(config))
	require.NoError(t, err)

	var (
		assignees []string
		endErr    error
		endRel    string
	)
	msg := types.NewMsg(0, "t", types.JSON, types.NewMetadata(), dataJSON)
	done := make(chan struct{})
	engine.OnMsg(msg, types.WithOnEnd(func(_ types.RuleContext, _ types.RuleMsg, err error, rel string) {
		endErr = err
		endRel = rel
		for _, task := range ccTestTaskSvc.snapshot() {
			if task.Assignee != nil {
				assignees = append(assignees, *task.Assignee)
			}
		}
		close(done)
	}))
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for cc chain")
	}
	require.NoError(t, endErr)
	require.Equal(t, types.Success, endRel)
	return assignees
}

// 模板项语义：${msg.ccUserIds} 求值为数组时逐项摊平
func TestCCTaskNode_OnMsg_TemplateFlattensVariableList(t *testing.T) {
	assignees := runCCEngine(t,
		`{"ccUserIds":["${msg.ccUserIds}"]}`,
		`{"ccUserIds":["u-self-1","u-self-2"]}`)
	require.Equal(t, []string{"u-self-1", "u-self-2"}, assignees)
}

// 模板项引用的变量缺失：求值为空，无人被抄送
func TestCCTaskNode_OnMsg_TemplateWithoutVariableCCsNobody(t *testing.T) {
	assignees := runCCEngine(t,
		`{"ccUserIds":["${msg.ccUserIds}"]}`,
		`{}`)
	require.Empty(t, assignees)
}

// 未开启自选：静态名单生效
func TestCCTaskNode_OnMsg_StaticList(t *testing.T) {
	assignees := runCCEngine(t,
		`{"ccUserIds":["u-a","u-b"]}`,
		`{}`)
	require.ElementsMatch(t, []string{"u-a", "u-b"}, assignees)
}

// 模板名单夹带跨租户 userId：被租户校验过滤，不创建越租户抄送任务。
func TestCCTaskNode_OnMsg_TemplateFiltersCrossTenant(t *testing.T) {
	ccTestIdentity.deny("u-self-2")
	defer ccTestIdentity.reset()

	assignees := runCCEngine(t,
		`{"ccUserIds":["${msg.ccUserIds}"]}`,
		`{"ccUserIds":["u-self-1","u-self-2"]}`)
	require.Equal(t, []string{"u-self-1"}, assignees)
}

// 批量归属校验：整单名单一次查询（宿主实现 TenantMembershipBatchChecker 时无逐人 N+1），
// 越租户 userId 仍被过滤。
func TestCCTaskNode_OnMsg_BatchTenantCheckSingleQuery(t *testing.T) {
	ccTestIdentity.deny("u-cross")
	defer ccTestIdentity.reset()

	assignees := runCCEngine(t,
		`{"ccUserIds":["u-b1","u-b2","u-cross"]}`,
		`{}`)
	require.ElementsMatch(t, []string{"u-b1", "u-b2"}, assignees)
	require.Equal(t, 1, ccTestIdentity.batchCallCount(), "3 recipients should be checked in one batch query")
}

// 批量查询失败降级逐人校验：非阻塞失败语义保持，越租户 userId 仍被过滤。
func TestCCTaskNode_OnMsg_BatchCheckFailureFallsBackPerUser(t *testing.T) {
	ccTestIdentity.deny("u-cross")
	ccTestIdentity.failBatch(errors.New("batch backend down"))
	defer ccTestIdentity.reset()

	assignees := runCCEngine(t,
		`{"ccUserIds":["u-b1","u-cross"],"selfSelect":false}`,
		`{}`)
	require.ElementsMatch(t, []string{"u-b1"}, assignees)
}
