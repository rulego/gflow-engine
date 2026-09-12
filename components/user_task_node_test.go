package components

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
)

// Helpers

// strPtr is a helper to create a *string from a string literal.
func strPtr(s string) *string { return &s }

// makeTask creates a minimal WfTask with the given status and optional endReason.
func makeTask(status string, endReason *string) *model.WfTask {
	return &model.WfTask{
		ID:        "task-1",
		Status:    status,
		EndReason: endReason,
	}
}

// makeAssignedTask creates a WfTask with status, endReason and an assignee.
func makeAssignedTask(status string, endReason *string, assignee string) *model.WfTask {
	return &model.WfTask{
		ID:        "task-1",
		Status:    status,
		EndReason: endReason,
		Assignee:  &assignee,
	}
}

// makeSequentialTask creates a WfTask whose variables carry the _sequentialAssignees
// cache written by createUserTasks when ApprovalTypeSequential is in use. This lets
// checkTasksCompletion correctly compare completed tasks against the expected total.
func makeSequentialTask(status string, endReason *string, assignees []string) *model.WfTask {
	vars := map[string]interface{}{
		"_sequentialAssignees": assignees,
	}
	raw, _ := json.Marshal(vars)
	s := string(raw)
	return &model.WfTask{
		ID:        "seq-task",
		Status:    status,
		EndReason: endReason,
		Variables: &s,
	}
}

// 序列化辅助：serializeVariables

func TestSerializeVariablesDoesNotMutateInput(t *testing.T) {
	original := map[string]interface{}{
		"data": map[string]interface{}{"key": "value"},
		"var1": "hello",
		"num":  42,
	}

	origCopy, _ := json.Marshal(original)
	result := serializeVariables(original)

	var resultMap map[string]interface{}
	if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
		t.Fatalf("serializeVariables returned invalid JSON: %v", err)
	}
	if _, ok := resultMap["data"]; ok {
		t.Error("serializeVariables should not contain 'data' key in output")
	}

	afterCopy, _ := json.Marshal(original)
	if string(origCopy) != string(afterCopy) {
		t.Errorf("serializeVariables mutated the input map:\n  before: %s\n  after:  %s", origCopy, afterCopy)
	}

	if _, ok := original["data"]; !ok {
		t.Error("serializeVariables deleted 'data' from the input map")
	}
}

func TestSerializeVariablesEmpty(t *testing.T) {
	if got := serializeVariables(map[string]interface{}{}); got != "{}" {
		t.Errorf("serializeVariables(empty) = %q, want {}", got)
	}
}

func TestSerializeVariablesNil(t *testing.T) {
	if got := serializeVariables(nil); got != "{}" {
		t.Errorf("serializeVariables(nil) = %q, want {}", got)
	}
}

// 审批结果评估：evaluateApproval

func TestEvaluateApproval(t *testing.T) {
	tests := []struct {
		name          string
		approvalType  string
		approvalRule  string
		tasks         []*model.WfTask
		approvedCount int
		rejectedCount int
		wantApproved  bool
	}{
		// ---- Single approval ----
		{
			name:          "single: approved",
			approvalType:  string(enums.ApprovalTypeSingle),
			tasks:         []*model.WfTask{makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)))},
			approvedCount: 1, rejectedCount: 0, wantApproved: true,
		},
		{
			name:          "single: rejected",
			approvalType:  string(enums.ApprovalTypeSingle),
			tasks:         []*model.WfTask{makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultRejected)))},
			approvedCount: 0, rejectedCount: 1, wantApproved: false,
		},
		{
			name:          "single: no completed tasks",
			approvalType:  string(enums.ApprovalTypeSingle),
			tasks:         []*model.WfTask{makeTask(string(enums.TaskStatusActive), nil)},
			approvedCount: 0, rejectedCount: 0, wantApproved: false,
		},

		// ---- Or-sign ----
		{
			name:         "or: one approved out of three",
			approvalType: string(enums.ApprovalTypeAny),
			tasks: []*model.WfTask{
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
				makeTask(string(enums.TaskStatusActive), nil),
				makeTask(string(enums.TaskStatusActive), nil),
			},
			approvedCount: 1, rejectedCount: 0, wantApproved: true,
		},
		{
			name:         "or: one rejected, no approved",
			approvalType: string(enums.ApprovalTypeAny),
			tasks: []*model.WfTask{
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultRejected))),
				makeTask(string(enums.TaskStatusActive), nil),
				makeTask(string(enums.TaskStatusActive), nil),
			},
			approvedCount: 0, rejectedCount: 1, wantApproved: false,
		},

		// ---- Sequential ----
		{
			name:         "sequential: all approved",
			approvalType: string(enums.ApprovalTypeSequential),
			tasks: []*model.WfTask{
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
			},
			approvedCount: 2, rejectedCount: 0, wantApproved: true,
		},
		{
			name:         "sequential: one rejected",
			approvalType: string(enums.ApprovalTypeSequential),
			tasks: []*model.WfTask{
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultRejected))),
			},
			approvedCount: 1, rejectedCount: 1, wantApproved: false,
		},

		// ---- Countersign ----
		{
			name:         "countersign: all approved",
			approvalType: string(enums.ApprovalTypeCountersign),
			tasks: []*model.WfTask{
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
			},
			approvedCount: 3, rejectedCount: 0, wantApproved: true,
		},
		{
			name:         "countersign: one rejected",
			approvalType: string(enums.ApprovalTypeCountersign),
			tasks: []*model.WfTask{
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultRejected))),
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
			},
			approvedCount: 2, rejectedCount: 1, wantApproved: false,
		},

		// ---- Vote with weights ----
		{
			name:         "vote: weighted all approved",
			approvalType: string(enums.ApprovalTypeVote),
			approvalRule: `{"weights":{"user1":0.4,"user2":0.3,"user3":0.3},"threshold":0.5}`,
			tasks: []*model.WfTask{
				makeAssignedTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)), "user1"),
				makeAssignedTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)), "user2"),
				makeAssignedTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)), "user3"),
			},
			approvedCount: 3, rejectedCount: 0, wantApproved: true,
		},
		{
			name:         "vote: rejected overrides weights",
			approvalType: string(enums.ApprovalTypeVote),
			approvalRule: `{"weights":{"user1":0.4,"user2":0.3,"user3":0.3},"threshold":0.5}`,
			tasks: []*model.WfTask{
				makeAssignedTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)), "user1"),
				makeAssignedTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)), "user2"),
				makeAssignedTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultRejected)), "user3"),
			},
			approvedCount: 2, rejectedCount: 1, wantApproved: false,
		},

		// ---- Default/unknown type ----
		{
			name:          "unknown type: approved",
			approvalType:  "unknown",
			tasks:         []*model.WfTask{makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)))},
			approvedCount: 1, rejectedCount: 0, wantApproved: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &UserTaskNode{
				Config: UserTaskNodeConfiguration{
					ApproveMode: tt.approvalType,
				},
			}
			if got := node.evaluateApproval(tt.tasks, tt.approvedCount, tt.rejectedCount); got != tt.wantApproved {
				t.Errorf("evaluateApproval() = %v, want %v", got, tt.wantApproved)
			}
		})
	}
}

// 审批完成判定：checkTasksCompletion（仅非 countersign 分支，避免依赖 RuleContext）

func TestCheckTasksCompletion(t *testing.T) {
	tests := []struct {
		name         string
		approvalType string
		tasks        []*model.WfTask
		wantComplete bool
		wantErr      bool
	}{
		{
			name:         "empty task list (single)",
			approvalType: string(enums.ApprovalTypeSingle),
			tasks:        []*model.WfTask{},
			wantComplete: true,
		},
		{
			name:         "single: task completed",
			approvalType: string(enums.ApprovalTypeSingle),
			tasks: []*model.WfTask{
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
			},
			wantComplete: true,
		},
		{
			name:         "single: task still active",
			approvalType: string(enums.ApprovalTypeSingle),
			tasks:        []*model.WfTask{makeTask(string(enums.TaskStatusActive), nil)},
			wantComplete: false,
		},
		{
			name:         "or: 1 of 3 completed => true",
			approvalType: string(enums.ApprovalTypeAny),
			tasks: []*model.WfTask{
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
				makeTask(string(enums.TaskStatusActive), nil),
				makeTask(string(enums.TaskStatusActive), nil),
			},
			wantComplete: true,
		},
		{
			name:         "or: 0 of 3 completed => false",
			approvalType: string(enums.ApprovalTypeAny),
			tasks: []*model.WfTask{
				makeTask(string(enums.TaskStatusActive), nil),
				makeTask(string(enums.TaskStatusActive), nil),
			},
			wantComplete: false,
		},
		{
			name:         "sequential: all completed",
			approvalType: string(enums.ApprovalTypeSequential),
			tasks: []*model.WfTask{
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
			},
			wantComplete: true,
		},
		{
			name:         "sequential: some completed",
			approvalType: string(enums.ApprovalTypeSequential),
			tasks: []*model.WfTask{
				makeTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved))),
				makeTask(string(enums.TaskStatusActive), nil),
			},
			wantComplete: false,
		},
		// 1 个任务已完成但 _sequentialAssignees 缓存指示还有更多顺序审批人时必须返回 false；
		// 若按 completedTasks==len(tasks) 判定会误判为全部完成，跳过后续审批人直接进 end。
		{
			name:         "sequential: 1/3 done with cached assignees => NOT complete",
			approvalType: string(enums.ApprovalTypeSequential),
			tasks: []*model.WfTask{
				makeSequentialTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)),
					[]string{"admin-001", "user_manager_001", "user_hr_001"}),
			},
			wantComplete: false,
		},
		{
			name:         "sequential: 2/3 done with cached assignees => NOT complete",
			approvalType: string(enums.ApprovalTypeSequential),
			tasks: []*model.WfTask{
				makeSequentialTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)),
					[]string{"admin-001", "user_manager_001", "user_hr_001"}),
				makeSequentialTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)),
					[]string{"admin-001", "user_manager_001", "user_hr_001"}),
			},
			wantComplete: false,
		},
		{
			name:         "sequential: 3/3 done with cached assignees => complete",
			approvalType: string(enums.ApprovalTypeSequential),
			tasks: []*model.WfTask{
				makeSequentialTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)),
					[]string{"admin-001", "user_manager_001", "user_hr_001"}),
				makeSequentialTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)),
					[]string{"admin-001", "user_manager_001", "user_hr_001"}),
				makeSequentialTask(string(enums.TaskStatusCompleted), strPtr(string(enums.ApprovalResultApproved)),
					[]string{"admin-001", "user_manager_001", "user_hr_001"}),
			},
			wantComplete: true,
		},
		// vote/countersign 完成判定委托 service 层 CheckCountersignSubTaskCompletion（需 TaskService+DAO），
		// 节点层无法独立验证；阈值/早终止覆盖见 service/e2e 的 TestE2E_VoteApproval_* 与 countersign e2e。
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &UserTaskNode{Config: UserTaskNodeConfiguration{ApproveMode: tt.approvalType}}
			got, err := node.checkTasksCompletion(nil, types.RuleMsg{}, tt.tasks)
			if (err != nil) != tt.wantErr {
				t.Fatalf("checkTasksCompletion() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantComplete {
				t.Errorf("checkTasksCompletion() = %v, want %v", got, tt.wantComplete)
			}
		})
	}
}

// 审批人解析：resolveAssignees（仅静态类型，避免依赖 IdentityService）

func TestResolveAssignees(t *testing.T) {
	tests := []struct {
		name          string
		approverType  string
		userIds       []string
		selfApproval  string
		owner         string
		wantAssignees []string
	}{
		{
			name:          "approver.type user: returns userIds",
			approverType:  "user",
			userIds:       []string{"user1", "user2"},
			owner:         "owner1",
			wantAssignees: []string{"user1", "user2"},
		},
		{
			name:          "approver.type user: deduplicates userIds",
			approverType:  "user",
			userIds:       []string{"user1", "user2", "user1"},
			owner:         "owner1",
			wantAssignees: []string{"user1", "user2"},
		},
		{
			name:          "approver.type user: skips empty userIds",
			approverType:  "user",
			userIds:       []string{"user1", "", "user2"},
			owner:         "owner1",
			wantAssignees: []string{"user1", "user2"},
		},
		{
			name:          "approver.type initiatorSelf: returns owner",
			approverType:  "initiatorSelf",
			owner:         "owner1",
			wantAssignees: []string{"owner1"},
		},
		{
			name:          "approver.type initiatorSelf: empty owner returns empty",
			approverType:  "initiatorSelf",
			owner:         "",
			wantAssignees: nil,
		},
		{
			name:          "selfApproval skip: removes owner from list",
			approverType:  "user",
			userIds:       []string{"owner1", "user2", "owner1"},
			selfApproval:  "skip",
			owner:         "owner1",
			wantAssignees: []string{"user2"},
		},
		{
			name:          "selfApproval allow: keeps owner in list",
			approverType:  "user",
			userIds:       []string{"owner1", "user2"},
			selfApproval:  "none",
			owner:         "owner1",
			wantAssignees: []string{"owner1", "user2"},
		},
		{
			name:          "unknown approverType: returns empty",
			approverType:  "nonexistent",
			owner:         "owner1",
			wantAssignees: nil,
		},
		{
			name:          "empty approverType: returns empty",
			approverType:  "",
			owner:         "owner1",
			wantAssignees: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &UserTaskNode{
				Config: UserTaskNodeConfiguration{
					Approver:     ApproverConfig{Type: tt.approverType, UserIds: tt.userIds},
					SelfApproval: tt.selfApproval,
				},
			}
			got, err := node.resolveAssignees(context.Background(), "tenant1", tt.owner, nil)
			if err != nil {
				t.Fatalf("resolveAssignees() unexpected error: %v", err)
			}
			if len(got) != len(tt.wantAssignees) {
				t.Errorf("resolveAssignees() = %v, want %v", got, tt.wantAssignees)
				return
			}
			for i := range got {
				if got[i] != tt.wantAssignees[i] {
					t.Errorf("resolveAssignees()[%d] = %q, want %q", i, got[i], tt.wantAssignees[i])
				}
			}
		})
	}
}

// 自审处理：handleSelfApproval

func TestHandleSelfApproval(t *testing.T) {
	tests := []struct {
		name          string
		selfApproval  string
		owner         string
		assignees     []string
		wantAssignees []string
	}{
		{
			name:          "allow: keeps owner",
			selfApproval:  "none",
			owner:         "owner1",
			assignees:     []string{"owner1", "user2"},
			wantAssignees: []string{"owner1", "user2"},
		},
		{
			name:          "skip: removes owner",
			selfApproval:  "skip",
			owner:         "owner1",
			assignees:     []string{"owner1", "user2", "user3"},
			wantAssignees: []string{"user2", "user3"},
		},
		{
			name:          "skip: owner not in list => unchanged",
			selfApproval:  "skip",
			owner:         "owner1",
			assignees:     []string{"user2", "user3"},
			wantAssignees: []string{"user2", "user3"},
		},
		{
			name:          "autoApprove: keeps owner",
			selfApproval:  "autoApprove",
			owner:         "owner1",
			assignees:     []string{"owner1", "user2"},
			wantAssignees: []string{"owner1", "user2"},
		},
		{
			name:          "empty owner: returns unchanged",
			selfApproval:  "skip",
			owner:         "",
			assignees:     []string{"user1", "user2"},
			wantAssignees: []string{"user1", "user2"},
		},
		// IdentityService 未注入时，delegateToDeptManager 不得 panic，
		// 应与 getDelegateManager 的兜底一致——保持原审批人不变。
		{
			name:          "delegateToDeptManager: nil identity service keeps assignees",
			selfApproval:  "delegateToDeptManager",
			owner:         "owner1",
			assignees:     []string{"owner1", "user2"},
			wantAssignees: []string{"owner1", "user2"},
		},
		{
			name:          "delegateToManager: nil identity service keeps assignees",
			selfApproval:  "delegateToManager",
			owner:         "owner1",
			assignees:     []string{"owner1", "user2"},
			wantAssignees: []string{"owner1", "user2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &UserTaskNode{
				Config: UserTaskNodeConfiguration{SelfApproval: tt.selfApproval},
			}
			got := node.handleSelfApproval(context.Background(), "tenant1", tt.owner, tt.assignees, nil)
			if len(got) != len(tt.wantAssignees) {
				t.Errorf("handleSelfApproval() = %v, want %v", got, tt.wantAssignees)
				return
			}
			for i := range got {
				if got[i] != tt.wantAssignees[i] {
					t.Errorf("handleSelfApproval()[%d] = %q, want %q", i, got[i], tt.wantAssignees[i])
				}
			}
		})
	}
}

// 驳回策略与拒绝类型字段

// TestRejectStrategyConfig 验证驳回策略字段在常见取值下可正确存取
func TestRejectStrategyConfig(t *testing.T) {
	cases := []struct {
		strategy   string
		targetNode string
	}{
		{"", ""},
		{"terminate", ""},
		{"toStarter", ""},
		{"toPrev", ""},
		{"toNode", "nodeKey_abc"},
	}
	for _, c := range cases {
		node := &UserTaskNode{
			Config: UserTaskNodeConfiguration{
				Reject: RejectConfig{Strategy: c.strategy, Target: c.targetNode},
			},
		}
		if node.Config.Reject.Strategy != c.strategy {
			t.Errorf("Reject.Strategy set %q got %q", c.strategy, node.Config.Reject.Strategy)
		}
		if node.Config.Reject.Target != c.targetNode {
			t.Errorf("Reject.Target set %q got %q", c.targetNode, node.Config.Reject.Target)
		}
	}
}

// TestNew_PropagatesRuntimeService 验证 New() 正确传播 RuntimeService
// rulego 通过 New() 实例化每个节点，依赖必须显式复制
func TestNew_PropagatesRuntimeService(t *testing.T) {
	// 使用 nil 模拟未注入的 RuntimeService（实际生产由 router 注入）
	proto := &UserTaskNode{
		RuntimeService: nil,
	}
	clone := proto.New().(*UserTaskNode)
	if clone == nil {
		t.Fatal("New() returned nil")
	}
	if clone.RuntimeService != proto.RuntimeService {
		t.Errorf("New() did not propagate RuntimeService")
	}
}

// 包级辅助：toInt / toStringSlice

func TestToInt(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		def  int
		want int
	}{
		{"nil", nil, 1, 1},
		{"int", 42, 0, 42},
		{"int32", int32(7), 0, 7},
		{"int64", int64(99), 0, 99},
		{"float64", 3.7, 0, 3},
		{"valid string", "123", 0, 123},
		{"empty string uses default", "", 5, 5},
		{"invalid string uses default", "abc", 5, 5},
		{"unknown type uses default", struct{}{}, 5, 5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := toInt(c.in, c.def); got != c.want {
				t.Errorf("toInt(%v, %d) = %d, want %d", c.in, c.def, got, c.want)
			}
		})
	}
}

func TestToStringSlice(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want []string
	}{
		{"nil", nil, nil},
		{"[]string", []string{"a", "b"}, []string{"a", "b"}},
		{"[]interface{}", []interface{}{"x", 1, true}, []string{"x", "1", "true"}},
		{"comma string", "a, b ,c", []string{"a", "b", "c"}},
		{"empty string", "", nil},
		{"unknown type", 42, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := toStringSlice(c.in)
			if len(got) != len(c.want) {
				t.Errorf("toStringSlice(%v) = %v, want %v", c.in, got, c.want)
				return
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Errorf("toStringSlice(%v)[%d] = %q, want %q", c.in, i, got[i], c.want[i])
				}
			}
		})
	}
}

// 驳回策略初始化校验：未知值仅告警不报错，运行时按 terminate 处理

func TestIsValidUserTaskRejectStrategy(t *testing.T) {
	valid := []string{"", RejectStrategyTerminate, RejectStrategyToStarter, RejectStrategyToPrev, RejectStrategyToNode}
	for _, s := range valid {
		if !isValidUserTaskRejectStrategy(s) {
			t.Errorf("isValidUserTaskRejectStrategy(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"bogus", "terminate ", "REJECTTOPREV"} {
		if isValidUserTaskRejectStrategy(s) {
			t.Errorf("isValidUserTaskRejectStrategy(%q) = true, want false", s)
		}
	}
}

func TestInit_UnknownRejectStrategyStillInits(t *testing.T) {
	node := &UserTaskNode{}
	err := node.Init(types.Config{}, types.Configuration{
		"approver": map[string]interface{}{"type": "user", "userIds": []string{"u1"}},
		"reject":   map[string]interface{}{"strategy": "noSuchStrategy"},
	})
	if err != nil {
		t.Fatalf("Init with unknown reject strategy should only warn, got error: %v", err)
	}
	if node.Config.Reject.Strategy != RejectStrategyTerminate {
		t.Errorf("unknown reject strategy should fall back to terminate, got %q", node.Config.Reject.Strategy)
	}
}

// 审批人去重辅助：addUnique

func TestAddUnique(t *testing.T) {
	var list []string
	set := make(map[string]bool)
	list = addUnique(list, set, "a")
	list = addUnique(list, set, "a") // 重复
	list = addUnique(list, set, "")  // 空值
	list = addUnique(list, set, "b")
	if len(list) != 2 || list[0] != "a" || list[1] != "b" {
		t.Errorf("addUnique result = %v, want [a b]", list)
	}
}

// 主管层级解析：manager 的 levels 与 multi_level_manager 的 levels/-1

// managerChainIdentity 按 managers 表返回直接上级，不在表中的用户无上级。
type managerChainIdentity struct{}

var managerChain = map[string]string{
	"staff": "m1",
	"m1":    "m2",
	// m2 无上级（组织顶层）
}

func (managerChainIdentity) GetUserManagerID(_ context.Context, _, userID string) (string, error) {
	return managerChain[userID], nil
}

func (managerChainIdentity) GetUserIDsByRoleID(context.Context, string, string) ([]string, error) {
	return nil, nil
}

func (managerChainIdentity) GetDepartmentManagerUserID(context.Context, string, string) (string, error) {
	return "", nil
}

func (managerChainIdentity) GetUserIDsByGroupID(context.Context, string, string) ([]string, error) {
	return nil, nil
}

func (managerChainIdentity) GetUserIDsByDepartmentID(context.Context, string, string) ([]string, error) {
	return nil, nil
}

func (managerChainIdentity) GetUserManagerHierarchy(context.Context, string, string, int) ([]string, error) {
	return nil, nil
}

func (managerChainIdentity) GetUserDepartmentID(context.Context, string, string) (string, error) {
	return "", nil
}

func (managerChainIdentity) GetRoleIDsByUserID(context.Context, string, string) ([]string, error) {
	return nil, nil
}

func (managerChainIdentity) GetDepartmentIDsByUserID(context.Context, string, string) ([]string, error) {
	return nil, nil
}

func TestResolveAssignees_ManagerLevels(t *testing.T) {
	newNode := func(ct string, levels int) *UserTaskNode {
		return &UserTaskNode{
			IdentityService: managerChainIdentity{},
			Config: UserTaskNodeConfiguration{
				Approver: ApproverConfig{Type: ct, Levels: levels},
			},
		}
	}

	t.Run("manager 默认取第 1 级", func(t *testing.T) {
		got, err := newNode("manager", 0).resolveAssignees(context.Background(), "t", "staff", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"m1"}, got)
	})

	t.Run("manager levels=2 只取第 2 级（终点单审批人）", func(t *testing.T) {
		got, err := newNode("manager", 2).resolveAssignees(context.Background(), "t", "staff", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"m2"}, got)
	})

	t.Run("manager 层级不足时报错而不是静默降级", func(t *testing.T) {
		_, err := newNode("manager", 5).resolveAssignees(context.Background(), "t", "staff", nil)
		require.ErrorContains(t, err, "no manager found at level 3")
	})

	t.Run("multiLevelManager levels=2 逐级全审", func(t *testing.T) {
		got, err := newNode("multiLevelManager", 2).resolveAssignees(context.Background(), "t", "staff", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"m1", "m2"}, got)
	})

	t.Run("multiLevelManager levels=-1 审到组织顶层", func(t *testing.T) {
		got, err := newNode("multiLevelManager", -1).resolveAssignees(context.Background(), "t", "staff", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"m1", "m2"}, got)
	})

	// 组织关系成环（a→b→a）：levels=-1 必须按到顶停止，死循环会让本用例超时失败
	t.Run("multiLevelManager levels=-1 组织环不死循环", func(t *testing.T) {
		node := &UserTaskNode{
			IdentityService: cyclicManagerIdentity{},
			Config: UserTaskNodeConfiguration{
				Approver: ApproverConfig{Type: "multiLevelManager", Levels: -1},
			},
		}
		done := make(chan []string, 1)
		go func() {
			got, err := node.resolveAssignees(context.Background(), "t", "a", nil)
			require.NoError(t, err)
			done <- got
		}()
		select {
		case got := <-done:
			// a 的上级 b 审批；b 的上级回到 a（发起人，不在审批人列表）→ 停止
			require.Equal(t, []string{"b"}, got)
		case <-time.After(3 * time.Second):
			t.Fatal("resolveAssignees hung on cyclic manager chain (infinite loop)")
		}
	})
}

// cyclicManagerIdentity 组织关系成环：a 的上级是 b，b 的上级是 a。
type cyclicManagerIdentity struct{}

func (cyclicManagerIdentity) GetUserManagerID(_ context.Context, _, userID string) (string, error) {
	switch userID {
	case "a":
		return "b", nil
	case "b":
		return "a", nil
	}
	return "", nil
}
func (cyclicManagerIdentity) GetUserIDsByRoleID(context.Context, string, string) ([]string, error) {
	return nil, nil
}
func (cyclicManagerIdentity) GetDepartmentManagerUserID(context.Context, string, string) (string, error) {
	return "", nil
}
func (cyclicManagerIdentity) GetUserIDsByGroupID(context.Context, string, string) ([]string, error) {
	return nil, nil
}
func (cyclicManagerIdentity) GetUserIDsByDepartmentID(context.Context, string, string) ([]string, error) {
	return nil, nil
}
func (cyclicManagerIdentity) GetUserManagerHierarchy(context.Context, string, string, int) ([]string, error) {
	return nil, nil
}
func (cyclicManagerIdentity) GetUserDepartmentID(context.Context, string, string) (string, error) {
	return "", nil
}
func (cyclicManagerIdentity) GetRoleIDsByUserID(context.Context, string, string) ([]string, error) {
	return nil, nil
}
func (cyclicManagerIdentity) GetDepartmentIDsByUserID(context.Context, string, string) ([]string, error) {
	return nil, nil
}

// 自审委托成功路径：owner 被替换为直接上级/部门负责人；委托目标已在名单时不产生重复。
// managerChainIdentity 的部门查询返回空，这里用带部门信息的独立 mock。
type delegateIdentity struct {
	managerChainIdentity
}

var delegateDept = map[string]string{"owner1": "d1"}
var delegateDeptManager = map[string]string{"d1": "boss"}

func (delegateIdentity) GetUserDepartmentID(_ context.Context, _, userID string) (string, error) {
	return delegateDept[userID], nil
}
func (delegateIdentity) GetDepartmentManagerUserID(_ context.Context, _, deptID string) (string, error) {
	return delegateDeptManager[deptID], nil
}

func TestHandleSelfApproval_Delegate(t *testing.T) {
	newNode := func(selfType string) *UserTaskNode {
		return &UserTaskNode{
			IdentityService: delegateIdentity{},
			Config:          UserTaskNodeConfiguration{SelfApproval: selfType},
		}
	}

	t.Run("delegateToManager 替换发起人为直接上级", func(t *testing.T) {
		got := newNode("delegateToManager").handleSelfApproval(context.Background(), "t", "staff", []string{"staff", "user2"}, nil)
		require.Equal(t, []string{"m1", "user2"}, got)
	})

	t.Run("delegateToManager 上级已在名单不重复", func(t *testing.T) {
		got := newNode("delegateToManager").handleSelfApproval(context.Background(), "t", "staff", []string{"staff", "m1"}, nil)
		require.Equal(t, []string{"m1"}, got, "委托目标已在名单时应去重，避免同一人多个任务")
	})

	t.Run("delegateToManager 无上级回退部门负责人", func(t *testing.T) {
		// owner1 无上级（不在 managerChain），getDelegateManager 回退查部门 d1 → 负责人 boss
		got := newNode("delegateToManager").handleSelfApproval(context.Background(), "t", "owner1", []string{"owner1"}, nil)
		require.Equal(t, []string{"boss"}, got)
	})

	t.Run("delegateToDeptManager 替换为部门负责人", func(t *testing.T) {
		got := newNode("delegateToDeptManager").handleSelfApproval(context.Background(), "t", "owner1", []string{"owner1", "user2"}, nil)
		require.Equal(t, []string{"boss", "user2"}, got)
	})

	t.Run("delegateToDeptManager 负责人已在名单不重复", func(t *testing.T) {
		got := newNode("delegateToDeptManager").handleSelfApproval(context.Background(), "t", "owner1", []string{"owner1", "boss"}, nil)
		require.Equal(t, []string{"boss"}, got)
	})

	t.Run("delegateToDeptManager 无部门保持原名单", func(t *testing.T) {
		// staff 无部门映射 → 保持原审批人
		got := newNode("delegateToDeptManager").handleSelfApproval(context.Background(), "t", "staff", []string{"staff", "user2"}, nil)
		require.Equal(t, []string{"staff", "user2"}, got)
	})
}

// 超时策略的到期时间解析：timeout.dueInMinutes 为相对任务创建时刻的时长
func TestResolveDueDate_Timeout(t *testing.T) {
	n := &UserTaskNode{}

	// 1) 无策略 → 无到期时间
	if got := n.resolveDueDate(); got != nil {
		t.Errorf("no policy: got %v, want nil", got)
	}
	// 2) 90 分钟策略 → 约 now+90m（允许 5s 时钟误差）
	n.Config.Timeout = &TimeoutPolicy{DueInMinutes: 90, Action: "autoApprove"}
	got := n.resolveDueDate()
	wantLo := time.Now().Add(90*time.Minute - 5*time.Second)
	wantHi := time.Now().Add(90*time.Minute + 5*time.Second)
	if got.Before(wantLo) || got.After(wantHi) {
		t.Errorf("policy 90m: got %v, want ~now+90m", got)
	}
	// 3) 非法时长 → 无到期时间
	n.Config.Timeout = &TimeoutPolicy{DueInMinutes: 0, Action: "remind"}
	if got := n.resolveDueDate(); got != nil {
		t.Errorf("invalid policy: got %v, want nil", got)
	}
	// 4) 策略时长生效但未配动作 → 仍计算到期（sweep 侧按 remind 兜底）
	n.Config.Timeout = &TimeoutPolicy{DueInMinutes: 60}
	if got := n.resolveDueDate(); got == nil {
		t.Errorf("policy without action should still set due date, got nil")
	}
}

// 构造 nodeGraph 邻接表的小工具：nodes 为 id:type 对，edges 为 from>to 对
func testNodeGraph(nodes []string, edges []string) *nodeGraph {
	g := &nodeGraph{
		nodeTypes: make(map[string]string),
		forward:   make(map[string][]string),
		backward:  make(map[string][]string),
	}
	for _, spec := range nodes {
		id, typ, _ := strings.Cut(spec, ":")
		g.nodeTypes[id] = typ
	}
	for _, spec := range edges {
		from, to, _ := strings.Cut(spec, ">")
		g.forward[from] = append(g.forward[from], to)
		g.backward[to] = append(g.backward[to], from)
	}
	return g
}

// 驳回回跳的跨并行分支判定：区域内含 fork/join/inclusive 即拦截，
// 分支内部回退与线性链回退放行
func TestRejectRegionCrossesParallel(t *testing.T) {
	// start(虚拟) → fork → (ta1 → ta2 | tb) → join → end
	forkGraph := testNodeGraph(
		[]string{"fork:fork", "ta1:userTask", "ta2:userTask", "tb:userTask", "join:join", "end:end"},
		[]string{"fork>ta1", "fork>tb", "ta1>ta2", "ta2>join", "tb>join", "join>end"},
	)
	// start → n1 → n2 → end 线性链
	linearGraph := testNodeGraph(
		[]string{"n1:userTask", "n2:userTask", "end:end"},
		[]string{"n1>n2", "n2>end"},
	)
	cases := []struct {
		name   string
		g      *nodeGraph
		target string
		self   string
		want   bool
	}{
		{"fork 前目标回退穿 fork", forkGraph, "fork", "tb", true},
		{"join 前目标回退穿 join", forkGraph, "ta2", "tb", false},
		{"分支内部回退放行", forkGraph, "ta1", "ta2", false},
		{"跨分支目标区域为空", forkGraph, "ta1", "tb", false},
		{"线性链回退放行", linearGraph, "n1", "n2", false},
		{"nil 图放行", nil, "n1", "n2", false},
		{"空目标放行", forkGraph, "", "tb", false},
	}
	for _, c := range cases {
		if got := rejectRegionCrossesParallel(c.g, c.target, c.self); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// 链首重跑判定：链首下游可达任何并行网关即拦截，纯线性链放行
func TestRejectRestartTouchesGateway(t *testing.T) {
	// start(虚拟) → fork → (ta | tb) → join → end，链首节点 ta 在分支内
	forkGraph := testNodeGraph(
		[]string{"ta:userTask", "tb:userTask", "fork:fork", "join:join", "end:end"},
		[]string{"fork>ta", "fork>tb", "ta>join", "tb>join", "join>end"},
	)
	// start → n1 → n2 → end 线性链
	linearGraph := testNodeGraph(
		[]string{"n1:userTask", "n2:userTask", "end:end"},
		[]string{"n1>n2", "n2>end"},
	)
	// start → inc → (ta | tb) → join → end，inclusive 分裂
	inclusiveGraph := testNodeGraph(
		[]string{"inc:inclusive", "ta:userTask", "tb:userTask", "join:join", "end:end"},
		[]string{"inc>ta", "inc>tb", "ta>join", "tb>join", "join>end"},
	)
	cases := []struct {
		name  string
		g     *nodeGraph
		start string
		want  bool
	}{
		{"链首在分支内仍会重喂 join", forkGraph, "ta", true},
		{"线性链放行", linearGraph, "n1", false},
		{"inclusive 分裂拦截", inclusiveGraph, "inc", true},
		{"空链首放行", forkGraph, "", false},
		{"nil 图放行", nil, "n1", false},
	}
	for _, c := range cases {
		if got := rejectRestartTouchesGateway(c.g, c.start); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
