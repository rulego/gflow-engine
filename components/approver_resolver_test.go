package components

import (
	"context"
	"errors"
	"testing"

	"github.com/rulego/gflow-engine/types/enums"
)

// testPositionResolver 测试用自定义策略：按 Vars["position"] 解析成员，
// 组池语义返回 person 候选。
type testPositionResolver struct{}

func (testPositionResolver) Resolve(in ApproverResolveInput) ([]string, error) {
	if in.Vars == nil {
		return nil, nil
	}
	switch v := in.Vars["position"].(type) {
	case string:
		if v == "" {
			return nil, nil
		}
		return []string{v}, nil
	case []string:
		return dedupeIDs(v), nil
	}
	return nil, nil
}

func (testPositionResolver) Validate(cfg *ApproverConfig) []string {
	if cfg.Expression == "" {
		return []string{"approver.expression is empty"}
	}
	return nil
}

func (testPositionResolver) PoolCandidates(cfg *ApproverConfig) []PoolCandidateGroup {
	return []PoolCandidateGroup{{EntityType: "person", IDs: []string{cfg.Expression}}}
}

func TestApproverResolver_CustomType(t *testing.T) {
	const customType = enums.CandidateType("testPosition")
	if err := RegisterApproverResolver(customType, testPositionResolver{}); err != nil {
		t.Fatalf("register custom resolver: %v", err)
	}

	// 重复注册返回哨兵错误
	err := RegisterApproverResolver(customType, testPositionResolver{})
	if !errors.Is(err, ErrApproverResolverAlreadyRegistered) {
		t.Fatalf("duplicate register = %v, want ErrApproverResolverAlreadyRegistered", err)
	}

	// 解析走自定义策略
	node := &UserTaskNode{
		Config: UserTaskNodeConfiguration{
			Approver: ApproverConfig{Type: string(customType), Expression: "pos-1"},
		},
	}
	got, err := node.resolveAssignees(context.Background(), "t1", "owner1", map[string]interface{}{"position": "user9"})
	if err != nil {
		t.Fatalf("resolveAssignees: %v", err)
	}
	if len(got) != 1 || got[0] != "user9" {
		t.Fatalf("resolveAssignees() = %v, want [user9]", got)
	}

	// 部署期校验走自定义策略
	cfg := UserTaskNodeConfiguration{Approver: ApproverConfig{Type: string(customType)}}
	cfg.Normalize()
	issues := cfg.Validate()
	if len(issues) != 1 || issues[0] != "approver.expression is empty" {
		t.Fatalf("Validate() = %v, want [approver.expression is empty]", issues)
	}
	cfg.Approver.Expression = "pos-1"
	if issues = cfg.Validate(); len(issues) != 0 {
		t.Fatalf("Validate() = %v, want pass", issues)
	}

	// 组池语义走自定义策略
	groups := approverPoolGroups(&cfg.Approver)
	if len(groups) != 1 || groups[0].EntityType != "person" || len(groups[0].IDs) != 1 || groups[0].IDs[0] != "pos-1" {
		t.Fatalf("approverPoolGroups() = %v, want person/pos-1", groups)
	}

	// 未知类型：未注册时解析为不分配审批人，部署期校验报不支持
	unknown := UserTaskNodeConfiguration{Approver: ApproverConfig{Type: "noSuchType"}}
	unknown.Normalize()
	issues = unknown.Validate()
	if len(issues) != 1 {
		t.Fatalf("unknown type Validate() = %v, want 1 issue", issues)
	}
	got, err = (&UserTaskNode{Config: unknown}).resolveAssignees(context.Background(), "t1", "owner1", nil)
	if err != nil || got != nil {
		t.Fatalf("unknown type resolveAssignees() = (%v, %v), want (nil, nil)", got, err)
	}
}

// 内置类型经注册表解析，名单与 enums 全集一致。
func TestApproverResolver_BuiltinsRegistered(t *testing.T) {
	for _, ct := range enums.GetAllCandidateTypes() {
		if LookupApproverResolver(ct) == nil {
			t.Errorf("builtin candidate type %q has no resolver", ct)
		}
	}
}
