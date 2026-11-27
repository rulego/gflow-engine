package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/rulego/api/types"
)

// chainFromDSL 用设计器 envelope DSL 构造 RuleChain（与 ToRuleChain 同口径）。
func chainFromDSL(t *testing.T, dsl string) *types.RuleChain {
	t.Helper()
	p := &model.WfProcess{DefinitionJSON: dsl}
	chain, err := p.ToRuleChain()
	if err != nil {
		t.Fatalf("ToRuleChain failed: %v", err)
	}
	return chain
}

const validSwitchDSL = `{
  "ruleChain": {"id": "p1", "name": "流程"},
  "metadata": {
    "nodes": [
      {"id": "n1", "type": "switch", "name": "分支", "configuration": {
        "cases": [
          {"case": "msg.amount >= 10000", "then": "b1"},
          {"case": "msg.type == 'A' && msg.amount > 100", "then": "b2"}
        ]
      }},
      {"id": "n2", "type": "inclusive", "name": "并行分支", "configuration": {
        "cases": [{"case": "msg.level > 3", "then": "b3"}]
      }},
      {"id": "n3", "type": "serviceTask", "name": "服务", "configuration": {
        "functionName": "serialNo"
      }}
    ],
    "connections": []
  }
}`

func TestValidateChainExpressions_ValidChain_NoIssues(t *testing.T) {
	SetServiceFunctionChecker(func(name string) bool { return name == "serialNo" })
	defer SetServiceFunctionChecker(nil)

	issues := ValidateChainExpressions(chainFromDSL(t, validSwitchDSL))
	if len(issues) != 0 {
		t.Errorf("expected no issues, got: %v", issues)
	}
}

func TestValidateChainExpressions_IllegalOperator_ReportsIssue(t *testing.T) {
	dsl := strings.Replace(validSwitchDSL,
		`"case": "msg.amount >= 10000"`,
		`"case": "msg.amount !contains 'x'"`, 1)

	issues := ValidateChainExpressions(chainFromDSL(t, dsl))
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue, got: %v", issues)
	}
	got := issues[0]
	if got.NodeID != "n1" || got.NodeName != "分支" || got.NodeType != "switch" {
		t.Errorf("issue node info mismatch: %+v", got)
	}
	if got.CaseIndex != 1 {
		t.Errorf("expected caseIndex 1 (1-based), got %d", got.CaseIndex)
	}
	if !strings.Contains(got.Expression, "!contains") || got.Error == "" {
		t.Errorf("issue should carry expression and compile error: %+v", got)
	}
}

func TestValidateChainExpressions_EmptyCase_ReportsIssue(t *testing.T) {
	dsl := strings.Replace(validSwitchDSL,
		`"case": "msg.amount >= 10000"`, `"case": ""`, 1)

	issues := ValidateChainExpressions(chainFromDSL(t, dsl))
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue for empty case, got: %v", issues)
	}
}

func TestValidateChainExpressions_ServiceTaskUnknownFunction(t *testing.T) {
	SetServiceFunctionChecker(func(name string) bool { return false })
	defer SetServiceFunctionChecker(nil)

	issues := ValidateChainExpressions(chainFromDSL(t, validSwitchDSL))
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue, got: %v", issues)
	}
	got := issues[0]
	if got.NodeID != "n3" || got.NodeType != "serviceTask" || got.Expression != "serialNo" {
		t.Errorf("issue should point to the serviceTask node: %+v", got)
	}
}

// 校验器未注入（宿主未调 Register）时跳过函数名校验，不误报。
func TestValidateChainExpressions_NoChecker_SkipsFunctionCheck(t *testing.T) {
	issues := ValidateChainExpressions(chainFromDSL(t, validSwitchDSL))
	if len(issues) != 0 {
		t.Errorf("expected no issues without checker, got: %v", issues)
	}
}

func TestValidateChainExpressions_ServiceTaskEmptyFunctionName(t *testing.T) {
	SetServiceFunctionChecker(func(name string) bool { return true })
	defer SetServiceFunctionChecker(nil)

	dsl := strings.Replace(validSwitchDSL, `"functionName": "serialNo"`, `"functionName": ""`, 1)
	issues := ValidateChainExpressions(chainFromDSL(t, dsl))
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue for empty functionName, got: %v", issues)
	}
}

// Deploy 必须在落库前拒绝坏表达式（nil DAO 不 panic 即证明校验先于 DAO）。
func TestProcessServiceImpl_Deploy_BadCondition_Rejected(t *testing.T) {
	s := &ProcessServiceImpl{} // nil DAO：若校验缺失会 panic
	dsl := strings.Replace(validSwitchDSL,
		`"case": "msg.amount >= 10000"`, `"case": "msg.amount !contains 'x'"`, 1)

	_, err := s.Deploy(context.Background(), Actor{UserID: "tester", TenantID: "t1", WorkflowAdmin: true}, &model.WfProcess{
		Name:           "Test",
		ProcessKey:     "key1",
		TenantID:       "t1",
		DefinitionJSON: dsl,
	}, false)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation, got: %v", err)
	}
	if !strings.Contains(err.Error(), "n1") {
		t.Errorf("error should locate the offending node, got: %v", err)
	}
}

func TestValidateExpressions_RejectsUnknownStarterField(t *testing.T) {
	valid := []string{
		`"dept" in msg._starter.deptPathIds`,
		`"部门" in msg._starter.deptPathNames`,
		`last(msg._starter.deptPathIds) == "dept"`,
	}
	if errs := ValidateExpressions(valid); len(errs) != 0 {
		t.Fatalf("valid starter expressions should pass: %v", errs)
	}
	errs := ValidateExpressions([]string{`"dept" in msg._starter.deptPathId`})
	if len(errs) != 1 || !strings.Contains(errs[0], "deptPathId") {
		t.Fatalf("expected unknown starter field error, got: %v", errs)
	}
}

func TestFormatConditionIssues(t *testing.T) {
	msg := FormatConditionIssues([]ConditionIssue{{
		NodeID: "n1", NodeName: "分支", NodeType: "switch",
		CaseIndex: 2, Expression: "bad expr", Error: "syntax error",
	}})
	if !strings.Contains(msg, "n1") || !strings.Contains(msg, "bad expr") || !strings.Contains(msg, "syntax error") {
		t.Errorf("formatted message should carry node/expression/error, got: %s", msg)
	}
}

func TestValidateExpressions(t *testing.T) {
	errs := ValidateExpressions([]string{"msg.amount >= 100", "msg.x !contains 'y'", ""})
	if len(errs) != 2 {
		t.Fatalf("expected errors at index 1,2 got: %v", errs)
	}
	if errs[0] != "" || errs[1] == "" || errs[2] == "" {
		t.Errorf("expected index 0 ok, 1/2 failed: %v", errs)
	}

	if errs := ValidateExpressions([]string{"true"}); len(errs) != 0 {
		t.Errorf("valid expression should pass: %v", errs)
	}
}

// TestServiceFunctionChecker_ConcurrentAccess 验证校验器读写并发安全：
// 宿主装配（SetServiceFunctionChecker）与 Deploy/Update 请求路径的读取
// validateServiceTaskFunction 可能并发，须经 RWMutex 保护；在 go test -race
// 下可捕获漏同步，普通运行下仅演练读写路径。
func TestServiceFunctionChecker_ConcurrentAccess(t *testing.T) {
	defer SetServiceFunctionChecker(nil)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				SetServiceFunctionChecker(func(name string) bool { return name == "serialNo" })
				if fn := serviceFunctionCheckerForRead(); fn == nil {
					t.Errorf("checker should be set during write phase")
					return
				}
			}
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = serviceFunctionCheckerForRead()
			}
		}()
	}
	wg.Wait()
}
