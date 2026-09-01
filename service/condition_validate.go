package service

import (
	"fmt"
	"strings"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/el"
)

// conditionNodeTypes 携带 cases[].case 条件表达式的节点类型。
// switch 家族与 inclusive 在 Init 时用 el.NewExprTemplate 编译 case；
// condition 为设计器独立条件节点（单元素 cases），同口径校验。
var conditionNodeTypes = map[string]bool{
	constants.NodeTypeSwitch:        true,
	constants.NodeTypeJsSwitch:      true,
	constants.NodeTypeMsgTypeSwitch: true,
	constants.NodeTypeInclusive:     true,
	"condition":                     true,
}

// ConditionIssue DSL 部署期校验的一条问题（条件表达式编译失败或服务函数未注册）。
type ConditionIssue struct {
	NodeID     string `json:"nodeId"`
	NodeName   string `json:"nodeName"`
	NodeType   string `json:"nodeType"`
	CaseIndex  int    `json:"caseIndex"` // 1-based；serviceTask 无 case 时为 -1
	Expression string `json:"expression"`
	Error      string `json:"error"`
}

// serviceFunctionChecker serviceTask 函数名存在性校验器，由组件层
// （components.Services 所在包）经 SetServiceFunctionChecker 注入，避免
// service→components 循环依赖。nil 时跳过函数名校验（宿主未完成装配的场景）。
var serviceFunctionChecker func(name string) bool

// SetServiceFunctionChecker 注入 serviceTask 函数名校验器；传 nil 恢复跳过。
func SetServiceFunctionChecker(fn func(name string) bool) {
	serviceFunctionChecker = fn
}

// ValidateChainExpressions 校验链内所有分支条件表达式可编译、serviceTask
// 函数名已注册，返回问题列表（空=通过）。
//
// 背景：链加载（PreloadChain/initExecution）失败目前只 warn，语法错误的
// case 能部署成功、实例启动才炸。本方法供部署/更新/编辑期调用，把失败
// 提前到写入前。注意 el.NewExprTemplate 走 AllowUndefinedVariables，
// 只能抓语法/结构错误（如 !contains），抓不到拼错的 msg 字段名。
func ValidateChainExpressions(chain *types.RuleChain) []ConditionIssue {
	if chain == nil {
		return nil
	}
	var issues []ConditionIssue
	for _, node := range chain.Metadata.Nodes {
		cfg := map[string]interface{}(node.Configuration)
		if cfg == nil {
			continue
		}
		if conditionNodeTypes[node.Type] {
			issues = append(issues, validateNodeCases(node, cfg)...)
		}
		if node.Type == constants.NodeTypeServiceTask {
			issues = append(issues, validateServiceTaskFunction(node, cfg)...)
		}
	}
	return issues
}

// validateNodeCases 逐条编译 cases[].case，与 rulego SwitchNode.Init 同口径。
func validateNodeCases(node *types.RuleNode, cfg map[string]interface{}) []ConditionIssue {
	cases, _ := cfg["cases"].([]interface{})
	var issues []ConditionIssue
	for idx, raw := range cases {
		c, _ := raw.(map[string]interface{})
		if c == nil {
			continue
		}
		expr, _ := c["case"].(string)
		issue := ConditionIssue{
			NodeID: node.Id, NodeName: node.Name, NodeType: node.Type,
			CaseIndex: idx + 1, Expression: expr,
		}
		if expr == "" {
			issue.Error = "empty case expression"
			issues = append(issues, issue)
			continue
		}
		if _, err := el.NewExprTemplate(expr); err != nil {
			issue.Error = err.Error()
			issues = append(issues, issue)
		}
	}
	return issues
}

// validateServiceTaskFunction 校验 functionName 非空且已注册。
// 空 functionName 在运行时必然 TellFailure（ServiceTaskNode.New 清空了默认值），
// 未注册函数同样运行时才炸——两者都应在部署期拦截。
func validateServiceTaskFunction(node *types.RuleNode, cfg map[string]interface{}) []ConditionIssue {
	name, _ := cfg["functionName"].(string)
	issue := ConditionIssue{
		NodeID: node.Id, NodeName: node.Name, NodeType: node.Type,
		CaseIndex: -1, Expression: name,
	}
	if name == "" {
		issue.Error = "functionName is empty"
		return []ConditionIssue{issue}
	}
	if serviceFunctionChecker != nil && !serviceFunctionChecker(name) {
		issue.Error = "function is not registered"
		return []ConditionIssue{issue}
	}
	return nil
}

// ValidateExpressions 编译校验裸条件表达式（设计器编辑期逐条校验用）。
// 返回 下标→编译错误；全部通过时返回空 map。
func ValidateExpressions(expressions []string) map[int]string {
	errs := make(map[int]string)
	for i, expr := range expressions {
		if expr == "" {
			errs[i] = "empty expression"
			continue
		}
		if _, err := el.NewExprTemplate(expr); err != nil {
			errs[i] = err.Error()
		}
	}
	return errs
}

// FormatConditionIssues 把问题列表拼成单行可读错误（用于 ErrValidation 消息）。
func FormatConditionIssues(issues []ConditionIssue) string {
	parts := make([]string, 0, len(issues))
	for _, i := range issues {
		if i.CaseIndex > 0 {
			parts = append(parts, fmt.Sprintf("node %s(%s, type=%s) case #%d %q: %s",
				i.NodeID, i.NodeName, i.NodeType, i.CaseIndex, i.Expression, i.Error))
		} else {
			parts = append(parts, fmt.Sprintf("node %s(%s, type=%s) functionName %q: %s",
				i.NodeID, i.NodeName, i.NodeType, i.Expression, i.Error))
		}
	}
	return strings.Join(parts, "; ")
}
