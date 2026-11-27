package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
)

// RoutingContextProvider 在正式发起时生成发起人组织快照。宿主可选实现；
// 只有定义显式引用 msg._starter 时才会调用，不影响既有流程与引擎集成方。
type RoutingContextProvider interface {
	BuildStarterContext(ctx context.Context, tenantID, userID string) (map[string]interface{}, error)
}

// requiresStarterContext 判断流程定义的条件表达式是否引用了 _starter。
// 先搜一次子串快速排除大多数流程；命中后再解析 JSON，只看 switch/condition
// 的 cases[].case 和 jsSwitch 的 jsScript，节点名、描述、注释里出现同字样不算。
func requiresStarterContext(definitionJSON string) bool {
	if !strings.Contains(definitionJSON, constants.VarsStarter) {
		return false
	}
	var doc struct {
		RuleChain struct {
			Metadata struct {
				Nodes []struct {
					Type          string                 `json:"type"`
					Configuration map[string]interface{} `json:"configuration"`
				} `json:"nodes"`
			} `json:"metadata"`
		} `json:"ruleChain"`
		Metadata struct {
			Nodes []struct {
				Type          string                 `json:"type"`
				Configuration map[string]interface{} `json:"configuration"`
			} `json:"nodes"`
		} `json:"metadata"`
	}
	// 解析不了就当引用处理：漏注入会让组织条件静默走默认分支
	if err := json.Unmarshal([]byte(definitionJSON), &doc); err != nil {
		return true
	}
	// envelope 与扁平 DSL 的节点都在顶层 metadata.nodes，rulego 原生嵌套在 ruleChain 下
	nodes := append(doc.Metadata.Nodes, doc.RuleChain.Metadata.Nodes...)
	for _, node := range nodes {
		if !conditionNodeTypes[node.Type] {
			continue
		}
		if node.Type == constants.NodeTypeJsSwitch {
			// 脚本不做语法解析，用字段级正则区分引用与注释
			if script, _ := node.Configuration["jsScript"].(string); starterFieldPattern.MatchString(script) {
				return true
			}
		}
		rawCases, _ := node.Configuration["cases"].([]interface{})
		for _, raw := range rawCases {
			c, _ := raw.(map[string]interface{})
			if c == nil {
				continue
			}
			if expr, _ := c["case"].(string); starterFieldPattern.MatchString(expr) {
				return true
			}
		}
	}
	return false
}

func (s *RuntimeServiceImpl) getRoutingContextProvider() RoutingContextProvider {
	if s.starterProvider != nil {
		return s.starterProvider
	}
	if s.workflowEngine != nil {
		if engine, ok := s.workflowEngine.(interface {
			GetRoutingContextProvider() RoutingContextProvider
		}); ok {
			return engine.GetRoutingContextProvider()
		}
	}
	return nil
}

// resolveForecastVariables 预测后续节点时和实际驱动取同一份 _starter（以实例
// 快照为准），其余任务变量保持原有 Go 类型。
func resolveForecastVariables(variables map[string]interface{}, instVars *string) map[string]interface{} {
	if variables == nil {
		parsed, err := ParseVariablesJSON(instVars)
		if err != nil {
			return nil
		}
		return parsed
	}
	instanceData, err := ParseVariablesJSON(instVars)
	if err != nil {
		instanceData = nil
	}
	trusted, hasTrusted := instanceData[constants.VarsStarter]
	_, hasStarter := variables[constants.VarsStarter]
	if !hasTrusted && !hasStarter {
		return variables
	}
	out := make(map[string]interface{}, len(variables)+1)
	for k, v := range variables {
		out[k] = v
	}
	if hasTrusted {
		out[constants.VarsStarter] = trusted
	} else {
		delete(out, constants.VarsStarter)
	}
	return out
}

func starterPathValues(snapshot map[string]interface{}, key string) ([]string, error) {
	value, ok := snapshot[key]
	if !ok {
		return nil, fmt.Errorf("%s is missing", key)
	}
	if values, ok := value.([]string); ok {
		return append([]string(nil), values...), nil
	}
	items, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%s must be a string array", key)
	}
	values := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%s must be a string array", key)
		}
		values = append(values, text)
	}
	return values, nil
}

func normalizeStarterContext(snapshot map[string]interface{}) (map[string]interface{}, error) {
	ids, err := starterPathValues(snapshot, "deptPathIds")
	if err != nil {
		return nil, err
	}
	names, err := starterPathValues(snapshot, "deptPathNames")
	if err != nil {
		return nil, err
	}
	if len(ids) != len(names) {
		return nil, fmt.Errorf("deptPathIds and deptPathNames must have the same length")
	}
	for i := range ids {
		if ids[i] == "" || names[i] == "" {
			return nil, fmt.Errorf("deptPathIds and deptPathNames cannot contain empty values")
		}
	}
	return map[string]interface{}{"deptPathIds": ids, "deptPathNames": names}, nil
}

// prepareStartVariables 复制业务变量并剥掉调用方预埋的 _starter；草稿延到正式
// 激活时才取快照。流程引用了 _starter 而宿主没接 provider 时直接报错，不做静默降级。
func (s *RuntimeServiceImpl) prepareStartVariables(ctx context.Context, processDef *model.WfProcess,
	initiator Actor, variables map[string]interface{}, isDraft bool, parentInstanceID string) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(variables))
	for k, v := range variables {
		if k != constants.VarsStarter {
			out[k] = v
		}
	}
	if isDraft || !requiresStarterContext(processDef.DefinitionJSON) {
		return out, nil
	}
	if parentInstanceID != "" {
		if inherited, ok := variables[constants.VarsStarter].(map[string]interface{}); ok {
			starter, err := normalizeStarterContext(inherited)
			if err != nil {
				return nil, fmt.Errorf("invalid inherited starter routing context: %w", err)
			}
			out[constants.VarsStarter] = starter
			return out, nil
		}
	}
	provider := s.getRoutingContextProvider()
	if provider == nil {
		return nil, fmt.Errorf("routing context provider is required by process %s", processDef.ID)
	}
	snapshot, err := provider.BuildStarterContext(ctx, processDef.TenantID, initiator.UserID)
	if err != nil {
		return nil, fmt.Errorf("build starter routing context: %w", err)
	}
	starter, err := normalizeStarterContext(snapshot)
	if err != nil {
		return nil, fmt.Errorf("invalid starter routing context: %w", err)
	}
	out[constants.VarsStarter] = starter
	return out, nil
}
