package components

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
)

const (
	// StartProcessNodeType 发起审批节点类型（规则链面板专用）
	StartProcessNodeType = "startProcess"
	// KeyProcessInstanceID 发起成功后写回消息 metadata 的实例ID键
	KeyProcessInstanceID = "processInstanceId"
)

// StartProcessNodeConfig 发起审批节点配置
type StartProcessNodeConfig struct {
	// ProcessKey 流程定义 Key（取该租户下最新版本），支持 ${msg.xxx} 占位
	ProcessKey string `json:"processKey" label:"流程定义Key" desc:"要发起的审批流程定义 Key（最新版本），支持 ${msg.xxx} 占位" required:"true"`
	// Initiator 发起人用户ID（实例 start_user_id），支持 ${msg.xxx} 占位
	Initiator string `json:"initiator" label:"发起人用户ID" desc:"以该用户身份发起流程；支持 ${msg.xxx} 占位" required:"true"`
	// BusinessKey 业务Key（可选）；同定义下已有同 businessKey 的 active 实例时发起冲突走 Failure
	BusinessKey string `json:"businessKey" label:"业务Key" desc:"可选，业务关联键；同定义下已有同 businessKey 的活跃实例时发起失败"`
	// Variables 启动变量（表单数据）；值支持 ${msg.xxx}/${metadata.xxx} 占位；留空则把当前消息数据作为表单变量
	Variables map[string]interface{} `json:"variables" label:"表单变量" desc:"发起时写入的表单数据（键=表单字段）；值支持 ${msg.xxx} 占位；留空则把当前消息数据作为表单变量" component:"{\"type\":\"map\"}"`
}

// StartProcessNode 发起审批节点：在规则链内以指定发起人启动一条 BPM 流程实例。
// 典型用法：定时链（endpoint/schedule 触发）经此节点实现"定时自动发起审批"。
//
// 租户从消息 metadata 的 tenant_id 解析（与 aiAgent 节点同款约定）：
//   - BPM→链 调用（automation/aiAgent 节点）由 AutomationExecutor 自动携带；
//   - 定时触发的链由 gflow 触发配置把 tenant_id 写进 schedule 端点的静态 metadata；
//   - 编辑器手动测试需在调试 metadata 里补 tenant_id。
//
// 发起成功后实例ID写入 metadata.processInstanceId，并合并进 msg.Data 顶层
// （msg.Data 非 JSON 对象时整体替换为 {"processInstanceId":...}）。
type StartProcessNode struct {
	Config         StartProcessNodeConfig
	RuntimeService service.RuntimeServiceInternal

	processKeyTmpl  el.Template
	initiatorTmpl   el.Template
	businessKeyTmpl el.Template
	// Variables 逐值预编译模板（string 值才编译；非字符串值原样透传）
	variablesTmpls map[string]el.Template
}

func (x *StartProcessNode) Type() string { return StartProcessNodeType }

// Category 面板分组。gflow-ui 经 locales.category 注入「流程/审批」标签。
func (x *StartProcessNode) Category() string { return "bpm" }

func (x *StartProcessNode) New() types.Node {
	return &StartProcessNode{
		RuntimeService: x.RuntimeService, // 从注册原型传播到 New 出的实例
	}
}

func (x *StartProcessNode) Init(_ types.Config, cfg types.Configuration) error {
	normalized, err := normalizeVariablesConfig(cfg)
	if err != nil {
		return err
	}
	if err := maps.Map2Struct(normalized, &x.Config); err != nil {
		return err
	}
	if strings.TrimSpace(x.Config.ProcessKey) == "" {
		return fmt.Errorf("startProcess node processKey is required")
	}
	if strings.TrimSpace(x.Config.Initiator) == "" {
		return fmt.Errorf("startProcess node initiator is required")
	}
	if x.processKeyTmpl, err = el.NewTemplate(x.Config.ProcessKey); err != nil {
		return fmt.Errorf("invalid processKey template: %w", err)
	}
	if x.initiatorTmpl, err = el.NewTemplate(x.Config.Initiator); err != nil {
		return fmt.Errorf("invalid initiator template: %w", err)
	}
	if strings.TrimSpace(x.Config.BusinessKey) != "" {
		if x.businessKeyTmpl, err = el.NewTemplate(x.Config.BusinessKey); err != nil {
			return fmt.Errorf("invalid businessKey template: %w", err)
		}
	}
	for k, v := range x.Config.Variables {
		s, ok := v.(string)
		if !ok || strings.TrimSpace(s) == "" {
			continue
		}
		tmpl, err := el.NewTemplate(s)
		if err != nil {
			return fmt.Errorf("invalid variables[%s] template: %w", k, err)
		}
		if x.variablesTmpls == nil {
			x.variablesTmpls = map[string]el.Template{}
		}
		x.variablesTmpls[k] = tmpl
	}
	return nil
}

// normalizeVariablesConfig 归一化 variables 配置：DSL 中该字段可以是 JSON 对象，
// 也可以是序列化后的 JSON 对象字符串（存量 DSL 与 codeEditor 场景），
// 此处把字符串形态解析回 map，两种写法统一进 Config.Variables。
func normalizeVariablesConfig(cfg types.Configuration) (types.Configuration, error) {
	raw, ok := cfg["variables"]
	if !ok {
		return cfg, nil
	}
	s, ok := raw.(string)
	if !ok {
		return cfg, nil
	}
	s = strings.TrimSpace(s)
	if s == "" {
		delete(cfg, "variables")
		return cfg, nil
	}
	var vars map[string]interface{}
	if err := json.Unmarshal([]byte(s), &vars); err != nil {
		return cfg, fmt.Errorf("startProcess variables is not a valid JSON object: %w", err)
	}
	cfg["variables"] = vars
	return cfg, nil
}

func (x *StartProcessNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	defer recoverNodePanic(ctx, msg, StartProcessNodeType, ctx.GetSelfId())
	if x.RuntimeService == nil {
		ctx.TellFailure(msg, fmt.Errorf("startProcess node RuntimeService not injected"))
		return
	}
	evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)

	tenantID := metaValue(msg, constants.KeyTenantID)
	if tenantID == "" {
		ctx.TellFailure(msg, fmt.Errorf(
			"startProcess requires metadata.%s (scheduled chains carry it automatically; add it to debug metadata for manual testing)", constants.KeyTenantID))
		return
	}
	processKey := x.processKeyTmpl.ExecuteAsString(evn)
	if processKey == "" {
		ctx.TellFailure(msg, fmt.Errorf("startProcess processKey resolves empty"))
		return
	}
	initiator := x.initiatorTmpl.ExecuteAsString(evn)
	if initiator == "" {
		ctx.TellFailure(msg, fmt.Errorf("startProcess initiator resolves empty"))
		return
	}
	var businessKey string
	if x.businessKeyTmpl != nil {
		businessKey = x.businessKeyTmpl.ExecuteAsString(evn)
	}

	variables, err := x.resolveVariables(evn, msg)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	actor := service.Actor{UserID: initiator, UserName: initiator, TenantID: tenantID}
	instanceID, err := x.RuntimeService.StartProcessInstanceByKey(
		ctx.GetContext(), actor, processKey, businessKey, variables)
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("startProcess %s: %w", processKey, err))
		return
	}

	msg.GetMetadata().PutValue(KeyProcessInstanceID, instanceID)
	mergeProcessInstanceToMsg(msg, instanceID)
	ctx.TellSuccess(msg)
}

func (x *StartProcessNode) Destroy() {}

// resolveVariables 解析启动变量：配置了 Variables 时对每个 string 值做 ${} 模板
// 渲染（非字符串值原样透传）；未配置则把当前消息数据（JSON 对象）作为表单变量，
// 解析失败按空变量处理（发起本身仍有效）。
func (x *StartProcessNode) resolveVariables(evn map[string]interface{}, msg types.RuleMsg) (map[string]interface{}, error) {
	if len(x.Config.Variables) > 0 {
		vars := make(map[string]interface{}, len(x.Config.Variables))
		for k, v := range x.Config.Variables {
			if tmpl, ok := x.variablesTmpls[k]; ok {
				vars[k] = tmpl.ExecuteAsString(evn)
				continue
			}
			vars[k] = v
		}
		return vars, nil
	}
	raw := strings.TrimSpace(msg.GetData())
	if raw == "" {
		return map[string]interface{}{}, nil
	}
	var vars map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &vars); err != nil || vars == nil {
		return map[string]interface{}{}, nil
	}
	return vars, nil
}

// mergeProcessInstanceToMsg 把实例ID合并进 msg.Data 顶层；
// 原 Data 非 JSON 对象时整体替换（metadata.processInstanceId 始终保留）。
func mergeProcessInstanceToMsg(msg types.RuleMsg, instanceID string) {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(msg.GetData()), &m); err != nil || m == nil {
		m = map[string]interface{}{}
	}
	m[KeyProcessInstanceID] = instanceID
	if b, err := json.Marshal(m); err == nil {
		msg.SetData(string(b))
	}
}
