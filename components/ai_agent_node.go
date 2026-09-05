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
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/str"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/maps"
)

// AIAgentNodeType 节点类型字符串（取自 types/constants 的 DSL 节点类型组）
const AIAgentNodeType = constants.NodeTypeAIAgent

// defaultAIAgentTimeoutSec 未配置 timeoutSec 时的默认超时（秒）
const defaultAIAgentTimeoutSec = 120

// MetaKeyAIDecision 裁决结果写入 metadata 的 key（PASS/REJECT/UNRESOLVED/HUMAN_PASS/HUMAN_REJECT）。
const MetaKeyAIDecision = "aiDecision"

// aiDecisionProtocol 裁决协议：decision 启用时追加到 user 消息末尾。
// 模型按协议在输出最后一行单独输出标记行，引擎用正则提取（见 ExtractDecision），
// 路由不依赖输出整体是合法 JSON。
const aiDecisionProtocol = "【流程裁决】本次调用来自审批流程。完成分析后，必须在输出的最后一行单独输出裁决标记，格式严格为 AI_DECISION: PASS 或 AI_DECISION: REJECT（PASS=同意放行，REJECT=拒绝）。标记必须独占一行，其余内容保持你原本的输出格式。"

// 未裁决策略（decision.unresolved 取值）
const (
	// UnresolvedStrategyHuman 转人工：给 AI 异常兜底负责人建待办（默认）
	UnresolvedStrategyHuman = "human"
	// UnresolvedStrategyPass 放行并在 metadata/审批记录标记 UNRESOLVED
	UnresolvedStrategyPass = "pass"
	// UnresolvedStrategyReject 按拒绝处理（走 rejectStrategy）
	UnresolvedStrategyReject = "reject"
)

// AIDecision 裁决结果
type AIDecision string

const (
	// AIDecisionPass 智能体明确放行
	AIDecisionPass AIDecision = "PASS"
	// AIDecisionReject 智能体明确拒绝
	AIDecisionReject AIDecision = "REJECT"
	// AIDecisionUnresolved 标记缺失/无法识别
	AIDecisionUnresolved AIDecision = "UNRESOLVED"
	// AIDecisionHumanPass 人工兜底待办被同意
	AIDecisionHumanPass AIDecision = "HUMAN_PASS"
	// AIDecisionHumanReject 人工兜底待办被拒绝
	AIDecisionHumanReject AIDecision = "HUMAN_REJECT"
)

// AIAgentNodeConfiguration AI 智能体节点配置
//
// 节点本身不配置 LLM/Prompt/Tools，这些由专门的智能体定义界面维护。
// 节点的职责：选定智能体 → 组装上下文 → 调用智能体规则链 → 裁决路由输出。
type AIAgentNodeConfiguration struct {
	// AgentID 目标智能体规则链 ID（必填）
	AgentID string `json:"agentId"`

	// Async 异步触发：true=只发起调用不等待结果，主流程立即向下。
	// 异步分支不注入裁决协议，decision、failureHandler 均不生效，仅记录日志。
	Async bool `json:"async"`

	// TimeoutSec 同步等待超时秒数，默认 120
	TimeoutSec int `json:"timeoutSec"`

	// InputAssembly 提示词与上下文组装配置
	InputAssembly InputAssemblyConfig `json:"inputAssembly"`

	// Decision AI 裁决配置，nil 表示仅参考模式（输出只合并进 msg，恒通过）。
	// 存在即启用：节点向智能体注入裁决协议，按输出末行的 AI_DECISION 标记路由。
	Decision *DecisionConfig `json:"decision,omitempty"`

	// FailureHandler AI 异常兜底负责人 userId 列表，两种触发共用一份名单：
	//   - 调用失败（超时/API 错误/agentId 缺失）
	//   - 裁决未明确（标记缺失/输出损坏）且 unresolved=human
	// 给这些人创建 userTask 待办；同意 → 流程继续下一节点，拒绝 → 走拒绝策略。
	// 为空：调用失败走 TellFailure；未裁决按 unresolved 策略降级（无兜底人时放行并标记）。
	FailureHandler []string `json:"failureHandler"`

	// OutputMappings 可选数据映射：把智能体输出 JSON 的字段提升为流程变量
	// （如 $.approved → aiApproved，后续节点/审批面板用 msg.aiApproved 读取）。
	// 纯数据用途，与裁决路由无关（路由只认 AI_DECISION 标记）。
	OutputMappings []OutputMapping `json:"outputMappings,omitempty"`

	// FlattenOutput 输出模式（与 httpCall 节点同语义同默认）：true=平铺（智能体输出
	// JSON 对象的顶层字段并入 msg.Data 顶层，同名覆盖表单，冲突字段应在 OutputMappings
	// 里改名规避）；false=隔离（完整输出只放 ReservedKey 下，不碰表单）。
	// 缺省（nil 或未配置）=平铺。无论开关如何，完整输出始终保留在 msg.Data[ReservedKey] 下。
	FlattenOutput *bool `json:"flattenOutput,omitempty"`

	// ReservedKey 智能体输出的隔离 key，默认 "_ai"。
	ReservedKey string `json:"reservedKey,omitempty"`
}

// DecisionConfig AI 裁决配置
type DecisionConfig struct {
	// RejectStrategy 拒绝时的处理：
	//   - "terminate"（默认）：终止流程实例
	//   - "backToInitiator"：退回发起人（跳到开始节点）
	RejectStrategy string `json:"rejectStrategy"`
	// Unresolved 未明确裁决（标记缺失/输出损坏）时的处理：
	//   - "human"（默认）：转人工兜底（需配置 failureHandler，未配置则放行并标记）
	//   - "pass"：放行并在审批记录标记「AI未裁决」
	//   - "reject"：按拒绝处理（走 rejectStrategy）
	Unresolved string `json:"unresolved"`
}

// InputAssemblyConfig 提示词与上下文组装配置
//
// 节点只会向智能体发送 user 角色的消息——智能体自身的 systemPrompt 由智能体定义维护，
// 调用方不覆盖。CustomPrompt 留空时不向消息里注入任何额外内容。
type InputAssemblyConfig struct {
	// CustomPrompt 自定义提示词（作为 user 消息的前置片段），支持 ${msg.field} / ${metadata.key} 占位符。
	// 为空则不注入。
	CustomPrompt string `json:"customPrompt"`
	// ContextSources 上下文来源开关
	ContextSources ContextSourcesConfig `json:"contextSources"`
}

// ContextSourcesConfig 上下文来源开关
type ContextSourcesConfig struct {
	// FormData 流程变量（审批表单数据）
	FormData bool `json:"formData"`
	// Attachments 附件主开关：附件清单文本 + 图片送识别 + 文档摘要（从 metadata.attachments
	// 或表单上传字段 attachments 读取）
	Attachments bool `json:"attachments"`
	// AttachmentsImages 图片送识别（nil=跟随 Attachments 主开关）。
	// 开启时图片以 image_url 内容片随消息发送：模型支持视觉则直接看图，
	// 不支持则由共享库自动降级为 [图片：路径] 文本。
	AttachmentsImages *bool `json:"attachmentsImages,omitempty"`
	// AttachmentsDocs 文档摘要送识别（nil=跟随 Attachments 主开关）。
	// 开启时文字型文档（PDF/TXT/MD）抽取文本后以章节形式随消息发送。
	AttachmentsDocs *bool `json:"attachmentsDocs,omitempty"`
	// ProcessInfo 流程元信息（processKey/instanceId/initiator/已用时长）
	ProcessInfo bool `json:"processInfo"`
	// PrevComments 前序节点审批意见（从 metadata.comments 读取）
	PrevComments bool `json:"prevComments"`
	// Initiator 发起人信息（owner/current_user）
	Initiator bool `json:"initiator"`
}

// 裁决标记解析：两级匹配，均取最后一次命中（模型先复述协议再给结论时，最后的才是真裁决）。
//   - 严格：整行只有标记（允许句末标点），协议文本被复述时因行尾有"或…"不会误命中
//   - 宽松：行首标记前缀即可，行尾允许其他内容；跳过同一行同时出现通过/拒绝两极词的
//     行（协议原文复述的特征），避免把说明文字当成裁决
//
// 中文等价值（通过/批准/放行/同意/拒绝/驳回）需带 AI_DECISION 或 裁决 前缀才认，
// 裸中文行误报率高，不纳入。
var (
	aiDecisionStrictRe = regexp.MustCompile(`(?im)^\s*AI_DECISION\s*[:：]\s*(PASS|APPROVE|APPROVED|REJECT|REJECTED|DENY|DENIED|通过|批准|放行|同意|拒绝|驳回)\s*[。.!！]?\s*$`)
	aiDecisionLooseRe  = regexp.MustCompile(`(?im)^\s*(?:AI_DECISION|裁决)\s*[:：]\s*(PASS|APPROVE|APPROVED|REJECT|REJECTED|DENY|DENIED|通过|批准|放行|同意|拒绝|驳回)`)
	aiPassTokenRe      = regexp.MustCompile(`(?i)PASS|APPROVE|通过|批准|放行|同意`)
	aiRejectTokenRe    = regexp.MustCompile(`(?i)REJECT|DENY|拒绝|驳回`)
)

// ExtractDecision 从智能体输出提取裁决结果。
// 严格整行匹配优先；其次逐行宽松匹配（跳过两极词同行的复述行），取最后一次命中；
// 都没有则 UNRESOLVED。
func ExtractDecision(output string) AIDecision {
	if ms := aiDecisionStrictRe.FindAllStringSubmatch(output, -1); len(ms) > 0 {
		return normalizeDecisionToken(ms[len(ms)-1][1])
	}
	var lastTok string
	for _, line := range strings.Split(output, "\n") {
		m := aiDecisionLooseRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		// 同一行既有通过词又有拒绝词 → 协议复述/说明文字，不是裁决
		if aiPassTokenRe.MatchString(line) && aiRejectTokenRe.MatchString(line) {
			continue
		}
		lastTok = m[1]
	}
	if lastTok != "" {
		return normalizeDecisionToken(lastTok)
	}
	return AIDecisionUnresolved
}

// normalizeDecisionToken 把标记词归一化为 PASS/REJECT；无法识别返回 UNRESOLVED。
func normalizeDecisionToken(tok string) AIDecision {
	switch strings.ToUpper(strings.TrimSpace(tok)) {
	case "PASS", "APPROVE", "APPROVED", "通过", "批准", "放行", "同意":
		return AIDecisionPass
	case "REJECT", "REJECTED", "DENY", "DENIED", "拒绝", "驳回":
		return AIDecisionReject
	}
	return AIDecisionUnresolved
}

// AIAgentNode AI 智能体节点
//
// 通过注入的 service.RuleChainExecutor 调用指定智能体规则链（agentId）——目标链由
// rulego-server 管理（agent 定义即一条规则链），执行器负责跨池桥接、租户隔离与全局兜底。
// 节点把流程上下文组装成 OpenAI 标准 MultiTurnChatRequest 作为 msg.Data 传入；
// 智能体定义里的 systemPrompt、tools、skills 完全保留。
//
// 裁决路由（decision 启用时）：节点在 user 消息末尾注入裁决协议，智能体在输出末行
// 输出 AI_DECISION 标记，引擎按标记路由——不依赖输出是合法 JSON：
//   - REJECT → 拒绝策略（terminate/backToInitiator）
//   - PASS   → 放行
//   - UNRESOLVED → 未裁决策略（human/pass/reject），绝不静默
//
// 人工兜底闭环：调用失败或未裁决转人工时创建 userTask 待办；完成待办重入本节点时
// 由守卫读取人工结论直接路由（同意→下一节点，拒绝→拒绝策略），不会再次调用 AI。
type AIAgentNode struct {
	Config         AIAgentNodeConfiguration
	CurrentNodeDef types.RuleNode
	Executor       service.RuleChainExecutor
	RuntimeService service.RuntimeServiceInternal
	TaskService    service.TaskServiceInternal
	logger         types.Logger

	// customPromptTmpl 预编译的提示词模板，支持 ${msg.field} / ${metadata.key}
	customPromptTmpl el.Template
}

// Type 返回节点类型
func (n *AIAgentNode) Type() string {
	return AIAgentNodeType
}

// New 创建新实例。
// Executor 取自包级 globalAutomationExecutor（由 register.go 经 SetAutomationExecutor
// 注入）。RuntimeService/TaskService 从原型拷贝。
func (n *AIAgentNode) New() types.Node {
	return &AIAgentNode{
		Executor:       getAutomationExecutor(),
		RuntimeService: n.RuntimeService,
		TaskService:    n.TaskService,
	}
}

// Init 初始化节点
func (n *AIAgentNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	if err := maps.Map2Struct(configuration, &n.Config); err != nil {
		return fmt.Errorf("failed to parse configuration: %w", err)
	}

	if strings.TrimSpace(n.Config.AgentID) == "" {
		return fmt.Errorf("agentId is required")
	}

	if n.Config.TimeoutSec <= 0 {
		n.Config.TimeoutSec = defaultAIAgentTimeoutSec
	}

	n.CurrentNodeDef = base.NodeUtils.GetSelfDefinition(configuration)
	n.logger = ruleConfig.Logger

	if n.Config.Decision != nil {
		// 未知取值初始化期告警，运行时按默认处理
		if s := strings.TrimSpace(n.Config.Decision.RejectStrategy); !isValidAIAgentRejectStrategy(s) {
			logrus.Warnf("aiAgent node %s has unknown rejectStrategy %q; will terminate as fallback at runtime", n.GetSelfId(), n.Config.Decision.RejectStrategy)
		}
		if u := strings.TrimSpace(n.Config.Decision.Unresolved); !isValidAIAgentUnresolved(u) {
			logrus.Warnf("aiAgent node %s has unknown unresolved %q; will use human as fallback at runtime", n.GetSelfId(), n.Config.Decision.Unresolved)
		}
		if n.Config.Async {
			logrus.Warnf("aiAgent node %s: async=true 时不注入裁决协议，decision 不生效", n.GetSelfId())
		}
	}

	// 预编译 customPrompt 模板（支持 ${msg.field} / ${metadata.key} 等 EL 表达式）。
	// customPrompt 经完整 rulego EL 引擎渲染，${include}/${fileExists}/${env.XXX} 可读文件、
	// 泄露环境变量，只能由受信任的流程设计者配置，不能来自终端用户输入。
	if cp := strings.TrimSpace(n.Config.InputAssembly.CustomPrompt); cp != "" {
		tmpl, err := el.NewTemplate(cp)
		if err != nil {
			return fmt.Errorf("failed to compile customPrompt template: %w", err)
		}
		n.customPromptTmpl = tmpl
	}

	if n.logger != nil {
		n.logger.Debugf("AIAgentNode.Init: agentId=%s, async=%v, timeoutSec=%d, decision=%v, flatten=%v",
			n.Config.AgentID, n.Config.Async, n.Config.TimeoutSec, n.Config.Decision != nil, n.flattenOutput())
	}
	return nil
}

// flattenOutput 输出合并的平铺开关：缺省（nil）=平铺，与 httpCall 节点同默认。
func (n *AIAgentNode) flattenOutput() bool {
	return n.Config.FlattenOutput == nil || *n.Config.FlattenOutput
}

// OnMsg 处理消息
func (n *AIAgentNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	defer recoverNodePanic(ctx, msg, AIAgentNodeType, n.GetSelfId())
	if n.Executor == nil {
		ctx.TellFailure(msg, fmt.Errorf("rule chain executor not configured"))
		return
	}

	logrus.WithFields(logrus.Fields{
		"nodeType":   AIAgentNodeType,
		"nodeId":     n.GetSelfId(),
		"agentId":    n.Config.AgentID,
		"instanceId": metaValue(msg, constants.KeyInstanceID),
		"taskId":     metaValue(msg, constants.KeyTaskID),
		"async":      n.Config.Async,
		"timeoutSec": n.Config.TimeoutSec,
		"decision":   n.Config.Decision != nil,
	}).Debug("AIAgentNode OnMsg enter")

	// 人工兜底重入守卫：本节点已有人工待办（或已有人工结论）时不调用 AI。
	if n.routeByHumanDecision(ctx, msg) {
		return
	}

	// 重驱/恢复重入本节点时，实例变量里已有输出则直接续跑，不再调用 LLM
	if !n.Config.Async && n.reuseCachedOutput(ctx, msg) {
		return
	}

	payload, err := n.assembleInput(ctx, msg)
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("failed to assemble input: %w", err))
		return
	}

	agentMsg := msg.Copy()
	agentMsg.DataType = types.JSON
	agentMsg.SetData(str.ToString(payload))

	if n.Config.Async {
		// fire-and-forget：executor.Execute 本身非阻塞，直接触发即返回
		if err := n.Executor.Execute(n.Config.AgentID, agentMsg); err != nil {
			ctx.TellFailure(msg, fmt.Errorf("trigger agent failed: %w", err))
			return
		}
		ctx.TellSuccess(msg)
		return
	}

	// 同步等待结果（超时由 executor 兜底，不会永久挂起）
	start := time.Now()
	output, runErr := n.Executor.ExecuteAndCollect(n.Config.AgentID, agentMsg,
		time.Duration(n.Config.TimeoutSec)*time.Second)

	var decision AIDecision
	if runErr == nil && n.decisionEnabled() {
		decision = ExtractDecision(output.GetData())
	}
	n.auditLog(ctx, msg, output, runErr, decision, time.Since(start))

	if runErr != nil {
		n.handleFailure(ctx, msg, runErr)
		return
	}

	// 成功输出落实例变量，重驱/恢复重入本节点时直接复用，不再调用 LLM
	n.persistOutputForReuse(ctx, msg, output.GetData())

	// 合并智能体输出的 metadata + data 到主流程 msg
	wrapperMsg := msg.Copy()
	if wrapperMsg.Metadata == nil {
		wrapperMsg.Metadata = types.NewMetadata()
	}
	if output.Metadata != nil {
		output.Metadata.ForEach(func(k, v string) bool {
			wrapperMsg.Metadata.PutValue(k, v)
			return true
		})
	}
	// 输出合并（固定三规则）：完整输出始终挂 reservedKey（默认 _ai，保护表单不被
	// 同名覆盖）；FlattenOutput 开启时顶层字段平铺；OutputMappings 永远最后执行。
	if err := MergeAgentOutput(&wrapperMsg, []byte(output.GetData()), n.Config.OutputMappings,
		n.reservedKey(), n.flattenOutput()); err != nil {
		// 合并失败时保留原 msg，不覆盖（避免冲掉表单）。
		logrus.WithError(err).Warn("AIAgentNode merge output failed, keep original msg (agent output dropped)")
	}

	if n.decisionEnabled() {
		wrapperMsg.Metadata.PutValue(MetaKeyAIDecision, string(decision))
		switch decision {
		case AIDecisionReject:
			n.handleReject(ctx, wrapperMsg)
			return
		case AIDecisionUnresolved:
			n.handleUnresolved(ctx, wrapperMsg, output)
			return
		}
	}
	ctx.TellSuccess(wrapperMsg)
}

// decisionEnabled 是否启用裁决路由
func (n *AIAgentNode) decisionEnabled() bool {
	return n.Config.Decision != nil
}

// assembleInput 组装 MultiTurnChatRequest。
// 节点只发送 user 角色消息：customPrompt（如非空）作前置片段，业务上下文作正文，
// 裁决启用且同步模式时在末尾追加裁决协议，拼接成单条 user 消息。
// 附件开启且解析出图片时，content 升级为 OpenAI 多模态 parts 数组
// （[文本, image_url...]），由引擎按模型视觉能力分流；无图片时为纯字符串。
// 不发送 system 消息。三者皆空时注入最小占位消息。
func (n *AIAgentNode) assembleInput(ctx types.RuleContext, msg types.RuleMsg) ([]byte, error) {
	cfg := n.Config.InputAssembly

	var contextParts []string
	// 只取扁平业务变量，不含 id/ts/metadata 信封（避免把租户/用户等内部信息写进提示词）
	vars := extractVariables(ctx, msg)

	if cfg.ContextSources.FormData {
		if formData := serializeVariables(vars); formData != "" && formData != "{}" {
			contextParts = append(contextParts, "## 表单数据\n"+formData)
		}
	}
	// 附件引用：优先读 metadata（历史入口），回退读表单上传字段 attachments
	//（值为上传组件写入的文件名/下载地址数组）
	attRefs := extractAttachmentRefs(msg, vars)
	attImages := []service.ResolvedAttachment{}
	attDocs := []service.ResolvedAttachment{}
	if cfg.ContextSources.Attachments {
		if list := attachmentListText(attRefs); list != "" {
			contextParts = append(contextParts, "## 附件\n"+list)
		}
		// 图片/文档解析：宿主注入的解析器把附件变成模型可用形态（绝对路径/文本）。
		// 解析器未注入（nil）或开关关闭时保持纯文本行为。
		if resolver := getAttachmentResolver(); resolver != nil {
			tenantID := metaValue(msg, constants.KeyTenantID)
			if n.attachmentsImagesEnabled() {
				attImages = resolver.ResolveImages(tenantID, attRefs)
			}
			if n.attachmentsDocsEnabled() {
				attDocs = resolver.ResolveDocs(tenantID, attRefs)
			}
			if len(attImages) > 0 || len(attDocs) > 0 {
				contextParts[len(contextParts)-1] = "## 附件\n" + annotatedAttachmentList(attRefs, attImages, attDocs)
			}
		}
	}
	if cfg.ContextSources.ProcessInfo {
		contextParts = append(contextParts, "## 流程信息\n"+n.buildProcessInfo(msg))
	}
	if cfg.ContextSources.PrevComments {
		if comments := n.buildPrevComments(ctx, msg); comments != "" {
			contextParts = append(contextParts, "## 前序审批意见\n"+comments)
		}
	}
	if cfg.ContextSources.Initiator {
		contextParts = append(contextParts, "## 发起人\n"+n.buildInitiatorInfo(ctx, msg))
	}

	// 文档摘要作为正文章节（在附件清单之后，与图片 part 的顺序约定：先文本后图）
	for _, d := range attDocs {
		contextParts = append(contextParts, "## 文档："+d.Name+"\n"+d.Text)
	}

	userContent := strings.Join(contextParts, "\n\n")

	customPrompt := cfg.CustomPrompt
	if customPrompt != "" && n.customPromptTmpl != nil {
		env := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
		customPrompt = n.customPromptTmpl.ExecuteAsString(env)
	}

	fullContent := userContent
	if strings.TrimSpace(customPrompt) != "" {
		if fullContent != "" {
			fullContent = customPrompt + "\n\n---\n\n" + fullContent
		} else {
			fullContent = customPrompt
		}
	}
	// 裁决协议固定追加在末尾（最后指令优先级最高，模型遵循度最好）
	if n.decisionEnabled() && !n.Config.Async && strings.TrimSpace(aiDecisionProtocol) != "" {
		if fullContent != "" {
			fullContent = fullContent + "\n\n---\n\n" + aiDecisionProtocol
		} else {
			fullContent = aiDecisionProtocol
		}
	}

	messages := make([]map[string]interface{}, 0, 1)
	content := fullContent
	if strings.TrimSpace(content) == "" {
		content = "请处理"
	}

	if len(attImages) > 0 {
		// 多模态：文本 part 在前、图片 part 在后
		parts := []map[string]interface{}{{
			"type": "text",
			"text": content,
		}}
		for _, img := range attImages {
			parts = append(parts, map[string]interface{}{
				"type":      "image_url",
				"image_url": map[string]interface{}{"url": img.Source},
			})
		}
		messages = append(messages, map[string]interface{}{
			"role":    "user",
			"content": parts,
		})
	} else {
		messages = append(messages, map[string]interface{}{
			"role":    "user",
			"content": content,
		})
	}

	return json.Marshal(map[string]interface{}{"messages": messages})
}

// attachmentsImagesEnabled 图片送识别开关：nil=跟随附件主开关
func (n *AIAgentNode) attachmentsImagesEnabled() bool {
	cs := n.Config.InputAssembly.ContextSources
	return cs.Attachments && (cs.AttachmentsImages == nil || *cs.AttachmentsImages)
}

// attachmentsDocsEnabled 文档摘要送识别开关：nil=跟随附件主开关
func (n *AIAgentNode) attachmentsDocsEnabled() bool {
	cs := n.Config.InputAssembly.ContextSources
	return cs.Attachments && (cs.AttachmentsDocs == nil || *cs.AttachmentsDocs)
}

// extractAttachmentRefs 提取附件引用清单：优先 metadata.attachments（历史入口，
// JSON 字符串），回退表单上传字段 vars["attachments"]（[]map{name,url[,path]}）。
func extractAttachmentRefs(msg types.RuleMsg, vars map[string]interface{}) []service.AttachmentRef {
	var refs []service.AttachmentRef
	appendRaw := func(raw interface{}) {
		items, ok := raw.([]interface{})
		if !ok {
			return
		}
		for _, it := range items {
			m, ok := it.(map[string]interface{})
			if !ok {
				continue
			}
			refs = append(refs, service.AttachmentRef{
				Name: str.ToString(m["name"]),
				URL:  str.ToString(m["url"]),
				Path: str.ToString(m["path"]),
			})
		}
	}
	if att := metaValue(msg, "attachments"); att != "" {
		var parsed interface{}
		if err := json.Unmarshal([]byte(att), &parsed); err == nil {
			appendRaw(parsed)
		}
	}
	if len(refs) == 0 && vars != nil {
		appendRaw(vars["attachments"])
	}
	return refs
}

// attachmentListText 附件纯文本清单（每行一个附件）
func attachmentListText(refs []service.AttachmentRef) string {
	if len(refs) == 0 {
		return ""
	}
	lines := make([]string, 0, len(refs))
	for _, r := range refs {
		lines = append(lines, "- "+r.Name+" "+r.URL)
	}
	return strings.Join(lines, "\n")
}

// annotatedAttachmentList 带识别状态标注的附件清单：让模型把图片 part / 文档章节
// 与清单里的文件名对应起来（"（已附图）"/"（已附文本摘要）"）。
func annotatedAttachmentList(refs []service.AttachmentRef, images, docs []service.ResolvedAttachment) string {
	imageNames := map[string]bool{}
	for _, img := range images {
		imageNames[img.Name] = true
	}
	docNames := map[string]bool{}
	for _, d := range docs {
		docNames[d.Name] = true
	}
	lines := make([]string, 0, len(refs))
	for _, r := range refs {
		note := ""
		switch {
		case imageNames[r.Name]:
			note = "（已附图）"
		case docNames[r.Name]:
			note = "（已附文本摘要）"
		}
		lines = append(lines, "- "+r.Name+note)
	}
	return strings.Join(lines, "\n")
}

func (n *AIAgentNode) buildProcessInfo(msg types.RuleMsg) string {
	info := map[string]interface{}{
		"processKey":  metaValue(msg, constants.KeyProcessKey),
		"instanceId":  metaValue(msg, constants.KeyInstanceID),
		"businessKey": metaValue(msg, constants.KeyBusinessKey),
		"tenantId":    metaValue(msg, constants.KeyTenantID),
	}
	data, _ := json.Marshal(info)
	return string(data)
}

func (n *AIAgentNode) buildInitiatorInfo(ctx types.RuleContext, msg types.RuleMsg) string {
	// owner（发起人）由引擎信封写入 metadata；current_user 只存在于 Go ctx
	// （SetUserToCtx 写入的 *Actor，如刚完成前序审批的人），metadata 键为预留回退。
	info := map[string]interface{}{
		"owner": metaValue(msg, constants.KeyOwner),
	}
	if v := ctx.GetContext().Value(constants.KeyCurrentUser); v != nil {
		if actor, ok := v.(*service.Actor); ok && actor != nil {
			info["currentUser"] = actor.UserName
		}
	} else {
		info["currentUser"] = metaValue(msg, string(constants.KeyCurrentUser))
	}
	data, _ := json.Marshal(info)
	return string(data)
}

// buildPrevComments 前序审批意见：按完成时间正序聚合本实例已完成 userTask 的意见
// （含 AI 兜底待办上的人工意见）。审批完成时意见同事务落 wf_task_comment 表，
// task.Comment 为其快照字段，这里直接读快照避免逐任务查库。
// metadata.comments 为历史预留入口，非空时优先。
func (n *AIAgentNode) buildPrevComments(ctx types.RuleContext, msg types.RuleMsg) string {
	if c := metaValue(msg, "comments"); c != "" {
		return c
	}
	if n.TaskService == nil {
		return ""
	}
	instanceID := metaValue(msg, constants.KeyInstanceID)
	if instanceID == "" {
		return ""
	}
	iid := instanceID
	tasks, _, err := n.TaskService.GetTaskList(ctx.GetContext(), service.ActorFromCtx(ctx.GetContext()), &dto.TaskQuery{
		InstanceID:     &iid,
		ParentIDIsNull: true,
		PageRequest:    dto.PageRequest{PageSize: 200},
	})
	if err != nil {
		logrus.WithError(err).Warnf("AIAgentNode %s: query prev comments failed", n.GetSelfId())
		return ""
	}
	type commentEntry struct {
		Node    string `json:"node"`
		Comment string `json:"comment"`
	}
	var entries []commentEntry
	for _, t := range tasks {
		if t == nil || t.TaskType != UserTaskNodeType || t.Status != string(enums.TaskStatusCompleted) {
			continue
		}
		if t.Comment != nil && strings.TrimSpace(*t.Comment) != "" {
			entries = append(entries, commentEntry{Node: t.Name, Comment: strings.TrimSpace(*t.Comment)})
		}
	}
	if len(entries) == 0 {
		return ""
	}
	b, _ := json.Marshal(entries)
	return string(b)
}

// reservedKey 解析输出隔离用的保留 key（默认 _ai）。
func (n *AIAgentNode) reservedKey() string {
	if k := strings.TrimSpace(n.Config.ReservedKey); k != "" {
		return k
	}
	return AIAgentReservedKey
}

// routeByHumanDecision 人工兜底重入守卫。
// 本节点存在 userTask 型人工任务（调用失败/未裁决兜底待办）时，重入不再调用 AI：
//   - 仍有未完成待办 → DoOnEnd 继续等待
//   - 人工已同意（approved）→ 直接 TellSuccess 走下一节点
//   - 人工已拒绝（rejected）→ handleReject（terminate/backToInitiator）
//
// 过滤 TaskType=userTask：TaskCreator 切面给节点自动创建/完成的 aiAgent 型任务不参与判定。
// 返回 true 表示本轮 OnMsg 已终结，调用方直接 return。
func (n *AIAgentNode) routeByHumanDecision(ctx types.RuleContext, msg types.RuleMsg) bool {
	if n.TaskService == nil {
		return false
	}
	instanceID := metaValue(msg, constants.KeyInstanceID)
	if instanceID == "" {
		return false
	}
	iid := instanceID
	tasks, _, err := n.TaskService.GetTaskList(ctx.GetContext(), service.ActorFromCtx(ctx.GetContext()), &dto.TaskQuery{
		InstanceID:     &iid,
		TaskDefKey:     n.GetSelfId(),
		ParentIDIsNull: true,
	})
	if err != nil {
		logrus.WithError(err).Warnf("AIAgentNode %s: query fallback tasks failed, proceed to AI call", n.GetSelfId())
		return false
	}

	approved, rejected, active := 0, 0, 0
	for _, t := range tasks {
		if t == nil || t.TaskType != UserTaskNodeType {
			continue
		}
		switch t.Status {
		case string(enums.TaskStatusCompleted):
			if t.EndReason != nil {
				switch *t.EndReason {
				case string(enums.ApprovalResultApproved):
					approved++
				case string(enums.ApprovalResultRejected):
					rejected++
				}
			}
		case string(enums.TaskStatusTerminated), string(enums.TaskStatusWithdrawn):
			// 终止/撤回的兜底任务不构成结论
		default:
			active++
		}
	}
	if approved+rejected+active == 0 {
		return false
	}

	if msg.Metadata == nil {
		msg.Metadata = types.NewMetadata()
	}
	if active > 0 {
		logrus.Infof("AIAgentNode %s: %d human fallback task(s) still active, wait", n.GetSelfId(), active)
		ctx.DoOnEnd(msg, nil, "")
		return true
	}
	if rejected > 0 {
		msg.Metadata.PutValue(MetaKeyAIDecision, string(AIDecisionHumanReject))
		logrus.Infof("AIAgentNode %s: human rejected on fallback task → reject strategy, instance=%s", n.GetSelfId(), instanceID)
		n.handleReject(ctx, msg)
		return true
	}
	// approved>0，或全终态但无审批结论（异常数据，按同意处理并告警）
	if approved == 0 {
		logrus.Warnf("AIAgentNode %s: fallback tasks all terminal without approval result, treat as pass", n.GetSelfId())
	}
	msg.Metadata.PutValue(MetaKeyAIDecision, string(AIDecisionHumanPass))
	logrus.Infof("AIAgentNode %s: human approved on fallback task → continue to next node, instance=%s", n.GetSelfId(), instanceID)
	ctx.TellSuccess(msg)
	return true
}

// aiOutputVarKey 节点结果缓存的实例变量键（按节点隔离）。
func (n *AIAgentNode) aiOutputVarKey() string {
	return "ai_out_" + n.GetSelfId()
}

// actorFromCtxOrMeta 取节点回调用身份：ctx 无身份（内部驱动）时按系统动作处理，
// 并从链元数据补齐租户（实例变量的租户校验依赖 TenantID）。
func actorFromCtxOrMeta(ctx types.RuleContext, msg types.RuleMsg) service.Actor {
	actor := service.ActorFromCtx(ctx.GetContext())
	if actor.TenantID == "" {
		actor.TenantID = metaValue(msg, constants.KeyTenantID)
	}
	return actor
}

// persistOutputForReuse 把同步调用的成功输出写入实例变量（引擎实例锁事务内合并）。
// 空输出不落：无法与未执行过区分。写失败仅告警，不阻断本轮流转。
func (n *AIAgentNode) persistOutputForReuse(ctx types.RuleContext, msg types.RuleMsg, output string) {
	if n.RuntimeService == nil {
		return
	}
	if strings.TrimSpace(output) == "" {
		return
	}
	instanceID := metaValue(msg, constants.KeyInstanceID)
	if instanceID == "" {
		return
	}
	if err := n.RuntimeService.SetProcessInstanceVariable(
		ctx.GetContext(), actorFromCtxOrMeta(ctx, msg), instanceID, n.aiOutputVarKey(), output); err != nil {
		logrus.WithError(err).Warnf("AIAgentNode %s: persist output for reuse failed", n.GetSelfId())
	}
}

// reuseCachedOutput 实例变量里已有本节点的成功输出时跳过 LLM 调用，把缓存
// 输出与消息合并后按首次成功路径继续流转（启用裁决时同样重新提取裁决路由）。
// 返回 true 表示本轮 OnMsg 已终结。
func (n *AIAgentNode) reuseCachedOutput(ctx types.RuleContext, msg types.RuleMsg) bool {
	if n.RuntimeService == nil {
		return false
	}
	instanceID := metaValue(msg, constants.KeyInstanceID)
	if instanceID == "" {
		return false
	}
	v, err := n.RuntimeService.GetProcessInstanceVariable(
		ctx.GetContext(), actorFromCtxOrMeta(ctx, msg), instanceID, n.aiOutputVarKey())
	if err != nil {
		logrus.WithError(err).Warnf("AIAgentNode %s: read cached output failed, proceed to AI call", n.GetSelfId())
		return false
	}
	cached, ok := v.(string)
	if !ok || strings.TrimSpace(cached) == "" {
		return false
	}
	logrus.Infof("AIAgentNode %s: reuse cached output, skip LLM call, instance=%s", n.GetSelfId(), instanceID)

	wrapperMsg := msg.Copy()
	if wrapperMsg.Metadata == nil {
		wrapperMsg.Metadata = types.NewMetadata()
	}
	if err := MergeAgentOutput(&wrapperMsg, []byte(cached), n.Config.OutputMappings,
		n.reservedKey(), n.flattenOutput()); err != nil {
		// 合并失败时保留原 msg（避免冲掉表单），缓存仍视为已消费，继续流转
		logrus.WithError(err).Warn("AIAgentNode reuse cached output merge failed, keep original msg")
	}
	if n.decisionEnabled() {
		decision := ExtractDecision(cached)
		wrapperMsg.Metadata.PutValue(MetaKeyAIDecision, string(decision))
		switch decision {
		case AIDecisionReject:
			n.handleReject(ctx, wrapperMsg)
			return true
		case AIDecisionUnresolved:
			n.handleUnresolved(ctx, wrapperMsg, types.NewMsg(0, "wf_ai_reuse", types.JSON, nil, cached))
			return true
		}
	}
	ctx.TellSuccess(wrapperMsg)
	return true
}

// dataVarsFromMsg 把 msg.Data(JSON)解析成变量 map；解析失败返回 nil（调用方回退到实例变量）。
func dataVarsFromMsg(msg types.RuleMsg) map[string]interface{} {
	raw := msg.GetData()
	if len(strings.TrimSpace(raw)) == 0 {
		return nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil
	}
	return m
}

// handleReject 处理拒绝（AI 明确拒绝、人工兜底拒绝、未裁决策略=reject 共用）。
//   - "backToInitiator"：ExecuteNext 跳到开始节点（按 FirstNodeIndex 解析真实开始节点 ID）
//   - "terminate"（默认/空值/未知值）：终止流程实例
func (n *AIAgentNode) handleReject(ctx types.RuleContext, msg types.RuleMsg) {
	instanceID := metaValue(msg, constants.KeyInstanceID)
	strategy := ""
	if n.Config.Decision != nil {
		strategy = strings.TrimSpace(n.Config.Decision.RejectStrategy)
	}

	if n.RuntimeService == nil {
		logrus.Warnf("AIAgentNode %s: RuntimeService unavailable, falling back to TellFailure for reject", n.GetSelfId())
		ctx.TellFailure(msg, fmt.Errorf("reject: runtime service unavailable"))
		return
	}

	switch strategy {
	case RejectStrategyBackToInitiator:
		logrus.Infof("AIAgentNode %s: reject → backToInitiator, instance=%s", n.GetSelfId(), instanceID)
		startID := getStartNodeID(ctx)
		if startID == "" {
			logrus.Errorf("AIAgentNode %s: cannot resolve start node id for instance %s, falling back to terminate", n.GetSelfId(), instanceID)
			terminateInstance(n.RuntimeService, n.GetSelfId(), ctx, msg, instanceID, "AI拒绝：开始节点缺失，降级终止")
			return
		}
		// 跳转前清理目标节点上一轮任务，避免重入时旧记录被判定为已完成
		if n.TaskService != nil {
			if _, err := n.TaskService.SupersedeNodeTasks(ctx.GetContext(), instanceID, startID, "AI拒绝退回，清理上一轮任务"); err != nil {
				logrus.WithError(err).Warnf("AIAgentNode %s: supersede target node tasks before backToInitiator failed", n.GetSelfId())
			}
		}
		// 带上当前 msg.Data（含 _ai 拒绝理由 + 表单数据）传入，退回后发起人能看到被退原因与原始数据。
		if err := n.RuntimeService.ExecuteNext(ctx.GetContext(), instanceID, startID, dataVarsFromMsg(msg)); err != nil {
			logrus.Errorf("AIAgentNode %s: backToInitiator failed: %v, falling back to terminate", n.GetSelfId(), err)
			terminateInstance(n.RuntimeService, n.GetSelfId(), ctx, msg, instanceID, "AI拒绝：退回发起人失败，降级终止")
			return
		}
		ctx.DoOnEnd(msg, nil, "")
	default:
		logrus.Infof("AIAgentNode %s: reject → terminate, instance=%s", n.GetSelfId(), instanceID)
		terminateInstance(n.RuntimeService, n.GetSelfId(), ctx, msg, instanceID, "AI拒绝：终止流程")
	}
}

// handleUnresolved 处理未裁决（智能体未按协议输出明确标记）。
//   - human（默认）：给 AI 异常兜底负责人建待办；未配置兜底人则放行并标记（不静默，不阻塞）
//   - pass：放行，metadata 已标记 UNRESOLVED（审批详情可见）
//   - reject：按拒绝策略处理
func (n *AIAgentNode) handleUnresolved(ctx types.RuleContext, msg types.RuleMsg, output types.RuleMsg) {
	strategy := ""
	if n.Config.Decision != nil {
		strategy = strings.TrimSpace(n.Config.Decision.Unresolved)
	}
	if !isValidAIAgentUnresolved(strategy) {
		strategy = UnresolvedStrategyHuman
	}

	switch strategy {
	case UnresolvedStrategyReject:
		logrus.Warnf("AIAgentNode %s: unresolved → reject by strategy, instance=%s", n.GetSelfId(), metaValue(msg, constants.KeyInstanceID))
		n.handleReject(ctx, msg)
	case UnresolvedStrategyPass:
		logrus.Warnf("AIAgentNode %s: unresolved → pass with marker (aiDecision=UNRESOLVED)", n.GetSelfId())
		ctx.TellSuccess(msg)
	default:
		if len(n.Config.FailureHandler) == 0 || n.TaskService == nil {
			logrus.Warnf("AIAgentNode %s: unresolved → human, but no failure handler configured; pass with marker (aiDecision=UNRESOLVED)", n.GetSelfId())
			ctx.TellSuccess(msg)
			return
		}
		raw := strings.TrimSpace(output.GetData())
		if len(raw) > 800 {
			raw = raw[:800] + "...(截断)"
		}
		desc := fmt.Sprintf("智能体未按判定协议输出明确标记（AI_DECISION: PASS/REJECT）。AI 原始输出：\n%s\n\n请人工判定本节点：同意则流程继续；拒绝将按拒绝策略处理。", raw)
		n.createHumanFallbackTasks(ctx, msg, "AI智能体未判定", desc)
		ctx.DoOnEnd(msg, nil, "")
	}
}

// handleFailure 处理调用失败（超时/API 错误/agentId 缺失等）。
//   - failureHandler 非空且 TaskService 可用：给这些人创建兜底待办（同意→下一节点，拒绝→拒绝策略）
//   - 否则走 TellFailure，让失败在链路上可见
func (n *AIAgentNode) handleFailure(ctx types.RuleContext, msg types.RuleMsg, err error) {
	if len(n.Config.FailureHandler) == 0 {
		ctx.TellFailure(msg, err)
		return
	}
	if n.TaskService == nil {
		ctx.TellFailure(msg, fmt.Errorf("failure handler configured but task service unavailable: %w", err))
		return
	}

	logrus.WithFields(logrus.Fields{
		"nodeType":       AIAgentNodeType,
		"nodeId":         n.GetSelfId(),
		"agentId":        n.Config.AgentID,
		"instanceId":     metaValue(msg, constants.KeyInstanceID),
		"taskId":         metaValue(msg, constants.KeyTaskID),
		"failureHandler": n.Config.FailureHandler,
		"error":          err.Error(),
	}).Warn("AIAgentNode failure → creating handler tasks")

	desc := fmt.Sprintf("智能体调用失败: %v\n\n请人工处理本节点：同意则流程继续；拒绝将按拒绝策略处理。", err)
	n.createHumanFallbackTasks(ctx, msg, "AI智能体调用失败", desc)
	ctx.DoOnEnd(msg, nil, "")
}

// createHumanFallbackTasks 给 AI 异常兜底负责人创建人工待办（每人一条）。
// 任务 TaskDefKey=本节点 ID，Variables 携带当时的流程变量快照（含 _ai 原始输出），
// 供待办详情面板高亮展示 AI 输出；完成待办后引擎重入本节点，由 routeByHumanDecision
// 读取人工结论直接路由（不会再次调用 AI）。
func (n *AIAgentNode) createHumanFallbackTasks(ctx types.RuleContext, msg types.RuleMsg, title, desc string) {
	instanceID := metaValue(msg, constants.KeyInstanceID)
	processID := metaValue(msg, constants.KeyProcessID)
	tenantID := metaValue(msg, constants.KeyTenantID)

	now := time.Now()
	instanceIDPtr := instanceID
	vars := msg.GetData()
	// 待办标题带节点显示名（如「AI合规初审」）而非 nodeKey，处理人一眼知道是哪个环节；
	// 显示名缺失时回退 nodeKey。
	nodeLabel := strings.TrimSpace(n.CurrentNodeDef.Name)
	if nodeLabel == "" {
		nodeLabel = n.GetSelfId()
	}
	for _, userID := range n.Config.FailureHandler {
		userID = strings.TrimSpace(userID)
		if userID == "" {
			continue
		}
		assignee := userID
		d := desc
		v := vars
		task := &model.WfTask{
			ProcessInstanceID: &instanceIDPtr,
			ProcessID:         processID,
			TaskType:          UserTaskNodeType,
			TaskDefKey:        n.GetSelfId(),
			Name:              fmt.Sprintf("%s - %s", title, nodeLabel),
			Description:       &d,
			Assignee:          &assignee,
			Status:            string(enums.TaskStatusActive),
			Variables:         &v,
			TenantID:          tenantID,
			CreatedBy:         constants.UserSystem,
			CreatedAt:         now,
		}
		if _, createErr := n.TaskService.CreateTask(ctx.GetContext(), service.SystemActor(), task); createErr != nil {
			logrus.Errorf("AIAgentNode %s: failed to create fallback task for user %s: %v",
				n.GetSelfId(), userID, createErr)
		}
	}
}

// auditLog 审计日志
func (n *AIAgentNode) auditLog(ctx types.RuleContext, msg types.RuleMsg, output types.RuleMsg, err error, decision AIDecision, duration time.Duration) {
	status := AuditStatusSuccess
	errMsg := ""
	if err != nil {
		status = AuditStatusFailed
		errMsg = err.Error()
	}
	// output 在失败路径下可能是零值（Metadata 为 nil），需防御
	metaGet := func(m types.RuleMsg, key string) string {
		if m.Metadata == nil {
			return ""
		}
		return m.GetMetadata().GetValue(key)
	}
	fields := logrus.Fields{
		"nodeType":     AIAgentNodeType,
		"nodeId":       n.CurrentNodeDef.Id,
		"agentId":      n.Config.AgentID,
		"instanceId":   metaGet(msg, constants.KeyInstanceID),
		"taskId":       metaGet(msg, constants.KeyTaskID),
		"processKey":   metaGet(msg, constants.KeyProcessKey),
		"tenantId":     metaGet(msg, constants.KeyTenantID),
		"status":       status,
		"async":        n.Config.Async,
		"decision":     string(decision),
		"duration_ms":  duration.Milliseconds(),
		"tokens_in":    metaGet(msg, "tokens_in"),
		"tokens_out":   metaGet(output, "tokens_out"),
		"total_tokens": metaGet(output, "total_tokens"),
	}
	if errMsg != "" {
		fields["error"] = errMsg
	}
	logrus.WithFields(fields).Info("AIAgentNode execution")
}

// Destroy 销毁节点
func (n *AIAgentNode) Destroy() {}

// GetSelfId 获取当前节点 ID
func (n *AIAgentNode) GetSelfId() string {
	return selfID(n.CurrentNodeDef, AIAgentNodeType)
}
