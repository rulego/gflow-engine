# GFlow Engine 流程节点（组件）文档

`components` 包在 RuleGo 节点体系上扩展出一组 BPM 风格流程节点。流程定义即 RuleGo 规则链：`nodes` 描述步骤，`connections` 描述流转，审批语义由各节点 `configuration` 驱动。

## 节点总览

| 节点类型 | 职责 | 阻塞性 |
|---|---|---|
| `startTask` | 流程起点标记，不做鉴权（归发起层） | 非阻塞 |
| `userTask` | 人工审批：单人/或签/顺序/会签/投票 | 阻塞，等审批完成 |
| `ccTask` | 抄送知会，创建即完成 | 非阻塞 |
| `serviceTask` | 调用 Go 函数（`action.Functions` 注册） | 同步 |
| `automation` | fire-and-forget 触发自动化规则链 | 非阻塞 |
| `subProcess` | Call Activity：启动子流程实例，父流程挂起等回调 | 阻塞 |
| `aiAgent` | 调用智能体规则链并按输出路由 | 同步/异步 |
| `httpCall` | 同步 HTTP 调用，响应合并进流程变量 | 同步 |
| `startProcess` | **规则链专用**：链内发起 BPM 流程实例（定时自动发起审批） | 同步 |

RuleGo 原生节点（`switch` 条件分支、`fork`/`inclusive`/`join` 并行汇聚等）可直接混用，见 [RuleGo 标准组件](https://rulego.cc/pages/standard-components/)。

## 注册方式与共享约定

### 注册

推荐入口（引擎启动后一行完成装配，依赖自取）：

```go
err := components.RegisterFromEngine(engine,
    components.WithServiceFuncs(serviceFuncs),          // 可选：宿主服务任务函数，见下文
    components.WithAutomationExecutor(executor),        // 可选：默认取引擎的 GetRuleChainExecutor
)
```

前置：`engine.Start(ctx)` 已成功返回——内部服务在 Start 时才装配完成；未启动引擎会在注册前报错拦截，避免 nil 依赖占坑全局注册表。注册是幂等的：重复注册会跳过并打 Debug 日志（rulego registry 首注册占坑），后续注册依赖与首次不一致时会打 Warn（新依赖不会生效）。TaskService/IdentityService/RuntimeService 从引擎自取，必填校验在 `Register` 内做；`components.Register(components.ComponentDeps{...})` 仍作为底层原语保留，供需要手工装配的高级场景使用。**注意**：监听器经 Builder 注入引擎后由 `RegisterFromEngine` 自动带到节点侧，两侧天然同一实例（若用底层 `Register` 手工装配则需自行传同一实例，详见 [events.md](events.md)）。

### 装配顺序与分层（为什么注册不在 Build 里）

引擎分两层：`service`（引擎核心：Builder/Build/Start、任务与运行时服务，**不认识任何 BPM 节点**）与 `components`（BPM 节点插件层，依赖 core）。`RegisterFromEngine` 属于插件层，标准启动顺序固定为：

```go
engine, _ := service.NewWorkflowEngineBuilder()./*...*/.Build()
_ = engine.Start(ctx) // 内部服务此时才初始化完成
_ = components.RegisterFromEngine(engine) // 依赖由引擎自取
```

`RegisterFromEngine` 不能折叠进 `Build`：Builder 在 `service` 包，注册组件需要反向依赖 `components` 包，构成循环依赖（Go 编译器强制）；且节点原型绑定的是启动后才存在的具体服务实例。只想要核心引擎、不要 BPM 节点的集成方，跑到 `Start` 为止即可。

### 生命周期

每个节点实现 RuleGo `types.Node` 三段式：

- `New()`：从注册的原型拷贝，保留注入的 TaskService/IdentityService 等依赖；
- `Init()`：`maps.Map2Struct` 解析配置、预编译 EL 模板、校验必填项，失败则节点初始化报错；
- `OnMsg()`：执行业务，以 `TellSuccess` / `TellFailure` / `DoOnEnd`（空 relation 直达结束）收尾。

### 元数据 key

| Key | 含义 |
|---|---|
| `constants.KeyInstanceID` | 流程实例 ID |
| `constants.KeyProcessID` | 流程定义 ID |
| `constants.KeyTenantID` | 租户 ID |
| `constants.KeyOwner` | 发起人 |

### 出边语义

- 业务**驳回**走自定义关系 `RelationReject`（`"Reject"`），与系统错误 `types.Failure` 分离，便于 DSL 分别连线；
- 系统级错误（配置非法、HTTP 失败等）走 `Failure`。

---

## startTask

流程起点。`OnMsg` 直接 `TellSuccess`，不设置 `roleIds` 鉴权（发起权限校验归宿主应用）。

## userTask

核心审批节点。每次到达时按审批模式创建任务，并在每次 `OnMsg`（任务完成、认领等）时检查完成度、推进顺序审批、触发驳回策略。

### 审批模式（approvalType）

| 模式 | 语义 |
|---|---|
| `single` | 单人审批，完成即过 |
| `or` | 任一人通过即过 |
| `sequential` | 顺序审批：按解析出的审批人名单依次逐个审批（任一步拒绝即触发驳回策略） |
| `countersign` | 会签：按 `approvalRule` 比例/人数通过 |
| `vote` | 投票：达阈值通过或驳回 |

### 候选解析（candidateType）

共 7 种取值（`types/enums.CandidateType`）：

- `user`：`candidateConfig.userIds` 直达（设为候选执行人）；
- `role`：`candidateConfig.roleIds` 指定角色，候选落库为"待认领"任务（候选组写入 `wf_task_assignee`），认领后展开为执行人；
- `dept`：`candidateConfig.departmentIds` 指定部门，同 `role` 产生待认领任务；
- `direct_manager`：经 IdentityService 解析发起人的直接上级（`levels`>1 时取第 N 级主管）；
- `multi_level_manager`：沿上级链展开多级审批人（`levels`>0 固定审批到第 N 级，`levels`<0 直到最上层）；
- `initiator_select`：`candidateConfig.selected` 表达式模板按流程变量求值得到审批人 ID 列表；
- `initiator_self`：发起人本人。

解析依赖 IdentityService 的候选类型（`role` / `dept` / `direct_manager` / `multi_level_manager`）
在 IdentityService 未注入或解析失败时**返回错误，节点走 `Failure`**，不会静默丢失审批人。

### 配置字段

| 字段 | 说明 |
|---|---|
| `taskName` / `taskDescription` | 任务标题/描述，覆盖节点名 |
| `formKey` | 关联表单，落库 `wf_task.form_key` |
| `candidateType` | user / role / dept / direct_manager / multi_level_manager / initiator_select / initiator_self |
| `candidateConfig` | `{userIds, roleIds, departmentIds, levels, selected}`，按 `candidateType` 取用 |
| `approvalType` | 见上表 |
| `approvalRule` | 会签/投票阈值规则（`dto.CountersignRule`：`{type, value, isSequential}`，字段语义见 [dsl-reference.md](dsl-reference.md) 2.1.2） |
| `selfApprovalType` | 发起人自审批策略 |
| `dueDate` | 期限（节点级静态配置） |
| `timeoutPolicy` | `{dueInMinutes, action}`：到期时间相对**每个任务创建时刻**（配置后优先于静态 `dueDate`）；`action`（remind/autoApprove/autoReject）由宿主逾期巡检（`TaskService.ScanOverdueTasks`）执行，引擎自身不执行动作 |
| `rejectStrategy` | `terminate`(默认) / `rejectToStarter` / `rejectToPrev` / `rejectToNode` |
| `rejectTargetNode` | `rejectToNode` 目标 |
| additionalInfo | `description`、`rejectStrategy`、`rejectTargetNode`、`formPermissions`、`actionPermissions` |

### 驳回策略

`terminate` 终止实例（默认）；`rejectToStarter` 回开始节点；`rejectToPrev` 回上一个 userTask；`rejectToNode` 回 `rejectTargetNode`。`aiAgent` 共享 `terminate` 语义，另支持 `backToInitiator`。

## ccTask

抄送知会。按 `ccUserIds` 创建即完成的任务（endReason=`cc`），经 `CCTaskCreatedListener` 通知宿主；`selfSelect` 支持发起时自选抄送人。不阻塞流程。

## serviceTask

嵌入 rulego `action.FunctionsNode`，仅覆盖 `Type()`/`New()`。函数元数据由 `service_registry.go` 的 `Services` 注册表管理，支持按租户过滤（`VisibilityProvider`）。

### 服务任务函数注册（集成方指南）

服务任务的函数实现属于**宿主应用**（查订单、算评分等业务逻辑），引擎只提供注册表与运行时调度。两种注册路径等价，任选其一：

**路径 A（推荐）：随组件引导一次性注册**——`WithServiceFuncs`：

```go
err := components.RegisterFromEngine(engine, components.WithServiceFuncs([]components.ServiceFunc{
    {
        Def: components.ServiceFuncDef{
            Name:     "crm:checkCredit",       // 全局唯一 = 节点 functionName
            Label:    "查征信",
            Desc:     "按用户ID查询征信评分",
            Category: "风控",                   // 设计器分组名
            Fields: []components.ServiceFuncField{ // 参数 schema → 设计器动态表单
                {Name: "userId", Label: "用户ID", Type: "string", Required: true},
                {Name: "level", Label: "等级", Type: "string",
                    Component: map[string]any{"type": "select", "options": []map[string]any{
                        {"value": "A", "label": "A 级"},
                    }}},
            },
            // Visible: []string{"tenant-1"}, // 可选：租户白名单，空=全部租户可见
        },
        Fn: func(ctx types.RuleContext, msg types.RuleMsg) {
            var args struct {
                UserID string `json:"userId"`
            }
            if err := json.Unmarshal([]byte(msg.GetData()), &args); err != nil {
                ctx.TellFailure(msg, err) // 失败 → 流程走 Failure 出边
                return
            }
            // ...业务逻辑，结果以 JSON 写回 msg.Data 供下游 msg.xxx 读取...
            ctx.TellSuccess(msg) // 必须终结路由
        },
    },
}))
```

**路径 B：逐个注册**——`components.Services.Register(def, fn)`，时机在流程执行前即可（运行时才查找函数，设计器目录实时读 catalog）。

**参数如何到达函数**：设计器保存的参数 JSON 序列化进 DSL 的 `param` 模板 → 运行时先做 `${msg.字段}`/`${metadata.key}` 变量渲染 → 渲染后的整个字符串经 `msg.GetData()` 进入函数，函数自行 `json.Unmarshal`。即参数值里的 `${msg.amount}` 在流程运行时被表单值替换。

**纪律**：

- 必须经 `Services.Register`（或 `ServiceFuncs`），**勿直调** rulego 的 `action.Functions.Register`——绕过入口会导致设计器目录看不到元数据；
- 同名重复注册会覆盖并打 Warn（支持幂等重注册，但撞名需留意）；
- 函数内必须 `TellSuccess` 或 `TellFailure` 终结路由；失败走 `Failure` 出边，链上无 Failure 出边时实例终止；
- `Fn: nil` 的条目被跳过（元数据先行、实现后补场景）；
- 测试可用 `Services.Unregister(name)` 清理（同步清 catalog 与运行时实现）。

**租户可见性**（二选一）：`ServiceFuncDef.Visible` 白名单（空 = 全租户可见）；或 `Services.SetVisibilityProvider(p)` 注入自有权限系统实现（注入后忽略 Visible 字段）。

## automation

通过注入的 `RuleChainExecutor` 跨规则链池触发目标链（`targetId`），fire-and-forget：触发成功即 `TellSuccess`，无执行结果回流主流程，失败不重试。**触发失败走 `TellFailure`**——链上没有 Failure 出边时整个流程实例失败终止，把自动化当"顺带发通知"用的流程需确保目标链稳定可用。执行日志含 nodeType/nodeId/instanceId/durationMs 便于排障。

## subProcess

Call Activity。按 `targetId` 启动独立的子流程实例，父流程实例挂起；子流程结束后经回调恢复父流程继续推进。

## aiAgent

组装上下文（表单/附件/流程信息/前序审批意见/发起人）调用智能体规则链（`agentId`），按输出末尾的裁决标记 `AI_DECISION: PASS|REJECT` 路由——判定协议由引擎注入到消息末尾并从输出提取，不依赖输出是 JSON。无法得出明确判定或调用失败时按未决策略兜底（默认转人工）。完整输出始终写入 `msg._ai`。

### 配置字段

| 字段 | 说明 |
|---|---|
| `agentId` | 目标智能体规则链 |
| `async` | 异步触发（输出、裁决与失败均不反馈主流程） |
| `timeoutSec` | 调用超时 |
| `inputAssembly` | `{customPrompt, contextSources{formData,attachments,processInfo,prevComments,initiator}}`，按勾选拼装 user 消息 |
| `decision.rejectStrategy` | 判定为拒绝时的处理：`terminate` / `backToInitiator` |
| `decision.unresolved` | 未裁决兜底：`human`（默认，给兜底负责人建人工待办）/ `pass`（放行并标记）/ `reject`（按拒绝策略处理） |
| `failureHandler` | 调用失败 + 未裁决转人工共用的兜底负责人（用户 ID 列表） |
| `flattenOutput` | 输出模式（`*bool`，**缺省=平铺(true)**）：`true`=平铺，顶层字段并入流程变量（同名覆盖表单）；`false`=隔离，完整输出只在 `_ai`。**与 `httpCall` 节点语义与默认值已统一**（历史版本本节点为值类型、缺省 false=隔离，升级时注意行为变化） |
| `outputMappings` | `[{from,to}]`，永远最后执行（优先级最高），两种输出模式下都生效 |

人工兜底闭环：兜底待办完成（同意/拒绝）后引擎重入本节点直接路由人工结论，**不会再次调用 AI**。用户视角详见 gflow 用户文档《智能体（AI 审批）》。

> ⚠️ `customPrompt` 走完整 EL 引擎，模板可读文件/环境变量，仅应视为流程设计者可信输入，不要把终端用户输入拼进该字段。

## httpCall

同步 HTTP 调用。流程：渲染 `url`/`headers`/`body`（rulego EL 模板，支持 `${msg.xxx}`/`${metadata.xxx}`）→ 发请求 → 状态码 ≥400 走 `Failure` → 响应按 **MergeAgentOutput 三规则**合并进 `msg.Data`（与 `aiAgent` 同一套模型；保留表单字段是相对 rulego 原生 `restApiCall` 的核心差异：原生节点成功后 `SetData` 整体覆盖会冲掉流程表单变量）。规则：① 完整响应始终写入 `msg._http`（对象存对象，纯文本/数组存原文）；② `flattenOutput=true`（默认）时对象顶层字段平铺进 `msg.Data`；③ `outputMappings` 永远最后执行（优先级最高）。

### 配置字段

| 字段 | 默认 | 说明 |
|---|---|---|
| `url` | 必填 | 支持 EL 变量 |
| `method` | `GET` | |
| `headers` | | value 支持 EL 变量 |
| `body` | | 模板；空则不发 body |
| `timeoutMs` | `10000` | |
| `flattenOutput` | `true` | 输出模式（`*bool`，**缺省=平铺(true)**）：`true`=平铺（响应顶层字段并入 `msg.Data`，同名覆盖表单）；`false`=隔离（完整响应只在 `_http`，不碰表单）。**与 `aiAgent` 节点语义与默认值已统一** |
| `outputMappings` | 空 | 按 `{from,to}` 显式映射，在输出模式之后最后执行；目标写 `metadata.k` 进消息元数据 |
| `reservedKey` | `_http` | 完整响应的存放 key；存量 DSL 可自定义，设计器不暴露 |
| `allowedHosts` | 空 | SSRF 主机白名单，支持 `host` / `host:port`，重定向逐跳校验 |
| `blockPrivateNetworks` | `false` | 是否拦截 RFC1918 私有网段（BPM 常需调内网服务，默认放行） |
| `insecureSkipVerify` | `false` | 跳过 TLS 校验（危险项，设计器不暴露 UI） |
| `proxyUrl` | | http/https 代理 |

### SSRF 防护（为什么有 IP 安全检查）

`url` 支持 `${msg.xxx}` 动态渲染，即 URL 可能由流程变量（最终来自终端用户输入）控制，存在经典 SSRF 风险：借流程发起打云元数据（169.254.169.254）、回环服务、内网端口，或经 `file://`/`gopher://` 等协议读文件。因此节点内置分层防护：

1. **scheme 白名单**：仅允许 `http`/`https`；
2. **主机白名单**：配置 `allowedHosts` 后主机必须命中。按【字面 IP/host:port】信任时视为显式指定地址，完全放行（内网/回环回调可用）；按【域名】信任时拨号期仍保留回环/链路本地/元数据段的兜底拦截（防 DNS 劫持到云元数据）；
3. **动态主机拦截**：仅当 URL 模板的**主机部分**含 `${...}` 时（`urlHostIsDynamic` 判定，路径含变量不算），对渲染结果做 DNS 解析并逐 IP 校验——回环（127/8、::1）、链路本地/云元数据（169.254/16、fe80::/10）、未指定、组播地址**始终拦截**；RFC1918 私有段仅在 `blockPrivateNetworks=true` 时拦截（避免误伤合法的内网服务调用）；解析失败按拒绝处理；
4. **重定向逐跳校验**：`CheckRedirect` 对每个 30x 目标重复上述校验，防跳转绕过；
5. **响应体上限 10MB**（`maxHTTPResponseBytes`），防超大响应打爆内存。

DNS rebinding 防护：动态主机与按域名信任的白名单主机在【拨号时】二次解析并 pin 首个放行 IP 直连（校验与连接用同一地址，两次解析的翻转窗口被封死）；按字面 IP 白名单与纯静态 URL 不安装拨号守卫（DSL 作者显式写死的目标视为完全可信）。

---

## startProcess（规则链专用）

在规则链（自动化）内以指定发起人启动一条 BPM 流程实例，经注入的
`RuntimeService.StartProcessInstanceByKey` 同步发起。典型场景：**定时链自动发起审批**
（`endpoint/schedule` 触发 → jsTransform 算动态变量 → startProcess），gflow 前端
「定时发起审批」纯配置模式生成的 DSL 即为该链路。

### 租户约定

节点从消息 metadata 的 `tenant_id` 解析租户（与 `aiAgent` 同款约定）：

- BPM→链 调用（`automation`/`aiAgent` 节点）由 AutomationExecutor 自动携带；
- 定时触发的链由 gflow 触发配置把 `tenant_id` 写进 schedule 端点的静态 metadata（params[2]）；
- 编辑器手动测试需在调试 metadata 里补 `tenant_id`。

### 配置字段

| 字段 | 必填 | 说明 |
|---|---|---|
| `processKey` | ✓ | 流程定义 Key（该租户最新版本），支持 `${msg.xxx}` |
| `initiator` | ✓ | 发起人用户ID（实例 start_user_id），支持 `${msg.xxx}` |
| `businessKey` | | 业务键；同定义下已有同 businessKey 活跃实例时冲突走 `Failure` |
| `variables` | | 启动变量 JSON，值支持 `${msg.xxx}` 占位；留空则把当前消息数据作为表单变量 |

出边语义：成功 `Success`（实例ID写 `metadata.processInstanceId` 并合并进 `msg.Data`
顶层，保留原字段）；失败（定义不存在/发起人无权限/冲突）`Failure`。

> 面板归属：`Category() = "bpm"`（gflow 前端注入「流程/审批」分组标签）；BPM 流程设计器
> 不提供此节点（流程内用 startTask 发起），仅出现在规则链编辑器组件面板。

## 注意事项

- 任务创建路径（初始创建与顺序审批推进）按 实例ID+节点ID 做进程内串行化，防并发重入重复建任务；**多实例部署**仍需上层注入分布式锁（见 README 扩展点）；
- 多人任务/会签任务批量创建失败时会回滚（删除）本次已创建的任务，避免部分参与者收到待办而节点整体失败；
- `role` / `department` / `direct_manager` / `multi_level_manager` 候选类型在 IdentityService 未注入或解析出错时返回错误（节点走 `Failure`），不会静默丢失审批人；
- `aiAgent` 配置了 `failureHandler` 但 TaskService 不可用时走 `Failure`，不会静默挂起实例；
- `automation` 执行器为包级全局注入，多引擎实例共享。

更多机制细节见源码注释，事件钩子见 [events.md](events.md)，并行限制见 [parallel-limitations.md](parallel-limitations.md)。
