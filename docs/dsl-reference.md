# 流程 DSL 参考：顶层结构与节点配置

> 本文档是**设计器/DSL 编写向**的配置参考：流程定义（规则链）顶层结构、各节点 `configuration` 字段表与 JSON 示例、组件清单接口。
> 原名《rulego节点API接口文档》（2026-06 快照）误置于 rulego 仓库，现迁回引擎仓库并校对节点类型名。
>
> 相关文档：
> - [components.md](components.md)——节点**机制**（注册、生命周期、出边语义、httpCall SSRF 防护），面向引擎集成者
> - gflow-doc 站点 `guide/dsl.md` / `guide/features/nodes.md`——用户向精简版
>
> 类型名校对（相对原始快照）：`functions`→`serviceTask`（rulego 原生注册名 functions 仍指同类）、`flow`→`automation`、`restApiCall`→`httpCall`（BPM 专用实现，字段全换，见 2.1.11）。

## 1. 规则链（流程定义）配置说明

### 1.1 顶层结构

| 字段          | 类型     | 说明           |
|-------------|--------|--------------|
| `ruleChain` | object | 规则链基础信息与全局配置 |
| `metadata`  | object | 节点与连线的结构化元数据 |

### 1.2 `ruleChain` 字段

| 字段               | 类型      | 说明                    |
|------------------|---------|-----------------------|
| `id`             | string  | 规则链ID（唯一），填processKey |
| `name`           | string  | 规则链名称                 |
| `root`           | boolean | 是否为根规则链               |
| `debugMode`      | boolean | 是否开启调试模式              |
| `additionalInfo` | object  | 扩展信息                  |

`additionalInfo` 子字段：

| 字段                  | 类型     | 说明                                                                 |
|---------------------|--------|--------------------------------------------------------------------|
| `description`       | string | 流程/规则链描述                                                           |
| `formType` | string | 表单三态：`design`（默认，内嵌 `form` schema）/ `system` + `formKey` 引用宿主 forms 表共享模板 / `external` + `formUrl` 挂外部表单（iframe 只读展示） |
| `formKey` | string | formType=system 时引用的共享表单 ID |
| `form` | object | formType=design 时的内嵌表单 schema（设计器产出） |
| `category`          | string | 流程分类                                                               |
| `icon`              | string | 流程图标                                                               |
| `processType`       | string | 流程类型（默认 `main`）                                                    |
| `actionPermissions` | object | **流程级发起人动作权限**（"高级设置"对应）：`withdraw`/`resubmit`/`urge`/`suspend`/`terminate` |

- **ruleChain.additionalInfo.actionPermissions 键**（发起人视角，按"发起人 + 实例状态"触发）：

| 键           | 值类型     | 默认值    | 说明                                            |
|--------------|---------|--------|-----------------------------------------------|
| `withdraw`   | boolean | `true` | 是否显示"撤回"按钮（active 实例 + 发起人）                   |
| `resubmit`   | boolean | `true` | 是否显示"重新提交"按钮（failed/terminated 实例 + 发起人）      |
| `urge`       | boolean | `true` | 是否显示"催办"按钮（active 实例 + 发起人催办审批人）              |
| `suspend`    | boolean | `true` | 是否显示"挂起"按钮（active 实例 + 发起人）                   |
| `terminate`  | boolean | `true` | 是否显示"终止流程"按钮（suspended 实例 + 发起人；与 userTask 上的 terminate 取 OR） |

  实例级必备动作（不开放给设计器）：`activate`（draft/suspended/failed→恢复）、`delete`（draft→删除）。
  合并语义：`按钮可见 = 状态允许 AND 设计器允许`。

### 1.3 `metadata` 字段

| 字段               | 类型           | 说明             |
|------------------|--------------|-----------------|
| `firstNodeIndex` | number       | 起始节点索引（默认从0开始） |
| `nodes`          | Node[]       | 节点列表（详见 11.4）  |
| `connections`    | Connection[] | 连线列表（详见 11.5）  |
| `endpoints`      | EndpointDsl[] | 触发端点列表（定时触发，详见 1.7；gflow 前端不在此编辑，见「触发设置」） |

### 1.4 节点通用字段（`Node`）

| 字段                               | 类型      | 说明              |
|----------------------------------|---------|-----------------|
| `id`                             | string  | 节点ID（唯一）        |
| `type`                           | string  | 节点类型（详见第二章各节点） |
| `name`                           | string  | 节点名称            |
| `debugMode`                      | boolean | 节点级调试开关         |
| `configuration`                  | object  | 节点类型专属配置（见第12章） |
| `additionalInfo.description`     | string  | 节点描述            |
| `additionalInfo.layoutX`         | number  | 画布X坐标（可视化）      |
| `additionalInfo.layoutY`         | number  | 画布Y坐标（可视化）      |
| `additionalInfo.formPermissions` | object  | 表单权限配置          |

> 说明：不同 `type` 的节点，其 `configuration` 字段结构不同；请参考第13章的表格化配置说明。

### 1.5 连线字段（`Connection`）

| 字段       | 类型     | 说明                                       |
|----------|--------|------------------------------------------|
| `fromId` | string | 源节点ID                                    |
| `toId`   | string | 目标节点ID                                   |
| `type`   | string | 连线类型/标签，除了条件分支可以自定义，一般是Success 和 Failure |

### 1.6 示例

```json
{
  "ruleChain": {
    "id": "leave_approval",
    "name": "leave_approval_process",
    "root": true,
    "debugMode": true,
    "additionalInfo": {
      "description": "请假审批工作流程",
      "formType": "system",
      "formKey": "leave_request_form"
    }
  },
  "metadata": {
    "firstNodeIndex": 0,
    "nodes": [
      {
        "id": "node_s0",
        "type": "startTask",
        "name": "发起人",
        "debugMode": false,
        "configuration": {
        },
        "additionalInfo": {
          "description": "发起人",
          "layoutX": 100,
          "layoutY": 100,
          "formPermissions":{
            "field1":"r",
            "field2":"r"
          }
        }
      },
      {
        "id": "node_s1",
        "type": "switch",
        "name": "判断请假天数",
        "debugMode": false,
        "configuration": {
          "cases": [
            {"case": "msg.days <= 3", "then": "manager_approval"},
            {"case": "msg.days > 3", "then": "hr_approval"}
          ]
        },
        "additionalInfo": {
          "description": "根据请假天数判断审批流程",
          "layoutX": 100,
          "layoutY": 100
        }
      },
      {
        "id": "node_manager_approval",
        "type": "userTask",
        "name": "经理审批",
        "debugMode": false,
        "configuration": {
          "candidateType": "direct_manager",
          "approvalType": "single"
        },
        "additionalInfo": {
          "description": "直属经理审批",
          "layoutX": 300,
          "layoutY": 50,
          "formPermissions":{
            "field1":"r",
            "field2":"r"
          },
          "actionPermissions":{
            "transfer":true,
            "urge":true
          }
        }
      },
      {
        "id": "end",
        "type": "end",
        "name": "结束",
        "additionalInfo": {"description": "结束", "layoutX": 500, "layoutY": 200}
      }
    ],
    "connections": [
      {"fromId": "node_s0", "toId": "node_s1", "type": "Success"},
      {"fromId": "node_s1", "toId": "node_manager_approval", "type": "manager_approval"},
      {"fromId": "node_manager_approval", "toId": "end", "type": "Success"}
    ]
  }
}
```

## 1.7 定时触发（`metadata.endpoints`）

流程/规则链的定时触发用 rulego 原生 `endpoint/schedule` 端点表达（6 字段秒级 cron，
robfig/cron WithSeconds 约定）。**触发器是链级配置，不出现在画布**——gflow 前端编辑器
装载时剥离 endpoints、保存时回注（`to.path` 动态解析为当前首节点），用户经编辑器头部
「触发设置」抽屉或新建向导以可视化方式（ZCode 式内联下拉）配置，不感知 cron 细节。

```json
"metadata": {
  "endpoints": [{
    "id": "ep_timer",
    "type": "endpoint/schedule",
    "routers": [{
      "from": { "path": "0 30 9 1 * *" },
      "to":   { "path": "<chainId>:<首节点id>" },
      "params": [ "", "JSON", {"reportType": "monthly", "tenant_id": "1"} ]
    }]
  }]
}
```

| 字段 | 说明 |
|---|---|
| `routers[].from.path` | 6 字段秒级 cron：`秒 分 时 日 月 周`（如 `0 30 9 1 * *` = 每月 1 日 09:30） |
| `routers[].to.path` | 触发后进入的节点，格式 `chainId:nodeId`；gflow 保存时固定解析为首节点（触发即进入流程第一步） |
| `routers[].params[0]` | 触发时的初始 msg body（字符串；通常留空） |
| `routers[].params[1]` | msgType，通常 `JSON` |
| `routers[].params[2]` | 静态 metadata KV；gflow 会自动注入 `tenant_id`（`startProcess` 等节点解析租户用） |

触发时引擎自动注入 `metadata.triggerSource = endpoint/schedule`。**静态 params 不支持
触发时刻的动态值**（如"上个月的月份"）——由链上首个 `jsTransform` 节点用 `new Date()`
计算（「定时发起审批」纯配置模式自动生成该节点）。

> 已知限制：**集群模式下
> 定时触发不可用**，详见下段。

运行条件与链生命周期一致：单机部署 endpoint 框架默认开启，链**部署**后 cron 即生效，
**下线**即停止；集群模式 rulego 端点被禁用（避免多副本重复触发），定时链需待 gflow
自有调度器（P1，读同一份 DSL、Redis 幂等）。

## 2. 节点配置

### 13.1 节点配置

#### 2.1.1 开始节点
- `type`: startTask
- 说明: 标记流程开始节点
##### 配置：无

`StartTaskNodeConfiguration` 为空结构体（见 components/start_node.go）：节点仅作流程起点
标记，**没有任何配置字段**。谁可发起当前不做限制，如需范围鉴权应在流程发起层（宿主应用）实现。
##### 节点 additionalInfo配置

| 字段                | 类型     | 说明                             |
|-------------------|--------|--------------------------------|
| `formPermissions` | object | 表单字段权限映射（`r`=只读、`w`=可写、`h`=隐藏） |

> 发起人视角的动作权限（`withdraw`/`resubmit`/`urge`/`suspend`/`terminate`）属于**流程级**配置，
> 存放在 `ruleChain.additionalInfo.actionPermissions`，由设计器的"高级设置"维护，
> 不挂在 startTask 节点上。详见 1.2 节。

#### 2.1.2 用户任务节点/审批节点
- `type`: userTask
- 说明: 由用户办理的审批任务，支持单人、或签、顺序审批、会签与票签。

##### 配置

| 字段                 | 类型           | 说明                                                                                                                            |
|--------------------|--------------|-------------------------------------------------------------------------------------------------------------------------------|
| `candidateType`    | string       | 审批人员类型，可选：`user`（指定成员）/ `role`（指定角色）/ `dept`（指定部门，产生待认领任务）/ `direct_manager`（直接上级）/ `multi_level_manager`（多级上级）/ `initiator_select`（发起人自选）/ `initiator_self`（发起人自己） |
| `candidateConfig`  | object       | 类型对应的子配置键：`userIds:string[]`（user 型指定成员）/ `roleIds:string[]`（role 型指定角色）/ `departmentIds:string[]`（dept 型指定部门）/ `levels:number`（direct_manager、multi_level_manager 的上级层级数）/ `selected:string`（initiator_select 型审批人表达式模板，如 `${msg.selectedUsers}`，运行时以流程变量求值得到审批人ID列表） |
| `approvalType`     | string       | 审批类型：`single(单人), or(或签), sequential(按顺序依次审批), countersign(会签), vote(票签)                                                      |
| `approvalRule`     | string(JSON) | 多人审批通过规则（结构为 `dto.CountersignRule`）：`{"type":"all","value":0,"isSequential":false}`。`type` 可选 `all`（全部通过，**缺省**）/ `any`（任一通过）/ `majority`（过半）/ `percent`（按百分比通过，`value` 取 0~100）/ `count`（固定票数，`value` 为通过票数）；`isSequential=true` 表示子任务按创建顺序逐个激活（false 为并行）。仅 countersign/vote 等多人会签场景消费，single 忽略 |
| `selfApprovalType` | string       | 自审策略：可选：allow(发起人自己审批)/skip(自动跳过)/auto_approve(见下方警示)/delegate_to_manager(转交直接上级)/delegate_to_department_manager(转交部门负责人)                         |
| `dueDate`          | string       | 静态到期时间（可选），支持 RFC3339 / `2006-01-02 15:04:05` / `2006-01-02`；节点级配置，该节点所有任务实例共用同一到期时刻，不支持流程变量；解析失败仅告警 |
| `timeoutPolicy`    | object       | 超时策略 `{dueInMinutes:number, action:string}`（可选）：到期时间改为相对**每个任务创建时刻**的时长，配置后优先于静态 `dueDate`；`action`（remind/autoApprove/autoReject）由宿主逾期巡检执行，引擎自身不执行动作，详见 [components.md](components.md#userTask) |

> ⚠️ `selfApprovalType=auto_approve`：名为"自动通过"，但**当前行为与 `allow` 一致**——仅保留发起人为
> 审批人，不会产生任何自动通过标记（见 types/enums/enums.go 中 `SelfApprovalTypeAutoApprove`
> 的注释）。依赖"发起人自动通过"语义的场景请勿使用该取值。

##### 节点 additionalInfo配置

| 字段                  | 类型     | 说明                                                             |
|---------------------|--------|----------------------------------------------------------------|
| `formPermissions`   | object | 表单字段权限映射（`r`=只读、`w`=可写、`h`=隐藏）                                 |
| `actionPermissions` | object | **审批人视角**动作权限；仅设计器可控动作写入此对象（详见下表）                              |

- **userTask 设计器可控动作**（写入 `actionPermissions`，审批人在不同任务状态下可见）：

| 键                  | 值类型     | 默认值    | 说明                                        |
|--------------------|---------|--------|-------------------------------------------|
| `addComment`       | boolean | `true` | 是否显示"评论"按钮（active 任务）                     |
| `transfer`         | boolean | `true` | 是否显示"转办"按钮（active 任务）                     |
| `return`           | boolean | `true` | 是否显示"回退"按钮（active 任务）                     |
| `terminate`        | boolean | `true` | 是否显示"终止流程"按钮（active 任务；与 startTask 上的 terminate 取 OR） |
| `awaken`           | boolean | `true` | 是否显示"唤醒"按钮（suspended 任务 + 办理人）           |
| `uploadAttachment` | boolean | `false` | 是否显示"上传附件"按钮（active 任务）                   |
| `rejectStrategy`   | string  | `""`   | 驳回策略（见下方）                                 |
| `rejectTargetNode` | string  | `""`   | 仅当 `rejectStrategy="rejectToNode"` 时有效     |

  - `rejectStrategy` 取值：`""`（使用默认/终止流程）/`rejectToStarter`（驳回到开始节点）/`rejectToNode`（驳回到指定节点）/`rejectToPrev`（驳回到上一个节点）/`terminate`（终止流程）

- **任务级后端强制动作**（不进入设计器，按运行态状态强制）：

| 键          | 触发条件                                       |
|------------|--------------------------------------------|
| `approve`  | active 任务（核心审批动作，必备）                       |
| `reject`   | active 任务（核心审批动作，必备）                       |
| `claim`    | pending 任务 + 候选人（不开放给设计器，避免流程卡死）          |
| `unclaim`  | active 任务 + 当前用户为 assignee 且已签收（不开放给设计器）  |

  合并语义：`按钮可见 = 状态允许 AND 设计器允许`。设计器可控键缺省视为"未禁用"（默认值见上表）；后端强制键忽略设计器配置。

  - 发起人视角动作（`withdraw`/`resubmit`/`urge`/`suspend`/`terminate`）属于**流程级**配置（`ruleChain.additionalInfo.actionPermissions`，见 1.2 节），不在 userTask 上。

  
  - 示例（单人：直属上级）
  ```json
  {
    "id": "node_manager_approval",
    "type": "userTask",
    "name": "经理审批",
    "configuration": {
      "candidateType": "direct_manager",
      "approvalType": "single"
    }
  }
  ```
  - 示例（或签：指定成员）
  ```json
  {
    "id": "node_multi_or",
    "type": "userTask",
    "name": "多人或签审批",
    "configuration": {
      "candidateType": "user",
      "candidateConfig": {"userIds": ["user_001", "user_002"]},
      "approvalType": "or"
    }
  }
  ```
  - 示例（顺序审批：多级上级）
  ```json
  {
    "id": "node_sequential",
    "type": "userTask",
    "name": "逐级审批",
    "configuration": {
      "candidateType": "multi_level_manager",
      "candidateConfig": {"levels": 3},
      "approvalType": "sequential"
    }
  }
  ```
  - 示例（票签：60% 通过，并行投票）
  ```json
  {
    "id": "node_vote",
    "type": "userTask",
    "name": "票签审批",
    "configuration": {
      "candidateType": "user",
      "candidateConfig": {"userIds": ["user_001", "user_002", "user_003"]},
      "approvalType": "vote",
      "approvalRule": "{\"type\":\"percent\",\"value\":60,\"isSequential\":false}"
    }
  }
  ```
  - 示例（发起人自选：表达式返回ID列表）
  ```json
  {
    "id": "node_initiator_select",
    "type": "userTask",
    "name": "发起人自选审批人",
    "configuration": {
      "candidateType": "initiator_select",
      "approvalType": "or"
    }
  }
  ```
#### 2.1.3 抄送服务节点
- `type`: ccTask
- 说明: 将当前审批内容抄送给指定人员或渠道，用于知会不参与办理的对象。
##### 配置

| 字段           | 类型       | 说明                                           |
|--------------|----------|----------------------------------------------|
| `ccUserIds`  | string[] | 抄送用户ID列表                                     |
| `selfSelect` | boolean  | 是否发起人自选；当为 `true` 时，发起流程时候改节点用户可以自选抄送人用户ID列表 |
##### 节点 additionalInfo配置

| 字段                | 类型     | 说明                             |
|-------------------|--------|--------------------------------|
| `formPermissions` | object | 表单字段权限映射（`r`=只读、`w`=可写、`h`=隐藏） |
  - 示例
  ```json
  {
    "id": "node_cc",
    "type": "ccTask",
    "name": "抄送通知",
    "configuration": {
      "ccUserIds": ["user_hr_001", "user_mgr_001"],
      "selfSelect": false
    }
  }
  ```
#### 2.1.4 系统服务节点（serviceTask）
- `type`: serviceTask（rulego 原生名 `functions` 指同类节点）
- 说明: 通过函数名调用经 `components.Services.Register` 注册的 Go 函数。
##### 配置
  
| 字段             | 类型     | 说明                                 |
|----------------|--------|------------------------------------|
| `functionName` | string | 要调用的函数名称                           |
| `param`        | string | 函数入参。支持表达式取值进行替换。如果空，则使用当前消息负荷作为参数 | 


  - 示例
  ```json
  {
    "id": "node_notify",
    "type": "serviceTask",
    "name": "函数调用",
    "configuration": {
      "functionName": "NotificationService.sendApprovalNotification"
    }
  }
  ```
#### 2.1.5 并行分支
- `type`: fork

- 说明: 将流程并行分发至多个分支。
##### 配置:无
  
  - 示例
  ```json
  {
    "id": "node_fork",
    "type": "fork",
    "name": "并行分支",
    "additionalInfo": {"description": "并行到多个审批支路"}
  }
  ```

#### 2.1.6 包容分支
- `type`: inclusive
- 说明: 按顺序匹配条件，所有满足的条件都会独立执行后续链路；若全部不满足则走 Default 链。与 `switch` 的区别在于：`switch` 只路由到第一个匹配分支，而 `inclusive` 路由到所有匹配分支。
##### 配置
  
| 字段             | 类型     | 说明                                  |
|----------------|--------|-------------------------------------|
| `cases`        | array  | 条件数组（全部评估，命中的分支均路由）                 |
| `cases[].case` | string | 表达式（布尔），可使用 `msg`、`metadata`变量      |
| `cases[].then` | string | 命中后的连线关系名称（与 `connections.type` 对应） |

  说明：包容分支通常需要在后续添加“聚合分支（join）”用于汇总并行结果；此节点不需要用户手动添加，生成 DSL 时自动补充。

  - 示例
  ```json
  {
    "id": "node_inclusive",
    "type": "inclusive",
    "name": "包容分支",
    "configuration": {
      "cases": [
        {"case": "msg.risk >= 80", "then": "high"},
        {"case": "msg.amount > 10000", "then": "large"}
      ]
    }
  }
  ```
`risk`、`amount` 为表单的字段，保存到DSL，需要补充上前缀`msg.`，例如：msg.risk
#### 2.1.7 条件分支
- `type`: switch
- 说明: 按顺序匹配条件，仅执行第一条满足的后续链路；若全部不满足则走 Default 链。
##### 配置
  
| 字段             | 类型     | 说明                                  |
|----------------|--------|-------------------------------------|
| `cases`        | array  | 条件数组（按顺序评估）                         |
| `cases[].case` | string | 表达式（布尔），可使用 `msg`、`metadata`变量      |
| `cases[].then` | string | 命中后的连线关系名称（与 `connections.type` 对应） |

  - 示例
  ```json
  {
    "id": "node_s1",
    "type": "switch",
    "name": "判断请假天数",
    "configuration": {
      "cases": [
        {"case": "msg.days <= 3", "then": "manager_approval"},
        {"case": "msg.days > 3", "then": "hr_approval"}
      ]
    }
  }
  ```
`days` 为表单的字段，保存到DSL，需要补充上前缀`msg.`，例如：msg.days

#### 2.1.8 延迟节点
- `type`: delay
- 说明: 提供消息延迟能力，支持静态数值与占位符动态解析，并可控制队列上限与覆盖模式；队列溢出走失败链。
##### 配置
  
| 字段               | 类型      | 说明                                       |
|------------------|---------|------------------------------------------|
| `delayMs`        | string  | 延迟毫秒数 |
  - 示例
  ```json
  {
    "id": "node_delay",
    "type": "delay",
    "name": "延迟节点",
    "configuration": {
      "delayMs": "60000"
    }
  }
  ```
#### 2.1.9 自动化节点（automation）
- `type`: automation
- 说明: 调用一条 rulego 规则链（即"自动化"执行单元）。底层嵌入 `flow.ChainNode`，与子流程节点共享同一套实现，仅 type 名不同，便于前端按语义区分。
##### 配置

| 字段 | 类型 | 说明 |
|---|--|---|
| `targetId` | string | 目标规则链 ID（前端下拉已部署的规则链列表）|

> 底层嵌入 `flow.ChainNode`，但只消费 `targetId`；`extend` 等其余 ChainNode 配置项
> **不生效**（components/automation_call_node.go 的配置结构仅含 targetId），不要在 DSL
> 中配置，存量 DSL 里的 `extend` 会被忽略。
  - 示例
  ```json
  {
    "id": "node_automation",
    "type": "automation",
    "name": "自动化",
    "configuration": {
      "targetId": "rule_chain_01"
    }
  }
  ```
#### 2.1.10 子流程节点
- `type`: subProcess
- 说明: 嵌套调用另一条 BPM 工作流。底层同样使用 `flow.ChainNode`，与自动化节点的差别仅在 type 名（前端按语义区分：自动化=调用规则链，子流程=嵌套 BPM 流程）。
##### 配置

| 字段 | 类型 | 说明 |
|---|--|---|
| `targetId` | string | 目标 BPM 流程 ID（前端下拉流程定义列表）|

> 底层嵌入 `flow.ChainNode` 仅复用其配置结构，`OnMsg` 完全覆写：只消费 `targetId`，
> `extend` 等其余 ChainNode 配置项**不生效**（见 components/sub_process_node.go 注释），
> 不要在 DSL 中配置。
  - 示例
  ```json
  {
    "id": "node_subflow",
    "type": "subProcess",
    "name": "子流程",
    "configuration": {
      "targetId": "leave_approval"
    }
  }
  ```
#### 2.1.11 HTTP调用节点（httpCall）
- `type`: httpCall
- 说明: 同步调用外部接口，响应按映射合并进流程变量。BPM 专用实现（非 rulego 原生 `restApiCall`）：原生节点成功后 `SetData` 整体覆盖会冲掉流程表单变量，本节点改为增量合并。内置 SSRF 分层防护（scheme 白名单/主机白名单/动态主机拦截/重定向逐跳校验/响应体上限），详见 [components.md](components.md#httpcall)。
##### 配置

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `url` | string | 必填 | 目标地址，支持 EL 变量 `${msg.xxx}` / `${metadata.xxx}` |
| `method` | string | `GET` | HTTP 方法 |
| `headers` | object | | 请求头，value 支持 EL 变量 |
| `body` | string | | 请求体模板；空则不发 body |
| `timeoutMs` | number | `10000` | 超时（毫秒） |
| `flattenOutput` | bool | `true` | 输出模式（`*bool`，**缺省=平铺(true)**）：`true`=响应对象顶层字段平铺进 `msg.Data`（同名覆盖表单）；`false`=隔离（完整响应只写 `reservedKey`，不碰表单）。与 aiAgent 节点语义与默认值已统一 |
| `outputMappings` | array | 空 | 按 `[{from,to}]` 显式映射，在输出模式之后最后执行（优先级最高） |
| `reservedKey` | string | `_http` | 非 object 响应（数组/纯文本）整体写入的 key，不污染表单字段 |
| `allowedHosts` | array | 空 | SSRF 主机白名单，支持 `host` / `host:port` |
| `blockPrivateNetworks` | bool | `false` | 是否拦截 RFC1918 私有网段 |
| `insecureSkipVerify` | bool | `false` | 跳过 TLS 校验（危险项，设计器不暴露） |
| `proxyUrl` | string | | http/https 代理 |

##### 示例

- 基础调用 + 响应平铺
```json
{
  "id": "node_http",
  "type": "httpCall",
  "name": "查询风控评分",
  "configuration": {
    "url": "https://api.example.com/risk/score",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json",
      "Authorization": "Bearer ${metadata.token}"
    },
    "timeoutMs": 5000
  }
}
```

- 显式映射（只取需要的字段，避免覆盖表单变量）
```json
{
  "id": "node_http2",
  "type": "httpCall",
  "configuration": {
    "url": "https://api.example.com/users/${msg.userId}",
    "method": "GET",
    "outputMappings": [
      { "from": "level", "to": "userLevel" },
      { "from": "dept", "to": "userDept" }
    ]
  }
}
```

#### 2.1.12 发起审批节点（startProcess，规则链专用）
- `type`: startProcess
- 说明: 在规则链内以指定发起人启动一条 BPM 流程实例。典型用法：定时链经此实现
  "定时自动发起审批"（如每月 1 日自动发起月报复审）。仅出现在规则链（自动化）编辑器
  组件面板的「流程/审批」分组；BPM 流程设计器不提供（流程内直接用 startTask 发起）。

##### 配置

| 字段 | 类型 | 说明 |
|---|--|---|
| `processKey` | string | 要发起的流程定义 Key（取该租户下最新版本），支持 `${msg.xxx}` 占位 |
| `initiator` | string | 发起人用户ID（实例 start_user_id），支持 `${msg.xxx}` 占位 |
| `businessKey` | string | 可选业务键；同定义下已有同 businessKey 的活跃实例时发起失败走 `Failure` |
| `variables` | string(JSON) | 启动变量（表单数据），值支持 `${msg.xxx}`/`${metadata.xxx}` 占位；留空则把当前消息数据作为表单变量 |

- 租户约定：节点从消息 metadata 的 `tenant_id` 解析租户（BPM→链调用自动携带；
  定时触发的链由触发配置注入；编辑器手动测试需在调试 metadata 里补 `tenant_id`）。
- 出边：成功走 `Success`（实例ID写入 `metadata.processInstanceId` 并合并进 `msg.Data` 顶层），
  失败（流程定义不存在 / 发起人无权限 / businessKey 冲突等）走 `Failure`。

  - 示例（定时发起月报复审：jsTransform 算出上月月份后发起）
  ```json
  {
    "id": "node_start",
    "type": "startProcess",
    "name": "发起审批",
    "configuration": {
      "processKey": "monthly_report",
      "initiator": "480356539643727872",
      "variables": "{\"month\": \"${msg.prevMonth}\", \"title\": \"月报复审\"}"
    }
  }
  ```

#### 2.1.13 聚合分支
- `type`: join
- 说明: 合并多个并行分支的执行结果。
##### 配置
  
| 字段           | 类型   | 说明                  |
|--------------|------|---------------------|
| `timeout`    | number | 执行超时时间（秒）；`0` 表示不限制 |
| `mergeToMap` | bool | 填：true (不需要用户选)     |
  - 示例
  ```json
  {
    "id": "node_join",
    "type": "join",
    "name": "聚合分支",
    "configuration": {
      "timeout": 30,
      "mergeToMap": true
    }
  }
  ```

#### 2.1.14 结束节点
- `type`: end
- 说明: 流程终点，无需配置。
##### 配置: 无
  
  - 示例
  ```json
  {
    "id": "end",
    "type": "end",
    "name": "结束"
  }
  ```

## 3. 组件清单接口（宿主 API）

### 3.1 获取组件列表
- URL: `/api/v1/components?scope=bpm|rulechain`（宿主 gflow 提供；`scope=bpm` 为流程设计器组件，`scope=rulechain` 为规则链编辑器组件，默认 bpm）
- Method: `GET`
- 说明: 获取所有可用节点组件列表与内置服务配置，用于前端渲染流程设计器组件面板。
  - `nodes`: 所有节点组件列表，包含组件的配置字段定义。
  - `builtins.serviceTask`: 服务组件（`serviceTask`）可用的服务列表。

#### 响应结构

| 字段 | 类型 | 说明 |
|---|---|---|
| `nodes` | ComponentForm[] | 所有可用节点组件列表，用于渲染组件面板 |
| `builtins` | object | 内置选项配置，包含服务列表等 |

#### `nodes` 元素结构说明 (`ComponentForm`)

| 字段 | 类型 | 说明 |
|---|---|---|
| `type` | string | 组件类型唯一标识符 |
| `category` | string | 组件分类 |
| `label` | string | 组件显示名称 |
| `desc` | string | 组件描述 |
| `icon` | string | 组件图标标识符 |
| `relationTypes` | string[] | 下一个节点的连接关系名称列表（如 `Success`, `Failure`） |
| `disabled` | boolean | 是否在编辑器中隐藏 |
| `version` | string | 组件版本 |
| `componentKind` | string | 组件种类：`dc` (动态), `nc` (原生), `ec` (端点) |
| `fields` | Field[] | 组件配置字段列表 |

#### `fields` 元素结构说明 (`ComponentFormField`)

| 字段 | 类型 | 说明 |
|---|---|---|
| `name` | string | 字段名称（对应配置结构体字段） |
| `type` | string | 字段数据类型（string, int, bool 等） |
| `label` | string | 字段显示标签 |
| `desc` | string | 字段描述/提示信息 |
| `defaultValue` | any | 默认值 |
| `required` | boolean | 是否必填 |
| `rules` | object[] | 前端验证规则列表（如 `[{"required": true, "message": "必填"}]`） |
| `component` | object | UI渲染组件配置（如 `{"type": "codeEditor", "language": "json"}`） |
| `fields` | Field[] | 嵌套字段列表（用于复杂对象类型） |

#### 返回示例

```json
{
  "code": 200,
  "data": {
    "builtins": {
      "serviceTask": [
        {
          "name": "test",
          "label": "测试服务",
          "desc": "这是一个测试服务"
        }
      ]
    },
    "nodes": [
      {
        "type": "delay",
        "category": "action",
        "fields": [
          {
            "name": "maxPendingMsgs",
            "type": "int",
            "defaultValue": 1000,
            "label": "",
            "desc": "",
            "validate": "",
            "rules": null,
            "fields": null,
            "component": null,
            "required": false
          },
          {
            "name": "delayMs",
            "type": "string",
            "defaultValue": "60000",
            "label": "",
            "desc": "",
            "validate": "",
            "rules": null,
            "fields": null,
            "component": null,
            "required": false
          },
          {
            "name": "overwrite",
            "type": "bool",
            "defaultValue": false,
            "label": "",
            "desc": "",
            "validate": "",
            "rules": null,
            "fields": null,
            "component": null,
            "required": false
          },
          {
            "name": "periodInSeconds",
            "type": "int",
            "defaultValue": 0,
            "label": "",
            "desc": "",
            "validate": "",
            "rules": null,
            "fields": null,
            "component": null,
            "required": false
          },
          {
            "name": "periodInSecondsPattern",
            "type": "string",
            "defaultValue": "",
            "label": "",
            "desc": "",
            "validate": "",
            "rules": null,
            "fields": null,
            "component": null,
            "required": false
          }
        ],
        "label": "DelayNode",
        "desc": "",
        "icon": "",
        "relationTypes": [
          "Success",
          "Failure"
        ],
        "disabled": false,
        "version": "",
        "componentKind": "nc"
      }
    ]
  }
}
```

