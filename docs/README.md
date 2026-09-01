# GFlow Engine 文档

GFlow Engine 是基于 RuleGo 的可嵌入式工作流（审批流）引擎。本目录是面向集成者的文档，按需阅读：

| 文档 | 内容 | 适合谁 |
|---|---|---|
| [README.md](../README.md) / [README_ZH.md](../README_ZH.md) | 安装、数据库初始化、快速开始、IdentityService 接入、扩展点 | 所有集成者（从这里开始） |
| [components.md](components.md) | 9 种流程节点的机制参考：注册与生命周期、审批模式、驳回策略、httpCall SSRF 防护 | 引擎集成者 |
| [dsl-reference.md](dsl-reference.md) | 流程 DSL 编写参考：顶层结构（ruleChain/metadata/additionalInfo）、逐节点 configuration 字段表与 JSON 示例、组件清单接口 | 流程设计器实现方、DSL 编写者 |
| [task-lifecycle.md](task-lifecycle.md) | 任务状态机、认领/转办/委托/完成操作、会签父子结构、历史归档 | 待办/审批界面实现方 |
| [events.md](events.md) | 任务与实例生命周期事件目录、监听器注册、异步派发语义 | 通知/审计/推送实现方 |
| [persistence.md](persistence.md) | 7 张数据表的职责与关键字段、运行/历史表分离、多租户与多实例部署 | 后端与 DBA |
| [parallel-limitations.md](parallel-limitations.md) | fork/inclusive/join 并行网关的支持范围、硬失败条件、ForceResumeInstance 运维兜底 | 使用并行流程的所有人 |
| [migration.md](migration.md) | 数据库初始化与升级指南：全新安装、schema 兼容性承诺与演进策略 | 部署/集成的所有人 |

REST API 说明：引擎是纯 Go 库，不暴露 HTTP 接口；宿主应用基于 `service` 层自行定义 REST 契约（参考实现见 GFlow Platform 的 `/api/v1/workflow/*`）。
