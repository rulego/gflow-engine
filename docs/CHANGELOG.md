# 更新日志（Changelog）

## [v1.0.0] - 2026-08-31

- feat: 流程 DSL 多版本管理——draft → active 生命周期、同 processKey 版本族、
  旧版本 retire；删除仍有活跃实例的定义返回冲突错误
- feat: 多租户隔离贯穿定义、实例、任务与历史；归属一律取操作者租户，
  载荷中的 `TenantID` 不作为归属依据
- feat: 按 key / ID 发起实例（`...StartOption` 可变参数，草稿用 `WithDraft()`）；
  挂起/恢复、终止、重启（保留原实例、派生新实例）
- feat: 子流程嵌套：继承父流程业务变量，失败沿声明的 Failure 边传播
- feat: 并行/包容网关的断点恢复；卡死实例发现与重驱动（fork 图缓存支持跨副本失效）
- feat: 实例级行锁（`FOR UPDATE`）与重入门闸，并发审批不会重复驱动同一链路
- feat: 任务操作全集——认领、完成、委派、转办、改派、加签/减签、退回、撤回、
  挂起/激活；重复提交返回幂等错误
- feat: 会签/票签五种阈值规则（全部通过、任一通过、多数决、百分比、固定票数），
  达成后提前终止剩余子任务
- feat: 候选人池（候选人 / 角色 / 部门），待认领 → 认领 → 审批完整链路
- feat: 全部操作基于显式 Actor 身份校验（受理人/候选人 + 租户匹配）；
  节点级 actionPermissions 可由设计器禁用回退、加签等操作
- feat: `RegisterFromEngine(engine, opts...)` 一步装配全部 BPM 节点
- feat: `userTask` 审批节点——单人/会签/票签/候选人/发起人自选，拒绝策略与
  退回目标可配，表单挂接（formKey）
- feat: `aiAgent` AI 审批节点——LLM 决策路由通过/拒绝分支，失败降级人工兜底
- feat: `start` / `endNode` / `serviceTask` / `httpCall`（内置 SSRF 防护）/
  `ccTask`（动态名单）/ `subProcess`（同步、异步可选）/ `startProcess`
- feat: 任务创建/完成/认领等事件异步派发，抄送事件回调承接上层通知
- feat: 待办/已办/超期等多维计数与完成趋势、分类分布、时长明细统计；
  运行数据归档至独立历史表
- feat: 基于 GORM 的 PostgreSQL / MySQL / SQLite（内存库零依赖上手）
- feat: 自定义数据库方言注册（附达梦接入示例）与建表初始化脚本
- feat: `IdentityService` SPI 解析用户/角色/部门/主管关系；分布式锁经
  `lock.Locker` 注入（内置本地内存锁）
- feat: 租户校验强制化——跨租户的列表、操作与归档删除一律拒绝；
  全量恢复仅限系统身份或 SuperAdmin
- feat: httpCall SSRF 防护：scheme/主机白名单、拨号期 IP pin 封死
  DNS rebinding、重定向逐跳复检、响应体上限
- feat: 全部 BPM 节点 OnMsg panic 兜底，转 Failure 边而非打穿执行器
