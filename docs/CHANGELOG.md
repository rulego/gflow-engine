# 更新日志（Changelog）

## [v1.2.0] - 2026-09-14

- feat: 重构审批节点配置——审批人、审批方式（依次/会签/或签）、审批人为空策略、
  驳回与退回策略统一在一处配置，设计器与引擎语义对齐
- feat: 后续审批人预测，发起与审批前可预览流程后续各节点的办理人
- feat: 审批人是发起人时自动通过，单人/会签/顺序审批全支持
- feat: 驳回/退回目标限上游节点、禁跨并行分支
- feat: 实例状态桶计数与列表同口径，已办/我的申请支持按实例状态筛选，
  拒绝撤回按 end_reason 前缀识别
- feat: AI 节点支持附件图片/文档送识别，多模态消息组装与附件开关拆分
- feat: 租户成员归属校验收口 TenantMembershipGuard，支持批量校验与装配期严格开关
- refactor: 鉴权口径收口 operator_authz 单一模块，Actor.SuperAdmin 更名 WorkflowAdmin
- fix: 任务签收链路加固——候选可见性、详情放行、claimed_at 清理、assignee 残留
- fix: 全链路补齐属主/租户/管理员鉴权，修复任务操作、历史归档、实例变量、
  任务评论等场景的横向越权
- fix: 优化多实例部署一致性
- fix: 会签/或签边界状态守卫——加签/减签/终止/失败路径状态一致，
  或签并发审批不再复活已终止任务，any 会签首票拒绝不再一票否决
- fix: 已终止实例不再捞回待办，退回动作归入已办；删除已归档实例同步标记历史行；
  委派归还补审批意见留痕与归还事件；实例联合查询默认排除软删除
- fix: aiAgent 结果落实例变量并掺输入指纹，实例重驱动不再重复调用模型
- chore: rulego 升级至 v0.37.2

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
