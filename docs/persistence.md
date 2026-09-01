# GFlow Engine 数据模型与持久化

引擎自带 7 张表，由 GORM 管理，建表 SQL 在 `scripts/00.init_bpm_pg.sql` / `00.init_bpm_mysql.sql`，需在建库后手工执行（或由宿主应用集成到自身的迁移流程）。引擎不管理用户/角色/部门等身份数据——那由宿主应用提供（见 README 的 IdentityService 接入章节）。

## 表清单

| 表 | 用途 | 生命周期 |
|---|---|---|
| `wf_process` | 流程定义（DSL、版本、状态） | 持久 |
| `wf_instance` | 流程实例（运行中的流程） | 完结后归档到 `wf_hi_instance` 并删除 |
| `wf_task` | 任务（运行中的待办） | 完结后归档到 `wf_hi_task` 并删除 |
| `wf_task_assignee` | 任务候选（角色/部门候选组） | 随任务 |
| `wf_task_comment` | 任务处理意见 | 持久 |
| `wf_hi_instance` | 实例历史 | 持久 |
| `wf_hi_task` | 任务历史 | 持久 |

运行表（`wf_instance` / `wf_task`）只存"活的"数据，列表查询天然小；完结数据全部进 `*_hi_*` 历史表，审计与已办列表从历史表查。

## wf_process（流程定义）

- `id` / `name` / `process_key`（业务键，同 key 多版本）/ `version`
- `definition_json`（RuleGo 规则链 JSON，节点+连线+各节点 configuration）
- `status`：`active` / `retired`
- `category` / `description` / `icon` / `publish_time`（版本发布时间）/ `process_type`（main 主流程、sub 子流程）/ `ext`（结构化扩展字段）
- `tenant_id`：多租户隔离
- 子流程通过 `subProcess` 节点的 `targetId` 引用另一条流程定义

## wf_instance（流程实例）

关键字段：

- `process_id` → `wf_process.id`；`business_key` 业务系统单号（可选）
- `status`：`draft` / `active` / `suspended` / `completed` / `terminated` / `cancelled` / `failed`（`deleted` 仅出现在归档表 `wf_hi_instance`，活表不落该值，见 `types/enums.InstanceStatus`）
- `variables`：流程变量 JSON——**表单数据就存在这里**，任务/节点输出合并的目标（合并规则见 components.md httpCall 一节的三规则说明）
- `current_activity`：当前节点定义 ID（监控展示用）
- `parent_id`：父实例 ID，`subProcess`（call activity）子实例指向父实例
- `start_user_id` / `created_by`：发起人；系统触发时为空字符串
- `end_reason` / `ended_at` / `duration`：完结信息；`duration` 为 **BIGINT，单位毫秒**（`wf_instance` 与 `wf_hi_instance` 一致）

## wf_task（任务）

关键字段：

- `process_instance_id` / `task_def_key`（节点定义 ID）/ `task_type`（userTask / ccTask）
- `status` + `assignee`（当前办理人）/ `owner`（委托前原办理人）
- `parent_id` + `sequence_order`：会签/票签的父子结构与序号
- `approval_type` + `approval_rule`：审批模式与阈值规则
- `form_key`：关联表单标识，透传给前端渲染办理页
- `variables`：创建时刻的流程变量快照
- `claimed_at` / `delegate_from` / `due_date` / `priority` / `comment`
- `end_reason`：审批结果（approved / rejected / cc 等）

状态机与操作语义见 [task-lifecycle.md](task-lifecycle.md)。

## wf_task_assignee（任务候选）

角色/部门/个人候选任务的候选组记录（`entity_type` = `person` / `role` / `department` + `entity_id` 列表）。认领（Claim）时据此校验当前用户是否在候选组内，部门候选展开成员由 IdentityService 完成。

## 历史表

`wf_hi_instance` / `wf_hi_task` 与运行表结构一致，任务/实例完结或被 `SupersedeNodeTasks`（驳回回跳清理）时整行迁移，供已办列表与审计查询。各表 `duration` 列均为 **BIGINT（毫秒）**：实例表（`wf_instance`/`wf_hi_instance`）自 v1.0.0 起由 INTEGER 扩为 BIGINT，任务表（`wf_task`/`wf_hi_task`）本就是 BIGINT。

## 与宿主应用的边界

- 引擎只写上述 7 张表；通知、业务单据、附件、评论扩展等由宿主经事件监听器自行落库（参考 [events.md](events.md)）。
- 所有表带 `tenant_id`，查询接口按租户过滤；跨租户访问由宿主在 service 层拦截。
- 多实例部署：流程推进依赖实例级互斥，须注入分布式锁（`WorkflowEngineBuilder.SetLocker`，如 Redis 实现）。
- 换数据库：实现 `service.DialectProvider` 即可扩展方言（内置 PostgreSQL/MySQL，示例见 examples/custom_dialect）。
