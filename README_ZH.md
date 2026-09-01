# GFlow Engine

[![GoDoc](https://pkg.go.dev/badge/github.com/rulego/gflow-engine)](https://pkg.go.dev/github.com/rulego/gflow-engine)
[![Go Report](https://goreportcard.com/badge/github.com/rulego/gflow-engine)](https://goreportcard.com/report/github.com/rulego/gflow-engine)

[English](README.md)| 简体中文

> **GFlow** · 审批如风，极速流转
>
> **GFlow Engine**——GFlow 产品家族的核心，可嵌入的审批工作流引擎（开源版）。
> 以 Go 库形式发布，不含界面、不提供 HTTP 服务，由宿主应用集成驱动。
>
> 需要开箱即用？**GFlow Platform（极风工作流）** 即 GFlow 企业版，
> 提供流程设计器、表单设计器、审批界面与 AI 审批。
> 官网 <https://gflow.rulego.cc/> · 文档 <https://gflow.rulego.cc/> · 在线演示 <http://8.134.32.225:8081>（`admin` / `admin123`）

`GFlow Engine` 是一个基于 [RuleGo](https://github.com/rulego/rulego) 的轻量级、可嵌入审批工作流引擎。流程定义复用 `RuleGo` 规则链 DSL（JSON），审批任务、流程实例、历史归档等状态由引擎持久化到关系数据库，无需部署独立的流程中间件。

> 注意：DSL 为 JSON 格式的类 BPMN 审批流，不解析 BPMN 2.0 XML。

## 特性

* **规则链即流程：** 流程 DSL 复用 `RuleGo` 规则链，网关（`switch` 条件分支）、并行分支（`fork`/`inclusive`/`join`）等原生节点可直接编排，流程即规则、规则即流程。
* **完整审批语义：** 或签（or）、会签（并行/顺序，全票/多数）、动态加签/减签、转办、委托、签收/抢单、退回、撤回、挂起/恢复、超时催办。
* **候选组待办：** 任务可按人员/角色/部门发起，候选人池（`wf_task_assignee`）独立存储，查询时经 `IdentityService` 展开。
* **子流程：** `subProcess` 节点启动独立子流程实例，嵌套审批闭环。
* **与规则引擎联动：** `serviceTask` 调用 Go 函数，`automation` 节点调用 `RuleGo` 规则链，`aiAgent` 节点对接智能体规则链（[rulego-components-ai](https://github.com/rulego/rulego-components-ai)），`httpCall` 节点同步调用外部接口。
* **运行时/历史双表：** 进行中数据与归档数据分离，报表和审计查询不拖累运行时。
* **多租户：** 全链路 `tenant_id` 隔离，规则链执行池按租户划分。
* **可插拔身份体系：** 实现 `IdentityService` 对接真实的用户/角色/部门数据（按角色、部门、组、多级主管解析审批人）；内置 Mock 实现仅用于测试。
* **可插拔数据库方言：** 内置 PostgreSQL / MySQL，通过 `DialectProvider` 可扩展其它数据库（SQLite、达梦、人大金仓等，见 examples 目录）。
* **流程定义版本化：** 同一 `process_key` 按 `version` 递增保留多个发布版本，存量实例继续运行旧版本。
* **审批意见：** 评论存于 `wf_task_comment`，任务归档后仍可读写；审批动作与意见在同一事务落库。
* **事件钩子：** 17 个任务全生命周期事件（发起/分配/候选创建/认领/取消认领/通过/驳回/转办/加签/减签/委派归还/退回/撤回/挂起/恢复/完成/终止）均在事务提交后派发；构建器支持注册一个或多个监听器。平台级钩子：跨租户逾期扫描（`ScanOverdueTasks`）、批量可认领实例判断（`GetClaimableInstanceIDs`）。
* **轻量部署：** 无必须的外部中间件（内置本地内存锁，分布式锁可插拔），适合嵌入现有应用。

## 安装

```bash
go get github.com/rulego/gflow-engine
```

要求 Go 1.24+、RuleGo v0.37+。数据库表初始化脚本为 `scripts/00.init_bpm_pg.sql`（PostgreSQL）与 `scripts/00.init_bpm_mysql.sql`（MySQL），初始化与升级说明见 [docs/migration.md](docs/migration.md)。

## 初始化数据库

引擎只维护自己的工作流表，用户/角色/部门等系统表由宿主应用负责。

**PostgreSQL：**

```bash
createdb gflow   # 首次使用先建库（或 psql -c "CREATE DATABASE gflow"）
psql -d gflow -f scripts/00.init_bpm_pg.sql
```

**MySQL：**

```bash
mysql -u root -p -e "CREATE DATABASE gflow DEFAULT CHARACTER SET utf8mb4"
mysql -u root -p gflow < scripts/00.init_bpm_mysql.sql
```

建表清单：`wf_process`（流程定义）、`wf_instance` / `wf_hi_instance`（实例运行时/历史）、`wf_task` / `wf_hi_task`（任务运行时/历史）、`wf_task_assignee`（候选人池）、`wf_task_comment`（审批意见）。

> 初始化脚本为幂等设计（`CREATE TABLE IF NOT EXISTS`），在已有库上重跑不会删除/改写任何数据。单元测试使用 SQLite 内存库，无需执行脚本；可选的真实库行锁测试（`TestWithInstanceTx_RealDB_ForUpdateSerializes`）仅在设置 `TEST_PG_DSN` 或 `TEST_MYSQL_DSN`（指向任意现存库）时运行，会自建并清理 `gflow_locktest` 临时库。

## 快速开始

```go
package main

import (
	"context"
	"log"

	"github.com/rulego/gflow-engine/components"
	"github.com/rulego/gflow-engine/config"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
)

func main() {
	ctx := context.Background()

	// 1. 数据库配置并启动引擎
	cfg := &config.Config{
		Database: &config.DatabaseConfig{
			Driver: "postgres",
			Dsn:    "host=127.0.0.1 user=postgres password=postgres dbname=gflow port=5432 sslmode=disable",
		},
	}
	engine, err := service.NewWorkflowEngineBuilder().
		SetName("demo").
		SetConfig(cfg).
		SetIDGenerator(service.NewIDGenerator()).
		Build()
	if err != nil {
		log.Fatalf("build engine: %v", err)
	}
	if err := engine.Start(ctx); err != nil {
		log.Fatalf("启动引擎失败: %v", err)
	}
	defer engine.Stop(ctx)

	// 2. 注册工作流节点组件（userTask/serviceTask/automation/...）
	//    依赖由引擎自取；服务函数经 WithServiceFuncs 随引导注册（元数据进
	//    设计器目录，实现进运行时，用法见 docs/components.md）。
	//    注意：必须先 engine.Start 成功，否则注册会报错拦截。
	if err := components.RegisterFromEngine(engine); err != nil {
		log.Fatalf("注册组件失败: %v", err)
	}

	// 3. 部署流程定义（DSL 为 rulego 规则链 JSON，见 examples/leave_approval）。
	//    所有变更类操作的第一个业务参数都是显式 actor——用于审计与权限校验的操作人。
	admin := service.Actor{UserID: "admin", UserName: "admin", TenantID: "default"}
	_, err = engine.GetProcessService().Deploy(ctx, admin, &model.WfProcess{
		ProcessKey:     "leave_approval",
		Name:           "请假审批",
		DefinitionJSON: leaveApprovalDSL, // 流程 DSL JSON
		TenantID:       "default",
		CreatedBy:      "admin",
	}, true)
	if err != nil {
		log.Fatalf("部署流程失败: %v", err)
	}

	// 4. 发起流程实例（追加可变参数 service.WithDraft() 即草稿模式；普通启动不传）
	instanceID, err := engine.GetRuntimeService().StartProcessInstanceByKey(
		ctx,
		service.Actor{UserID: "emp001", UserName: "张三", TenantID: "default"},
		"leave_approval",
		"leave_emp001_1", // 业务键
		map[string]interface{}{"days": 5, "managerId": "mgr001", "reason": "家中事务"},
	)
	if err != nil {
		log.Fatalf("发起流程失败: %v", err)
	}
	_ = instanceID
}
```

审批人处理待办（需额外引入 `github.com/rulego/gflow-engine/types/dto` 与 `.../types/enums`）：

```go
tasks, _, err := engine.GetTaskService().GetTaskList(ctx, service.Actor{
	UserID:   "mgr001",
	TenantID: "default",
}, &dto.TaskQuery{
	Assignee: "mgr001",
	PageRequest: dto.PageRequest{
		Status:   []string{string(enums.TaskStatusPending), string(enums.TaskStatusActive)},
		PageSize: 10,
	},
})
if err != nil || len(tasks) == 0 {
	return
}

err = engine.GetTaskService().CompleteWithApproval(ctx, service.Actor{
	UserID:   "mgr001",
	TenantID: "default",
}, &service.ApprovalRequest{
	TaskID:         tasks[0].ID,
	ApprovalResult: enums.ApprovalResultApproved,
	Comment:        "同意",
})
```

> **安全说明——操作人由显式 `actor Actor` 参数传入：**
> 所有审批/变更类操作（`CompleteWithApproval`/`Approve`/`Reject`/`Claim`/`Unclaim`/
> `Transfer`/`Reassign`/`Return`/`Withdraw`/`WithdrawByInstance`/`DeleteTask` 等）的
> 操作人都是显式 `actor service.Actor` 参数，引擎以 `actor.UserID` 校验 assignee 权限。
> 没有任何操作人身份的调用会以 `ErrAuthenticationRequired` 拒绝；操作人不是任务
> 办理人则以 `ErrPermissionDenied` 拒绝。`Actor` 必须由宿主服务端从认证层
> （session/token）构造，绝不直接透传客户端输入。

完整可运行示例（单签、并行会签、顺序会签）见 [examples/leave_approval](examples/leave_approval)——默认跑在内存 SQLite 上，零依赖直接运行（`GFLOW_DSN` 可切 PostgreSQL/MySQL）；`httpCall` + `switch` 组合示例（查询外部接口 → 响应映射进流程变量 → 按结果路由）见 [examples/http_call](examples/http_call)。

## 接入组织架构（IdentityService）

引擎不绑定任何用户体系。按角色/部门/主管发起的审批任务，办理人统一通过 `service.IdentityService` 接口解析——生产环境必须注入宿主应用自己的实现（对接真实的用户/角色/部门表），内置的内存 Mock 仅供测试：

```go
// 实现 service.IdentityService 的全部 8 个方法，对接你自己的组织架构表
type OrgIdentityService struct {
	db *gorm.DB // 宿主应用数据源
}

// 按角色查用户（role 候选任务展开），其余方法同理
func (s *OrgIdentityService) GetUserIDsByRoleID(ctx context.Context, tenantID, roleID string) ([]string, error) {
	var userIDs []string
	err := s.db.WithContext(ctx).
		Table("user_roles").
		Where("tenant_id = ? AND role_id = ?", tenantID, roleID).
		Pluck("user_id", &userIDs).Error
	return userIDs, err
}

// 其余待实现方法与用途：
//   GetUserIDsByDepartmentID        按部门查用户（dept 候选任务）
//   GetDepartmentManagerUserID      查部门主管（dept 候选任务）
//   GetUserManagerID                查直接主管（direct_manager 候选任务）
//   GetUserManagerHierarchy         查多级主管（multi_level_manager 候选任务）
//   GetUserDepartmentID             按用户反查部门
//   GetRoleIDsByUserID              按用户反查角色（role 候选任务的待办可见性）
//   GetUserIDsByGroupID             按自定义组查用户（预留扩展）
```

通过 Builder 注入：

```go
engine, err := service.NewWorkflowEngineBuilder().
	SetName("demo").
	SetConfig(cfg).
	SetIdentityService(&OrgIdentityService{db: gormDB}).
	Build()
```

候选人配置（`candidateType`）与接口方法的对应关系：

| candidateType | 解析用的接口方法 |
|---|---|
| `user` | 无需身份服务（`candidateUsers` 直接给用户 ID） |
| `role` | `GetUserIDsByRoleID`；待办可见性走 `GetRoleIDsByUserID` |
| `dept` | `GetUserIDsByDepartmentID` / `GetDepartmentManagerUserID` |
| `direct_manager` | `GetUserManagerID` |
| `multi_level_manager` | `GetUserManagerHierarchy` |
| `initiator_select` / `initiator_self` | 无需身份服务（发起人自选 / 发起人本人） |

## 流程 DSL

流程定义是一条 `RuleGo` 规则链：`nodes` 描述节点，`connections` 描述流转边，表单和审批配置写在节点 `configuration` 中。BPM 扩展节点如下：

| 节点 type | 说明 |
|---|---|
| `startTask` | 发起节点：流程起点标记，不做鉴权 |
| `userTask` | 用户审批任务：或签/会签、候选人（人员/角色/部门）、表单（`formKey` 透传到 `wf_task.form_key`）、优先级、截止时间；`configuration.taskName`/`taskDescription` 可覆盖节点名与描述 |
| `ccTask` | 抄送任务：生成抄送记录并通过 `CCTaskCreatedListener` 回调宿主应用 |
| `serviceTask` | 服务任务：调用 Go 函数（经 `action.Functions` 注册） |
| `automation` | 自动化节点：调用 `RuleGo` 规则链 |
| `subProcess` | 子流程：启动独立子流程实例，结束后回到主流程 |
| `aiAgent` | AI 智能体节点：调用智能体规则链并路由输出 |
| `httpCall` | HTTP 调用：同步请求外部接口，响应按映射合并进流程变量 |
| `startProcess` | 规则链专用：链内发起 BPM 流程实例（如定时自动发起审批） |

`RuleGo` 原生节点（`switch` 条件分支、`fork`/`inclusive`/`join` 并行汇聚等）可直接参与编排，详见 [RuleGo 标准组件](https://rulego.cc/pages/standard-components/)。

各节点的完整参考（配置字段、审批模式、驳回策略、`httpCall` SSRF 防护）见 [docs/components.md](docs/components.md)。面向集成者的完整文档（任务生命周期、事件、数据模型）索引见 [docs/README.md](docs/README.md)。

## 扩展点

* **身份服务：** 实现 `service.IdentityService`，按角色/部门/组/多级主管解析审批人。
* **数据库方言：** 实现 `service.DialectProvider` 注册新数据库，见 [examples/custom_dialect](examples/custom_dialect)（达梦、人大金仓）。
* **分布式锁：** 引擎内置本地内存锁（`lock.NewLocalLock`）；多实例部署请自行实现 `lock.Locker` 接口（如基于 Redis 的 SET NX + Lua 脚本释放），并经 `WorkflowEngineBuilder.SetLocker` 注入。
* **任务事件：** `TaskEventListener` / `CCTaskCreatedListener` 接收任务生命周期事件，驱动站内通知等副作用；完整事件目录、载荷字段与对接示例见 [docs/events.md](docs/events.md)。

## 生态

- [RuleGo](https://github.com/rulego/rulego) ：底层规则引擎
- 文档：<https://gflow.rulego.cc/>
- GFlow Engine 源码：[Gitee](https://gitee.com/rulego/gflow-engine) · [GitHub](https://github.com/rulego/gflow-engine)
- **GFlow Platform**（极风工作流）：GFlow 企业版，基于 GFlow Engine 的开箱即用审批工作流平台（服务端 + 前端 + 设计器），官网 <https://gflow.rulego.cc/>，在线演示 <http://8.134.32.225:8081>

## 联系与商业授权

GFlow Engine 按 Apache-2.0 开源免费使用。如需源码交付、开箱即用的企业版
**GFlow Platform**（极风工作流，含流程/表单设计器、审批界面、AI 审批）或商业支持，
请访问 <https://gflow.rulego.cc/>，或通过以下方式联系（添加烦请注明来意）：

- QQ：[2215016127](tencent://message/?uin=2215016127&Site=&Menu=yes)
- 微信：`rulegoteam`
- 邮件：[rulego@outlook.com](mailto:rulego@outlook.com)

## 许可

`GFlow Engine` 使用 Apache 2.0 许可证，详情请参见 [LICENSE](LICENSE) 文件。
