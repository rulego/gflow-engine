# 请假审批工作流示例

这个示例展示了如何使用 GFlow Engine 工作流引擎实现一个完整的请假审批流程。

## 流程说明

请假审批流程包含以下步骤：

1. **提交请假申请** - 员工提交请假申请
2. **判断请假天数** - 根据请假天数决定审批路径
   - 3 天以内：直属经理审批（单签）
   - 3~7 天：经理 + HR 并行会签（全部通过）
   - 7 天以上：顺序会签（按 user001 → user002 → user003 顺序，多数通过）
3. **经理审批 / 会签** - 对应审批人处理任务
4. **发送通知** - 发送审批结果通知
5. **结束** - 流程结束

## 流程图

```
[提交请假申请] -> [判断请假天数] -> [经理审批(≤3天)] -> [发送通知] -> [结束]
                              -> [HR并行会签(3~7天)] -> [发送通知] -> [结束]
                              -> [顺序会签(>7天)] -> [发送通知] -> [结束]
```

## 数据结构

### 请假申请 (LeaveRequest)

```go
type LeaveRequest struct {
    EmployeeID   string    // 员工ID
    EmployeeName string    // 员工姓名
    LeaveType    string    // 请假类型：annual(年假), sick(病假), personal(事假)
    StartDate    time.Time // 开始日期
    EndDate      time.Time // 结束日期
    Days         int       // 请假天数
    Reason       string    // 请假原因
    ManagerID    string    // 直属经理ID
    HrID         string    // HR ID
}
```

## 数据存储

默认使用**内存 SQLite**（纯 Go 驱动，零依赖开箱即跑，进程退出数据即清空，
演示模式自动建全部工作流表）。设置 `GFLOW_DSN` 环境变量即切换到外部数据库
（`GFLOW_DRIVER` 指定驱动，默认 `postgres`）：

```bash
# 默认：内存 SQLite，无需任何准备
go run . short

# PostgreSQL（先建库建表，引擎不会自动在外部库上建表）
createdb gflow
psql -d gflow -f scripts/00.init_bpm_pg.sql
GFLOW_DSN="host=127.0.0.1 user=postgres password=postgres dbname=gflow port=5432 sslmode=disable" go run . short

# MySQL
mysql -u root -p -e "CREATE DATABASE gflow DEFAULT CHARACTER SET utf8mb4"
mysql -u root -p gflow < scripts/00.init_bpm_mysql.sql
GFLOW_DRIVER=mysql \
GFLOW_DSN="root:root@tcp(127.0.0.1:3306)/gflow?charset=utf8mb4&parseTime=True&loc=Local" \
go run . short
```

> SQLite 演示模式没有 `SELECT ... FOR UPDATE` 行锁，并发审批的完成判定串行化
> 只在 PostgreSQL/MySQL 上成立（单进程顺序演示不受影响）。SQLite 方言的
> 现成实现见 [examples/internal/demo](../internal/demo/demo.go)。

- 用户/角色/部门**不存引擎库**：`mgr001`/`hr001`/`user001` 等 ID 由宿主应用
  或 `IdentityService` 维护，本例使用引擎内置的内存 Mock（仅测试用）

主要表：

| 表 | 内容 |
|---|---|
| `wf_process` | 流程定义（DSL JSON） |
| `wf_instance` / `wf_hi_instance` | 进行中 / 已归档流程实例 |
| `wf_task` / `wf_hi_task` | 进行中 / 已归档任务 |
| `wf_task_comment` | 审批意见 |
| `wf_task_assignee` | 候选人池（role/dept 候选展开） |

## 使用方法

### 1. 初始化工作流引擎

示例代码里这步用的是 `databaseConfig()`（默认内存 SQLite，见「数据存储」一节）；
接入你自己的应用时配置真实数据库（PG/MySQL 需先执行初始化脚本）：

```go
cfg := &config.Config{
    Database: &config.DatabaseConfig{
        Driver: "postgres",
        Dsn:    "host=127.0.0.1 user=postgres password=postgres dbname=gflow port=5432 sslmode=disable",
    },
}

// 用 Builder 构建（NewWorkflowEngine 不会初始化 TaskService/RuntimeService 等依赖）
engine, err := service.NewWorkflowEngineBuilder().
    SetName("LeaveApprovalEngine").
    SetConfig(cfg).
    SetIDGenerator(service.NewIDGenerator()).
    Build()
if err != nil {
    log.Fatalf("Failed to build workflow engine: %v", err)
}
if err := engine.Start(context.Background()); err != nil {
    log.Fatalf("Failed to start workflow engine: %v", err)
}
defer engine.Stop(context.Background())
```

### 2. 注册工作流组件并创建工作流

```go
if err := components.Register(components.ComponentDeps{
    TaskService:     engine.GetTaskService(),
    IdentityService: engine.GetIdentityService(),
    RuntimeService:  engine.GetRuntimeService(),
}); err != nil {
    log.Fatalf("Failed to register components: %v", err)
}

leaveWorkflow := NewLeaveApprovalWorkflow(engine)
```

### 3. 部署流程定义

```go
if err := leaveWorkflow.DeployLeaveApprovalProcess(ctx); err != nil {
    log.Fatalf("Failed to deploy leave approval process: %v", err)
}
```

### 4. 启动流程实例

```go
leaveRequest := &LeaveRequest{
    EmployeeID:   "emp001",
    EmployeeName: "张三",
    LeaveType:    "annual",
    StartDate:    time.Now().AddDate(0, 0, 7),
    EndDate:      time.Now().AddDate(0, 0, 9),
    Days:         3,
    Reason:       "家庭事务",
    ManagerID:    "mgr001",
    HrID:         "hr001",
}

processInstanceID, err := leaveWorkflow.StartLeaveApprovalProcess(ctx, leaveRequest)
```

### 5. 处理用户任务

```go
// 获取用户待办任务
tasks, err := leaveWorkflow.GetUserTasks(ctx, "mgr001")

// 审批任务
if len(tasks) > 0 {
    taskID := tasks[0].ID
    err := leaveWorkflow.ApproveLeaveRequest(ctx, taskID, "mgr001", true, "同意请假")
}
```

> 审批操作必须传操作人 `UserID`，引擎据此做 assignee 校验：
> 无身份拒绝（`ErrAuthenticationRequired`），非办理人拒绝（`ErrPermissionDenied`）。

### 6. 查询流程状态

```go
status, err := leaveWorkflow.GetProcessInstanceStatus(ctx, processInstanceID)
fmt.Printf("Process status: %s\n", status.Status)
```

## 流程变量

流程执行过程中使用的变量：

- `employeeId` - 员工ID
- `employeeName` - 员工姓名
- `leaveType` - 请假类型
- `startDate` - 开始日期
- `endDate` - 结束日期
- `days` - 请假天数
- `reason` - 请假原因
- `managerId` - 直属经理ID
- `hrId` - HR ID
- `status` - 审批状态
- `approved` - 是否通过
- `comment` - 审批意见
- `approvedBy` - 审批人
- `approvedAt` - 审批时间

## 任务类型

### 用户任务 (userTask)
- **经理审批** - 经理审批请假申请
- **HR并行会签** - 经理 + HR 共同审批（并行会签，全部通过）
- **顺序会签** - 多人按顺序审批（majority 过半通过）

### 条件分支 (switch)

- **判断请假天数** - 按请假天数路由，与 GFlow 设计器的"条件分支"节点同款 DSL：

```json
{
  "type": "switch",
  "configuration": {
    "cases": [
      { "case": "msg.days <= 3", "then": "node_manager_approval" },
      { "case": "msg.days > 3 && msg.days <= 7", "then": "node_hr_approval" }
    ]
  }
}
```

`case` 是 rulego EL 布尔表达式（引用流程变量用 `msg.字段名`），按顺序评估、首个命中生效；
`then` 的值必须与对应连接的 `type` 一致；所有 case 未命中时走 `type: "Default"` 的出边
（本例 >7 天的顺序会签分支）。也支持 `jsSwitch` 节点用 JS 脚本返回关系名路由，设计器默认产出 `switch`。

### 服务任务 (serviceTask)
- **发送通知** - 发送审批结果通知（示例注册在 `main()` 的 `action.Functions.Register`）

## 审批行为说明

- **通过**：单签/会签满足规则后，流程走到 `node_notify` → `end`，实例完成。
- **拒绝**：引擎默认策略是**终止整个流程实例**（`rejectStrategy` 默认为 `terminate`），
  DSL 里 userTask 画的 `Failure` 出边仅作为兜底逃生通道，正常拒绝不会走到 `node_notify`。
  如需"驳回后回退/继续"，需在 userTask 节点配置 `rejectStrategy: rejectToStarter/rejectToPrev/rejectToNode`。
- **顺序会签**：`approvalRule: {"isSequential":true,"type":"majority"}` 表示按
  user001 → user002 → user003 顺序审批，严格过半同意即通过，剩余未审任务自动终止。

## 扩展功能

可以根据业务需求扩展以下功能：

1. **多级审批** - 支持多级经理审批（`multi_level_manager`）
2. **会签功能** - 支持多人会签审批（`countersign`）
3. **委托功能** - 支持审批任务委托（`Transfer`）
4. **加签功能** - 支持动态加签审批人（`AddCountersignAssignee`）
5. **退回功能** - 支持审批退回修改（`Return`）
6. **抄送功能** - 支持抄送相关人员（`ccTask` 节点）
7. **超时处理** - 支持审批超时自动处理（`ScanOverdueTasks`）
8. **审批历史** - 记录完整的审批历史（`wf_hi_task`）

## 注意事项

1. 外部库模式（设置了 `GFLOW_DSN`）需先创建 `gflow` 库、执行初始化脚本并确认连接配置正确；默认 SQLite 模式无需任何数据库
2. 确保用户ID在系统中存在（本例的 `mgr001`/`hr001`/`user001` 来自内置 Mock）
3. 流程变量的数据类型要匹配（`days` 需为数字，switch 的 cases 表达式里 `msg.days <= 3` 按数值比较）
4. 生产环境必须通过 `SetIdentityService` 注入对接真实组织架构的实现
5. 异常处理要完善
6. 日志记录要详细

## 运行示例

```bash
cd examples/leave_approval
go run .            # 依次演示三条审批路径（默认内存 SQLite，零依赖）
go run . short      # 只演示 ≤3 天：经理单签
go run . long       # 只演示 3~7 天：经理+HR 并行会签
go run . sequential # 只演示 >7 天：三人顺序会签
```

连接 PostgreSQL/MySQL 见「数据存储」一节的 `GFLOW_DSN` / `GFLOW_DRIVER` 用法。

流程 DSL 在 [dsl.json](dsl.json)，由示例启动时加载部署（`DeployLeaveApprovalProcess`），
可在设计器里打开调整节点/连线后重新运行。

## 同步与异步语义

- **流程启动后的首个任务创建是异步的**（节点在独立 goroutine 中执行）：
  示例里的 `waitAndApprove` 通过轮询等待待办出现（100ms 间隔，10 秒超时），
  并同时监听实例终态——会签达到阈值提前完成时，剩余审批人不会再有待办；
- **审批驱动的后续推进是同步的**：`CompleteWithApproval` 返回时，
  下一个任务/流程分支已经生成；
- **实例完结是异步归档的**：完成后从 `wf_instance` 移入 `wf_hi_instance`；
- 生产系统建议注册 `TaskEventListener`（见 [docs/events.md](../../docs/events.md)）
  用事件推送替代轮询。
