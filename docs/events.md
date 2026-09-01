# GFlow Engine 事件钩子对接指南

引擎通过**事件监听器**向上层应用暴露流程生命周期钩子。上层注册监听器即可感知审批/流转/状态变更，自行实现审计日志、消息通知、外部系统同步等扩展。

## 注册方式

```go
engine, err := service.NewWorkflowEngineBuilder().
    SetName("my-app").
    SetConfig(cfg).
    SetIdentityService(myIdentityService).          // 生产必填
    SetTaskEventListener(myTaskEventListener).       // 任务/实例生命周期事件
    SetCCTaskCreatedListener(myCCListener).          // 抄送事件（可选）
    RequireIdentityService().
    Build()

// 之后经 components.RegisterFromEngine(engine) 装配组件：
// 监听器自动从引擎取到并注入节点侧，无需（也不能）重复传参。
```

> ⚠️ **双重注册提醒**：`RegisterFromEngine` 会把 Builder 注入的监听器自动带到节点侧，两侧天然同一实例。只有用底层 `components.Register(ComponentDeps{...})` 手工装配时才需自行传入同一监听器（节点内部持有该引用）——漏传时节点侧事件（assigned/candidateCreated/rejected/CC）不会派发，且因 rulego registry 首注册占坑而非常隐蔽——务必检查启动日志中的监听器注入告警。

**多监听器**：`SetTaskEventListener` 之后可继续 `AddTaskEventListener`（CC 事件同理）追加监听器，按注册顺序依次调用——通知、审计、WebSocket 推送可分模块各自注册，互不覆盖。

## 派发语义（所有事件一致）

- **异步**：监听器在新 goroutine 中执行，慢 IO 不会阻塞流程推进；但仍建议监听器快速返回并自行管理重试。
- **panic 隔离**：监听器 panic 被 recover 并记录日志，不影响引擎主流程。
- **取消剥离**：派发给监听器的 ctx 经 `context.WithoutCancel` 处理——事务提交/回滚取消原 ctx 不会打断监听器，但保留了租户/用户等值。
- **尽力投递**：事件为进程内 fire-and-forget，无持久化。监听器写库失败需自行告警/补救；进程崩溃时未执行的事件会丢失（需要不丢失保证请在上层引入 outbox 类机制）。

## 任务事件（TaskEvent）目录

| 类型 | 触发时机 | TaskID | TaskDefKey | FromUser | Reason | 说明 |
|---|---|:-:|:-:|:-:|:-:|---|
| `started` | 流程实例发起成功（非草稿） | ✗ | ✗ | ✅ 发起人 | ✗ | 首节点任务的 `assigned` 随后派发 |
| `assigned` | 任务分配：userTask 创建、顺序流转下一人、会签子任务激活 | ✅ | ✅ | ✅ 尽力 | ✗ | 会签子任务带 `ParentTaskID`；加签即此类型 |
| `candidateCreated` | 候选任务创建（角色/部门/岗位候选组） | ✅ | ✅ | ✅ 尽力 | ✗ | `ToUsers` 为展开后的候选成员 |
| `claimed` | 候选任务被认领 | ✅ | ✅ | ✅ 认领人 | ✗ | `ToUsers` 为其他候选成员 |
| `unclaimed` | 取消认领，任务回到候选池 | ✅ | ✅ | ✅ 操作人 | ✗ | `ToUsers` 为其他候选成员 |
| `approved` | 审批通过（仅 API 审批路径） | ✅ | ✅ | ✅ 审批人 | ✅ 审批意见 | 发起人自审不派发 |
| `rejected` | 审批驳回：terminate 与 jump 回退路径均派发 | 尽力 | ✅ | ✅ 驳回人 | ✅ | `Reason` 区分终止/回退至发起人/上一节点/指定节点 |
| `forwarded` | 转办/委托 | ✅ | ✅ | ✅ 操作人 | ✅ | `ToUsers` 为新处理人 |
| `resolved` | 委派归还（任务回到原 owner） | ✅ | ✅ | ✅ 操作人 | ✗ | `ToUsers` 为原 owner |
| `addSign` | 加签 | ✅ | ✅ | ✅ 操作人 | ✅ | `ToUsers` 为被加签人 |
| `reduceSign` | 减签 | ✅ | ✅ | ✅ 操作人 | ✅ | `ToUsers` 为被移除的审批人 |
| `returned` | 任务退回到指定节点 | ✅ | ✅ | ✅ 操作人 | ✅ | 退回后由 ExecuteNext 在目标节点重新派生任务 |
| `terminated` | 流程终止（含级联） | ✗ | ✗ | ✅ 尽力 | ✅ | **看 `Source` 归因**：api / withdraw / reject |
| `withdrawn` | 发起人撤回实例 | ✅ | ✅ | ✅ 撤回人 | ✅ | 随后会有一条 `Source=withdraw` 的 terminated |
| `completed` | 流程实例完成 | ✗ | ✗ | ✅ 尽力 | ✗ | `ToUsers` 为发起人 |
| `suspended` | 实例挂起（含级联任务挂起） | ✗ | ✗ | ✅ 操作人 | ✗ | `ToUsers` 为发起人 |
| `activated` | 实例恢复 / 草稿激活 | ✗ | ✗ | ✅ 操作人 | ✗ | 同上 |

"尽力" = 依赖调用链 ctx 携带 Identity，引擎内部驱动（恢复/重放）时为空。

### 字段说明

| 字段 | 用途 |
|---|---|
| `EventID` | 每条派发的事件唯一非空，**幂等去重与跨系统追踪的锚点** |
| `TaskDefKey` | 任务定义节点 key（设计期稳定标识），按节点聚合审批轨迹用 |
| `FromUser` | 触发操作的**用户 ID**（不含姓名，上层按需查用户表） |
| `ToUsers` | 通知接收人；assigned/candidateCreated 语义为被分配人/候选人群 |
| `Source` | 事件来源：`api`（默认）/ `withdraw` / `reject` / `internal`，主要用于 terminated 归因 |
| `ProcessName` | 引擎不填充，上层按需回查流程定义 |

### ctx 携带信息

派发给监听器的 ctx 保留了链路值：`service.GetUserFromCtx(ctx)` 可取 `*Actor`（字段 `UserID` / `UserName` / `TenantID`，见 service/identity.go）。事件载荷本身已含 TenantID/FromUser，通常无需再读 ctx。

## 抄送事件（CCEvent）

| 字段 | 说明 |
|---|---|
| `TaskID` / `ProcessInstanceID` / `ProcessID` / `TenantID` | 定位信息 |
| `AssigneeUserID` | 抄送对象 |
| `TaskName` / `CreatedAt` | 展示信息 |

派发语义与任务事件一致（异步、panic 隔离）。

## 对接示例：写审计日志

```go
auditSvc := service.NewAuditService(db)
listener := service.TaskEventListener(func(ctx context.Context, evt service.TaskEvent) {
    auditSvc.Write(ctx, &AuditLog{
        EventID:    evt.EventID, // 唯一 ID，审计表可对其建唯一索引实现幂等
        TenantID:   evt.TenantID,
        OperatorID: evt.FromUser,
        Action:     string(evt.Type),
        TargetType: "task",
        TargetID:   evt.TaskID,
        InstanceID: evt.InstanceID,
        Detail:     fmt.Sprintf("节点 %s %s：%s", evt.TaskDefKey, evt.Type, evt.Reason),
    })
})
```

处理 `terminated` 时按 `Source` 区分真实动作：

```go
case service.TaskEventTerminated:
    switch evt.Source {
    case service.EventSourceWithdraw: // 撤回级联，withdrawn 事件已单独派发，通常跳过
    case service.EventSourceReject:   // 驳回策略 terminate 的级联
    default:                          // API 直接终止
    }
```
