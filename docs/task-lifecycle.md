# GFlow Engine 任务生命周期与操作指南

任务是上层应用与引擎交互的主要对象：待办列表、审批按钮、转办委托、意见评论都围绕任务展开。本文说明任务的状态机、操作入口（`service.TaskService`）与会签结构，供宿主应用集成参考。

## 任务状态

| 状态 | 含义 |
|---|---|
| `created` | 已创建（数据库默认状态） |
| `assigned` | 已分配（签收/转办后） |
| `waiting` | 待激活（保留值，当前版本引擎不会产生） |
| `pending` | 待领取（角色/部门候选任务的公共池状态） |
| `active` | 处理中 |
| `delegated` | 已委托 |
| `completed` | 完成 |
| `returned` | 退回 |
| `withdrawn` | 已撤回：发起人撤回已提交的申请（`Withdraw`/`WithdrawByInstance`） |
| `suspended` | 挂起 |
| `terminated` | 终止 |

终态为 `completed` / `returned` / `withdrawn` / `terminated`，其余为运行态。减签是 `AddSign`/`ReduceSign` 操作（动作），不是任务终态。审批结果记录在 `end_reason` 字段（`approved` / `rejected` / `returned` / `delegated` / `pending`，见 `types/enums.ApprovalResult`）。

实例（`wf_instance.status`）状态全集为 `draft` / `active` / `completed` / `suspended` / `terminated` / `cancelled` / `failed` / `deleted`（`deleted` 仅出现在归档表 `wf_hi_instance`，活表不落该值），状态迁移合法性由 `enums.CanTransitionInstanceStatus` 校验。

## 任务类型与审批模式

- `task_type`：`userTask`（审批）、`ccTask`（抄送，创建即 `completed`，`end_reason="cc"`）等；
- `approval_type`：`single`（单人）/ `or`（或签）/ `sequential`（顺序）/ `countersign`（会签）/ `vote`（票签），语义详见 [components.md](components.md) 的 userTask 章节。

## 会签/票签的父子结构

`countersign` / `vote` 复用同一结构：一条**主任务**（`parent_id` 为空）+ 每个审批人一条**子任务**（`parent_id` 指向主任务，`sequence_order` 记录序号）。阈值判定（`approval_rule`：all/any/majority/percent/count）在 service 层 `CheckCountersignSubTaskCompletion` 完成；达到阈值后剩余子任务被终止。

## 常用操作（TaskService）

所有变更类操作的第一个业务参数都是显式的 `service.Actor`（操作人身份：UserID/UserName/TenantID），
引擎据此做受理人/候选人校验与租户匹配。下面代码块统一以 `actor` 表示，不再逐行标注：

```go
actor := service.Actor{UserID: "user_001", TenantID: "t1"}
```

### 认领（候选任务）

角色/部门候选的任务先落库为 `pending`（无 assignee，候选组在 `wf_task_assignee`），成员认领后展开为个人任务：

```go
err := engine.GetTaskService().Claim(ctx, actor, taskID)    // 认领
err = engine.GetTaskService().Unclaim(ctx, actor, taskID)   // 取消认领，任务回到公共池
```

### 转办 / 委托 / 归还

```go
SetAssignee(ctx, actor, taskID, userID)       // 转办：直接更换办理人
Delegate(ctx, actor, taskID, userID, reason)  // 委托：办理权交出，owner 保留原办理人
Resolve(ctx, actor, taskID)                   // 委托归还：任务回到 owner
```

### 完成 / 审批

```go
Complete(ctx, actor, taskID, variables)                 // 完成（可携带新变量）
Approve(ctx, actor, taskID, comment, variables)         // 同意（记录意见）
Reject(ctx, actor, taskID, comment, variables)          // 拒绝（触发节点驳回策略）
CompleteWithApproval(ctx, actor, &ApprovalRequest{...}) // 完成 + 审批结果一步提交
```

最后一个审批人完成后，节点自动推进（内部经 RuntimeService.ExecuteNext 触发下游节点）。

### 变量

```go
GetTaskVariables(ctx, actor, taskID)             // 读取任务变量（快照自查）
SetTaskVariables(ctx, actor, taskID, variables)  // 写入任务变量
```

任务变量是流程变量在任务创建时刻的快照（含顺序审批的 `_sequentialAssignees` 缓存），供办理页回显；流程的真实变量以 `wf_instance.variables` 为准，完成审批时合并回写。

### 挂起 / 恢复 / 其他

```go
SuspendTask(ctx, actor, taskID) / ActivateTask(ctx, actor, taskID)         // 任务级挂起/恢复
SetPriority(ctx, actor, taskID, priority) / SetDueDate(ctx, actor, taskID, dueDate)
```

实例级挂起/恢复走 `RuntimeService.SuspendProcessInstance` / `RestoreProcessInstance`（后者需显式 actor，恢复前校验实例租户归属）。

## 历史归档

任务与实例完结后归档到 `wf_hi_task` / `wf_hi_instance`（运行表删除、历史表保留，见 [persistence.md](persistence.md)）。查询历史：

```go
engine.GetTaskService().GetHistoryTasksByProcessInstanceID(ctx, instanceID)
engine.GetTaskService().GetHistoryTask(ctx, taskID)
```

处理意见/comments 走 `wf_task_comment`。

## 事件联动

认领、完成、拒绝、候选创建等操作会派发任务事件（assigned / completed / rejected / candidateCreated / cc），宿主通过 `TaskEventListener` 消费，用于通知、审计、WebSocket 推送——完整目录见 [events.md](events.md)。

## 驳回后的任务清理

驳回回跳（`rejectToStarter` / `rejectToPrev` / `rejectToNode`）会调用 `SupersedeNodeTasks` 归档跳转涉及节点上一轮的终态任务，保证目标节点重入时重新生成待办而不是读到旧记录。管理员处理卡死实例可用 `ForceResumeInstance`（见 [parallel-limitations.md](parallel-limitations.md)）。
