# 并行流程（Fork / Inclusive / Join）限制与支持

引擎对并行网关的支持范围、已知限制与管理员兜底手段。

## 支持的拓扑

- **fork → join（并行分支）**：全部分支到达 join 后聚合推进。
- **inclusive（包容分支）**：包容分支的暂停/恢复与 fork 行为一致
  （`analyzeForkResume` 对两者做同样的拓扑分析）。
- **分支内含暂停节点（userTask 等）**：`ExecuteNext` 检测到 startNodeId 处于
  `fork → suspend node(s) → join` 拓扑时，改走 multi-node 恢复路径
  （与 `RestoreProcessInstance` 同路径），通过 LCA 重建 fork 父上下文，
  避免 join 的 `TellCollect` 丢失父上下文而永远收不齐分支消息。

## 限制清单

### hard fail（返回错误，不静默降级）

- **嵌套 fork**（fork 分支内再 fork）：`analyzeForkResume` 无法确定唯一的
  恢复根节点，`ExecuteNext` 返回错误并记录 `unsupported fork topology`。
- **分支无暂停节点**：拓扑分析要求每个分支存在可恢复的暂停节点，
  不满足时同样显式报错。

### 已知功能限制

- 或签/会签与并行的组合语义以任务层（`cancelSiblingActiveTasks` /
  `cancelRemainingCountersignSubTasks`）为准，join 聚合不感知任务层阈值终止。
- `forkResumeSkip` 分支：非最后一个兄弟节点完成时不触发恢复，等待最后一个
  分支的 `ExecuteNext` 统一触发（日志 INFO 级，便于排查"用户都 approve 了
  实例还在 active"）。

## 管理员兜底：ForceResumeInstance

实例因上述限制或历史数据问题卡死时，管理员可调用：

```go
// actor 为操作人身份（service.Actor{UserID, TenantID, ...}），运维兜底建议传管理员身份
err := engine.GetRuntimeService().ForceResumeInstance(ctx, actor, processInstanceID)
```

它绕过拓扑分析直接按 multi-node 路径强制恢复实例。仅用于运维兜底，
正常流转不应依赖它。

## 排查入口

- `RuntimeServiceImpl.ExecuteNext` 中 `analyzeForkResume` 的三级日志
 （multi-node restore / skip / unsupported topology）。
- 卡死实例可配合 `GetStuckProcessInstances` 定位后用 ForceResumeInstance 恢复。
