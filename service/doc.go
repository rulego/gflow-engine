// Package service 实现 BPM 运行时：流程部署、实例生命周期、任务操作与
// 历史归档。
//
// # 集成入口
//
// 宿主通过 builder 装配引擎，之后只面向公共接口编程：
//
//	engine, err := service.NewWorkflowEngineBuilder().
//		SetConfig(cfg).
//		Build()
//	// ...
//	err = engine.Start(ctx)
//
//	tasks, total, err := engine.GetTaskService().GetTaskList(ctx, actor, query)
//
// builder 已提供默认实现（本地锁、UUID 生成器、内置数据库方言），需要
// 替换的部件用 Set* 注入。可运行的集成示例见 ../examples：leave_approval
// 是完整审批流，http_call 演示节点级能力。
//
// # 文件结构
//
// 所有文件同属一个包，按职责分四组：
//
//   - 公共 API（宿主只引用这些）：workflow_engine.go、各 *_service.go
//     接口文件、errors.go、identity.go、calling_mode.go、cc_event.go。
//   - 实现：*_service_impl.go 是结构体与构造函数；task_service_*.go、
//     runtime_*.go 按功能拆分方法体。
//   - 基础设施：instance_lock.go、instance_scope.go、
//     create_task_aspect.go、id_generator.go、dialect_registry.go、
//     default_dialects.go、rulechain_executor.go。
//   - 组装根：workflow_engine_impl.go、builder.go、base_service.go、
//     task_service_helpers.go。
//
// 保持单包：TaskService 与 RuntimeService 相互引用很重，拆包只会增加
// 间接层。
//
// # 事务与副作用
//
// 对流程实例的所有变更都走 WithInstanceTx（锁实例行）加 InstanceScope
// （向回调提供 tx 绑定的 DAO：scope.Tasks()、scope.Instances() 等）。
// 回调里拿不到裸的服务 DAO，不会在不知情中绕出事务。
//
// 副作用（ExecuteNext、通知）必须通过 scope.AfterCommit 注册，事务提交
// 成功后才执行。直接在事务里跑，rulego 同步的 OnMsg 会重入
// WithInstanceTx，而外层事务还握着行锁，直接死锁。契约细节见
// instance_lock.go 与 instance_scope.go。
//
// # 操作人（Actor）
//
// 引擎公共 API 的操作人是显式的：凡变更类操作（认领、审批、转办、挂起、
// 终止、部署等）一律以 `actor Actor` 参数显式传入操作人。列表、统计等
// “以谁视角查”的接口同样用 actor 表达视角（含 TenantID 租户范围）；纯按
// ID/键的查询不携带 actor。
//
// Actor（identity.go）携带 UserID/UserName/TenantID 与 SuperAdmin 标记。
// 引擎不含用户体系、不做认证：宿主从认证层（如 JWT）构造 Actor 后显式
// 传参，身份真实性由宿主保证。引擎内部机制代替用户执行的动作（节点自动
// 推进、驳回级联终止、巡检等）传 SystemActor()。
//
// 实现上，接收 actor 的公共方法在入口统一 bindActor：把 actor 绑进请求
// ctx 并标记 API 调用模式，既有内部管道（GetUserFromCtx 读身份、事件派发
// 的 FromUser、IDOR/租户校验）原样工作，不需要感知 API 与内部两种模式。
// 已标记 CallingModeInternal 的 ctx 保持内部模式（引擎内部回调跳过
// assignee 强校验）。反向地，引擎内部回调用 ActorFromCtx 把链执行 ctx 的
// 隐式身份转成显式 actor，取不到时回退 SystemActor 并从链元数据补齐租户。
//
// # 测试
//
// 单元测试与被测文件同目录；锁行为见 instance_lock_test.go；端到端测试
// 在 ../test/e2e。
package service
