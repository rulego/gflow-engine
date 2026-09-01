package service

import (
	"context"

	"github.com/rulego/gflow-engine/utils/lock"
	"gorm.io/gorm"
)

// Version 引擎版本号（单一来源：builder 与 GetVersion 均引用此常量，
// 发布时与 docs/CHANGELOG.md 同步更新）。
const Version = "1.0.0"

// WorkflowEngine 工作流引擎主接口
// 整合所有核心服务，提供统一的工作流引擎访问入口
type WorkflowEngine interface {
	// GetDB 返回引擎使用的 *gorm.DB 实例。
	// 上层应用（例如 gflow 的 CC 通知监听器）通过此方法共享同一连接池，
	// 避免在引擎事务之外另起独立连接，从而减少"事务已提交但通知写入失败"的窗口。
	GetDB() *gorm.DB

	// GetTaskService 获取任务服务
	GetTaskService() TaskService

	// GetProcessService 获取流程服务
	GetProcessService() ProcessService

	// GetRuntimeService 获取运行时服务
	GetRuntimeService() RuntimeService

	// GetTaskServiceInternal 获取引擎内部机制用的任务服务（TaskService + 会签/归档等内部方法）。
	// 供 components.Register 等引擎内部装配使用，宿主业务代码不应调用。
	GetTaskServiceInternal() TaskServiceInternal

	// GetRuntimeServiceInternal 获取引擎内部机制用的运行时服务
	// （RuntimeService + 规则链池/续跑等内部方法，含 rulego 类型）。
	// 供 components.Register 等引擎内部装配使用，宿主业务代码不应调用。
	GetRuntimeServiceInternal() RuntimeServiceInternal

	// CountTenantData 统计租户在引擎各表（wf_process/wf_instance/wf_task/wf_task_assignee/
	// wf_task_comment/wf_hi_instance/wf_hi_task）的行数（表名 -> count）。
	// 供宿主删除租户前检查引擎侧数据是否清空；任一表 count>0 表示仍有数据。
	CountTenantData(ctx context.Context, tenantID string) (map[string]int64, error)

	// GetHistoryService 获取历史服务
	GetHistoryService() HistoryService

	// GetIdentityService 获取身份服务。
	//
	// 引擎不内置生产级实现：Builder 未注入时返回 Mock 实现（对任意输入返回
	// 合成的测试用户/组织数据），角色/部门/主管解析将得到假数据。生产环境必须
	// 通过 Builder.SetIdentityService() 注入真实实现（可配合 RequireIdentityService()
	// 在启动时强制校验注入）。
	GetIdentityService() IdentityService

	// GetLocker 获取分布式锁
	GetLocker() lock.Locker

	// Start 启动工作流引擎：建立数据库连接、装配各服务，之后服务方法才可用。
	// 前置：通过 NewWorkflowEngineBuilder().Build() 或 NewWorkflowEngine() 构造。
	Start(ctx context.Context) error

	// Stop 停止工作流引擎（翻转运行态；各服务共享数据库连接池，无额外停机资源）
	Stop(ctx context.Context) error

	// IsRunning 检查引擎是否正在运行
	IsRunning() bool

	// GetName 获取引擎名称
	GetName() string

	// GetVersion 获取引擎版本
	GetVersion() string
	// GetIDGenerator 获取ID生成器
	GetIDGenerator() IDGenerator
	// GetRuleChainExecutor 获取规则链执行器
	GetRuleChainExecutor() RuleChainExecutor

	// GetCCTaskCreatedListener 获取已注册的 CC 抄送任务创建事件监听器。
	// 未通过 Builder.SetCCTaskCreatedListener 注册时返回 nil。
	GetCCTaskCreatedListener() CCTaskCreatedListener

	// GetTaskEventListener 获取已注册的任务事件监听器。
	// 未通过 Builder.SetTaskEventListener 注册时返回 nil。
	GetTaskEventListener() TaskEventListener
}
