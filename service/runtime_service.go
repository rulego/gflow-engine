package service

import (
	"context"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
)

// startOptions 聚合 StartOption 的可选项（包内使用）。
type startOptions struct {
	asDraft bool
}

// StartOption 启动流程实例的可变选项（functional options 模式），避免尾部裸 bool 参数的二义性。
type StartOption func(*startOptions)

// WithDraft 以草稿模式启动：实例置 Draft 状态且不触发引擎推进；
// 之后须调用 ActivateProcessInstance 激活并开始流转。
func WithDraft() StartOption {
	return func(o *startOptions) { o.asDraft = true }
}

// applyStartOptions 归并启动选项。
func applyStartOptions(opts []StartOption) startOptions {
	var o startOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// RuntimeService 运行时服务接口
// 提供流程实例的启动、管理、查询等运行时功能
type RuntimeService interface {
	// StartProcessInstanceByKey 根据流程定义Key启动流程实例（取该租户下此 Key 的最新版本）
	//       processDefinitionKey - 流程定义Key, businessKey - 业务Key（可选；同定义下已有同 businessKey
	//       的 active 实例时返回 ErrConflict）, variables - 启动变量（写入实例变量，驱动网关路由/表单回显）,
	//       opts - 可选启动选项（如 WithDraft() 草稿模式）
	StartProcessInstanceByKey(ctx context.Context, actor Actor, processDefinitionKey, businessKey string, variables map[string]interface{}, opts ...StartOption) (string, error)

	// StartProcessInstanceByID 根据流程定义ID启动流程实例（精确到版本）
	//       processDefinitionID - 流程定义ID, businessKey - 业务Key（可选；重复时返回 ErrConflict）,
	//       variables - 启动变量（写入实例变量）, opts - 可选启动选项（如 WithDraft() 草稿模式）
	StartProcessInstanceByID(ctx context.Context, actor Actor, processDefinitionID, businessKey string, variables map[string]interface{}, opts ...StartOption) (string, error)

	// GetProcessInstance 根据ID获取流程实例
	GetProcessInstance(ctx context.Context, actor Actor, processInstanceID string) (*model.WfInstance, error)

	// GetProcessInstanceList 获取流程实例列表（前端API专用）
	GetProcessInstanceList(ctx context.Context, actor Actor, query *dto.ProcessInstanceQueryDTO) ([]*model.WfInstance, int64, error)

	// GetProcessInstanceUnionList 获取流程实例列表（管理端视图：合并运行时与历史归档表）
	GetProcessInstanceUnionList(ctx context.Context, actor Actor, query *dto.ProcessInstanceQueryDTO) ([]*model.WfInstance, int64, error)

	// UpdateInstanceCurrentActivity 更新 active 实例的当前节点（userTask 创建任务时由节点回调）
	UpdateInstanceCurrentActivity(ctx context.Context, processInstanceID, activityKey string) error

	// GetStuckProcessInstances 找出 active 但无未决任务的卡死实例（对账巡检/管理端救援）
	GetStuckProcessInstances(ctx context.Context, tenantID string) ([]*model.WfInstance, error)

	// ReDriveProcessInstance 重驱动卡死实例：从当前节点补跑引擎推进
	ReDriveProcessInstance(ctx context.Context, actor Actor, processInstanceID string) error

	// TerminateProcessInstance 终止流程实例
	TerminateProcessInstance(ctx context.Context, actor Actor, processInstanceID, reason string) error

	// DeleteProcessInstance 删除流程实例
	DeleteProcessInstance(ctx context.Context, actor Actor, processInstanceID, reason string) error

	// DeleteProcessInstances 批量删除流程实例
	DeleteProcessInstances(ctx context.Context, actor Actor, processInstanceIDs []string, reason string) error

	// SuspendProcessInstance 挂起流程实例
	SuspendProcessInstance(ctx context.Context, actor Actor, processInstanceID string) error

	// ActivateProcessInstance 激活流程实例
	ActivateProcessInstance(ctx context.Context, actor Actor, processInstanceID string) error

	// GetProcessInstanceVariables 获取流程实例变量
	GetProcessInstanceVariables(ctx context.Context, actor Actor, processInstanceID string) (map[string]interface{}, error)

	// GetProcessInstanceVariable 获取指定流程实例变量
	GetProcessInstanceVariable(ctx context.Context, actor Actor, processInstanceID, variableName string) (interface{}, error)

	// SetProcessInstanceVariables 设置流程实例变量
	SetProcessInstanceVariables(ctx context.Context, actor Actor, processInstanceID string, variables map[string]interface{}) error

	// SetProcessInstanceVariable 设置指定流程实例变量
	SetProcessInstanceVariable(ctx context.Context, actor Actor, processInstanceID, variableName string, value interface{}) error

	// RestoreProcessInstance 恢复流程实例（actor 租户非空时校验实例归属）。
	RestoreProcessInstance(ctx context.Context, actor Actor, processInstanceID string) error

	// RestoreAllProcessInstances 恢复所有活跃的流程实例（跨租户全量扫描，
	// 仅限系统身份或 SuperAdmin：典型场景是宿主启动时的一致性恢复巡检）。
	RestoreAllProcessInstances(ctx context.Context, actor Actor) error

	// RemoveProcessInstanceVariable 删除流程实例变量
	RemoveProcessInstanceVariable(ctx context.Context, actor Actor, processInstanceID string, variableName string) error

	// ForceResumeInstance 强制触发 multi-node 恢复，用于救"最后一个 approve 失败"
	// 导致的卡死实例（fork-join 拓扑下所有 task 已 completed 但实例仍在 active）。
	// 实例不在 fork-join 拓扑里时返回 ErrUnsupportedForkTopology。
	ForceResumeInstance(ctx context.Context, actor Actor, processInstanceID string) error

	// RestartProcessInstance 重启流程实例：原实例保持终态不动，在其基础上派生新实例并
	// 从 activityID 对应节点重新推进，返回新实例 ID。注意三点：
	//   1) 新实例的 businessKey 为原值追加 "-restart" 后缀；
	//   2) 原实例的运行时上下文（当前节点等）不会迁移到新实例；
	//   3) 不会重新校验业务键唯一性（StartProcessInstanceByID 会做），调用方需自行保证。
	RestartProcessInstance(ctx context.Context, actor Actor, processInstanceID, activityID string) (string, error)

	// CompleteProcessInstance 完成流程实例并自动归档到历史表
	CompleteProcessInstance(ctx context.Context, actor Actor, processInstanceID, reason string) error

	// ===== 实例分组（按任务条件） =====
	//
	// 下列四个"我的 XX"接口返回的是流程实例视图（[]*model.WfInstance），
	// 不是任务列表——同一实例命中多个任务时经 UNION 去重只出现一次。
	// orderBy 为排序字段名（合法格式 ^[a-zA-Z_][a-zA-Z0-9_.]*$，如 created_at），
	// 传空或不合法时回退 created_at；pageSize<=0 按 10 处理。

	// GetTodoProcessInstanceList 获取我的待办实例列表。
	// 含：① assignee 为本人的任务；② 待认领且本人在候选人池的任务（候选组模式，
	// 角色经 IdentityService 展开）。
	GetTodoProcessInstanceList(ctx context.Context, actor Actor, page, pageSize int, keyword, orderBy string, orderDesc bool) ([]*model.WfInstance, int64, error)

	// GetDoneProcessInstanceList 获取我的已办实例列表。
	GetDoneProcessInstanceList(ctx context.Context, actor Actor, page, pageSize int, keyword, orderBy string, orderDesc bool) ([]*model.WfInstance, int64, error)

	// GetCcProcessInstanceList 获取抄送给我的实例列表。
	GetCcProcessInstanceList(ctx context.Context, actor Actor, page, pageSize int, keyword, orderBy string, orderDesc bool) ([]*model.WfInstance, int64, error)

	// GetMyApplicationsProcessInstanceList 获取我发起的申请实例列表
	// （按 start_user_id=发起人用户ID 过滤，与 token userId 同口径）
	GetMyApplicationsProcessInstanceList(ctx context.Context, actor Actor, page, pageSize int, keyword, orderBy string, orderDesc bool) ([]*model.WfInstance, int64, error)

	// CountMyApplications 统计我发起的申请数量（运行时+历史表，可按 created_at
	// 时间段过滤；from/to 任一可为 nil 表示不设该侧边界）。统计页日期筛选用。
	CountMyApplications(ctx context.Context, actor Actor, from, to *time.Time) (int64, error)

	// GetProcessInstanceDetail 获取实例详情（审批进度列表 + 流程定义 + 表单等装配视图）。
	// 权限：actor.UserID 必须是 发起人 / 该实例任一任务 assignee（含已办）/ CC 抄送归属，
	// 否则返回 ErrPermissionDenied；Actor.SuperAdmin 放行。
	GetProcessInstanceDetail(ctx context.Context, actor Actor, processInstanceID string) (*dto.InstanceDetailResponse, error)
}
