package service

import (
	"context"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// ProcessService 流程定义服务接口
// 提供流程定义的部署、管理、查询等核心功能
type ProcessService interface {
	// Deploy 部署流程定义（状态置 active，同 Key 旧 active 版本自动退役）
	Deploy(ctx context.Context, actor Actor, process *model.WfProcess, duplicate bool) (*model.WfProcess, error)

	// Create 保存流程定义（状态置 draft，不激活）
	Create(ctx context.Context, actor Actor, process *model.WfProcess, duplicate bool) (*model.WfProcess, error)

	// Update 更新流程定义（不换 processID/版本，definition_json 变更后自动失效拓扑缓存；
	// 禁止修改 processKey）
	Update(ctx context.Context, actor Actor, process *model.WfProcess) error

	// List 按条件分页查询流程定义。actor 租户非空时强制按租户过滤；
	// 空租户视为系统视角不做过滤。request - 查询条件。
	List(ctx context.Context, actor Actor, request *dto.ProcessQueryRequest) ([]*model.WfProcess, int64, error)

	// Get 根据ID获取流程定义
	Get(ctx context.Context, processID string) (*model.WfProcess, error)

	// GetByKey 根据Key获取最新版本的流程定义
	GetByKey(ctx context.Context, tenantID, processKey string) (*model.WfProcess, error)

	// GetByKeyAndVersion 根据Key和版本获取流程定义
	GetByKeyAndVersion(ctx context.Context, tenantID, processKey string, version int32) (*model.WfProcess, error)

	// GetVersions 根据Key分页获取所有版本的流程定义
	GetVersions(ctx context.Context, tenantID, processKey string, page, pageSize int) ([]*model.WfProcess, int64, error)

	// Delete 删除流程定义（仍有运行实例时返回 ErrConflict；删除后失效拓扑缓存）
	Delete(ctx context.Context, actor Actor, processDefinitionID string) error

	// Activate 激活流程定义
	Activate(ctx context.Context, actor Actor, processDefinitionID string) (*model.WfProcess, error)

	// Retire 停用流程定义
	Retire(ctx context.Context, actor Actor, processID string) error

	// UpdateStatus 更新流程定义状态（目标为 active 时走 Deploy 升版本，保证同 Key 单 active）
	UpdateStatus(ctx context.Context, actor Actor, processID string, status enums.ProcessStatus) error

	// UpdateStatusByKey 按Key更新最新版本的状态（目标为 active 时走 Deploy 升版本）
	UpdateStatusByKey(ctx context.Context, actor Actor, processKey string, status enums.ProcessStatus) error

	// IsFormReferenced 检查是否有生效（status=active）的流程定义在 definition_json
	// 里以 additionalInfo.formKey == formKey 引用了该表单。
	// 供宿主删除表单前做引用检查——被引用表单是发起/审批页渲染依赖，删了会 500。
	IsFormReferenced(ctx context.Context, tenantID, formKey string) (bool, error)
}
