package service

import (
	"strings"

	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
)

var _ ProcessService = (*ProcessServiceImpl)(nil)

// ProcessServiceImpl ProcessService的实现
type ProcessServiceImpl struct {
	processDAO  *dao.ProcessDAO
	instanceDAO *dao.InstanceDAO
	idGenerator IDGenerator
	// engine 用于在 Deploy 时预加载流程链到共享 enginePool，使 subProcess 子链可被父流程寻址。
	engine WorkflowEngine
}

// NewProcessService 创建ProcessService实例
func NewProcessService(workflowEngine WorkflowEngine) ProcessService {
	return &ProcessServiceImpl{
		processDAO:  dao.NewProcessDAO(),
		instanceDAO: dao.NewInstanceDAO(),
		idGenerator: workflowEngine.GetIDGenerator(),
		engine:      workflowEngine,
	}
}

// NewProcessServiceWithQuery 创建带Query参数的ProcessService实例
func NewProcessServiceWithQuery(query *query.Query, workflowEngine WorkflowEngine) ProcessService {
	return &ProcessServiceImpl{
		processDAO:  dao.NewProcessDAOWithQuery(query),
		instanceDAO: dao.NewInstanceDAOWithQuery(query),
		idGenerator: workflowEngine.GetIDGenerator(),
		engine:      workflowEngine,
	}
}

// Create 创建
func (s *ProcessServiceImpl) Create(ctx context.Context, actor Actor, process *model.WfProcess, duplicate bool) (*model.WfProcess, error) {
	ctx = bindActor(ctx, actor)
	return s.create(ctx, process, duplicate, string(enums.ProcessStatusDraft))
}

func (s *ProcessServiceImpl) Update(ctx context.Context, actor Actor, process *model.WfProcess) error {
	ctx = bindActor(ctx, actor)
	if process.ID == "" {
		return fmt.Errorf("process ID cannot be empty")
	}

	// 1. 获取现有流程定义以检查权限
	existingProcess, err := s.processDAO.Get(ctx, process.ID)
	if err != nil {
		return fmt.Errorf("failed to get existing process: %w", err)
	}
	if existingProcess == nil {
		return fmt.Errorf("process not found")
	}
	// 禁止改 processKey：避免破坏版本族 / 误 retire 同 key 流程
	if process.ProcessKey != "" && process.ProcessKey != existingProcess.ProcessKey {
		return fmt.Errorf("processKey cannot be changed: %w", ErrValidation)
	}
	// DSL 可解析校验：与 create() 同口径。DefinitionJSON 为空表示本次不改 DSL
	// （只改名等部分更新），跳过；非空则必须在写入前能被 ToRuleChain 解析。
	if process.DefinitionJSON != "" {
		chain, err := process.ToRuleChain()
		if err != nil {
			return fmt.Errorf("invalid process definition JSON: %w: %w", ErrValidation, err)
		}
		if issues := ValidateChainExpressions(chain); len(issues) > 0 {
			return fmt.Errorf("invalid node expressions: %w: %s", ErrValidation, FormatConditionIssues(issues))
		}
	}
	// 保持原 processKey（忽略传入值，防破坏版本族）
	process.ProcessKey = existingProcess.ProcessKey
	//如果状态为激活，则将其设置为停用
	if existingProcess.Status == string(enums.ProcessStatusActive) {
		process.Status = string(enums.ProcessStatusRetired)
	} else {
		process.Status = existingProcess.Status
	}
	// 2. 检查权限
	if u := GetUserFromCtx(ctx); u != nil {
		if err := ensureTenantAccess(ctx, "process", existingProcess.TenantID); err != nil {
			return err
		}
		// 确保不会修改租户ID
		process.TenantID = u.TenantID
		process.UpdatedBy = &u.UserID
	} else if process.TenantID != "" && existingProcess.TenantID != process.TenantID {
		// 如果上下文无用户但传入了TenantID，需确保匹配（防止跨租户修改）
		return fmt.Errorf("tenant ID mismatch: %w", ErrPermissionDenied)
	}

	now := time.Now()
	process.UpdatedAt = &now

	// 版本递增 + 并发冲突重试：每次 Update 都让 Version 自增，便于审计/版本回滚；
	// 已存在的运行实例（按 process_id + version 关联）保持原版本号不受影响，
	// 即不会因本次 Update 改变老实例归属的版本。
	// 递增基数必须是同 processKey 全家族的最大版本（Deploy 会在新行上创建更高版本，
	// 按旧行 version+1 改写会撞 uq_process_key_version 唯一约束）。
	// 并发编辑同 key 家族的两行仍可能同时算出同一个 maxVersion+1——
	// 撞唯一约束时重新取最大版本重试（最多 3 次），胜者落库、败者顺延。
	const maxUpdateAttempts = 3
	var updateErr error
	for attempt := 0; attempt < maxUpdateAttempts; attempt++ {
		maxVersion := existingProcess.Version
		if latest, lerr := s.processDAO.GetLatestByKey(ctx, existingProcess.TenantID, existingProcess.ProcessKey); lerr == nil && latest != nil && latest.Version > maxVersion {
			maxVersion = latest.Version
		}
		if maxVersion <= 0 {
			// 老数据或异常 0 值，按 1 处理
			process.Version = 1
		} else {
			process.Version = maxVersion + 1
		}
		updateErr = s.processDAO.Update(ctx, process)
		if updateErr == nil || !isUniqueViolation(updateErr) {
			break
		}
		logrus.WithFields(logrus.Fields{
			"processKey": existingProcess.ProcessKey,
			"attempt":    attempt + 1,
		}).Warn("concurrent definition update hit uq_process_key_version; retrying with bumped version")
	}
	if updateErr != nil {
		return fmt.Errorf("failed to update process: %w", updateErr)
	}

	// Update 不换 processID 但会改 definition_json，必须失效 forkGraph 缓存。
	// 否则下一次 ExecuteNext 还在用老拓扑，分支识别/嵌套检测会基于过期
	// 数据，可能让本可正常运行的新流程被错误 hard-fail，或让旧流程的错误缓存
	// 被沿用。
	InvalidateForkGraphCache(process.ID)

	// 同理驱逐 enginePool 已装载链：GetExecution 快路径不回读 DB，不驱逐则本副本
	// 继续用旧定义推进实例；多副本下另一副本装载的已是新定义，同一实例两套定义
	// 创建任务，join 永远收不齐或路由分叉。
	InvalidateExecutionCache(process.ID)

	return nil
}

// Deploy 部署流程定义
func (s *ProcessServiceImpl) Deploy(ctx context.Context, actor Actor, process *model.WfProcess, duplicate bool) (*model.WfProcess, error) {
	ctx = bindActor(ctx, actor)
	return s.create(ctx, process, duplicate, string(enums.ProcessStatusActive))
}

func (s *ProcessServiceImpl) create(ctx context.Context, process *model.WfProcess, duplicate bool, status string) (*model.WfProcess, error) {
	if process.Name == "" {
		return nil, fmt.Errorf("deployment name cannot be empty")
	}
	if process.ProcessKey == "" {
		return nil, fmt.Errorf("deployment processKey cannot be empty")
	}
	if process.TenantID == "" {
		return nil, fmt.Errorf("deployment tenantID cannot be empty")
	}
	// DSL 可解析校验：畸形 definitionJson 一旦落库（尤其 active 态），发起实例时
	// 引擎解析崩溃只能 500。这里在写入前用 ToRuleChain 做结构校验，失败按
	// ErrValidation 哨兵返回（宿主映射 400），错误信息带上具体解析原因。
	chain, err := process.ToRuleChain()
	if err != nil {
		return nil, fmt.Errorf("invalid process definition JSON: %w: %w", ErrValidation, err)
	}
	// 条件表达式/服务函数校验：坏 case 落库后链加载只 warn，实例启动才炸。
	// 部署期拦截并定位到具体节点（见 ValidateChainExpressions）。
	if issues := ValidateChainExpressions(chain); len(issues) > 0 {
		return nil, fmt.Errorf("invalid node expressions: %w: %s", ErrValidation, FormatConditionIssues(issues))
	}

	// 兜底归一化（一次性，部署/创建时）：
	// 0. MigrateRouteGateway：遗留 routeGateway（引擎未注册，装载必失败）转 switch，
	//    原 Success 后继改为 Default 出边保持行为等价。设计器加载侧也有同构迁移
	//    （前端 migrateRouteGatewayToSwitch，生成完整分支）；此处兜底不经设计器
	//    重新部署的存量 DSL（如 API 直接部署/导入）。
	// 1. EnsureEndNode：设计器早期版本/外部导入的 DSL 可能没有 end 节点——引擎只在
	//    end 节点触发 CompleteProcessInstance，缺 end 的流程所有任务完成后实例永远
	//    active。无 end 时自动补一个并把所有"无出边"的尾节点连过去。
	// 2. 为缺 Default 出边的 switch 补一条 Default→end 兜底边，防止非穷尽 switch
	//    无 case 命中时实例卡死。先补 end 再补 Default，Default 边才能指向新 end。
	process.MigrateRouteGateway()
	process.EnsureEndNode()
	process.EnsureSwitchDefaultEdges()

	existingProcess, err := s.processDAO.GetLatestByKey(ctx, process.TenantID, process.ProcessKey)

	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("failed to check existing process: %w", err)
	}

	if existingProcess != nil && !duplicate {
		return existingProcess, nil
	}

	version := int32(1)
	if existingProcess != nil {
		version = existingProcess.Version + 1
	}

	// 生成流程定义ID
	process.ID = s.idGenerator.GenerateID()
	process.Version = version
	process.CreatedAt = time.Now()
	process.Status = status
	// 租户归属：操作人租户非空时【强制】以 actor 租户为准（与 Update 同口径），
	// 防止租户 A 用户把定义部署进租户 B（载荷 TenantID 不可信）。
	// 系统视角（ctx 无用户或空租户）保留载荷值，用于初始化内置流程等场景。
	if currentUser := GetUserFromCtx(ctx); currentUser != nil && currentUser.TenantID != "" {
		process.TenantID = currentUser.TenantID
	}
	if process.TenantID == "" || process.CreatedBy == "" {
		currentUser := GetUserFromCtx(ctx)
		if currentUser == nil {
			return nil, fmt.Errorf("current user cannot be empty")
		}
		process.TenantID = currentUser.TenantID
		process.CreatedBy = currentUser.UserID
	}

	// 保存流程定义 + 退役旧 active 原子（保证同 key 仅一个 active，防并发/抖动产生多 active）
	if err := s.processDAO.Query.Transaction(func(tx *query.Query) error {
		txDAO := dao.NewProcessDAOWithQuery(tx)
		if err := txDAO.Create(ctx, process); err != nil {
			return fmt.Errorf("failed to deploy process: %w", err)
		}
		if status == string(enums.ProcessStatusActive) {
			if err := txDAO.RetireActivesByKey(ctx, process.TenantID, process.ProcessKey, process.ID); err != nil {
				return fmt.Errorf("failed to retire prior active versions: %w", err)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// 预加载链到租户专属 enginePool（best-effort，仅 active 部署）：
	// 使 subProcess 子链在 Deploy 时即进池，父流程用 ruleChain.id 别名 TellFlow 可命中。
	// 失败不阻塞部署——链仍可在实例启动/续跑时按需注册（initExecution 幂等）。
	if status == string(enums.ProcessStatusActive) && s.engine != nil {
		if rs := s.engine.GetRuntimeServiceInternal(); rs != nil {
			if err := rs.PreloadChain(process.TenantID, process.ID, process.DefinitionJSON); err != nil {
				logrus.WithError(err).Warnf("preload chain for process %s failed (non-fatal; will register on start)", process.ID)
			}
			// 版本 active 驱逐：新版部署后，best-effort 清理上一版链
			// （仅当上一版无 active 实例）。有活实例则保留，待其完成/终止时再驱逐。
			if existingProcess != nil {
				rs.EvictStaleChain(ctx, existingProcess.TenantID, existingProcess.ID)
			}
		}
	}

	return process, nil
}

// List 分页查询流程定义列表。
// 强制以 actor 租户为查询范围；空租户视为系统视角，不做租户过滤。
func (s *ProcessServiceImpl) List(ctx context.Context, actor Actor, request *dto.ProcessQueryRequest) ([]*model.WfProcess, int64, error) {
	ctx = bindActor(ctx, actor)
	if request == nil {
		request = &dto.ProcessQueryRequest{}
	}
	if u := GetUserFromCtx(ctx); u != nil && u.TenantID != "" {
		request.TenantID = u.TenantID
	}
	return s.processDAO.List(ctx, request)
}

// Get 获取流程定义。不存在时返回 ErrNotFound（与 TaskService.GetTask 口径一致）。
func (s *ProcessServiceImpl) Get(ctx context.Context, id string) (*model.WfProcess, error) {
	if id == "" {
		return nil, fmt.Errorf("process definition ID cannot be empty")
	}

	process, err := s.processDAO.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if process == nil {
		return nil, fmt.Errorf("%w: process definition", ErrNotFound)
	}

	if err := ensureTenantAccess(ctx, "process", process.TenantID); err != nil {
		return nil, err
	}

	return process, nil
}

// GetByKey 根据流程键获取最新版本的流程定义
func (s *ProcessServiceImpl) GetByKey(ctx context.Context, tenantID, processKey string) (*model.WfProcess, error) {
	if processKey == "" {
		return nil, fmt.Errorf("process key cannot be empty")
	}

	return s.processDAO.GetLatestByKey(ctx, tenantID, processKey)
}

// GetByKeyAndVersion 根据流程键和版本获取流程定义。
// version<=0（调用方不传版本）按控制器契约回退最新版本——按 key 引用流程的
// 前端调用（发起审批节点配置、定时发起审批表单回显）不感知版本号，
// 强校验版本会让这类调用一律 404。
func (s *ProcessServiceImpl) GetByKeyAndVersion(ctx context.Context, tenantID, processKey string, version int32) (*model.WfProcess, error) {
	if processKey == "" {
		return nil, fmt.Errorf("process key cannot be empty")
	}

	if version <= 0 {
		return s.processDAO.GetLatestByKey(ctx, tenantID, processKey)
	}
	return s.processDAO.GetByKeyAndVersion(ctx, tenantID, processKey, version)
}

// GetVersions 获取流程的所有版本
func (s *ProcessServiceImpl) GetVersions(ctx context.Context, tenantID, processKey string, page, pageSize int) ([]*model.WfProcess, int64, error) {
	if processKey == "" {
		return nil, 0, fmt.Errorf("process key cannot be empty")
	}
	var request = &dto.ProcessQueryRequest{
		PageRequest: dto.PageRequest{
			Page:     page,
			PageSize: pageSize,
			TenantID: tenantID,
		},
		ProcessKey: processKey,
		AllVersion: true, // 查询所有版本
	}
	// 直连 DAO：GetVersions 的租户范围由显式 tenantID 参数决定，
	// 不走 List 的"以 actor 租户强制覆盖"口径。
	return s.processDAO.List(ctx, request)
}

// Delete 删除流程定义
func (s *ProcessServiceImpl) Delete(ctx context.Context, actor Actor, processID string) error {
	ctx = bindActor(ctx, actor)
	tenantID := actor.TenantID
	if processID == "" {
		return fmt.Errorf("process definition ID cannot be empty")
	}

	// 检查是否有正在运行的流程实例
	// 如果有正在运行的实例，应该禁止删除或者提供强制删除选项
	instances, _, err := s.instanceDAO.GetByProcessID(ctx, processID, 1, 0)
	if err != nil {
		return fmt.Errorf("failed to check running instances: %w", err)
	}
	if len(instances) > 0 {
		return fmt.Errorf("cannot delete process definition with running instances: %w", ErrConflict)
	}

	if err := s.processDAO.Delete(ctx, tenantID, processID); err != nil {
		return fmt.Errorf("failed to delete process definition: %w", err)
	}

	// 失效 forkGraph 缓存。流程定义被删后老实例也跑不起来，缓存条目留着只会
	// 占内存。流程定义本身不可变，正常 Deploy 走新版本（新 processID）不命中老
	// 缓存——只有 Delete 才需要显式清理。
	InvalidateForkGraphCache(processID)
	InvalidateExecutionCache(processID)

	return nil
}

// Retire 停用流程定义
func (s *ProcessServiceImpl) Retire(ctx context.Context, actor Actor, processID string) error {
	ctx = bindActor(ctx, actor)
	return s.UpdateStatus(ctx, actor, processID, enums.ProcessStatusRetired)
}

// Activate 激活流程定义，并创建新的版本
func (s *ProcessServiceImpl) Activate(ctx context.Context, actor Actor, processID string) (*model.WfProcess, error) {
	ctx = bindActor(ctx, actor)
	process, err := s.Get(ctx, processID)
	if err != nil {
		return nil, err
	}
	err = s.updateStatus(ctx, process, enums.ProcessStatusActive)
	return process, err
}

func (s *ProcessServiceImpl) UpdateStatus(ctx context.Context, actor Actor, processID string, status enums.ProcessStatus) error {
	ctx = bindActor(ctx, actor)
	if processID == "" {
		return fmt.Errorf("process ID cannot be empty")
	}

	process, err := s.processDAO.Get(ctx, processID)
	if err != nil {
		return err
	}

	if process == nil {
		return fmt.Errorf("process definition not found")
	}

	if err := ensureTenantAccess(ctx, "process definition", process.TenantID); err != nil {
		return err
	}

	return s.updateStatus(ctx, process, status)
}

func (s *ProcessServiceImpl) UpdateStatusByKey(ctx context.Context, actor Actor, processKey string, status enums.ProcessStatus) error {
	ctx = bindActor(ctx, actor)
	tenantID := actor.TenantID
	if processKey == "" {
		return fmt.Errorf("process key cannot be empty")
	}

	process, err := s.processDAO.GetLatestByKey(ctx, tenantID, processKey)
	if err != nil {
		return err
	}
	return s.updateStatus(ctx, process, status)
}

func (s *ProcessServiceImpl) updateStatus(ctx context.Context, process *model.WfProcess, status enums.ProcessStatus) error {
	if process == nil {
		return fmt.Errorf("process cannot be nil")
	}

	if !enums.IsValidProcessStatus(status) {
		return fmt.Errorf("invalid process status")
	}

	//目标为激活：统一走部署流程（退役同 key 旧 active + 预加载链 + 升版本），
	//避免 draft→active 等路径直接改状态导致同 key 出现多个 active。
	if status == enums.ProcessStatusActive {
		// 目标为激活走 Deploy（升版本）：actor 取 ctx 已绑定身份（调用方已 bindActor）
		actor := ActorFromCtx(ctx)
		_, err := s.Deploy(ctx, actor, process, true)
		return err
	}

	// 更新状态
	process.Status = string(status)
	now := time.Now()
	process.UpdatedAt = &now
	if currentUser := GetUserFromCtx(ctx); currentUser != nil {
		process.UpdatedBy = &currentUser.UserID
	}

	if err := s.processDAO.Update(ctx, process); err != nil {
		return fmt.Errorf("failed to update process status: %w", err)
	}
	return nil
}

// IsFormReferenced 检查是否有生效（status=active）的流程定义在 definition_json
// 里以 additionalInfo.formKey == formKey 引用了该表单。
// 供宿主删除表单前做引用检查。
func (s *ProcessServiceImpl) IsFormReferenced(ctx context.Context, tenantID, formKey string) (bool, error) {
	if formKey == "" {
		return false, nil
	}
	count, err := s.processDAO.CountActiveReferencingForm(ctx, tenantID, formKey)
	if err != nil {
		return false, fmt.Errorf("failed to check active definitions referencing form %s: %w", formKey, err)
	}
	return count > 0, nil
}

// isUniqueViolation 判断是否唯一约束冲突（跨方言：PG 23505 / MySQL 1062 / SQLite UNIQUE）。
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "23505") || strings.Contains(msg, "1062") ||
		strings.Contains(msg, "duplicate key") || strings.Contains(msg, "UNIQUE constraint failed")
}
