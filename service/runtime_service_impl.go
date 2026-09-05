package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/utils/str"
	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/cast"
	"gorm.io/gorm"
)

var _ RuntimeServiceInternal = (*RuntimeServiceImpl)(nil)

// archiveBatchSize 归档任务批量写入 wf_hi_task 的每批条数（大会签实例免逐条往返）
const archiveBatchSize = 100

// RuntimeServiceImpl RuntimeService的实现
type RuntimeServiceImpl struct {
	BaseService
	workflowEngine WorkflowEngine
	instanceDAO    *dao.InstanceDAO
	hiInstanceDAO  *dao.HiInstanceDAO
	processDAO     *dao.ProcessDAO
	taskDAO        *dao.TaskDAO
	idGenerator    IDGenerator
	enginePool     types.RuleEnginePool
	// execGates ExecuteNext 的实例级可重入门闩表（instanceID -> *execGate）
	execGates sync.Map
	// enginePools 租户专属引擎池：tenantID -> types.RuleEnginePool（惰性创建）。
	// 引擎级租户隔离——每租户的链与别名(ruleChain.id)各自独立，杜绝跨租户别名撞。
	// 空 tenantID 回退到共享 enginePool（单租户部署）。
	enginePools sync.Map
	// processTenants 缓存 processID -> tenantID（不可变：processDef.ID 全局唯一、租户固定，无需失效）。
	// 让 GetExecution 在「缓存命中 + 池命中」时免 DB 查询（性能优化：避免每次续跑多查一次 processDef）。
	processTenants sync.Map
	// subProcessTargets 注册表：(tenantID|ruleChain.id) -> 子流程定义ID。
	// PreloadChain 时建立；subProcess 节点用 targetId(ruleChain.id) 经此解析到子 processDef 以启动子实例。
	subProcessTargets sync.Map
	// subProcessParentNodes 注册表：子实例ID -> 父流程的 subProcess 节点ID。
	// 子实例完成时据此恢复父流程（ExecuteNext 父实例到该节点 → 重跑 → 见子完成 → TellNext）。
	subProcessParentNodes sync.Map
}

// NewRuntimeService 创建RuntimeService实例
func NewRuntimeService(workflowEngine WorkflowEngine) RuntimeService {
	s := &RuntimeServiceImpl{
		instanceDAO:    dao.NewInstanceDAO(),
		hiInstanceDAO:  dao.NewHiInstanceDAO(),
		processDAO:     dao.NewProcessDAO(),
		taskDAO:        dao.NewTaskDAO(),
		idGenerator:    workflowEngine.GetIDGenerator(),
		enginePool:     rulego.NewRuleGo(),
		workflowEngine: workflowEngine,
	}
	runtimeServiceRegistry.Store(s, struct{}{})
	return s
}

// NewRuntimeServiceWithQuery 创建带Query参数的RuntimeService实例
func NewRuntimeServiceWithQuery(query *query.Query, workflowEngine WorkflowEngine) RuntimeService {
	s := &RuntimeServiceImpl{
		instanceDAO:    dao.NewInstanceDAOWithQuery(query),
		hiInstanceDAO:  dao.NewHiInstanceDAOWithQuery(query),
		processDAO:     dao.NewProcessDAOWithQuery(query),
		taskDAO:        dao.NewTaskDAOWithQuery(query),
		idGenerator:    workflowEngine.GetIDGenerator(),
		enginePool:     rulego.NewRuleGo(),
		workflowEngine: workflowEngine,
	}
	runtimeServiceRegistry.Store(s, struct{}{})
	return s
}

// StartProcessInstanceByKey 根据流程定义Key启动流程实例
func (s *RuntimeServiceImpl) StartProcessInstanceByKey(ctx context.Context, actor Actor, processDefinitionKey, businessKey string, variables map[string]interface{}, opts ...StartOption) (string, error) {
	ctx = bindActor(ctx, actor)
	initiator := actor
	if processDefinitionKey == "" {
		return "", fmt.Errorf("process definition key cannot be empty")
	}

	// 获取最新版本的流程定义
	processDef, err := s.processDAO.GetLatestByKey(ctx, initiator.TenantID, processDefinitionKey)
	if err != nil {
		return "", fmt.Errorf("failed to get process definition: %w", err)
	}

	if processDef == nil {
		return "", fmt.Errorf("process definition not found: %s", processDefinitionKey)
	}

	return s.StartProcessInstanceByID(ctx, initiator, processDef.ID, businessKey, variables, opts...)
}

// StartProcessInstanceByID 根据流程定义ID启动流程实例
func (s *RuntimeServiceImpl) StartProcessInstanceByID(ctx context.Context, actor Actor, processDefinitionID, businessKey string, variables map[string]interface{}, opts ...StartOption) (string, error) {
	ctx = bindActor(ctx, actor)
	initiator := actor
	isDraft := applyStartOptions(opts).asDraft
	if processDefinitionID == "" {
		return "", fmt.Errorf("process definition ID cannot be empty")
	}

	// 验证流程定义存在
	processDef, err := s.processDAO.Get(ctx, processDefinitionID)
	if err != nil {
		return "", fmt.Errorf("failed to get process definition: %w", err)
	}

	if processDef == nil {
		return "", fmt.Errorf("process definition not found: %s", processDefinitionID)
	}

	// 检查租户权限
	if err := ensureTenantAccess(ctx, "process definition", processDef.TenantID); err != nil {
		return "", err
	}

	// 仅 active 可发起新实例；停用/草稿定义的存量实例继续办理不经此处。
	// subProcess 子实例走 startInstanceCore 直调，不受此限制。
	if processDef.Status != string(enums.ProcessStatusActive) {
		return "", fmt.Errorf("process definition %s is %s and cannot start new instances: %w",
			processDef.ProcessKey, processDef.Status, ErrValidation)
	}

	// 发起人范围强校验（流程级 additionalInfo.starterScope；未配置=全员可发起）
	if err := s.checkStarterScope(ctx, processDef, initiator); err != nil {
		return "", err
	}

	// 如果有业务键，检查是否已存在相同业务键的活动实例
	if businessKey != "" {
		existingInstances, _, err := s.GetProcessInstanceList(ctx, initiator, &dto.ProcessInstanceQueryDTO{
			ProcessID:   processDefinitionID,
			BusinessKey: businessKey,
			PageRequest: dto.PageRequest{
				Status:   []string{string(enums.ProcessStatusActive)},
				PageSize: 1,
			},
		})
		if err != nil {
			return "", fmt.Errorf("failed to check existing instances: %w", err)
		}
		if len(existingInstances) > 0 {
			return "", fmt.Errorf("active process instance with business key '%s' already exists: %w", businessKey, ErrConflict)
		}
	}

	instanceID, engine, msg, err := s.startInstanceCore(ctx, processDef, initiator, businessKey, variables, isDraft, "")
	if err != nil {
		return "", err
	}
	if engine != nil {
		// 初始驱动与对端副本的恢复/AfterCommit 驱动共用同一把跨副本门闩，避免
		// 启动驱动与启动期恢复巡检并发重入同一实例重复建首任务。
		if unlock := s.acquireDistExecGate(ctx, instanceID); unlock != nil {
			defer unlock()
		}
		engine.OnMsg(msg) // 父实例同步驱动
	}
	// 发起事件：非草稿实例启动后派发（草稿在激活时发 activated）
	if !isDraft && s.workflowEngine.GetTaskEventListener() != nil {
		DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), TaskEvent{
			Type:       TaskEventStarted,
			InstanceID: instanceID,
			ProcessID:  processDefinitionID,
			TenantID:   initiator.TenantID,
			FromUser:   initiator.UserID,
			Source:     EventSourceAPI,
			Timestamp:  time.Now(),
		}, ctx)
	}
	return instanceID, nil
}

// startInstanceCore 创建流程实例并装配引擎/消息，但【不驱动】（交调用方同步或异步 OnMsg）。
// 契约：父实例由调用方同步 OnMsg 驱动；subProcess 子实例必须异步驱动，
// 否则子流程同步完成会重入父流程的 OnMsg。
// parentInstanceID 非空时标记为 subProcess 子实例（child.ParentID，创建时写入，先于驱动）。
// 返回 engine=nil 表示草稿（无需驱动）。
func (s *RuntimeServiceImpl) startInstanceCore(ctx context.Context, processDef *model.WfProcess, initiator Actor, businessKey string, variables map[string]interface{}, isDraft bool, parentInstanceID string) (string, types.RuleEngine, types.RuleMsg, error) {
	// 生成流程实例ID
	instanceID := s.idGenerator.GenerateInstanceID()

	// 序列化变量（失败仅告警：实例仍可启动，变量为空）
	var variablesJSON string
	if len(variables) > 0 {
		data, err := json.Marshal(variables)
		if err != nil {
			logrus.WithError(err).Warn("failed to marshal instance variables; instance will start with empty variables")
		} else {
			variablesJSON = string(data)
		}
	}
	now := time.Now()
	instanceStatus := enums.InstanceStatusActive
	if isDraft {
		instanceStatus = enums.InstanceStatusDraft
	}
	if businessKey == "" {
		businessKey = s.idGenerator.GenerateBusinessKey()
	}
	// 创建流程实例
	// CreatedBy 必须存发起人用户 ID（wf_instance.created_by 列注释即"发起人用户ID"）：
	// 它是 KeyOwner 元数据的唯一来源，direct_manager / multi_level_manager /
	// initiator_self 等审批人类型经 IdentityService（按用户 ID 查 user_departments）
	// 解析；若存用户名，真实用户（用户名≠ID）下解析必空 → no assignees found for task。
	// 展示层（前端实例/审批列表）按用户 ID 批量查 realName。
	instance := &model.WfInstance{
		ID:          instanceID,
		TenantID:    processDef.TenantID,
		ProcessID:   processDef.ID,
		Name:        processDef.Name,
		BusinessKey: &businessKey,
		StartUserID: initiator.UserID,
		Status:      string(instanceStatus),
		CreatedBy:   initiator.UserID,
		CreatedAt:   now,
		UpdatedAt:   &now,
		UpdatedBy:   &initiator.UserName,
		Variables:   &variablesJSON,
		Priority:    constants.PriorityNormal,
	}
	if parentInstanceID != "" {
		instance.ParentID = &parentInstanceID
	}

	// 保存流程实例
	if err := s.instanceDAO.Create(ctx, instance); err != nil {
		return "", nil, types.RuleMsg{}, fmt.Errorf("failed to create process instance: %w", err)
	}

	// 草稿不驱动
	if isDraft {
		return instanceID, nil, types.RuleMsg{}, nil
	}

	engine, err := s.initExecution(processDef.TenantID, processDef.ID, processDef.DefinitionJSON)
	if err != nil {
		return "", nil, types.RuleMsg{}, err
	}
	md := types.NewMetadata()
	md.PutValue(constants.KeyTenantID, processDef.TenantID)
	md.PutValue(constants.KeyInstanceID, instanceID)
	md.PutValue(constants.KeyBusinessKey, businessKey)
	md.PutValue(constants.KeyOwner, instance.CreatedBy)
	md.PutValue(constants.KeyProcessID, processDef.ID)
	md.PutValue(constants.KeyProcessKey, processDef.ProcessKey)
	var msg = types.NewMsg(0, "wf", types.JSON, md, cast.ToString(variables))

	return instanceID, engine, msg, nil // 不驱动，交调用方
}

// GetProcessInstance 根据ID获取流程实例
func (s *RuntimeServiceImpl) GetProcessInstance(ctx context.Context, actor Actor, processInstanceID string) (*model.WfInstance, error) {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return nil, fmt.Errorf("process instance ID cannot be empty")
	}

	instance, err := s.instanceDAO.Get(ctx, processInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get process instance: %w", err)
	}
	if instance == nil {
		// 活表查不到时回退历史表；DB 故障不进回退，避免用历史查询掩盖基础设施错误
		instance, err = s.hiInstanceDAO.Get(ctx, processInstanceID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get process instance: %w", err)
	}

	if instance == nil {
		return nil, fmt.Errorf("%w: process instance", ErrInstanceNotFound)
	}

	if err := ensureTenantAccess(ctx, "process instance", instance.TenantID); err != nil {
		return nil, err
	}

	// 装配当前节点名（宿主直接序列化该字段，见 model.WfInstance.CurrentActivityName）
	s.decorateCurrentActivityNames(ctx, []*model.WfInstance{instance})

	return instance, nil
}

// DeleteProcessInstance 删除流程实例并归档到历史表。
// 实例已归档（活表无行）时改为把历史行标记 deleted——删除落在终态归档之后的
// 实例是合法操作，仅剩历史行可标，标记后已办/抄送不再带出。
func (s *RuntimeServiceImpl) DeleteProcessInstance(ctx context.Context, actor Actor, processInstanceID, reason string) error {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return fmt.Errorf("process instance ID cannot be empty")
	}

	err := WithInstanceTx(ctx, s.instanceDAO.Query, processInstanceID, func(scope *InstanceScope) error {
		tx := scope.Tx()
		// 1. 在事务内读取实例（已持锁）
		instance, err := tx.WfInstance.WithContext(ctx).Where(tx.WfInstance.ID.Eq(processInstanceID)).First()
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: process instance", ErrInstanceNotFound)
			}
			return fmt.Errorf("failed to get process instance: %w", err)
		}

		if err := ensureTenantAccess(ctx, "process instance", instance.TenantID); err != nil {
			return err
		}

		// 2. 草稿未进入流转，无历史可归档，直接物理删除
		if instance.Status == string(enums.InstanceStatusDraft) {
			if _, err := tx.WfInstance.WithContext(ctx).Where(tx.WfInstance.ID.Eq(processInstanceID)).Delete(); err != nil {
				return fmt.Errorf("failed to delete draft instance: %w", err)
			}
			// 草稿创建可能已装载租户池，best-effort 驱逐
			s.evictStaleChain(ctx, tx, instance.TenantID, instance.ProcessID)
			return nil
		}

		// 3. 终止活跃任务并归档所有任务到历史表
		tasks, err := tx.WfTask.WithContext(ctx).Where(tx.WfTask.ProcessInstanceID.Eq(processInstanceID)).Find()
		if err != nil {
			return fmt.Errorf("failed to get tasks for archiving: %w", err)
		}

		deleteTime := time.Now()
		hiTasks := make([]*model.WfHiTask, 0, len(tasks))
		for _, task := range tasks {
			if task.Status == string(enums.TaskStatusActive) ||
				task.Status == string(enums.TaskStatusPending) ||
				task.Status == string(enums.TaskStatusSuspended) {
				task.Status = string(enums.TaskStatusTerminated)
				task.EndedAt = &deleteTime
				endReason := "流程实例被删除"
				task.EndReason = &endReason
			}
			hiTasks = append(hiTasks, taskToHiTask(task))
		}
		if len(hiTasks) > 0 {
			if err := tx.WfHiTask.WithContext(ctx).CreateInBatches(hiTasks, archiveBatchSize); err != nil {
				return fmt.Errorf("failed to archive tasks to history: %w", err)
			}
		}

		// 4. 归档实例到历史表
		now := time.Now()
		hiInstance := &model.WfHiInstance{
			ID:              instance.ID,
			ProcessID:       instance.ProcessID,
			BusinessKey:     instance.BusinessKey,
			Name:            instance.Name,
			Status:          string(enums.InstanceStatusDeleted),
			Variables:       instance.Variables,
			CurrentActivity: instance.CurrentActivity,
			Priority:        instance.Priority,
			ParentID:        instance.ParentID,
			TenantID:        instance.TenantID,
			CreatedBy:       instance.CreatedBy,
			CreatedAt:       instance.CreatedAt,
			UpdatedBy:       instance.UpdatedBy,
			UpdatedAt:       &now,
			EndReason:       &reason,
			Duration:        instance.Duration,
			EndedAt:         instance.EndedAt,
			StartUserID:     instance.StartUserID,
		}
		if err := tx.WfHiInstance.WithContext(ctx).Create(hiInstance); err != nil {
			return fmt.Errorf("failed to archive instance to history: %w", err)
		}

		// 5. 清理候选池 wf_task_assignee（避免孤儿，参照 TaskDAO.DeleteByProcessInstanceID）
		taskIDs := make([]string, 0, len(tasks))
		for _, t := range tasks {
			taskIDs = append(taskIDs, t.ID)
		}
		if len(taskIDs) > 0 {
			if _, err := tx.WfTaskAssignee.WithContext(ctx).Where(tx.WfTaskAssignee.TaskID.In(taskIDs...)).Delete(); err != nil {
				logrus.Warnf("failed to clean wf_task_assignee: %v", err)
			}
		}

		// 6. 删除原始任务记录
		if _, err := tx.WfTask.WithContext(ctx).Where(tx.WfTask.ProcessInstanceID.Eq(processInstanceID)).Delete(); err != nil {
			return fmt.Errorf("failed to delete tasks: %w", err)
		}

		// 7. 删除原始实例记录
		if _, err := tx.WfInstance.WithContext(ctx).Where(tx.WfInstance.ID.Eq(processInstanceID)).Delete(); err != nil {
			return fmt.Errorf("failed to delete instance: %w", err)
		}

		return nil
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrInstanceNotFound) {
		return s.markArchivedInstanceDeleted(ctx, processInstanceID, reason)
	}
	return err
}

// markArchivedInstanceDeleted 把已归档实例的历史行标为 deleted。
func (s *RuntimeServiceImpl) markArchivedInstanceDeleted(ctx context.Context, processInstanceID, reason string) error {
	q := s.instanceDAO.Query
	hi, err := q.WfHiInstance.WithContext(ctx).Where(q.WfHiInstance.ID.Eq(processInstanceID)).First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: process instance", ErrInstanceNotFound)
		}
		return fmt.Errorf("failed to get archived instance: %w", err)
	}
	if err := ensureTenantAccess(ctx, "process instance", hi.TenantID); err != nil {
		return err
	}
	if _, err := q.WfHiInstance.WithContext(ctx).
		Where(q.WfHiInstance.ID.Eq(processInstanceID)).
		Where(q.WfHiInstance.Status.Neq(string(enums.InstanceStatusDeleted))).
		Updates(map[string]interface{}{
			"status":     string(enums.InstanceStatusDeleted),
			"end_reason": reason,
			"updated_at": time.Now(),
		}); err != nil {
		return fmt.Errorf("failed to mark archived instance deleted: %w", err)
	}
	return nil
}

// DeleteProcessInstances 批量删除流程实例
func (s *RuntimeServiceImpl) DeleteProcessInstances(ctx context.Context, actor Actor, processInstanceIDs []string, reason string) error {
	ctx = bindActor(ctx, actor)
	for _, instanceID := range processInstanceIDs {
		if err := s.DeleteProcessInstance(ctx, actor, instanceID, reason); err != nil {
			return err
		}
	}
	return nil
}

// SuspendProcessInstance 挂起流程实例
func (s *RuntimeServiceImpl) SuspendProcessInstance(ctx context.Context, actor Actor, processInstanceID string) error {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return fmt.Errorf("process instance ID cannot be empty")
	}
	return WithInstanceTx(ctx, s.instanceDAO.Query, processInstanceID, func(scope *InstanceScope) error {
		return s.suspendProcessInstanceInternal(ctx, scope, processInstanceID)
	})
}

func (s *RuntimeServiceImpl) suspendProcessInstanceInternal(ctx context.Context, scope *InstanceScope, processInstanceID string) error {
	instanceDAO := scope.Instances()
	taskDAO := scope.Tasks()

	instance, err := instanceDAO.Get(ctx, processInstanceID)
	if err != nil {
		return fmt.Errorf("failed to get process instance: %w", err)
	}
	if instance == nil {
		return fmt.Errorf("%w: process instance", ErrInstanceNotFound)
	}

	// 幂等：已经是 Suspended
	if instance.Status == string(enums.InstanceStatusSuspended) {
		return nil
	}

	// 草稿不可挂起：挂起后再激活会跳过草稿激活闸（创建者校验、发起人范围重查、
	// 引擎首驱），实例以 Active 状态存在却从未创建过任何任务，成为无人可见的僵尸。
	// 草稿的生命周期动作是编辑、提交（激活）与删除。
	if instance.Status == string(enums.InstanceStatusDraft) {
		return fmt.Errorf("draft instance %s cannot be suspended; submit or delete it instead: %w",
			processInstanceID, ErrValidation)
	}

	if currentUser := GetUserFromCtx(ctx); currentUser != nil {
		if err := ensureTenantAccess(ctx, "process instance", instance.TenantID); err != nil {
			return err
		}
		instance.UpdatedBy = &currentUser.UserName
	} else {
		username := s.GetUsernameFromCtx(ctx)
		instance.UpdatedBy = &username
	}

	// 级联挂起所有活跃任务（使用 tx-scoped 查询）
	tx := scope.Tx()
	q := tx.WfTask
	activeTasks, err := q.WithContext(ctx).
		Where(q.ProcessInstanceID.Eq(processInstanceID)).
		Where(q.Status.In(string(enums.TaskStatusActive), string(enums.TaskStatusPending))).
		Find()
	if err != nil {
		logrus.Warnf("Failed to get tasks for suspend cascade: %v", err)
	} else {
		now := time.Now()
		for _, task := range activeTasks {
			task.Status = string(enums.TaskStatusSuspended)
			task.UpdatedAt = &now
			if err := taskDAO.Update(ctx, task); err != nil {
				logrus.Warnf("Failed to suspend task %s: %v", task.ID, err)
			}
		}
	}

	now := time.Now()
	instance.UpdatedAt = &now
	instance.Status = string(enums.InstanceStatusSuspended)
	if err := instanceDAO.Update(ctx, instance); err != nil {
		return err
	}

	// 挂起事件：AfterCommit 派发，回滚不产生幽灵事件
	if s.workflowEngine.GetTaskEventListener() != nil {
		fromUser := ""
		if u := GetUserFromCtx(ctx); u != nil {
			fromUser = u.UserID
		}
		var toUsers []string
		if instance.StartUserID != "" {
			toUsers = []string{instance.StartUserID}
		}
		suspendCtx := ctx
		scope.AfterCommit(func() error {
			DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), TaskEvent{
				Type:       TaskEventSuspended,
				InstanceID: processInstanceID,
				ProcessID:  instance.ProcessID,
				TenantID:   instance.TenantID,
				ToUsers:    toUsers,
				FromUser:   fromUser,
				Source:     EventSourceFromCtx(suspendCtx),
				Timestamp:  time.Now(),
			}, suspendCtx)
			return nil
		})
	}
	return nil
}

// ActivateProcessInstance 激活流程实例
//
// 草稿激活时启动 rulechain engine 这一步必须在事务外执行——engine.OnMsg 内部
// 回调 TaskService 创建任务时会各自进入 WithInstanceTx，若嵌套在同一事务会死锁。
// 草稿激活视为正式发起：按原创建者重查发起人范围，且仅创建者本人可激活。
func (s *RuntimeServiceImpl) ActivateProcessInstance(ctx context.Context, actor Actor, processInstanceID string) error {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return fmt.Errorf("process instance ID cannot be empty")
	}

	// 持锁执行实例状态变更 + 级联任务激活
	var isDraft bool
	var draftInstance *model.WfInstance
	if err := WithInstanceTx(ctx, s.instanceDAO.Query, processInstanceID, func(scope *InstanceScope) error {
		inst, startedDraft, err := s.activateProcessInstanceInternal(ctx, scope, processInstanceID)
		if err != nil {
			return err
		}
		draftInstance = inst
		// 是否草稿激活由持锁事务内的实际状态迁移决定。不能在事务外用
		// `active && CurrentActivity==nil` 反推：首次激活提交后引擎 OnMsg 在事务外
		// 跑，CurrentActivity 要等 userTask 回调才落库，这个窗口里并发的第二个请求
		// 会命中同样的特征而被误判成草稿激活，二次驱动引擎。
		isDraft = startedDraft
		return nil
	}); err != nil {
		return err
	}

	// 草稿激活：在事务外启动引擎（engine.OnMsg 会通过 aspect 回调 TaskService，
	// 那些调用各自进入 WithInstanceTx）
	if isDraft && draftInstance != nil {
		processDef, err := s.processDAO.Get(ctx, draftInstance.ProcessID)
		if err != nil {
			return fmt.Errorf("failed to get process definition: %w", err)
		}
		engine, err := s.initExecution(processDef.TenantID, processDef.ID, processDef.DefinitionJSON)
		if err != nil {
			return err
		}
		md := types.NewMetadata()
		md.PutValue(constants.KeyTenantID, processDef.TenantID)
		md.PutValue(constants.KeyInstanceID, draftInstance.ID)
		if draftInstance.BusinessKey != nil {
			md.PutValue(constants.KeyBusinessKey, *draftInstance.BusinessKey)
		}
		md.PutValue(constants.KeyOwner, draftInstance.CreatedBy)
		md.PutValue(constants.KeyProcessID, processDef.ID)
		md.PutValue(constants.KeyProcessKey, processDef.ProcessKey)

		var variablesStr string
		if draftInstance.Variables != nil {
			variablesStr = *draftInstance.Variables
		}
		var msg = types.NewMsg(0, "wf", types.JSON, md, variablesStr)
		engine.OnMsg(msg)
	}
	return nil
}

// activateProcessInstanceInternal 在已持锁事务内更新实例状态 + 级联激活任务。
// 返回更新后的 instance，以及本次调用是否真正把 draft 翻成了 active
// （startedDraft=true 时调用方负责在事务外启动引擎）。幂等返回与挂起恢复
// 都是 false：只有实际发生 draft→active 迁移的那一次才驱动引擎。
func (s *RuntimeServiceImpl) activateProcessInstanceInternal(ctx context.Context, scope *InstanceScope, processInstanceID string) (*model.WfInstance, bool, error) {
	instanceDAO := scope.Instances()
	taskDAO := scope.Tasks()

	instance, err := instanceDAO.Get(ctx, processInstanceID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get process instance: %w", err)
	}
	if instance == nil {
		return nil, false, fmt.Errorf("%w: process instance", ErrInstanceNotFound)
	}

	// 幂等：已经是 Active（非 draft）。租户校验前置，避免跨租户探测实例是否已激活。
	if instance.Status == string(enums.InstanceStatusActive) {
		if err := ensureTenantAccess(ctx, "process instance", instance.TenantID); err != nil {
			return nil, false, err
		}
		return instance, false, nil
	}

	// 租户校验先于草稿闸：跨租户一律按租户错误拒绝，不进入创建者/范围判断
	// （两类都是 ErrPermissionDenied，但错误信息不应泄露草稿归属细节）。
	if err := ensureTenantAccess(ctx, "process instance", instance.TenantID); err != nil {
		return nil, false, err
	}

	// 草稿激活 = 正式发起：范围可能在草稿创建后收紧，按原创建者重查一次
	// 发起人范围；且仅创建者本人可激活（系统身份不冒充创建者，跳过此限，见
	// IsSystemActor；挂起恢复不进此分支，仍允许他人操作）。StartUserID 为空的
	// 存量草稿不 restrict 创建者，但范围校验照跑（空用户过不了 user/role 名单，
	// fail-closed）。
	if instance.Status == string(enums.InstanceStatusDraft) {
		if currentUser := GetUserFromCtx(ctx); currentUser != nil && !IsSystemActor(currentUser) &&
			instance.StartUserID != "" && currentUser.UserID != instance.StartUserID {
			return nil, false, fmt.Errorf("only the creator can activate draft instance %s: %w", processInstanceID, ErrPermissionDenied)
		}
		processDef, err := s.processDAO.Get(ctx, instance.ProcessID)
		if err != nil {
			return nil, false, fmt.Errorf("failed to get process definition for draft activation: %w", err)
		}
		// 定义已删的孤儿草稿：明确报错，不得静默跳过范围校验放行激活
		if processDef == nil {
			return nil, false, fmt.Errorf("%w: %s (draft instance %s)", ErrProcessDefinitionNotFound, instance.ProcessID, processInstanceID)
		}
		initiator := Actor{UserID: instance.StartUserID, TenantID: instance.TenantID}
		if err := s.checkStarterScope(ctx, processDef, initiator); err != nil {
			return nil, false, err
		}
	}

	if currentUser := GetUserFromCtx(ctx); currentUser != nil {
		instance.UpdatedBy = &currentUser.UserName
	} else {
		username := s.GetUsernameFromCtx(ctx)
		instance.UpdatedBy = &username
	}

	wasDraft := instance.Status == string(enums.InstanceStatusDraft)

	now := time.Now()
	instance.UpdatedAt = &now
	instance.Status = string(enums.InstanceStatusActive)
	if err := instanceDAO.Update(ctx, instance); err != nil {
		return nil, false, err
	}

	// 恢复事件：AfterCommit 派发，覆盖挂起恢复与草稿激活两种路径
	if s.workflowEngine.GetTaskEventListener() != nil {
		fromUser := ""
		if u := GetUserFromCtx(ctx); u != nil {
			fromUser = u.UserID
		}
		var toUsers []string
		if instance.StartUserID != "" {
			toUsers = []string{instance.StartUserID}
		}
		activateCtx := ctx
		scope.AfterCommit(func() error {
			DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), TaskEvent{
				Type:       TaskEventActivated,
				InstanceID: processInstanceID,
				ProcessID:  instance.ProcessID,
				TenantID:   instance.TenantID,
				ToUsers:    toUsers,
				FromUser:   fromUser,
				Source:     EventSourceFromCtx(activateCtx),
				Timestamp:  time.Now(),
			}, activateCtx)
			return nil
		})
	}

	if !wasDraft {
		tx := scope.Tx()
		q := tx.WfTask
		suspendedTasks, err := q.WithContext(ctx).
			Where(q.ProcessInstanceID.Eq(processInstanceID)).
			Where(q.Status.Eq(string(enums.TaskStatusSuspended))).
			Find()
		if err != nil {
			logrus.Warnf("Failed to get tasks for activate cascade: %v", err)
		} else {
			now := time.Now()
			for _, task := range suspendedTasks {
				// 按挂起前语义恢复：有 assignee → active，无 assignee(候选组待认领) → pending
				if task.Assignee != nil && *task.Assignee != "" {
					task.Status = string(enums.TaskStatusActive)
				} else {
					task.Status = string(enums.TaskStatusPending)
				}
				task.UpdatedAt = &now
				if err := taskDAO.Update(ctx, task); err != nil {
					logrus.Warnf("Failed to activate task %s: %v", task.ID, err)
				}
			}
		}
	}

	// wasDraft 时由调用方在事务外启动引擎，此处统一返回
	return instance, wasDraft, nil
}

// GetProcessInstanceVariables 获取流程实例变量
func (s *RuntimeServiceImpl) GetProcessInstanceVariables(ctx context.Context, actor Actor, processInstanceID string) (map[string]interface{}, error) {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return nil, fmt.Errorf("process instance ID cannot be empty")
	}

	instance, err := s.instanceDAO.Get(ctx, processInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get process instance: %w", err)
	}
	if instance == nil {
		// 流程终止/完成后实例归档到历史表 wf_hi_instance，活表查不到时回退历史表，
		// 与 GetProcessInstance 行为对齐——否则 terminated 实例查变量会 404/500。
		// DB 故障不进回退，避免用历史查询掩盖基础设施错误。
		instance, err = s.hiInstanceDAO.Get(ctx, processInstanceID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get process instance: %w", err)
	}

	if instance == nil {
		return nil, fmt.Errorf("%w: process instance", ErrInstanceNotFound)
	}

	// 检查权限：actor 租户非空时实例必须同租户；空租户视为系统视角，跳过校验
	if err := ensureTenantAccess(ctx, "process instance", instance.TenantID); err != nil {
		return nil, err
	}

	return ParseVariablesJSON(instance.Variables)
}

// GetProcessInstanceVariable 获取指定流程实例变量
func (s *RuntimeServiceImpl) GetProcessInstanceVariable(ctx context.Context, actor Actor, processInstanceID, variableName string) (interface{}, error) {
	variables, err := s.GetProcessInstanceVariables(ctx, actor, processInstanceID)
	if err != nil {
		return nil, err
	}

	return variables[variableName], nil
}

// SetProcessInstanceVariables 批量设置流程实例变量。
// 读-合并-写必须在实例行锁事务内完成，否则并发写会用整 map 互相覆盖；
// 与单变量版 SetProcessInstanceVariable 同口径。
func (s *RuntimeServiceImpl) SetProcessInstanceVariables(ctx context.Context, actor Actor, processInstanceID string, variables map[string]interface{}) error {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return fmt.Errorf("process instance ID cannot be empty")
	}

	instance, err := s.instanceDAO.Get(ctx, processInstanceID)
	if err != nil {
		return fmt.Errorf("failed to get process instance: %w", err)
	}
	if instance == nil {
		return fmt.Errorf("%w: process instance", ErrInstanceNotFound)
	}

	if err := ensureTenantAccess(ctx, "process instance", instance.TenantID); err != nil {
		return err
	}

	return WithInstanceTx(ctx, s.instanceDAO.Query, processInstanceID, func(scope *InstanceScope) error {
		tx := scope.Tx()
		existingVars, err := s.getProcessInstanceVariablesInTx(ctx, tx, processInstanceID)
		if err != nil {
			return err
		}
		for k, v := range variables {
			existingVars[k] = v
		}
		return s.setProcessInstanceVariablesInTx(ctx, tx, processInstanceID, existingVars)
	})
}

// SetProcessInstanceVariable 设置指定流程实例变量
func (s *RuntimeServiceImpl) SetProcessInstanceVariable(ctx context.Context, actor Actor, processInstanceID, variableName string, value interface{}) error {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return fmt.Errorf("process instance ID cannot be empty")
	}
	if variableName == "" {
		return fmt.Errorf("variable name cannot be empty")
	}
	return WithInstanceTx(ctx, s.instanceDAO.Query, processInstanceID, func(scope *InstanceScope) error {
		tx := scope.Tx()
		variables, err := s.getProcessInstanceVariablesInTx(ctx, tx, processInstanceID)
		if err != nil {
			return err
		}
		variables[variableName] = value
		return s.setProcessInstanceVariablesInTx(ctx, tx, processInstanceID, variables)
	})
}

// RemoveProcessInstanceVariable 删除流程实例变量
func (s *RuntimeServiceImpl) RemoveProcessInstanceVariable(ctx context.Context, actor Actor, processInstanceID, variableName string) error {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return fmt.Errorf("process instance ID cannot be empty")
	}
	if variableName == "" {
		return fmt.Errorf("variable name cannot be empty")
	}
	return WithInstanceTx(ctx, s.instanceDAO.Query, processInstanceID, func(scope *InstanceScope) error {
		tx := scope.Tx()
		variables, err := s.getProcessInstanceVariablesInTx(ctx, tx, processInstanceID)
		if err != nil {
			return err
		}
		delete(variables, variableName)
		return s.setProcessInstanceVariablesInTx(ctx, tx, processInstanceID, variables)
	})
}

// getProcessInstanceVariablesInTx 读取实例变量（在调用方持锁事务内）。
func (s *RuntimeServiceImpl) getProcessInstanceVariablesInTx(ctx context.Context, tx *query.Query, processInstanceID string) (map[string]interface{}, error) {
	if processInstanceID == "" {
		return nil, fmt.Errorf("process instance ID cannot be empty")
	}
	instance, err := tx.WfInstance.WithContext(ctx).Where(tx.WfInstance.ID.Eq(processInstanceID)).First()
	if err != nil {
		return nil, fmt.Errorf("failed to get process instance: %w", err)
	}
	if err := ensureTenantAccess(ctx, "process instance", instance.TenantID); err != nil {
		return nil, err
	}
	return ParseVariablesJSON(instance.Variables)
}

// setProcessInstanceVariablesInTx 写入实例变量（在调用方持锁事务内）。
func (s *RuntimeServiceImpl) setProcessInstanceVariablesInTx(ctx context.Context, tx *query.Query, processInstanceID string, variables map[string]interface{}) error {
	variablesJSON, err := json.Marshal(variables)
	if err != nil {
		return fmt.Errorf("failed to marshal variables: %w", err)
	}
	variablesStr := string(variablesJSON)
	username := s.GetUsernameFromCtx(ctx)
	now := time.Now()
	if _, err := tx.WfInstance.WithContext(ctx).Where(tx.WfInstance.ID.Eq(processInstanceID)).
		Updates(map[string]interface{}{
			tx.WfInstance.Variables.ColumnName().String(): variablesStr,
			tx.WfInstance.UpdatedBy.ColumnName().String(): username,
			tx.WfInstance.UpdatedAt.ColumnName().String(): &now,
		}); err != nil {
		return fmt.Errorf("failed to update instance variables: %w", err)
	}
	return nil
}

// RestartProcessInstance 重启流程实例，返回新实例 ID。
//
// 实现策略：基于原实例的 process_id + 初始 variables 创建一条全新的 WfInstance，
// 然后从 activityID 指定的节点（为空则从头）驱动引擎。
// 不会修改原实例。
//
// 限制：
//   - 新实例 businessKey 为原值追加 "-restart" 后缀；不会重新校验原实例的业务键唯一性
//     （StartProcessInstanceByID 会做），调用方需自行保证；
//   - 不会迁移原实例的运行时上下文（current_activity / 任务状态），新实例从 activityID 开始；
//   - 原 variables JSON 反序列化失败时新实例以空变量启动，不阻断重启流程。
func (s *RuntimeServiceImpl) RestartProcessInstance(ctx context.Context, actor Actor, processInstanceID, activityID string) (string, error) {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return "", fmt.Errorf("process instance ID cannot be empty")
	}
	// 防御性检查：DAO 未注入时显式失败，避免 nil 解引用 panic。
	if s.instanceDAO == nil || s.processDAO == nil {
		return "", fmt.Errorf("RuntimeService not initialized: instanceDAO/processDAO is nil; cannot restart process instance %s", processInstanceID)
	}

	original, err := s.instanceDAO.Get(ctx, processInstanceID)
	if err != nil {
		return "", fmt.Errorf("failed to get original process instance: %w", err)
	}
	if original == nil {
		return "", fmt.Errorf("%w: process instance %s", ErrInstanceNotFound, processInstanceID)
	}

	if err := ensureTenantAccess(ctx, "process instance", original.TenantID); err != nil {
		return "", err
	}

	// 拿到原实例对应的流程定义（必须仍存在）
	processDef, err := s.processDAO.Get(ctx, original.ProcessID)
	if err != nil {
		return "", fmt.Errorf("failed to get process definition for restart: %w", err)
	}
	if processDef == nil {
		return "", fmt.Errorf("process definition not found: %s", original.ProcessID)
	}

	// 反序列化原实例 variables 作为新实例的初始变量；解析失败不阻断重启，
	// 以空变量启动新实例
	variables, _ := ParseVariablesJSON(original.Variables)
	if variables == nil {
		variables = map[string]interface{}{}
	}

	// 复用 StartProcessInstanceByID 的初始化路径。新 BusinessKey 加 -restart 后缀避免冲突。
	businessKey := ""
	if original.BusinessKey != nil && *original.BusinessKey != "" {
		businessKey = *original.BusinessKey + "-restart"
	}

	// CreatedBy 存发起人用户 ID（见 startInstanceCore 注释），重启沿用原发起人；
	// UserName 落新实例的 UpdatedBy，取执行重启的操作人（实例上未存原发起人用户名，
	// 不能把用户 ID 填进用户名字段）。
	initiator := Actor{
		UserID:   original.StartUserID,
		UserName: actor.UserName,
		TenantID: original.TenantID,
	}

	newInstanceID, err := s.StartProcessInstanceByID(ctx, initiator, processDef.ID, businessKey, variables)
	// 语义 actor：重启沿用原实例发起人（见上方 initiator 构造）
	if err != nil {
		return "", fmt.Errorf("failed to start restarted process instance: %w", err)
	}

	// 如果调用方指定了 activityID，从该节点重新驱动一次引擎；
	// 不指定则 StartProcessInstanceByID 内部已经从头跑过，无需再触发。
	if activityID != "" {
		if err := s.ExecuteNext(WithInternalCallingMode(ctx), newInstanceID, activityID, variables); err != nil {
			return "", fmt.Errorf("failed to advance restarted instance to activity %s: %w", activityID, err)
		}
	}

	return newInstanceID, nil
}

// CompleteProcessInstance 完成流程实例并自动归档到历史表
//
// 幂等性：对已经处于 Completed 状态的实例重复调用直接返回 nil（重试安全）；
// 其余非 active 状态返回 ErrValidation；实例不存在返回 ErrInstanceNotFound。
func (s *RuntimeServiceImpl) CompleteProcessInstance(ctx context.Context, actor Actor, processInstanceID, reason string) error {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return fmt.Errorf("process instance ID cannot be empty")
	}

	// 预读实例做租户/状态校验（廉价读）
	instance, err := s.GetProcessInstance(ctx, actor, processInstanceID)
	if err != nil {
		return fmt.Errorf("failed to get process instance: %w", err)
	}
	if instance == nil {
		return fmt.Errorf("%w: process instance %s", ErrInstanceNotFound, processInstanceID)
	}
	if err := ensureTenantAccess(ctx, "process instance", instance.TenantID); err != nil {
		return err
	}
	// 幂等：实例已经是 Completed（用户重试场景），直接返回
	if instance.Status == string(enums.InstanceStatusCompleted) {
		return nil
	}
	if instance.Status != string(enums.InstanceStatusActive) {
		return fmt.Errorf("process instance is not in running status, current status: %s: %w", instance.Status, ErrValidation)
	}

	now := time.Now()
	duration := now.Sub(instance.CreatedAt).Milliseconds()

	if err := WithInstanceTx(ctx, s.instanceDAO.Query, processInstanceID, func(scope *InstanceScope) error {
		tx := scope.Tx()
		// 1. 更新实例状态为已完成
		if _, err := tx.WfInstance.WithContext(ctx).Where(tx.WfInstance.ID.Eq(processInstanceID)).
			Updates(map[string]interface{}{
				tx.WfInstance.Status.ColumnName().String():    enums.InstanceStatusCompleted,
				tx.WfInstance.EndReason.ColumnName().String(): reason,
				tx.WfInstance.Duration.ColumnName().String():  duration,
				tx.WfInstance.EndedAt.ColumnName().String():   &now,
				tx.WfInstance.UpdatedAt.ColumnName().String(): &now,
			}); err != nil {
			return fmt.Errorf("failed to update instance status: %w", err)
		}

		// 2. 获取要归档的实例数据
		updatedInstance, err := tx.WfInstance.WithContext(ctx).Where(tx.WfInstance.ID.Eq(processInstanceID)).First()
		if err != nil {
			return fmt.Errorf("failed to get instance for archiving: %w", err)
		}

		// 3. 创建历史实例记录
		hiInstance := &model.WfHiInstance{
			ID:              updatedInstance.ID,
			ProcessID:       updatedInstance.ProcessID,
			BusinessKey:     updatedInstance.BusinessKey,
			Name:            updatedInstance.Name,
			Status:          updatedInstance.Status,
			Variables:       updatedInstance.Variables,
			CurrentActivity: updatedInstance.CurrentActivity,
			Priority:        updatedInstance.Priority,
			ParentID:        updatedInstance.ParentID,
			TenantID:        updatedInstance.TenantID,
			CreatedBy:       updatedInstance.CreatedBy,
			CreatedAt:       updatedInstance.CreatedAt,
			UpdatedBy:       updatedInstance.UpdatedBy,
			UpdatedAt:       updatedInstance.UpdatedAt,
			EndReason:       &reason,
			Duration:        updatedInstance.Duration,
			EndedAt:         updatedInstance.EndedAt,
			StartUserID:     updatedInstance.StartUserID,
		}

		if err := tx.WfHiInstance.WithContext(ctx).Create(hiInstance); err != nil {
			return fmt.Errorf("failed to create history instance: %w", err)
		}

		// 4. 获取该实例的所有任务
		tasks, err := tx.WfTask.WithContext(ctx).Where(tx.WfTask.ProcessInstanceID.Eq(processInstanceID)).Find()
		if err != nil {
			return fmt.Errorf("failed to get tasks for archiving: %w", err)
		}

		// 5. 归档所有任务到历史表（批量插入，大会签实例免逐条往返）
		hiTasks := make([]*model.WfHiTask, 0, len(tasks))
		for _, task := range tasks {
			hiTasks = append(hiTasks, taskToHiTask(task))
		}
		if len(hiTasks) > 0 {
			if err := tx.WfHiTask.WithContext(ctx).CreateInBatches(hiTasks, archiveBatchSize); err != nil {
				return fmt.Errorf("failed to archive tasks to history: %w", err)
			}
		}

		// 6. 清理候选池 wf_task_assignee（避免孤儿）
		taskIDs := make([]string, 0, len(tasks))
		for _, t := range tasks {
			taskIDs = append(taskIDs, t.ID)
		}
		if len(taskIDs) > 0 {
			if _, err := tx.WfTaskAssignee.WithContext(ctx).Where(tx.WfTaskAssignee.TaskID.In(taskIDs...)).Delete(); err != nil {
				logrus.Warnf("failed to clean wf_task_assignee: %v", err)
			}
		}

		// 7. 删除原始任务记录
		if _, err := tx.WfTask.WithContext(ctx).Where(tx.WfTask.ProcessInstanceID.Eq(processInstanceID)).Delete(); err != nil {
			return fmt.Errorf("failed to delete original tasks: %w", err)
		}

		// 8. 删除原始实例记录
		if _, err := tx.WfInstance.WithContext(ctx).Where(tx.WfInstance.ID.Eq(processInstanceID)).Delete(); err != nil {
			return fmt.Errorf("failed to delete original instance: %w", err)
		}

		// 触发完成事件（通知发起人）。AfterCommit 派发：监听器读到已提交状态，
		// 回滚不产生幽灵通知。
		if s.workflowEngine.GetTaskEventListener() != nil {
			toUsers := []string{}
			if updatedInstance.StartUserID != "" {
				toUsers = append(toUsers, updatedInstance.StartUserID)
			}
			if len(toUsers) > 0 {
				fromUser := ""
				if u := GetUserFromCtx(ctx); u != nil {
					fromUser = u.UserID
				}
				evt := TaskEvent{
					Type:       TaskEventCompleted,
					InstanceID: processInstanceID,
					ProcessID:  updatedInstance.ProcessID,
					TenantID:   updatedInstance.TenantID,
					ToUsers:    toUsers,
					FromUser:   fromUser,
					Timestamp:  time.Now(),
				}
				scope.AfterCommit(func() error {
					DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), evt, ctx)
					return nil
				})
			}
		}

		return nil
	}); err != nil {
		return err
	}
	// subProcess 嵌套闭环：子实例完成 → 恢复父流程（重跑父的 subProcess 节点 → 见子已完成 → TellNext）
	if instance.ParentID != nil && *instance.ParentID != "" {
		parentNodeID, ok := "", false
		if v, mok := s.subProcessParentNodes.LoadAndDelete(instance.ID); mok {
			parentNodeID, ok = v.(string), true // 快路径：内存映射
		} else {
			parentNodeID, ok = s.deriveSubProcessParentNode(ctx, *instance.ParentID, instance.ProcessID) // 慢路径：重启后内存映射丢失，从 DB 重推导父节点
		}
		if ok {
			if err := s.ExecuteNext(ctx, *instance.ParentID, parentNodeID, nil); err != nil {
				logrus.WithError(err).Warn("resume parent after subProcess child completed failed")
			}
		}
	}
	// 实例完成后 best-effort 驱逐：若其版本已无其它活实例且非最新版，清理租户池链（收内存）
	s.EvictStaleChain(ctx, instance.TenantID, instance.ProcessID)
	return nil
}

func (s *RuntimeServiceImpl) ExecuteNext(ctx context.Context, processInstanceID, startNodeId string, variables map[string]interface{}) error {
	release, reentrant := s.acquireExecGate(processInstanceID)
	defer release()
	if !reentrant {
		// 跨副本互斥：execGate 只串行化本进程；多副本共享库时由 Locker（宿主注入
		// Redis 锁，单机 LocalLock 无感）串行化两副本对同一实例的并发驱动。
		if unlock := s.acquireDistExecGate(ctx, processInstanceID); unlock != nil {
			defer unlock()
		}
		// 等门闩期间实例可能已被先到的驱动完成（实例行已删除）：幂等返回 nil，
		// 让并发审批的尾随 ExecuteNext 安静退出而不是报 instance not found。
		// 同 goroutine 重入不重复查库（外层驱动刚查过状态）。
		// 注意 Get 对不存在的行返回 (nil, nil)，真实 DB 错误须向上抛，
		// 否则会把故障误判为"实例已完成"而静默吞掉。
		inst, err := s.instanceDAO.Get(ctx, processInstanceID)
		if err != nil {
			return err
		}
		if inst == nil {
			return nil
		}
	}
	return s.executeNextLocked(ctx, processInstanceID, startNodeId, variables)
}

func (s *RuntimeServiceImpl) executeNextLocked(ctx context.Context, processInstanceID, startNodeId string, variables map[string]interface{}) error {

	processInstance, err := s.instanceDAO.Get(ctx, processInstanceID)
	if err != nil {
		return err
	}
	if processInstance == nil {
		return fmt.Errorf("%w: process instance %s", ErrInstanceNotFound, processInstanceID)
	}
	if processInstance.ProcessID == "" {
		return fmt.Errorf("process instance ProcessID is empty: %s", processInstanceID)
	}

	// 实例状态守卫：
	//   - API / 外部调用：Suspended / Terminated / Cancelled / Failed 都拒绝。
	//     Suspended 在外部调用时拒绝（先 Resume 再 ExecuteNext），避免在已挂起实例上推进。
	//   - 内部调用（CallingModeInternal，如 end-node aspect 完成 Completed 实例的尾任务清理）：
	//     仅拒绝 Terminated / Cancelled / Failed（终态）。允许 Completed 走尾任务收尾路径。
	//   - CallingModeUnknown：未标记调用模式，按内部调用规则处理（仍拒绝终态）。
	callingMode := GetCallingMode(ctx)
	blockForAPI := callingMode == CallingModeAPI
	switch processInstance.Status {
	case string(enums.InstanceStatusTerminated),
		string(enums.InstanceStatusCancelled),
		string(enums.InstanceStatusFailed):
		return fmt.Errorf("cannot ExecuteNext on process instance in terminal status: %s", processInstance.Status)
	case string(enums.InstanceStatusSuspended):
		if blockForAPI {
			return fmt.Errorf("process instance is suspended, resume it before ExecuteNext")
		}
		// 内部调用 / 未标记模式：放行。
	case string(enums.InstanceStatusCompleted):
		// 允许 end-node aspect 推进尾任务清理，放行所有调用模式。
	case string(enums.InstanceStatusActive),
		string(enums.InstanceStatusDraft):
		// 正常路径，放行。
	default:
		// 未知状态值（数据脏值），按保守策略拒绝以暴露问题。
		return fmt.Errorf("process instance has unknown status: %s", processInstance.Status)
	}

	e, err := s.GetExecution(ctx, processInstance.ProcessID)
	if err != nil {
		return err
	}
	var businessKey = processInstance.BusinessKey
	md := types.NewMetadata()
	md.PutValue(constants.KeyTenantID, processInstance.TenantID)
	md.PutValue(constants.KeyInstanceID, processInstance.ID)
	if businessKey != nil {
		md.PutValue(constants.KeyBusinessKey, *businessKey)
	}
	md.PutValue(constants.KeyOwner, processInstance.CreatedBy)
	md.PutValue(constants.KeyProcessID, processInstance.ProcessID)
	// process_key 与启动路径的信封对齐（auditLog / aiAgent processInfo 上下文读它）。
	// BPM 流程的 ruleChain.id 即 processKey，从已加载的引擎定义取，零额外查询。
	if def := e.Definition(); def.RuleChain.ID != "" {
		md.PutValue(constants.KeyProcessKey, def.RuleChain.ID)
	}

	// 流程变量载荷:优先用调用方传入的 variables;为空(nil/空 map)时回退到实例存储的启动
	// 业务变量(instance.Variables)。return/jump 等路径传 nil,若直接用空串会让重新生成的任务
	// 丢失全部业务变量(msg.Data 空),审批人回退后看不到原始表单数据。
	dataPayload := str.ToString(variables)
	if len(variables) == 0 && processInstance.Variables != nil && *processInstance.Variables != "" {
		dataPayload = *processInstance.Variables
	}
	msg := types.NewMsg(0, processInstanceID, types.JSON, md, dataPayload)

	// fork-join 拓扑：当 startNodeId 处于 fork → suspend node(s) → join 拓扑时，
	// 单节点重启会让 join 的 TellCollect 丢失 fork 父上下文，永远收不齐分支消息。
	// 因此改走 multi-node 恢复路径（与 RestoreProcessInstance 同一路径），
	// 让 processRestoreNodes 通过 LCA 自动重建 fork 父上下文。
	// 详见 docs/parallel-limitations.md。
	outcome, err := s.analyzeForkResume(ctx, processInstance, startNodeId)
	if err != nil {
		// 不支持的拓扑（嵌套 fork / 分支无暂停节点）：返回错误让上层暴露问题，
		// 不静默 fallback 到 broken 的单节点路径。
		logrus.WithError(err).
			WithField("instanceId", processInstanceID).
			WithField("startNodeId", startNodeId).
			Error("ExecuteNext: unsupported fork topology")
		return err
	}
	switch outcome.decision {
	case forkResumeMulti:
		logrus.WithField("instanceId", processInstanceID).
			WithField("startNodeId", startNodeId).
			Info("ExecuteNext: fork-join detected and all siblings completed; multi-node restore")
		e.OnMsg(msg, types.WithRestoreNodes(outcome.reqs...))
	case forkResumeSkip:
		// 等最后一个兄弟 approve/resume 时由它的 ExecuteNext 触发 multi-node 恢复。
		// INFO 级别（不是 Debug）让运维能排查"为什么用户都 approve 了实例还在 active"。
		logrus.WithField("instanceId", processInstanceID).
			WithField("startNodeId", startNodeId).
			WithField("pendingBranches", outcome.pendingBranches).
			Info("ExecuteNext: fork-join detected but siblings not all completed; waiting for pending branches")
	default:
		e.OnMsg(msg, types.WithStartNode(startNodeId))
	}
	return nil
}

// ForceResumeInstance 强制触发 multi-node 恢复路径，不检查兄弟分支状态。
//
// 用途：当 forkResumeSkip 路径下"最后一个 approve"因为某种原因（网络/超时/用户
// 关浏览器）没触发，实例卡在 active 但所有 task 已 completed 时，管理员调用此
// 方法手动救回。等价于跳过 analyzeForkResume 的状态检查，直接强制 multi-node。
//
// 如果实例不在 fork-join 拓扑里，返回 ErrUnsupportedForkTopology。
// 如果实例已是终态，返回 gorm.ErrRecordNotFound 风格错误。
func (s *RuntimeServiceImpl) ForceResumeInstance(ctx context.Context, actor Actor, processInstanceID string) error {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return fmt.Errorf("process instance ID cannot be empty")
	}
	inst, err := s.instanceDAO.Get(ctx, processInstanceID)
	if err != nil {
		return fmt.Errorf("get process instance: %w", err)
	}
	if inst == nil {
		return fmt.Errorf("%w: process instance %s", ErrInstanceNotFound, processInstanceID)
	}
	// 租户隔离：与其他 RuntimeService 方法保持一致（参考 RestartProcessInstance、
	// TerminateProcessInstance）。ForceResume 是危险操作，绝不能跨租户触发。
	if err := ensureTenantAccess(ctx, "process instance", inst.TenantID); err != nil {
		return err
	}
	if enums.IsTerminalInstanceStatus(enums.InstanceStatus(inst.Status)) {
		return fmt.Errorf("process instance is in terminal status: %s", inst.Status)
	}

	// 跨副本门闩：强制恢复是管理员手动触发的 multi-restore，同样须与对端副本的
	// AfterCommit 驱动互斥（读-判-驱窗口与 RestoreProcessInstance 同构）。
	if unlock := s.acquireDistExecGate(ctx, processInstanceID); unlock != nil {
		defer unlock()
	}

	e, err := s.GetExecution(ctx, inst.ProcessID)
	if err != nil {
		return err
	}

	graph, err := s.loadForkGraph(ctx, inst.ProcessID)
	if err != nil {
		return fmt.Errorf("load fork graph: %w", err)
	}

	// 找实例的 current_activity 所在的 fork 子树，作为强制恢复的入口。
	// 如果 current_activity 为空（aspect 来不及更新），fallback 到所有 fork 节点。
	candidateForks := make([]string, 0)
	if inst.CurrentActivity != nil && *inst.CurrentActivity != "" {
		if fid := graph.findForkAncestor(*inst.CurrentActivity); fid != "" {
			candidateForks = append(candidateForks, fid)
		}
	}
	if len(candidateForks) == 0 {
		// fallback：扫所有分支网关节点（fork / inclusive），找一个其下还有 Active task 的
		for id, t := range graph.nodeType {
			if isBranchingNode(t) {
				candidateForks = append(candidateForks, id)
			}
		}
	}
	if len(candidateForks) == 0 {
		return fmt.Errorf("%w: no fork/inclusive gateway found in process %s", ErrUnsupportedForkTopology, inst.ProcessID)
	}

	tasks, err := s.taskDAO.GetByProcessInstanceID(ctx, inst.ID)
	if err != nil {
		return fmt.Errorf("load tasks: %w", err)
	}
	tasksByKey := make(map[string][]*model.WfTask, len(tasks))
	for _, t := range tasks {
		if t.TaskDefKey == "" {
			continue
		}
		tasksByKey[t.TaskDefKey] = append(tasksByKey[t.TaskDefKey], t)
	}

	// 对每个候选 fork，构造 multi-node reqs。任一 fork 有可恢复分支就触发。
	// sanity check：所有分支出口的 task 必须全部 Completed。如果有 Active 的 task，
	// 说明实例没真正卡死（还有正常 pending 审批），此时 ForceResume 会重新 OnMsg
	// 那个 Active task（userTask 看到 Active 任务 → DoOnEnd 继续等），既没救活又
	// 浪费一次重启。返回 ErrForceResumeActiveBranches 提示调用方先确认。
	for _, forkID := range candidateForks {
		siblings := graph.children[forkID]
		if len(siblings) < 2 {
			continue
		}
		reqs := make([]types.NodeRequest, 0, len(siblings))
		exits := make([]string, 0, len(siblings))
		complete := true
		for _, root := range sortedKeys(siblings) {
			exit := graph.findFirstSuspendNode(root)
			if exit == "" {
				complete = false
				break
			}
			exits = append(exits, exit)
			reqs = append(reqs, restoreNodeRequest(exit, s.buildBranchResumeMsg(inst, exit, tasksByKey[exit])))
		}
		if !complete || len(reqs) < 2 {
			continue
		}

		// sanity check：每条分支出口 task 必须全 Completed。
		var activeExits []string
		for _, exitNodeId := range exits {
			rows := tasksByKey[exitNodeId]
			if len(rows) == 0 {
				// task 不存在：分支可能从未被驱动（fork 没分发到这条分支）。
				// 当作"未完成"处理，提示用户先排查。
				activeExits = append(activeExits, exitNodeId+" (no task)")
				continue
			}
			for _, r := range rows {
				if r.Status != string(enums.TaskStatusCompleted) {
					activeExits = append(activeExits, exitNodeId)
					break
				}
			}
		}
		if len(activeExits) > 0 {
			return fmt.Errorf("%w: fork %s has non-completed branches %v; "+
				"ForceResume is for stuck instances where all branches are completed but the join never fires; "+
				"if there are still active tasks, approve/reject them normally instead",
				ErrForceResumeActiveBranches, forkID, activeExits)
		}

		var variablesStr string
		if inst.Variables != nil {
			variablesStr = *inst.Variables
		} else {
			variablesStr = "{}"
		}
		md := types.NewMetadata()
		md.PutValue(constants.KeyTenantID, inst.TenantID)
		md.PutValue(constants.KeyInstanceID, inst.ID)
		md.PutValue(constants.KeyOwner, inst.CreatedBy)
		md.PutValue(constants.KeyProcessID, inst.ProcessID)
		defaultMsg := types.NewMsg(0, "FORCE_RESUME", types.JSON, md, variablesStr)

		logrus.WithField("instanceId", processInstanceID).
			WithField("forkId", forkID).
			WithField("branches", len(reqs)).
			Warn("ForceResumeInstance: manually triggering multi-node restore")
		e.OnMsg(defaultMsg, types.WithRestoreNodes(reqs...))
		return nil
	}

	return fmt.Errorf("%w: no resumable fork branch found in instance %s",
		ErrUnsupportedForkTopology, processInstanceID)
}

// RestoreProcessInstance 恢复流程实例（需显式 actor：租户校验后重建消息并重驱引擎）。
func (s *RuntimeServiceImpl) RestoreProcessInstance(ctx context.Context, actor Actor, processInstanceID string) error {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return fmt.Errorf("process instance ID cannot be empty")
	}

	// 1. 获取流程实例
	instance, err := s.instanceDAO.Get(ctx, processInstanceID)
	if err != nil {
		return fmt.Errorf("failed to get process instance: %w", err)
	}
	if instance == nil {
		return fmt.Errorf("%w: process instance", ErrInstanceNotFound)
	}
	if err := ensureTenantAccess(ctx, "process instance", instance.TenantID); err != nil {
		return err
	}

	// 跨副本门闩：恢复的"读任务 → 判定 → 驱动"窗口须与对端副本的 AfterCommit
	// 驱动互斥，否则可能基于过期任务快照重复 restore。
	if unlock := s.acquireDistExecGate(ctx, processInstanceID); unlock != nil {
		defer unlock()
	}

	// 2. 获取该实例的所有任务
	tasks, err := s.taskDAO.GetByProcessInstanceID(ctx, processInstanceID)
	if err != nil {
		return fmt.Errorf("failed to get tasks: %w", err)
	}

	var nodeRequests []types.NodeRequest
	var hasActiveTask bool

	for _, task := range tasks {
		// 只处理 Active 或 Pending 状态的任务
		if task.Status == string(enums.TaskStatusActive) || task.Status == string(enums.TaskStatusPending) {
			hasActiveTask = true

			// 准备消息
			var msg types.RuleMsg
			// 优先使用任务变量
			var variablesStr string
			if task.Variables != nil && *task.Variables != "" {
				variablesStr = *task.Variables
			} else if instance.Variables != nil {
				variablesStr = *instance.Variables
			} else {
				variablesStr = "{}"
			}

			// 构建元数据
			md := types.NewMetadata()
			md.PutValue(constants.KeyTenantID, instance.TenantID)
			md.PutValue(constants.KeyInstanceID, instance.ID)
			if instance.BusinessKey != nil {
				md.PutValue(constants.KeyBusinessKey, *instance.BusinessKey)
			}
			md.PutValue(constants.KeyOwner, instance.CreatedBy)
			md.PutValue(constants.KeyProcessID, instance.ProcessID)
			// 注入已有 task_id：TaskCreator aspect 在 Before 阶段会检查此字段，
			// 已存在则跳过 CreateTask，避免重启恢复时产生重复 wf_task 记录。
			md.PutValue(constants.KeyTaskID, task.ID)

			msg = types.NewMsg(0, "wf_restore", types.JSON, md, variablesStr)

			// delay 任务恢复时注入已等待的时长，避免重启后从头重新计时（见 KeyDelayOffsetMs）
			if task.TaskType == constants.TaskTypeDelay {
				offset := time.Since(task.CreatedAt).Milliseconds()
				msg.Metadata.PutValue(constants.KeyDelayOffsetMs, fmt.Sprintf("%d", offset))
			}

			// 添加恢复请求
			// 使用 ExecuteNodeWithMsg 确保每个节点使用自己的上下文（变量）
			nodeRequests = append(nodeRequests, restoreNodeRequest(task.TaskDefKey, msg))
		}
	}

	if !hasActiveTask {
		return fmt.Errorf("no active tasks found for instance: %s", processInstanceID)
	}

	// 3. 初始化引擎
	processDef, err := s.processDAO.Get(ctx, instance.ProcessID)
	if err != nil {
		return fmt.Errorf("failed to get process definition: %w", err)
	}
	engine, err := s.initExecution(processDef.TenantID, processDef.ID, processDef.DefinitionJSON)
	if err != nil {
		return err
	}

	// 4. 执行恢复
	var variablesStr string
	if instance.Variables != nil {
		variablesStr = *instance.Variables
	} else {
		variablesStr = "{}"
	}
	defaultMsg := types.NewMsg(0, "RESTORE_TRIGGER", types.JSON, nil, variablesStr)
	engine.OnMsg(defaultMsg, types.WithRestoreNodes(nodeRequests...))

	return nil
}

// RestoreAllProcessInstances 恢复所有活跃的流程实例（跨租户全量扫描，仅限系统身份
// 或 SuperAdmin 调用：典型场景是宿主启动时的一致性恢复巡检）。
func (s *RuntimeServiceImpl) RestoreAllProcessInstances(ctx context.Context, actor Actor) error {
	if !actor.SuperAdmin && !IsSystemActor(&actor) {
		return fmt.Errorf("%w: restore all instances requires system actor or super admin", ErrPermissionDenied)
	}
	ctx = bindActor(ctx, actor)
	page := 1
	pageSize := 100
	for {
		query := &dto.ProcessInstanceQueryDTO{
			PageRequest: dto.PageRequest{
				Status:   []string{string(enums.InstanceStatusActive)},
				Page:     page,
				PageSize: pageSize,
			},
		}
		instances, total, err := s.GetProcessInstanceList(ctx, actor, query)
		if err != nil {
			return err
		}
		for _, instance := range instances {
			if err := s.RestoreProcessInstance(ctx, actor, instance.ID); err != nil {
				logrus.Warnf("failed to restore process instance %s: %v", instance.ID, err)
			}
		}
		if int64(page*pageSize) >= total {
			break
		}
		page++
	}
	return nil
}

// GetStuckProcessInstances 找出"active 但无任何未决任务"的卡死实例。
//
// 成因：审批事务已提交（任务 Completed）但 post-commit 的引擎推进失败
// （客户端断连/DB 瞬断等）——任务没了、实例还 active、无待办可操作。
// 本方法为对账巡检与管理端救援提供发现能力。
func (s *RuntimeServiceImpl) GetStuckProcessInstances(ctx context.Context, tenantID string) ([]*model.WfInstance, error) {
	db := s.instanceDAO.Query.WfInstance.UnderlyingDB().WithContext(ctx)
	q := db.Table("wf_instance as i").
		Select("i.*").
		Where("i.status = ?", string(enums.InstanceStatusActive)).
		Where("NOT EXISTS (SELECT 1 FROM wf_task t WHERE t.process_instance_id = i.id AND t.status IN (?, ?))",
			string(enums.TaskStatusActive), string(enums.TaskStatusPending))
	if tenantID != "" {
		q = q.Where("i.tenant_id = ?", tenantID)
	}
	var list []*model.WfInstance
	if err := q.Find(&list).Error; err != nil {
		return nil, err
	}
	// raw SQL 无 Preload，这里统一补 Process 并装配当前节点名（宿主同口径序列化）
	s.decorateCurrentActivityNames(ctx, list)
	return list, nil
}

// ReDriveProcessInstance 重驱动卡死实例：从实例记录的当前节点重新执行引擎推进。
//
// 对"userTask 完成但流转未执行"的卡死态，等价于补跑缺失的那次 ExecuteNext：
// userTask 的 OnMsg 见无既有任务会重建审批任务（自愈）；若节点实为已完成，
// 引擎的幂等路径（已有任务则等待 / 终态保护）会拒绝重复推进。
// 变量传 nil 时引擎沿用实例变量。
func (s *RuntimeServiceImpl) ReDriveProcessInstance(ctx context.Context, actor Actor, processInstanceID string) error {
	ctx = bindActor(ctx, actor)
	instance, err := s.GetProcessInstance(ctx, actor, processInstanceID)
	if err != nil {
		return fmt.Errorf("failed to get process instance: %w", err)
	}
	if instance == nil {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, processInstanceID)
	}
	if instance.Status != string(enums.InstanceStatusActive) {
		return fmt.Errorf("instance %s is %s, only active instances can be re-driven", processInstanceID, instance.Status)
	}
	node := ""
	if instance.CurrentActivity != nil {
		node = *instance.CurrentActivity
	}
	if node == "" {
		return fmt.Errorf("instance %s has no currentActivity to re-drive; fix data manually", processInstanceID)
	}
	logrus.WithFields(logrus.Fields{
		"instance_id": processInstanceID,
		"node":        node,
	}).Warn("manual re-drive of possibly stuck instance")
	return s.ExecuteNext(ctx, processInstanceID, node, nil)
}

// GetProcessInstanceList 获取流程实例列表
func (s *RuntimeServiceImpl) GetProcessInstanceList(ctx context.Context, actor Actor, request *dto.ProcessInstanceQueryDTO) ([]*model.WfInstance, int64, error) {
	ctx = bindActor(ctx, actor)
	// 与 GetTaskList 同口径：nil 查询条件按空条件处理，避免解引用 panic
	if request == nil {
		request = &dto.ProcessInstanceQueryDTO{}
	}
	// 强制以 actor 租户为查询范围；空租户视为系统视角，不做租户过滤
	if u := GetUserFromCtx(ctx); u != nil && u.TenantID != "" {
		request.TenantID = u.TenantID
	}
	var (
		instances []*model.WfInstance
		total     int64
		err       error
	)
	if request.QueryHistory {
		instances, total, err = s.hiInstanceDAO.List(ctx, request)
	} else {
		instances, total, err = s.instanceDAO.List(ctx, request)
	}
	if err != nil {
		return nil, 0, err
	}
	s.decorateCurrentActivityNames(ctx, instances)
	return instances, total, nil
}

// GetProcessInstanceUnionList 获取流程实例列表（管理端视图：合并运行时表与历史归档表）。
//
// 背景：实例完成/终止/撤回后引擎会把行从 wf_instance 归档到 wf_hi_instance 并删除活表行，
// 只读活表的管理端列表会"看不见"已结束实例（无法审计追溯）。归档在单事务内完成
// （建历史行与删活行同 tx），两表不会有同 ID 双行，DAO 侧用 UNION ALL 免去去重开销。
func (s *RuntimeServiceImpl) GetProcessInstanceUnionList(ctx context.Context, actor Actor, request *dto.ProcessInstanceQueryDTO) ([]*model.WfInstance, int64, error) {
	ctx = bindActor(ctx, actor)
	// 与 GetProcessInstanceList 同口径：nil 查询条件按空条件处理
	if request == nil {
		request = &dto.ProcessInstanceQueryDTO{}
	}
	// 强制以 actor 租户为查询范围；空租户视为系统视角，不做租户过滤
	if u := GetUserFromCtx(ctx); u != nil && u.TenantID != "" {
		request.TenantID = u.TenantID
	}
	size := request.GetPageSize()
	offset := (request.GetPage() - 1) * size
	instances, total, err := s.instanceDAO.GetInstancesUnionPagination(ctx, request.TenantID, request.ProcessID, "", request.Status, request.Keyword, nil, nil, size, offset, request.InstanceID, request.BusinessKey, "")
	if err != nil {
		return nil, 0, err
	}
	s.decorateCurrentActivityNames(ctx, instances)
	return instances, total, nil
}

// UpdateInstanceCurrentActivity 更新 active 实例的当前节点。
// 只在节点创建任务时由组件回调；失败仅记日志不阻塞主流程（监控字段，非关键路径）。
func (s *RuntimeServiceImpl) UpdateInstanceCurrentActivity(ctx context.Context, processInstanceID, activityKey string) error {
	return s.instanceDAO.SetCurrentActivity(ctx, processInstanceID, activityKey)
}

// GetProcessInstancesByTaskConditions 基于任务条件关联查询流程实例列表
func (s *RuntimeServiceImpl) GetProcessInstancesByTaskConditions(ctx context.Context, req *dto.TaskQuery) ([]*model.WfInstance, int64, error) {
	return s.instanceDAO.ListByTaskConditions(ctx, req)
}

// GetTodoProcessInstanceList 获取我的待办实例列表
// 含：① assignee=userID 的任务；② status=Pending 且 user 在候选人池的未签收任务（候选组模式）。
// 候选人池依赖 wf_task_assignee 表；表不存在时退化为仅 ①。
// 过滤不含 returned：退回任务生成即归档进历史表（活表无此状态），
// 列入过滤只会经 ListByTaskConditions 的历史分支把已结束实例捞回待办。
func (s *RuntimeServiceImpl) GetTodoProcessInstanceList(ctx context.Context, actor Actor, page, pageSize int, keyword string, startUserIDs []string, orderBy string, orderDesc bool) ([]*model.WfInstance, int64, error) {
	ctx = bindActor(ctx, actor)
	userID, tenantID := actor.UserID, actor.TenantID
	q := &dto.TaskQuery{
		Assignee:         userID,
		CandidateUser:    userID,
		CandidateRoleIDs: s.candidateRoleIDs(ctx, tenantID, userID),
		CandidateDeptIDs: identityDeptIDs(ctx, s.workflowEngine.GetIdentityService(), tenantID, userID),
		TenantID:         tenantID,
		Keyword:          keyword,
		StartUserIDs:     startUserIDs,
		PageRequest: dto.PageRequest{
			Page:      page,
			PageSize:  pageSize,
			OrderBy:   orderBy,
			OrderDesc: orderDesc,
			Status:    []string{string(enums.TaskStatusPending), string(enums.TaskStatusActive)},
		},
	}
	instances, total, err := s.instanceDAO.ListByTaskConditions(ctx, q)
	if err != nil {
		return nil, 0, err
	}
	s.decorateCurrentActivityNames(ctx, instances)
	return instances, total, nil
}

// candidateRoleIDs 取用户角色 ID 列表（候选组待办按 role 匹配）。
func (s *RuntimeServiceImpl) candidateRoleIDs(ctx context.Context, tenantID, userID string) []string {
	return identityRoleIDs(ctx, s.workflowEngine.GetIdentityService(), tenantID, userID)
}

// isUserCandidate 判断用户是否为该任务的候选成员（详情页 claim 可见性）。
// 复用 GetTaskCandidates 读 wf_task_assignee + role 展开；无候选记录时回退 true，
// 真正的候选资格由 claim API 兜底校验。
func (s *RuntimeServiceImpl) isUserCandidate(ctx context.Context, task *model.WfTask, userID string) bool {
	if task == nil || task.TaskDefKey == "" || task.ProcessInstanceID == nil || *task.ProcessInstanceID == "" || userID == "" {
		return true
	}
	candidates, err := s.workflowEngine.GetTaskService().GetTaskCandidates(ctx, *task.ProcessInstanceID, task.TaskDefKey)
	if err != nil {
		// 展开失败按非候选处理（fail-closed）：与认领校验同口径，
		// identity 不可用时不能把候选实例当空池对全租户放行。
		return false
	}
	// 空池任务与认领口径一致：不限定候选人，同租户可见
	if len(candidates) == 0 {
		return true
	}
	for _, c := range candidates {
		if c != nil && c.EntityID == userID {
			return true
		}
	}
	return false
}

// GetDoneProcessInstanceList 获取我的已办实例列表
// 退回也是已办动作：returned 任务只在历史表，实例结束后经历史分支在此可见。
// instanceStatus 实例状态筛选桶（active/completed/rejected/withdrawn），空为不过滤。
func (s *RuntimeServiceImpl) GetDoneProcessInstanceList(ctx context.Context, actor Actor, page, pageSize int, keyword string, startUserIDs []string, orderBy string, orderDesc bool, instanceStatus string) ([]*model.WfInstance, int64, error) {
	ctx = bindActor(ctx, actor)
	userID, tenantID := actor.UserID, actor.TenantID
	q := &dto.TaskQuery{
		Assignee:     userID,
		TenantID:     tenantID,
		Keyword:      keyword,
		StartUserIDs: startUserIDs,
		PageRequest: dto.PageRequest{
			Page:      page,
			PageSize:  pageSize,
			OrderBy:   orderBy,
			OrderDesc: orderDesc,
			Status:    []string{string(enums.TaskStatusCompleted), string(enums.TaskStatusReturned)},
		},
	}
	q.InstanceStatuses, q.EndReasonPrefix, q.EndReasonNotPrefixes = instanceStatusScope(instanceStatus)
	instances, total, err := s.instanceDAO.ListByTaskConditions(ctx, q)
	if err != nil {
		return nil, 0, err
	}
	s.decorateCurrentActivityNames(ctx, instances)
	return instances, total, nil
}

// instanceStatusScope 状态筛选桶翻译为实例状态集合与 end_reason 前缀条件。
// 拒绝/撤回落库为 terminated + 固定前缀，无法用实例状态直接表达；
// 「已终止」桶 = terminated 且排除拒绝/撤回前缀（手动终止、系统终止等其余原因）。
func instanceStatusScope(bucket string) ([]string, string, []string) {
	switch bucket {
	case string(enums.InstanceStatusActive):
		return []string{bucket}, "", nil
	case string(enums.InstanceStatusCompleted):
		return []string{bucket}, "", nil
	case string(enums.InstanceStatusSuspended):
		return []string{bucket}, "", nil
	case string(enums.InstanceStatusDraft):
		return []string{bucket}, "", nil
	case "rejected":
		return []string{string(enums.InstanceStatusTerminated)}, constants.EndReasonPrefixRejected, nil
	case "withdrawn":
		return []string{string(enums.InstanceStatusTerminated)}, constants.EndReasonPrefixWithdrawn, nil
	case string(enums.InstanceStatusTerminated):
		return []string{string(enums.InstanceStatusTerminated)}, "", []string{constants.EndReasonPrefixRejected, constants.EndReasonPrefixWithdrawn}
	}
	return nil, "", nil
}

// instanceStatusBuckets 状态桶全集：经 instanceStatusScope 翻译，保证桶条件与列表过滤同口径。
// withDraft 为 true 时含草稿桶（草稿实例无任务，仅「我的申请」视角可见）。
func instanceStatusBuckets(withDraft bool) []dao.InstanceStatusBucket {
	names := []string{"active", "completed", "rejected", "withdrawn", "suspended", "terminated"}
	if withDraft {
		names = append(names, "draft")
	}
	out := make([]dao.InstanceStatusBucket, 0, len(names))
	for _, name := range names {
		statuses, prefix, notPrefixes := instanceStatusScope(name)
		out = append(out, dao.InstanceStatusBucket{Name: name, Statuses: statuses, EndReasonPrefix: prefix, EndReasonNotPrefixes: notPrefixes})
	}
	return out
}

// CountMyApplicationsByBuckets 我的申请页状态桶计数（与列表同口径：本人发起、排除已删除，含 keyword）
func (s *RuntimeServiceImpl) CountMyApplicationsByBuckets(ctx context.Context, actor Actor, keyword string) (map[string]int64, error) {
	ctx = bindActor(ctx, actor)
	userID, tenantID := actor.UserID, actor.TenantID
	if userID == "" {
		return nil, fmt.Errorf("user ID cannot be empty")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty")
	}
	return s.instanceDAO.CountInstancesUnionByBuckets(ctx, tenantID, "", userID, keyword, nil, nil, instanceStatusBuckets(true))
}

// CountDoneByBuckets 已办页状态桶计数（与已办列表同口径：我已办任务触达的实例，含 keyword 与申请人过滤）
func (s *RuntimeServiceImpl) CountDoneByBuckets(ctx context.Context, actor Actor, keyword string, startUserIDs []string) (map[string]int64, error) {
	ctx = bindActor(ctx, actor)
	if actor.UserID == "" {
		return nil, fmt.Errorf("user ID cannot be empty")
	}
	if actor.TenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty")
	}
	q := &dto.TaskQuery{
		Assignee:     actor.UserID,
		TenantID:     actor.TenantID,
		Keyword:      keyword,
		StartUserIDs: startUserIDs,
		PageRequest: dto.PageRequest{
			Status: []string{string(enums.TaskStatusCompleted), string(enums.TaskStatusReturned)},
		},
	}
	return s.instanceDAO.CountTaskInstancesByBuckets(ctx, q, instanceStatusBuckets(false))
}

// GetCcProcessInstanceList 获取抄送给我的实例列表
func (s *RuntimeServiceImpl) GetCcProcessInstanceList(ctx context.Context, actor Actor, page, pageSize int, keyword string, startUserIDs []string, orderBy string, orderDesc bool) ([]*model.WfInstance, int64, error) {
	ctx = bindActor(ctx, actor)
	userID, tenantID := actor.UserID, actor.TenantID
	q := &dto.TaskQuery{
		Assignee:     userID,
		TenantID:     tenantID,
		Keyword:      keyword,
		StartUserIDs: startUserIDs,
		ApprovalType: string(enums.ApprovalTypeCC),
		PageRequest: dto.PageRequest{
			Page:      page,
			PageSize:  pageSize,
			OrderBy:   orderBy,
			OrderDesc: orderDesc,
			Status:    []string{string(enums.TaskStatusCompleted)},
		},
	}
	instances, total, err := s.instanceDAO.ListByTaskConditions(ctx, q)
	if err != nil {
		return nil, 0, err
	}
	s.decorateCurrentActivityNames(ctx, instances)
	return instances, total, nil
}

// GetMyApplicationsProcessInstanceList 获取我发起的申请实例列表（按 start_user_id=发起人用户ID 过滤，与 token userId 同口径）
func (s *RuntimeServiceImpl) GetMyApplicationsProcessInstanceList(ctx context.Context, actor Actor, page, pageSize int, keyword, orderBy string, orderDesc bool, instanceStatus string) ([]*model.WfInstance, int64, error) {
	ctx = bindActor(ctx, actor)
	userID, tenantID := actor.UserID, actor.TenantID
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = dto.DefaultPageSize
	}
	// 运行时+历史表合并查询（DAO 支持 tenantID/startUserID 条件）
	statuses, endReasonPrefix, endReasonNotPrefixes := instanceStatusScope(instanceStatus)
	instances, total, err := s.instanceDAO.GetInstancesUnionPagination(ctx, tenantID, "", userID, statuses, keyword, nil, nil, pageSize, (page-1)*pageSize, "", "", endReasonPrefix, endReasonNotPrefixes...)
	if err != nil {
		return nil, 0, err
	}
	s.decorateCurrentActivityNames(ctx, instances)
	return instances, total, nil
}

// CountMyApplications 统计我发起的申请数量（运行时+历史表合并，created_at ∈ [from,to]，
// from/to 为 nil 表示该侧不设界）。供统计页按日期范围取"总申请数"。
func (s *RuntimeServiceImpl) CountMyApplications(ctx context.Context, actor Actor, from, to *time.Time) (int64, error) {
	ctx = bindActor(ctx, actor)
	userID, tenantID := actor.UserID, actor.TenantID
	if userID == "" {
		return 0, fmt.Errorf("user ID cannot be empty")
	}
	if tenantID == "" {
		return 0, fmt.Errorf("tenant ID cannot be empty")
	}
	_, total, err := s.instanceDAO.GetInstancesUnionPagination(ctx, tenantID, "", userID, nil, "", from, to, 1, 0, "", "", "")
	if err != nil {
		return 0, fmt.Errorf("failed to count my applications: %w", err)
	}
	return total, nil
}

// TerminateProcessInstance 终止流程实例并归档到历史表
func (s *RuntimeServiceImpl) TerminateProcessInstance(ctx context.Context, actor Actor, processInstanceID, reason string) error {
	ctx = bindActor(ctx, actor)
	if processInstanceID == "" {
		return fmt.Errorf("process instance ID cannot be empty")
	}
	if err := WithInstanceTx(ctx, s.instanceDAO.Query, processInstanceID, func(scope *InstanceScope) error {
		evt, err := s.TerminateInTx(ctx, scope.Tx(), processInstanceID, reason)
		if err != nil {
			return err
		}
		if evt != nil {
			scope.AfterCommit(func() error {
				DispatchTaskEvent(s.workflowEngine.GetTaskEventListener(), *evt, ctx)
				return nil
			})
		}
		return nil
	}); err != nil {
		return err
	}
	// 子失败传播:子实例被终止(如子流程 reject terminate)→ 通知父 subProcess 重入,
	// OnMsg 检测子 terminated 走父 Failure 边。事务提交后触发,避免脏读。
	s.resumeParentAfterChildTerminated(ctx, processInstanceID)
	return nil
}

// TerminateInTx 在调用方已持有实例行锁的事务内执行终止逻辑。
// 调用方契约：必须已通过 WithInstanceTx（或等价的 FOR UPDATE 事务）锁定 instance 行。
// 该方法是内部 API，仅供同 package（含跨服务级联调用，如 TaskService.Withdraw）
// 调用；外部调用方应使用 TerminateProcessInstance。
//
// 返回待派发的 terminated 事件（可能为 nil）：事件必须在事务提交后派发，
// 而本方法运行在事务内，故由持有 InstanceScope 的调用方 AfterCommit 派发。
func (s *RuntimeServiceImpl) TerminateInTx(ctx context.Context, tx *query.Query, processInstanceID, reason string) (*TaskEvent, error) {
	if processInstanceID == "" {
		return nil, fmt.Errorf("process instance ID cannot be empty")
	}
	if tx == nil {
		return nil, fmt.Errorf("TerminateInTx: tx is nil")
	}

	// 在事务内读取实例（调用方已持锁）
	instance, err := tx.WfInstance.WithContext(ctx).Where(tx.WfInstance.ID.Eq(processInstanceID)).First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("%w: process instance", ErrInstanceNotFound)
		}
		return nil, fmt.Errorf("failed to get process instance: %w", err)
	}

	if err := ensureTenantAccess(ctx, "process instance", instance.TenantID); err != nil {
		return nil, err
	}

	// 幂等：已经是终态，直接返回（WithInstanceTx 已经挡掉 terminal 状态，
	// 但在跨服务级联调用时仍可能进入此路径——例如 Withdraw 调用时实例可能
	// 已被并发分支终止）
	if instance.Status == string(enums.InstanceStatusTerminated) {
		return nil, nil
	}
	if instance.Status == string(enums.InstanceStatusCompleted) {
		return nil, fmt.Errorf("cannot terminate completed process instance: %w", ErrInstanceTerminal)
	}

	now := time.Now()
	username := s.GetUsernameFromCtx(ctx)
	endReason := "流程实例被终止"
	// 与 CompleteProcessInstance 同口径用 int64 毫秒（int32 约 24.8 天溢出）
	duration := now.Sub(instance.CreatedAt).Milliseconds()

	// 终止所有未完结的任务；同时收集这些任务的办理人——终止通知只发给
	// 终止时尚有未决工作的办理人，已完成节点的历史审批人不再打扰。
	liveAssignees := make([]string, 0, 4)
	tasks, err := tx.WfTask.WithContext(ctx).Where(tx.WfTask.ProcessInstanceID.Eq(processInstanceID)).Find()
	if err != nil {
		return nil, fmt.Errorf("failed to get tasks for termination: %w", err)
	}

	for _, t := range tasks {
		if t.Status == string(enums.TaskStatusActive) ||
			t.Status == string(enums.TaskStatusPending) ||
			t.Status == string(enums.TaskStatusSuspended) {
			if t.Assignee != nil && *t.Assignee != "" {
				liveAssignees = append(liveAssignees, *t.Assignee)
			}
			if _, err := tx.WfTask.WithContext(ctx).Where(tx.WfTask.ID.Eq(t.ID)).Updates(map[string]interface{}{
				tx.WfTask.Status.ColumnName().String():    enums.TaskStatusTerminated,
				tx.WfTask.EndedAt.ColumnName().String():   &now,
				tx.WfTask.EndReason.ColumnName().String(): endReason,
				tx.WfTask.UpdatedBy.ColumnName().String(): username,
				tx.WfTask.UpdatedAt.ColumnName().String(): &now,
			}); err != nil {
				return nil, fmt.Errorf("failed to terminate task %s: %w", t.ID, err)
			}
			t.Status = string(enums.TaskStatusTerminated)
			t.EndedAt = &now
			t.EndReason = &endReason
			t.UpdatedBy = &username
			t.UpdatedAt = &now
		}
	}

	// 归档所有任务到历史表（批量插入，大会签实例免逐条往返）
	hiTasks := make([]*model.WfHiTask, 0, len(tasks))
	for _, task := range tasks {
		hiTasks = append(hiTasks, taskToHiTask(task))
	}
	if len(hiTasks) > 0 {
		if err := tx.WfHiTask.WithContext(ctx).CreateInBatches(hiTasks, archiveBatchSize); err != nil {
			return nil, fmt.Errorf("failed to archive tasks to history: %w", err)
		}
	}

	// 创建历史实例记录
	hiInstance := &model.WfHiInstance{
		ID:              instance.ID,
		ProcessID:       instance.ProcessID,
		BusinessKey:     instance.BusinessKey,
		Name:            instance.Name,
		Status:          string(enums.InstanceStatusTerminated),
		Variables:       instance.Variables,
		CurrentActivity: instance.CurrentActivity,
		Priority:        instance.Priority,
		ParentID:        instance.ParentID,
		TenantID:        instance.TenantID,
		CreatedBy:       instance.CreatedBy,
		CreatedAt:       instance.CreatedAt,
		UpdatedBy:       &username,
		UpdatedAt:       &now,
		EndReason:       &reason,
		Duration:        &duration,
		EndedAt:         &now,
		StartUserID:     instance.StartUserID,
	}
	if err := tx.WfHiInstance.WithContext(ctx).Create(hiInstance); err != nil {
		return nil, fmt.Errorf("failed to archive instance to history: %w", err)
	}

	// 清理候选池 wf_task_assignee（避免孤儿）
	taskIDs := make([]string, 0, len(tasks))
	for _, t := range tasks {
		taskIDs = append(taskIDs, t.ID)
	}
	if len(taskIDs) > 0 {
		if _, err := tx.WfTaskAssignee.WithContext(ctx).Where(tx.WfTaskAssignee.TaskID.In(taskIDs...)).Delete(); err != nil {
			logrus.Warnf("failed to clean wf_task_assignee: %v", err)
		}
	}

	// 删除原始任务记录
	if _, err := tx.WfTask.WithContext(ctx).Where(tx.WfTask.ProcessInstanceID.Eq(processInstanceID)).Delete(); err != nil {
		return nil, fmt.Errorf("failed to delete original tasks: %w", err)
	}

	// 删除原始实例记录
	if _, err := tx.WfInstance.WithContext(ctx).Where(tx.WfInstance.ID.Eq(processInstanceID)).Delete(); err != nil {
		return nil, fmt.Errorf("failed to delete original instance: %w", err)
	}

	// 实例终止后 best-effort 驱逐：若其版本已无其它活实例且非最新版，清理租户池链（收内存）。
	// 传事务 tx：走全局连接会与本事务互等死锁。
	s.evictStaleChain(ctx, tx, instance.TenantID, instance.ProcessID)

	// 构造 terminated 事件（通知发起人 + 终止时的活跃办理人），交给调用方提交后派发
	if s.workflowEngine.GetTaskEventListener() != nil {
		toUsers := []string{}
		if instance.StartUserID != "" {
			toUsers = append(toUsers, instance.StartUserID)
		}
		toUsers = append(toUsers, liveAssignees...)
		toUsers = uniqueStrings(toUsers)
		if len(toUsers) > 0 {
			// FromUser 取 ctx Actor；Source 区分 api/withdraw/reject 来源
			fromUser := ""
			if u := GetUserFromCtx(ctx); u != nil {
				fromUser = u.UserID
			}
			return &TaskEvent{
				Type:       TaskEventTerminated,
				InstanceID: processInstanceID,
				ProcessID:  instance.ProcessID,
				TenantID:   instance.TenantID,
				ToUsers:    toUsers,
				FromUser:   fromUser,
				Reason:     reason,
				Source:     EventSourceFromCtx(ctx),
				Timestamp:  time.Now(),
			}, nil
		}
	}

	return nil, nil
}
