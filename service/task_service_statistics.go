// This file contains the read-only statistics / list methods on
// TaskServiceImpl: GetTaskList (frontend API list dispatcher) and the two
// approval-statistics aggregators (basic counts and the detail aggregator
// with trend / category / efficiency metrics).

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// statsPageSizeFetchAll 统计聚合需要拉全量任务，分页大小取一个远超单用户
// 待办量的值，避免聚合结果被截断。
const statsPageSizeFetchAll = 10000

// GetTaskList 获取任务列表（前端API专用）
func (s *TaskServiceImpl) GetTaskList(ctx context.Context, actor Actor, query *dto.TaskQuery) ([]*model.WfTask, int64, error) {
	ctx = bindActor(ctx, actor)
	if query == nil {
		query = &dto.TaskQuery{}
	}
	// 强制以 actor 租户为查询范围；空租户视为系统视角，不做租户过滤
	if u := GetUserFromCtx(ctx); u != nil && u.TenantID != "" {
		query.TenantID = u.TenantID
	}
	// QueryTasks 处理 nil query 与历史表路由，避免这里解引用空指针
	return s.QueryTasks(ctx, query)
}

// identityRoleIDs 取用户角色 ID 列表（候选/统计维度共用）。identity 未注入或查询失败返回 nil。
func identityRoleIDs(ctx context.Context, identity IdentityService, tenantID, userID string) []string {
	if identity == nil || userID == "" {
		return nil
	}
	roleIDs, err := identity.GetRoleIDsByUserID(ctx, tenantID, userID)
	if err != nil {
		return nil
	}
	return roleIDs
}

// identityDeptIDs 取用户部门 ID 列表（含非主部门；dept 候选组待办按 department 匹配）。
// identity 未注入或查询失败返回 nil。
func identityDeptIDs(ctx context.Context, identity IdentityService, tenantID, userID string) []string {
	if identity == nil || userID == "" {
		return nil
	}
	deptIDs, err := identity.GetDepartmentIDsByUserID(ctx, tenantID, userID)
	if err != nil {
		return nil
	}
	return deptIDs
}

// candidateRoleIDs 取用户角色 ID 列表（统计候选维度用）。
func (s *TaskServiceImpl) candidateRoleIDs(ctx context.Context, tenantID, userID string) []string {
	return identityRoleIDs(ctx, s.workflowEngine.GetIdentityService(), tenantID, userID)
}

// candidateDeptIDs 取用户部门 ID 列表（统计候选维度用）。
func (s *TaskServiceImpl) candidateDeptIDs(ctx context.Context, tenantID, userID string) []string {
	return identityDeptIDs(ctx, s.workflowEngine.GetIdentityService(), tenantID, userID)
}

// GetApprovalStatistics 卡片条无参聚合：覆盖待审批/已审批两页的正确语义。
func (s *TaskServiceImpl) GetApprovalStatistics(ctx context.Context, actor Actor) (map[string]interface{}, error) {
	ctx = bindActor(ctx, actor)
	userID, tenantID := actor.UserID, actor.TenantID
	if userID == "" {
		return nil, fmt.Errorf("user ID cannot be empty")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty")
	}

	now := time.Now()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	weekStart := todayStart.AddDate(0, 0, -int(now.Weekday()))
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	activeStatuses := []string{string(enums.TaskStatusPending), string(enums.TaskStatusActive)}
	completedStatus := []string{string(enums.TaskStatusCompleted)}
	roleIDs := s.candidateRoleIDs(ctx, tenantID, userID)
	deptIDs := s.candidateDeptIDs(ctx, tenantID, userID)

	// 待办（已签收 assignee + 候选组未签收）
	_, todoCount, err := s.QueryTasks(ctx, &dto.TaskQuery{
		Assignee: userID, TenantID: tenantID,
		PageRequest: dto.PageRequest{Status: activeStatuses, PageSize: 1},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query todo tasks: %w", err)
	}
	if candCnt, cerr := s.taskAssigneeDAO.CountCandidateTasks(ctx, tenantID, userID, roleIDs, deptIDs, []string{string(enums.TaskStatusPending)}, nil, nil); cerr == nil {
		todoCount += candCnt
	}

	// 进行中（已签收 active），供任务页"待领取/进行中"两卡分化
	_, activeCount, err := s.QueryTasks(ctx, &dto.TaskQuery{
		Assignee: userID, TenantID: tenantID,
		PageRequest: dto.PageRequest{Status: []string{string(enums.TaskStatusActive)}, PageSize: 1},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query active tasks: %w", err)
	}

	// 已办（运行时 + 历史表）
	doneCount, err := s.countTasksBothTables(ctx, userID, tenantID, &dto.TaskQuery{
		PageRequest: dto.PageRequest{Status: completedStatus, PageSize: 1},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query done tasks: %w", err)
	}

	// 抄送
	ccCount, err := s.countCcTasks(ctx, userID, tenantID)
	if err != nil {
		logrus.WithError(err).Warn("Failed to query cc tasks, defaulting to 0")
		ccCount = 0
	}
	// doneCount 排除纯抄送（CC 创建即完成、assignee=被抄送人，不计入"我办理的"）
	doneCount = clampSub(doneCount, ccCount)

	// 我发起的申请
	var myApplicationCount int64
	_, myApplicationCount, err = s.workflowEngine.GetRuntimeService().GetMyApplicationsProcessInstanceList(ctx, actor, 1, 1, "", "", false, "")
	if err != nil {
		logrus.WithError(err).Warn("Failed to query my application count, defaulting to 0")
		myApplicationCount = 0
	}

	// 今日新增待办（今日创建且未完成，含候选）
	_, todayArrived, err := s.QueryTasks(ctx, &dto.TaskQuery{
		Assignee: userID, TenantID: tenantID, CreatedAfter: &todayStart,
		PageRequest: dto.PageRequest{Status: activeStatuses, PageSize: 1},
	})
	if err != nil {
		logrus.WithError(err).Warn("Failed to query today arrived tasks")
		todayArrived = 0
	}
	if candCnt, cerr := s.taskAssigneeDAO.CountCandidateTasks(ctx, tenantID, userID, roleIDs, deptIDs, []string{string(enums.TaskStatusPending)}, &todayStart, nil); cerr == nil {
		todayArrived += candCnt
	}

	// 当前超时（已签收 active 且 dueDate<now + 候选未签收且逾期）
	overdueCount, err := s.countOverdueActiveTasks(ctx, userID, tenantID, now)
	if err != nil {
		logrus.WithError(err).Warn("Failed to query overdue tasks")
		overdueCount = 0
	}
	if candCnt, cerr := s.taskAssigneeDAO.CountCandidateTasks(ctx, tenantID, userID, roleIDs, deptIDs, []string{string(enums.TaskStatusPending)}, nil, &now); cerr == nil {
		overdueCount += candCnt
	}

	// 本周处理（completed this week，运行时 + 历史表）
	weekCompleted, err := s.countTasksBothTables(ctx, userID, tenantID, &dto.TaskQuery{
		EndedAfter: &weekStart, PageRequest: dto.PageRequest{Status: completedStatus, PageSize: 1},
	})
	if err != nil {
		logrus.WithError(err).Warn("Failed to query week completed tasks")
		weekCompleted = 0
	}
	// weekCompleted 同样排除本周抄送
	weekCompleted = clampSub(weekCompleted, s.countCcTasksEndedAfter(ctx, userID, tenantID, &weekStart))

	// 本月完成切片：派生通过/拒绝/平均耗时
	monthApproved, monthRejected, _, avgDurationMonth := s.aggregateMonthCompleted(ctx, userID, tenantID, monthStart)

	statistics := map[string]interface{}{
		// 基础计数
		"todoCount":        todoCount,
		"activeCount":      activeCount,
		"doneCount":        doneCount,
		"ccCount":          ccCount,
		"applicationCount": myApplicationCount,
		"totalCount":       todoCount + doneCount + ccCount + myApplicationCount,

		// 语义对齐字段（卡片条使用）
		"overdueCount":     overdueCount,
		"todayArrived":     todayArrived,
		"weekCompleted":    weekCompleted,
		"monthApproved":    monthApproved,
		"monthRejected":    monthRejected,
		"avgDurationMonth": avgDurationMonth,
	}

	return statistics, nil
}

// countTasksBothTables 统计运行时表与历史表的合计数量（归档后任务移入历史表）。
// base 已设好除 Assignee/TenantID/QueryHistory 外的过滤条件。
func (s *TaskServiceImpl) countTasksBothTables(ctx context.Context, userID, tenantID string, base *dto.TaskQuery) (int64, error) {
	runtime := *base
	runtime.Assignee = userID
	runtime.TenantID = tenantID
	_, runtimeCount, err := s.QueryTasks(ctx, &runtime)
	if err != nil {
		return 0, err
	}
	history := runtime
	history.QueryHistory = true
	_, historyCount, err := s.QueryTasks(ctx, &history)
	if err != nil {
		logrus.WithError(err).Warn("Failed to query history table, using runtime count only")
		return runtimeCount, nil
	}
	return runtimeCount + historyCount, nil
}

// countOverdueActiveTasks 当前未完成且已过 dueDate 的任务数。
func (s *TaskServiceImpl) countOverdueActiveTasks(ctx context.Context, userID, tenantID string, now time.Time) (int64, error) {
	tasks, _, err := s.QueryTasks(ctx, &dto.TaskQuery{
		Assignee: userID, TenantID: tenantID,
		PageRequest: dto.PageRequest{
			Status:   []string{string(enums.TaskStatusPending), string(enums.TaskStatusActive)},
			PageSize: statsPageSizeFetchAll,
		},
	})
	if err != nil {
		return 0, err
	}
	return countOverdueFromTasks(tasks, now), nil
}

// countOverdueFromTasks 统计已过 dueDate 的任务数（纯逻辑）。
func countOverdueFromTasks(tasks []*model.WfTask, now time.Time) int64 {
	var overdue int64
	for _, t := range tasks {
		if t.DueDate != nil && t.DueDate.Before(now) {
			overdue++
		}
	}
	return overdue
}

// aggregateMonthCompleted 本月已完成任务切片，派生通过/拒绝/平均耗时。
func (s *TaskServiceImpl) aggregateMonthCompleted(ctx context.Context, userID, tenantID string, monthStart time.Time) (approved, rejected, total, avgDuration int64) {
	tasks, _, err := s.QueryTasks(ctx, &dto.TaskQuery{
		Assignee: userID, TenantID: tenantID, EndedAfter: &monthStart,
		PageRequest: dto.PageRequest{Status: []string{string(enums.TaskStatusCompleted)}, PageSize: statsPageSizeFetchAll},
	})
	if err != nil {
		logrus.WithError(err).Warn("Failed to query month completed tasks")
	}
	histQuery := dto.TaskQuery{
		Assignee: userID, TenantID: tenantID, EndedAfter: &monthStart, QueryHistory: true,
		PageRequest: dto.PageRequest{Status: []string{string(enums.TaskStatusCompleted)}, PageSize: statsPageSizeFetchAll},
	}
	histTasks, _, histErr := s.QueryTasks(ctx, &histQuery)
	if histErr != nil {
		logrus.WithError(histErr).Warn("Failed to query month completed tasks from history")
	}
	tasks = append(tasks, histTasks...)

	return deriveMonthStats(tasks)
}

// deriveMonthStats 从本月完成任务切片派生通过/拒绝/总数/平均耗时（Duration 单位毫秒）。
func deriveMonthStats(tasks []*model.WfTask) (approved, rejected, total, avgDuration int64) {
	total = int64(len(tasks))
	var durationSum, durationCount int64
	for _, t := range tasks {
		if t.EndReason != nil {
			switch *t.EndReason {
			case string(enums.ApprovalResultApproved):
				approved++
			case string(enums.ApprovalResultRejected):
				rejected++
			}
		}
		if t.Duration != nil && *t.Duration > 0 {
			durationSum += *t.Duration
			durationCount++
		}
	}
	if durationCount > 0 {
		avgDuration = durationSum / durationCount
	}
	return
}

// GetApprovalStatisticsDetail 获取明细审批统计（支持时间范围筛选、完成趋势、流程分类分布与效率指标）
func (s *TaskServiceImpl) GetApprovalStatisticsDetail(ctx context.Context, actor Actor, startDate, endDate *time.Time) (map[string]interface{}, error) {
	ctx = bindActor(ctx, actor)
	userID, tenantID := actor.UserID, actor.TenantID
	if userID == "" {
		return nil, fmt.Errorf("user ID cannot be empty")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty")
	}

	now := time.Now()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	weekStart := todayStart.AddDate(0, 0, -int(now.Weekday()))
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())

	// ========== 基础计数（始终返回，不受日期筛选影响） ==========

	// 查询待办任务数量
	todoQuery := &dto.TaskQuery{
		Assignee: userID,
		TenantID: tenantID,
		PageRequest: dto.PageRequest{
			Status:   []string{string(enums.TaskStatusPending), string(enums.TaskStatusActive)},
			PageSize: 1,
		},
	}
	_, todoCount, err := s.QueryTasks(ctx, todoQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query todo tasks: %w", err)
	}
	if candCnt, cerr := s.taskAssigneeDAO.CountCandidateTasks(ctx, tenantID, userID, s.candidateRoleIDs(ctx, tenantID, userID), s.candidateDeptIDs(ctx, tenantID, userID), []string{string(enums.TaskStatusPending)}, nil, nil); cerr == nil {
		todoCount += candCnt
	}

	// 查询已办任务数量（运行时表 + 历史表；countTasksBothTables 统一两表口径）
	doneCount, err := s.countTasksBothTables(ctx, userID, tenantID, &dto.TaskQuery{
		PageRequest: dto.PageRequest{
			Status:   []string{string(enums.TaskStatusCompleted)},
			PageSize: 1,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query done tasks: %w", err)
	}

	// 抄送给我的任务数量（CC 任务创建即 completed，与 GetApprovalStatistics 同口径）
	ccCount, err := s.countCcTasks(ctx, userID, tenantID)
	if err != nil {
		logrus.WithError(err).Warn("Failed to query cc tasks, defaulting to 0")
		ccCount = 0
	}
	// CC 属抄送而非已办：从 doneCount 中扣除 ccCount（CC 任务创建即 completed，
	// 会同时命中"已完成"过滤），否则 totalCount=todo+done+cc+app 把 CC 各算一次。
	doneCount = clampSub(doneCount, ccCount)

	// 我发起的申请数量（全局，口径与列表页一致）
	var myApplicationCount int64
	_, myApplicationCount, err = s.workflowEngine.GetRuntimeService().GetMyApplicationsProcessInstanceList(ctx, actor, 1, 1, "", "", false, "")
	if err != nil {
		logrus.WithError(err).Warn("Failed to query my application count, defaulting to 0")
		myApplicationCount = 0
	}

	// 我发起的申请数量（按 startDate/endDate 过滤 created_at，统计页"总申请数"卡片用）
	applicationInRange, err := s.workflowEngine.GetRuntimeService().CountMyApplications(ctx, actor, startDate, endDate)
	if err != nil {
		logrus.WithError(err).Warn("Failed to count my applications in range, defaulting to 0")
		applicationInRange = 0
	}

	// ========== 时间段计数（使用当前时间） ==========

	// 今日已完成
	todayQuery := &dto.TaskQuery{
		Assignee:   userID,
		TenantID:   tenantID,
		EndedAfter: &todayStart,
		PageRequest: dto.PageRequest{
			Status:   []string{string(enums.TaskStatusCompleted)},
			PageSize: 1,
		},
	}
	_, todayCompletedRuntime, err := s.QueryTasks(ctx, todayQuery)
	if err != nil {
		logrus.WithError(err).Warn("Failed to query today completed tasks")
		todayCompletedRuntime = 0
	}
	todayQuery.QueryHistory = true
	_, todayCompletedHistory, terr := s.QueryTasks(ctx, todayQuery)
	if terr != nil {
		logrus.WithError(terr).Warn("Failed to query today completed tasks from history")
		todayCompletedHistory = 0
	}
	todayCompleted := todayCompletedRuntime + todayCompletedHistory
	todayCompleted = clampSub(todayCompleted, s.countCcTasksEndedAfter(ctx, userID, tenantID, &todayStart))

	// 本周已完成（运行时表 + 历史表）
	weekQuery := &dto.TaskQuery{
		Assignee:   userID,
		TenantID:   tenantID,
		EndedAfter: &weekStart,
		PageRequest: dto.PageRequest{
			Status:   []string{string(enums.TaskStatusCompleted)},
			PageSize: 1,
		},
	}
	_, weekCompletedRuntime, err := s.QueryTasks(ctx, weekQuery)
	if err != nil {
		logrus.WithError(err).Warn("Failed to query week completed tasks")
		weekCompletedRuntime = 0
	}
	weekQuery.QueryHistory = true
	_, weekCompletedHistory, werr := s.QueryTasks(ctx, weekQuery)
	if werr != nil {
		logrus.WithError(werr).Warn("Failed to query week completed tasks from history")
		weekCompletedHistory = 0
	}
	weekCompleted := weekCompletedRuntime + weekCompletedHistory
	weekCompleted = clampSub(weekCompleted, s.countCcTasksEndedAfter(ctx, userID, tenantID, &weekStart))

	// 本月已完成（运行时表 + 历史表）
	monthQuery := &dto.TaskQuery{
		Assignee:   userID,
		TenantID:   tenantID,
		EndedAfter: &monthStart,
		PageRequest: dto.PageRequest{
			Status:   []string{string(enums.TaskStatusCompleted)},
			PageSize: 1,
		},
	}
	_, monthCompletedRuntime, err := s.QueryTasks(ctx, monthQuery)
	if err != nil {
		logrus.WithError(err).Warn("Failed to query month completed tasks")
		monthCompletedRuntime = 0
	}
	monthQuery.QueryHistory = true
	_, monthCompletedHistory, merr := s.QueryTasks(ctx, monthQuery)
	if merr != nil {
		logrus.WithError(merr).Warn("Failed to query month completed tasks from history")
		monthCompletedHistory = 0
	}
	monthCompleted := monthCompletedRuntime + monthCompletedHistory
	monthCompleted = clampSub(monthCompleted, s.countCcTasksEndedAfter(ctx, userID, tenantID, &monthStart))

	// ========== 日期筛选的已完成任务（用于趋势、类型分布、效率指标） ==========

	filteredQuery := &dto.TaskQuery{
		Assignee: userID,
		TenantID: tenantID,
		PageRequest: dto.PageRequest{
			Status:   []string{string(enums.TaskStatusCompleted)},
			PageSize: statsPageSizeFetchAll,
		},
	}
	if startDate != nil {
		filteredQuery.EndedAfter = startDate
	}
	if endDate != nil {
		filteredQuery.EndedBefore = endDate
	}

	filteredTasks, _, err := s.QueryTasks(ctx, filteredQuery)
	if err != nil {
		logrus.WithError(err).Warn("Failed to query filtered completed tasks")
		filteredTasks = nil
	}
	// 也查询历史表，合并结果
	filteredQuery.QueryHistory = true
	filteredTasksHistory, _, histErr := s.QueryTasks(ctx, filteredQuery)
	if histErr != nil {
		logrus.WithError(histErr).Warn("Failed to query filtered completed tasks from history")
	} else if len(filteredTasksHistory) > 0 {
		filteredTasks = append(filteredTasks, filteredTasksHistory...)
	}
	// CC 不计入"我办理的"趋势/分布/效率分析，与 doneCount 排除口径一致
	filteredTasks = filterOutCc(filteredTasks)

	// ========== 趋势数据（按日期分组） ==========

	trendMap := make(map[string]int64)
	for _, task := range filteredTasks {
		if task.EndedAt != nil {
			dateKey := task.EndedAt.Format("2006-01-02")
			trendMap[dateKey]++
		}
	}
	trendData := make([]map[string]interface{}, 0, len(trendMap))
	for date, count := range trendMap {
		trendData = append(trendData, map[string]interface{}{
			"date":  date,
			"count": count,
		})
	}

	// ========== 类型分布（按流程分类） ==========

	processIDCounts := make(map[string]int64)
	for _, task := range filteredTasks {
		processIDCounts[task.ProcessID]++
	}

	categoryCounts := make(map[string]int64)
	for processID, count := range processIDCounts {
		category := "其他"
		proc, procErr := s.workflowEngine.GetProcessService().Get(ctx, processID)
		if procErr != nil {
			logrus.WithError(procErr).Warnf("Failed to query process %s for category, using default", processID)
		} else if proc != nil && proc.Category != nil && *proc.Category != "" {
			category = *proc.Category
		}
		categoryCounts[category] += count
	}

	typeDistribution := make([]map[string]interface{}, 0, len(categoryCounts))
	for category, count := range categoryCounts {
		typeDistribution = append(typeDistribution, map[string]interface{}{
			"type":  category,
			"count": count,
		})
	}

	// ========== 效率指标 ==========

	var totalDuration int64
	var minDuration int64 = -1
	var maxDuration int64
	var durationCount int64
	overtimeCount := int64(0)

	for _, task := range filteredTasks {
		if task.Duration != nil && *task.Duration > 0 {
			totalDuration += *task.Duration
			durationCount++
			if minDuration < 0 || *task.Duration < minDuration {
				minDuration = *task.Duration
			}
			if *task.Duration > maxDuration {
				maxDuration = *task.Duration
			}
		}
		if task.DueDate != nil && task.EndedAt != nil && task.EndedAt.After(*task.DueDate) {
			overtimeCount++
		}
	}

	var avgDuration int64
	if durationCount > 0 {
		avgDuration = totalDuration / durationCount
	}
	if minDuration < 0 {
		minDuration = 0
	}

	// ========== 驳回统计 ==========

	rejectedCount := int64(0)
	for _, task := range filteredTasks {
		if task.EndReason != nil && *task.EndReason == string(enums.ApprovalResultRejected) {
			rejectedCount++
		}
	}

	totalFiltered := int64(len(filteredTasks))
	var rejectionRate float64
	if totalFiltered > 0 {
		rejectionRate = float64(rejectedCount) / float64(totalFiltered) * 100
	}

	// ========== 构建返回结果 ==========

	// 超时率在服务端算好：分母是"区间内我已办的任务数"，与分子 overtimeCount
	// 同口径（前端 totalCount 是全局口径，做分母会稀释超时率）。
	var overtimeRate float64
	if totalFiltered > 0 {
		overtimeRate = float64(overtimeCount) / float64(totalFiltered) * 100
	}

	statistics := map[string]interface{}{
		// 基础计数
		"todoCount":          todoCount,
		"doneCount":          doneCount,
		"ccCount":            ccCount,
		"myApplicationCount": myApplicationCount,
		"totalCount":         todoCount + doneCount + ccCount + myApplicationCount,

		// 日期筛选口径（仅统计页 V2 使用）
		"applicationInRange": applicationInRange,
		"filteredCount":      totalFiltered,

		// 时间段计数
		"todayCompleted": todayCompleted,
		"weekCompleted":  weekCompleted,
		"monthCompleted": monthCompleted,

		// 趋势数据
		"trendData": trendData,

		// 类型分布
		"typeDistribution": typeDistribution,

		// 效率指标
		"avgDuration":   avgDuration,
		"minDuration":   minDuration,
		"maxDuration":   maxDuration,
		"overtimeCount": overtimeCount,
		"overtimeRate":  overtimeRate,

		// 驳回统计
		"rejectedCount": rejectedCount,
		"rejectionRate": rejectionRate,
	}

	return statistics, nil
}

// countCcTasks 统计抄送给指定用户的任务数量。
//
// CC（抄送）任务在 cc_task_node.go 中"创建即完成"：status=completed、
// approval_type=cc、assignee=被抄送人。本方法与 GetCcProcessInstanceList
// 的查询条件保持一致，确保统计口径与列表页一致。
//
// 同时查运行时表和历史表并合并（归档后 CC 任务会随实例移到历史表），
// 与 done_count 的统计策略相同。
func (s *TaskServiceImpl) countCcTasks(ctx context.Context, userID, tenantID string) (int64, error) {
	ccQuery := &dto.TaskQuery{
		Assignee:     userID,
		TenantID:     tenantID,
		ApprovalType: string(enums.ApprovalTypeCC),
		PageRequest: dto.PageRequest{
			Status:   []string{string(enums.TaskStatusCompleted)},
			PageSize: 1,
		},
	}
	_, ccCountRuntime, err := s.QueryTasks(ctx, ccQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to query cc tasks (runtime): %w", err)
	}
	ccQuery.QueryHistory = true
	_, ccCountHistory, err := s.QueryTasks(ctx, ccQuery)
	if err != nil {
		// 历史表查询失败不致命，仅用运行时计数
		logrus.WithError(err).Warn("Failed to query cc tasks from history, using runtime count only")
		return ccCountRuntime, nil
	}
	return ccCountRuntime + ccCountHistory, nil
}

// countCcTasksEndedAfter 抄送任务数（可选按完成时间过滤，运行时+历史表）。
func (s *TaskServiceImpl) countCcTasksEndedAfter(ctx context.Context, userID, tenantID string, after *time.Time) int64 {
	q := &dto.TaskQuery{
		Assignee: userID, TenantID: tenantID, ApprovalType: string(enums.ApprovalTypeCC), EndedAfter: after,
		PageRequest: dto.PageRequest{Status: []string{string(enums.TaskStatusCompleted)}, PageSize: 1},
	}
	_, rt, err := s.QueryTasks(ctx, q)
	if err != nil {
		logrus.WithError(err).Warn("Failed to query cc tasks (runtime)")
		return 0
	}
	hist := *q
	hist.QueryHistory = true
	_, hi, err := s.QueryTasks(ctx, &hist)
	if err != nil {
		return rt
	}
	return rt + hi
}

// filterOutCc 从任务切片剔除抄送任务(approval_type=cc)。
// CC "创建即完成+assignee=被抄送人"，属抄送而非"我办理"，趋势/分布/效率分析应排除。
func filterOutCc(tasks []*model.WfTask) []*model.WfTask {
	result := make([]*model.WfTask, 0, len(tasks))
	for _, t := range tasks {
		if t != nil && t.ApprovalType != string(enums.ApprovalTypeCC) {
			result = append(result, t)
		}
	}
	return result
}

// clampSub 非负减法（防御并发计数错位）。
func clampSub(a, b int64) int64 {
	if a > b {
		return a - b
	}
	return 0
}
