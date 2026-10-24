package service

import (
	"context"
	"time"
)

// HistoryService 历史服务接口
// 提供流程历史数据的查询和管理功能
// 查询类方法以 actor 传入查询视角：真实用户按 actor.TenantID 校验/过滤
// （单条读校验归属，列表读强制过滤，空租户拒绝）；系统身份空租户为全租户视角。
type HistoryService interface {
	// GetHistoricProcessInstances 获取历史流程实例列表
	GetHistoricProcessInstances(ctx context.Context, actor Actor, query *HistoricProcessInstanceQuery) ([]*HistoricProcessInstance, int64, error)

	// GetHistoricProcessInstance 根据ID获取历史流程实例
	GetHistoricProcessInstance(ctx context.Context, actor Actor, processInstanceID string) (*HistoricProcessInstance, error)

	// GetHistoricTaskInstances 获取历史任务实例列表
	GetHistoricTaskInstances(ctx context.Context, actor Actor, query *HistoricTaskInstanceQuery) ([]*HistoricTaskInstance, int64, error)

	// GetHistoricTaskInstance 根据ID获取历史任务实例
	GetHistoricTaskInstance(ctx context.Context, actor Actor, taskID string) (*HistoricTaskInstance, error)

	// DeleteHistoricProcessInstance 删除历史流程实例（actor 租户非空时校验归属）
	DeleteHistoricProcessInstance(ctx context.Context, actor Actor, processInstanceID string) error

	// DeleteHistoricTaskInstance 删除历史任务实例（actor 租户非空时校验归属）
	DeleteHistoricTaskInstance(ctx context.Context, actor Actor, taskID string) error
}

// HistoricProcessInstance 历史流程实例
type HistoricProcessInstance struct {
	ID                       string     `json:"id"`
	ProcessDefinitionID      string     `json:"processDefinitionId"`
	ProcessDefinitionKey     string     `json:"processDefinitionKey"`
	ProcessDefinitionName    string     `json:"processDefinitionName"`
	ProcessDefinitionVersion int        `json:"processDefinitionVersion"`
	BusinessKey              string     `json:"businessKey"`
	StartTime                time.Time  `json:"startTime"`
	EndTime                  *time.Time `json:"endTime"`
	DurationInMillis         *int64     `json:"durationInMillis"`
	StartUserID              string     `json:"startUserId"`
	StartActivityID          string     `json:"startActivityId"`
	EndActivityID            string     `json:"endActivityId"`
	SuperProcessInstanceID   string     `json:"superProcessInstanceId"`
	DeleteReason             string     `json:"deleteReason"`
	TenantID                 string     `json:"tenantId"`
}

// HistoricTaskInstance 历史任务实例
type HistoricTaskInstance struct {
	ID                  string     `json:"id"`
	ProcessDefinitionID string     `json:"processDefinitionId"`
	ProcessInstanceID   string     `json:"processInstanceId"`
	ExecutionID         string     `json:"executionId"`
	Name                string     `json:"name"`
	Description         string     `json:"description"`
	DeleteReason        string     `json:"deleteReason"`
	Owner               string     `json:"owner"`
	Assignee            string     `json:"assignee"`
	StartTime           time.Time  `json:"startTime"`
	EndTime             *time.Time `json:"endTime"`
	DurationInMillis    *int64     `json:"durationInMillis"`
	WorkTimeInMillis    *int64     `json:"workTimeInMillis"`
	ClaimTime           *time.Time `json:"claimTime"`
	TaskDefinitionKey   string     `json:"taskDefinitionKey"`
	FormKey             string     `json:"formKey"`
	Priority            int        `json:"priority"`
	DueDate             *time.Time `json:"dueDate"`
	ParentTaskID        string     `json:"parentTaskId"`
	Category            string     `json:"category"`
	TenantID            string     `json:"tenantId"`
	// Variables 任务变量快照（剥引擎保留标记键后的表单字段状态，即任务完成
	// 瞬间该提交人视角的表单全量）。单实例查询始终带出；列表查询仅在
	// IncludeTaskLocalVariables=true 时填充，避免列表响应无谓膨胀。
	Variables map[string]interface{} `json:"variables,omitempty"`
}

// HistoricProcessInstanceQuery 历史流程实例查询条件
type HistoricProcessInstanceQuery struct {
	ProcessInstanceID       string     `json:"processInstanceId"`
	ProcessInstanceIDs      []string   `json:"processInstanceIds"`
	ProcessDefinitionID     string     `json:"processDefinitionId"`
	ProcessDefinitionKey    string     `json:"processDefinitionKey"`
	ProcessDefinitionKeyIn  []string   `json:"processDefinitionKeyIn"`
	BusinessKey             string     `json:"businessKey"`
	BusinessKeyLike         string     `json:"businessKeyLike"`
	StartedBy               string     `json:"startedBy"`
	StartedBefore           *time.Time `json:"startedBefore"`
	StartedAfter            *time.Time `json:"startedAfter"`
	FinishedBefore          *time.Time `json:"finishedBefore"`
	FinishedAfter           *time.Time `json:"finishedAfter"`
	Finished                *bool      `json:"finished"`
	Unfinished              *bool      `json:"unfinished"`
	SuperProcessInstanceID  string     `json:"superProcessInstanceId"`
	ExcludeSubprocesses     *bool      `json:"excludeSubprocesses"`
	IncludeProcessVariables *bool      `json:"includeProcessVariables"`
	TenantID                string     `json:"tenantId"`
	TenantIDLike            string     `json:"tenantIdLike"`
	WithoutTenantID         *bool      `json:"withoutTenantId"`
	Page                    int        `json:"page"`
	PageSize                int        `json:"pageSize"`
	OrderBy                 string     `json:"orderBy"`
	SortOrder               string     `json:"sortOrder"`
}

// HistoricTaskInstanceQuery 历史任务实例查询条件
type HistoricTaskInstanceQuery struct {
	TaskID                    string     `json:"taskId"`
	TaskIDs                   []string   `json:"taskIds"`
	ProcessInstanceID         string     `json:"processInstanceId"`
	ProcessInstanceIDs        []string   `json:"processInstanceIds"`
	ProcessDefinitionID       string     `json:"processDefinitionId"`
	ProcessDefinitionKey      string     `json:"processDefinitionKey"`
	ProcessDefinitionName     string     `json:"processDefinitionName"`
	TaskName                  string     `json:"taskName"`
	TaskNameLike              string     `json:"taskNameLike"`
	TaskDescription           string     `json:"taskDescription"`
	TaskDescriptionLike       string     `json:"taskDescriptionLike"`
	TaskDefinitionKey         string     `json:"taskDefinitionKey"`
	TaskDeleteReason          string     `json:"taskDeleteReason"`
	TaskDeleteReasonLike      string     `json:"taskDeleteReasonLike"`
	TaskAssignee              string     `json:"taskAssignee"`
	TaskAssigneeLike          string     `json:"taskAssigneeLike"`
	TaskOwner                 string     `json:"taskOwner"`
	TaskOwnerLike             string     `json:"taskOwnerLike"`
	TaskInvolvedUser          string     `json:"taskInvolvedUser"`
	TaskPriority              *int       `json:"taskPriority"`
	Finished                  *bool      `json:"finished"`
	Unfinished                *bool      `json:"unfinished"`
	ProcessFinished           *bool      `json:"processFinished"`
	ProcessUnfinished         *bool      `json:"processUnfinished"`
	TaskCompletedBefore       *time.Time `json:"taskCompletedBefore"`
	TaskCompletedAfter        *time.Time `json:"taskCompletedAfter"`
	TaskCreatedBefore         *time.Time `json:"taskCreatedBefore"`
	TaskCreatedAfter          *time.Time `json:"taskCreatedAfter"`
	IncludeTaskLocalVariables *bool      `json:"includeTaskLocalVariables"`
	IncludeProcessVariables   *bool      `json:"includeProcessVariables"`
	TenantID                  string     `json:"tenantId"`
	TenantIDLike              string     `json:"tenantIdLike"`
	WithoutTenantID           *bool      `json:"withoutTenantId"`
	Page                      int        `json:"page"`
	PageSize                  int        `json:"pageSize"`
	OrderBy                   string     `json:"orderBy"`
	SortOrder                 string     `json:"sortOrder"`
}
