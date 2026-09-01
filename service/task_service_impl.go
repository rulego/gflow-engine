// This file is the anchor for the TaskServiceImpl type: it declares the
// struct, its package-level interface assertion, and the two constructors
// (NewTaskService and NewTaskServiceWithQuery). The concrete methods live in
// the task_service_*.go siblings within this package.

package service

import (
	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/query"
)

var _ TaskService = (*TaskServiceImpl)(nil)
var _ TaskServiceInternal = (*TaskServiceImpl)(nil)

// TaskServiceImpl TaskService接口的实现
type TaskServiceImpl struct {
	taskDAO         *dao.TaskDAO
	hiTaskDAO       *dao.HiTaskDAO
	taskAssigneeDAO *dao.TaskAssigneeDAO
	taskCommentDAO  *dao.TaskCommentDAO
	idGenerator     IDGenerator
	workflowEngine  WorkflowEngine
}

// NewTaskService 创建TaskService实例
func NewTaskService(workflowEngine WorkflowEngine) TaskService {
	return &TaskServiceImpl{
		taskDAO:         dao.NewTaskDAO(),
		hiTaskDAO:       dao.NewHiTaskDAO(),
		taskAssigneeDAO: dao.NewTaskAssigneeDAO(),
		taskCommentDAO:  dao.NewTaskCommentDAO(),
		idGenerator:     workflowEngine.GetIDGenerator(),
		workflowEngine:  workflowEngine,
	}
}

// NewTaskServiceWithQuery 创建带Query参数的TaskService实例
func NewTaskServiceWithQuery(query *query.Query, workflowEngine WorkflowEngine) TaskService {
	return &TaskServiceImpl{
		taskDAO:         dao.NewTaskDAOWithQuery(query),
		hiTaskDAO:       dao.NewHiTaskDAOWithQuery(query),
		taskAssigneeDAO: dao.NewTaskAssigneeDAOWithQuery(query),
		taskCommentDAO:  dao.NewTaskCommentDAOWithQuery(query),
		idGenerator:     workflowEngine.GetIDGenerator(),
		workflowEngine:  workflowEngine,
	}
}
