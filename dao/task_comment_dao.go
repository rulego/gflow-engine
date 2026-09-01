package dao

import (
	"context"
	"fmt"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
)

// TaskCommentDAO 任务审批意见数据访问对象。
// 读写经由构造时传入的 *query.Query：传入事务包装的 Query 时，
// 读写自动落在调用方事务内。
type TaskCommentDAO struct {
	Query *query.Query
}

// NewTaskCommentDAO 创建 TaskCommentDAO 实例
func NewTaskCommentDAO() *TaskCommentDAO {
	return &TaskCommentDAO{Query: query.Q}
}

// NewTaskCommentDAOWithQuery 创建绑定指定 Query（通常是事务）的 TaskCommentDAO
func NewTaskCommentDAOWithQuery(q *query.Query) *TaskCommentDAO {
	return &TaskCommentDAO{Query: q}
}

// Create 写入一条评论
func (d *TaskCommentDAO) Create(ctx context.Context, comment *model.WfTaskComment) error {
	if comment == nil {
		return fmt.Errorf("comment cannot be nil")
	}
	q := d.Query.WfTaskComment
	if err := q.WithContext(ctx).Create(comment); err != nil {
		return fmt.Errorf("failed to create task comment: %w", err)
	}
	return nil
}

// ListByTaskID 按时间正序返回任务的全部评论
func (d *TaskCommentDAO) ListByTaskID(ctx context.Context, taskID string) ([]*model.WfTaskComment, error) {
	if taskID == "" {
		return nil, fmt.Errorf("taskID cannot be empty")
	}
	q := d.Query.WfTaskComment
	comments, err := q.WithContext(ctx).
		Where(q.TaskID.Eq(taskID)).
		Order(q.CreatedAt).
		Find()
	if err != nil {
		return nil, fmt.Errorf("failed to list task comments: %w", err)
	}
	return comments, nil
}
