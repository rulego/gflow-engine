package service

import (
	"context"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/enums"
)

// commentTestDB 建 wf_task / wf_hi_task / wf_task_comment 内存表。
func commentTestDB(t *testing.T) *query.Query {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:comment_test?mode=memory&cache=shared&_busy_timeout=30000"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if c, e := db.DB(); e == nil {
			_ = c.Close()
		}
	})
	taskDDL := `(
		id TEXT PRIMARY KEY, process_instance_id TEXT, process_id TEXT, parent_id TEXT,
		task_def_key TEXT, name TEXT, task_type TEXT, description TEXT, status TEXT,
		assignee TEXT, owner TEXT, due_date DATETIME, priority INTEGER, form_key TEXT,
		variables TEXT, claimed_at DATETIME, sequence_order INTEGER, approval_type TEXT,
		approval_rule TEXT, delegate_from TEXT, delegate_reason TEXT, delegate_time DATETIME,
		ended_at DATETIME, comment TEXT, end_reason TEXT, duration INTEGER, tenant_id TEXT,
		created_by TEXT, created_at DATETIME, updated_by TEXT, updated_at DATETIME)`
	require.NoError(t, db.Exec("CREATE TABLE IF NOT EXISTS wf_task "+taskDDL).Error)
	require.NoError(t, db.Exec("CREATE TABLE IF NOT EXISTS wf_hi_task "+taskDDL).Error)
	require.NoError(t, db.Exec(`CREATE TABLE IF NOT EXISTS wf_task_comment (
		id TEXT PRIMARY KEY, task_id TEXT NOT NULL, process_instance_id TEXT NOT NULL DEFAULT '',
		tenant_id TEXT NOT NULL DEFAULT '', user_id TEXT NOT NULL,
		user_name TEXT NOT NULL DEFAULT '', content TEXT NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)`).Error)
	for _, tbl := range []string{"wf_task", "wf_hi_task", "wf_task_comment"} {
		require.NoError(t, db.Exec("DELETE FROM "+tbl).Error)
	}
	return query.Use(db)
}

func newCommentSvc(q *query.Query) *TaskServiceImpl {
	return &TaskServiceImpl{
		taskDAO:         dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:       dao.NewHiTaskDAOWithQuery(q),
		taskAssigneeDAO: dao.NewTaskAssigneeDAOWithQuery(q),
		taskCommentDAO:  dao.NewTaskCommentDAOWithQuery(q),
		idGenerator:     &testSeqIDGen{},
		workflowEngine:  &testEngineDouble{},
	}
}

// TestTaskComments_Roundtrip 评论读写回环：在办与已归档任务均可评论，
// 评论带身份快照（userID/userName/instanceID），跨租户按不存在处理。
func TestTaskComments_Roundtrip(t *testing.T) {
	q := commentTestDB(t)
	svc := newCommentSvc(q)
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "userA", UserName: "张三", TenantID: "t1"})
	now := time.Now()

	// 在办任务
	require.NoError(t, svc.taskDAO.Create(ctx, &model.WfTask{
		ID: "task-c1", Status: string(enums.TaskStatusActive), TenantID: "t1",
		ProcessInstanceID: secFixStrPtr("inst-c1"), Name: "审批", TaskType: "user_task",
		CreatedAt: now, CreatedBy: "sys",
	}))
	id1, err := svc.AddTaskComment(ctx, Actor{UserID: "userA", UserName: "张三", TenantID: "t1"}, "task-c1", "同意")
	require.NoError(t, err)
	require.NotEmpty(t, id1)

	// 归档任务（只在 wf_hi_task）同样可评论
	require.NoError(t, svc.hiTaskDAO.Create(ctx, &model.WfHiTask{
		ID: "task-c2", Status: string(enums.TaskStatusCompleted), TenantID: "t1",
		ProcessInstanceID: secFixStrPtr("inst-c2"), Name: "审批", TaskType: "user_task",
		CreatedAt: now, CreatedBy: "sys",
	}))
	_, err = svc.AddTaskComment(ctx, Actor{UserID: "userA", UserName: "张三", TenantID: "t1"}, "task-c2", "补充意见")
	require.NoError(t, err, "归档任务应可评论")

	comments, err := svc.GetTaskComments(ctx, ActorFromCtx(ctx), "task-c1")
	require.NoError(t, err)
	require.Len(t, comments, 1)
	require.Equal(t, "userA", comments[0].UserID)
	require.Equal(t, "张三", comments[0].UserName)
	require.Equal(t, "inst-c1", comments[0].ProcessInstanceID)
	require.Equal(t, "t1", comments[0].TenantID)

	// 跨租户读取按不存在处理
	ctxOther := SetUserToCtx(context.Background(), &Actor{UserID: "userB", UserName: "B", TenantID: "t2"})
	_, err = svc.GetTaskComments(ctxOther, ActorFromCtx(ctxOther), "task-c1")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = svc.AddTaskComment(ctxOther, Actor{UserID: "userB", UserName: "B", TenantID: "t2"}, "task-c1", "越权评论")
	require.ErrorIs(t, err, ErrNotFound)

	// 无身份拒绝
	// 空 userID 拒绝（显式操作人口径）
	_, err = svc.AddTaskComment(context.Background(), Actor{}, "task-c1", "匿名")
	require.ErrorIs(t, err, ErrAuthenticationRequired)

	// 空内容拒绝
	_, err = svc.AddTaskComment(ctx, Actor{UserID: "userA", UserName: "张三", TenantID: "t1"}, "task-c1", "   ")
	require.ErrorIs(t, err, ErrValidation)

	// 不存在的任务
	_, err = svc.GetTaskComments(ctx, ActorFromCtx(ctx), "task-ghost")
	require.ErrorIs(t, err, ErrNotFound)
}
