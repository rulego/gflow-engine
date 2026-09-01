package service

import (
	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/query"
)

// InstanceScope 是 WithInstanceTx 回调里唯一能拿到的 DB 句柄。
//
// 设计目的：让 tx 逃逸（在事务回调里通过默认 query 起新连接）这类 bug
// 在 API 层不可能发生。回调内只能通过 scope.* 访问 DB，所有 accessor 都是
// tx-bound；副作用（ExecuteNext / 发通知 / 调外部服务）通过 AfterCommit
// 推迟到事务提交后才执行。
//
// 使用约定：
//  1. 回调内只允许 scope.* 访问 DB——直接 s.taskDAO.X 是 bug 信号
//  2. AfterCommit 注册的回调里禁止使用 scope（tx 已关闭），必须从外层
//     拷贝需要的数据（指针值在 tx 阶段就要捕获）
//  3. ExecuteNext / 发通知 / 调外部服务一律 AfterCommit
type InstanceScope struct {
	tx         *query.Query
	postCommit []func() error
	// bare 标记该 scope 不在事务内（orphan/draft 任务等无实例行可锁的路径）。
	// bare scope 上注册的 AfterCommit 回调立即执行——没有提交可等。
	bare bool
}

// bareScope 构造无事务包裹的 scope，用于 orphan/draft 任务等
// 没有实例行可以锁定的变更路径。
func bareScope(q *query.Query) *InstanceScope {
	return &InstanceScope{tx: q, bare: true}
}

// Tasks 返回绑定了当前 tx 的 TaskDAO。
// 每次调用都新建 wrapper（dao.NewTaskDAOWithQuery 只设置一个指针字段，廉价）。
func (s *InstanceScope) Tasks() *dao.TaskDAO {
	return dao.NewTaskDAOWithQuery(s.tx)
}

// Instances 返回绑定了当前 tx 的 InstanceDAO。
func (s *InstanceScope) Instances() *dao.InstanceDAO {
	return dao.NewInstanceDAOWithQuery(s.tx)
}

// HiTasks 返回绑定了当前 tx 的 HiTaskDAO（历史任务归档）。
func (s *InstanceScope) HiTasks() *dao.HiTaskDAO {
	return dao.NewHiTaskDAOWithQuery(s.tx)
}

// HiInstances 返回绑定了当前 tx 的 HiInstanceDAO（历史实例归档）。
func (s *InstanceScope) HiInstances() *dao.HiInstanceDAO {
	return dao.NewHiInstanceDAOWithQuery(s.tx)
}

// Processes 返回绑定了当前 tx 的 ProcessDAO（流程定义）。
func (s *InstanceScope) Processes() *dao.ProcessDAO {
	return dao.NewProcessDAOWithQuery(s.tx)
}

// TaskAssignees 返回绑定了当前 tx 的 TaskAssigneeDAO（候选校验需在事务内读候选池）。
func (s *InstanceScope) TaskAssignees() *dao.TaskAssigneeDAO {
	return dao.NewTaskAssigneeDAOWithQuery(s.tx)
}

// TaskComments 返回绑定了当前 tx 的 TaskCommentDAO（审批意见随审批动作同事务落库）。
func (s *InstanceScope) TaskComments() *dao.TaskCommentDAO {
	return dao.NewTaskCommentDAOWithQuery(s.tx)
}

// AfterCommit 注册一个事务提交后执行的回调。可多次调用，按注册顺序执行。
// 任意一个返回 error，后续跳过，错误向上传播。
//
// bare scope（无事务）上注册的回调立即执行——副作用不能丢。
//
// ⚠️ 回调内禁止使用 scope（tx 已关闭）——必须从 tx 阶段拷贝需要的数据。
// ⚠️ 适合放 ExecuteNext、发通知、调外部 API 等副作用。
func (s *InstanceScope) AfterCommit(f func() error) {
	if s.bare {
		if err := f(); err != nil {
			logrus.WithError(err).Warn("bare-scope post-commit hook failed")
		}
		return
	}
	s.postCommit = append(s.postCommit, f)
}

// Tx 暴露底层 *query.Query，用于 raw SQL / 上面 accessor 没覆盖的高级场景。
// 日常使用应优先用上面的 typed accessor。
func (s *InstanceScope) Tx() *query.Query { return s.tx }
