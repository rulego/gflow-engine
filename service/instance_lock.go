package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/enums"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// terminalInstanceStatuses are instance statuses that no longer accept mutations.
// Once an instance reaches any of these, the row is considered frozen; all
// further task/instance writes are rejected so we never resurrect a finished
// workflow or duplicate its terminal side effects.
var terminalInstanceStatuses = map[string]bool{
	string(enums.InstanceStatusCompleted):  true,
	string(enums.InstanceStatusTerminated): true,
	string(enums.InstanceStatusCancelled):  true,
	string(enums.InstanceStatusFailed):     true,
}

// IsTerminalInstanceStatus reports whether the status is terminal.
// Exposed as a package-level function so callers (and tests) can reason about
// the same set of frozen statuses the helper enforces.
func IsTerminalInstanceStatus(status string) bool {
	return terminalInstanceStatuses[status]
}

// ErrInstanceNotFound is returned by WithInstanceTx when the instance row
// cannot be located. Exposed so callers can distinguish "missing" from other
// transaction failures without relying on string matching.
var ErrInstanceNotFound = errors.New("instance not found")

// ErrInstanceTerminal is returned by WithInstanceTx when the instance row is
// present but is already in a terminal status (completed/terminated/cancelled/failed).
var ErrInstanceTerminal = errors.New("instance is in terminal state")

// ErrProcessDefinitionNotFound 流程定义不存在（如草稿实例的定义已被删除）。
// 独立哨兵便于调用方用 errors.Is 区分"定义已删"与其他激活失败，映射成
// 可操作的提示而非泛化 500。
var ErrProcessDefinitionNotFound = errors.New("process definition not found")

// ErrInstanceLockTimeout is returned by WithInstanceTx when acquiring the
// instance row lock or running the transaction exceeds the application-level
// timeout. Exposed so callers can distinguish lock-contention timeouts from
// other failures (and retry or surface a clear message to the user).
var ErrInstanceLockTimeout = errors.New("instance lock acquisition timed out")

// defaultInstanceLockTimeout 是 WithInstanceTx 的应用层超时上限。
//
// 作用：调用方常直接传 HTTP/引擎的原始 ctx 而没有 deadline，FOR UPDATE 的锁
// 等待会一直撑到数据库的 lock_wait_timeout（MySQL 默认 50s），慢的级联操作
// 会卡住同实例的所有并发审批。本上限仅当传入 ctx 没有 deadline 时生效——
// 调用方显式设置的更短 deadline 会被尊重（context.WithDeadline 自动取较早者）。
// 30s 覆盖最慢的内部级联路径（如会签父任务更新 + ExecuteNext 链）。
const defaultInstanceLockTimeout = 30 * time.Second

// WithInstanceTx acquires a row-level lock (SELECT ... FOR UPDATE) on the
// wf_instance row identified by instanceID, then runs fn inside that
// transaction with an InstanceScope handle.
//
// 设计目的：把"持锁事务内的状态修改"和"提交后的副作用"在 API 层强制分开。
// 通过 scope 只暴露 tx-bound 的 DAO accessor，让 tx 逃逸（在事务里走默认连接
// 起新 query）这类 bug 在编译期不可能发生。副作用通过 scope.AfterCommit
// 注册，事务提交后才执行——避开了 rulego OnMsg 同步重入 WithInstanceTx 抢
// 同一行 FOR UPDATE 锁的死锁。
//
// 三条铁律（编译器 + code review 都能查）：
//  1. 回调内只允许 scope.* 访问 DB——s.taskDAO.X 是 bug 信号
//  2. AfterCommit 回调里禁止使用 scope（tx 已关），必须从 tx 阶段拷贝数据
//  3. ExecuteNext / 发通知 / 调外部服务一律 AfterCommit
//
// Lock timeout: 如果传入的 ctx 没有 deadline，会自动套一个
// defaultInstanceLockTimeout 上限（避免无限阻塞）。调用方显式设置的更短
// deadline 会被尊重。超时返回 ErrInstanceLockTimeout（包装底层 DB 错误）。
func WithInstanceTx(
	ctx context.Context,
	q *query.Query,
	instanceID string,
	fn func(scope *InstanceScope) error,
) error {
	if instanceID == "" {
		return fmt.Errorf("WithInstanceTx: instanceID is required")
	}
	if q == nil {
		return fmt.Errorf("WithInstanceTx: query is nil")
	}
	if fn == nil {
		return fmt.Errorf("WithInstanceTx: fn is required")
	}

	// 应用层超时保护：仅在 ctx 没有 deadline 时套一个默认上限。
	// 调用方已设置 deadline 的（更短或更长）都会被保留——context.WithDeadline
	// 取父 ctx 和新 deadline 的较早者，不会把调用方的短 deadline 拉长。
	lockCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		lockCtx, cancel = context.WithTimeout(ctx, defaultInstanceLockTimeout)
		defer cancel()
	}

	// 把 lockCtx 绑到事务连接上：BEGIN/COMMIT 与锁等待都受超时约束，
	// 慢级联不会无限占着实例行锁（锁等待超时经下方映射为 ErrInstanceLockTimeout）。
	scope := &InstanceScope{}
	err := q.ReplaceDB(q.RawDB().WithContext(lockCtx)).Transaction(func(tx *query.Query) error {
		// SELECT FOR UPDATE serializes concurrent mutations on this instance.
		db := tx.WfInstance.WithContext(lockCtx).
			Where(tx.WfInstance.ID.Eq(instanceID)).
			UnderlyingDB().
			Clauses(clause.Locking{Strength: "UPDATE"})

		var inst model.WfInstance
		if err := db.First(&inst).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
			}
			return fmt.Errorf("acquire instance lock: %w", err)
		}
		if IsTerminalInstanceStatus(inst.Status) {
			return fmt.Errorf("%w: instance %s status=%s", ErrInstanceTerminal, instanceID, inst.Status)
		}
		scope.tx = tx
		return fn(scope)
	})
	if err != nil {
		// 区分超时错误：ctx 超时会导致 DB 操作返回 context 相关错误，
		// 包装成 ErrInstanceLockTimeout 便于调用方识别和重试。
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("%w: instance %s (%v)", ErrInstanceLockTimeout, instanceID, err)
		}
		return err
	}
	// 事务已提交，按注册顺序执行 post-commit hooks。
	// 任一 hook 出错，后续跳过，错误向上传播。
	for _, hook := range scope.postCommit {
		if err := hook(); err != nil {
			return err
		}
	}
	return nil
}
