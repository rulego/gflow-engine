package service

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
)

// 跨副本链驱动门闩：ExecuteNext / Restore / ForceResume / Start 初始驱动在进程内
// execGate（runtime_exec_gate.go）之上再加一层分布式互斥。多副本共享同一数据库时，
// 两副本可能同时判定 fork-join 已收齐而各自 multi-restore，产生重复下游任务与
// 重复 end 执行；userTask 查-建去重与 end 去重锁同样只在本进程生效，跨副本互斥
// 只能由 Locker 承担——宿主多副本部署时注入 Redis 锁，单机默认 LocalLock（execGate
// 已串行化本进程驱动，LocalLock 恒可立即获得，无额外开销）。
//
// 可用性优先：等待预算耗尽或锁服务异常时放行执行，重复驱动由 userTask 查-建幂等、
// end 去重与 suspend 节点重入幂等兜底，不因锁故障卡死流程；持锁方崩溃由 TTL 兜底。
//
// var 而非 const：测试需要缩短重试预算。
var (
	distExecGateTTL      = 3 * time.Minute
	distExecGateInterval = 100 * time.Millisecond
	distExecGateRetries  = 150 // 100ms × 150 ≈ 15s 等待预算，与 WithInstanceTx 锁超时同量级
)

func distExecGateKey(instanceID string) string {
	return "bpm:exec-gate:" + instanceID
}

// acquireDistExecGate 获取实例级跨副本驱动门闩。返回 nil 表示未持锁（放行执行，
// 调用方无须释放）；返回非 nil 时驱动完成后须调用释放函数（幂等）。同 goroutine
// 重入由上层 execGate 的 reentrant 标记跳过，此处不做重入计数。
func (s *RuntimeServiceImpl) acquireDistExecGate(ctx context.Context, instanceID string) func() {
	if instanceID == "" {
		return nil
	}
	if s.workflowEngine == nil {
		return nil
	}
	locker := s.workflowEngine.GetLocker()
	if locker == nil {
		return nil
	}
	key := distExecGateKey(instanceID)
	value, err := locker.LockWithRetry(ctx, key, distExecGateTTL, distExecGateInterval, distExecGateRetries)
	if err != nil {
		logrus.WithError(err).WithField("instanceId", instanceID).
			Warn("dist exec gate: acquire failed, proceed without cross-replica mutual exclusion")
		return nil
	}
	return func() {
		if err := locker.Unlock(context.Background(), key, value); err != nil {
			logrus.WithError(err).WithField("instanceId", instanceID).Debug("dist exec gate: unlock failed")
		}
	}
}
