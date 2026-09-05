package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/utils/lock"
)

// gateTestEngine 仅实现 GetLocker 的最小引擎替身（嵌入接口满足其余方法签名，
// 不被调用）。acquireDistExecGate 只消费 GetLocker。
type gateTestEngine struct {
	WorkflowEngine
	l lock.Locker
}

func (e *gateTestEngine) GetLocker() lock.Locker { return e.l }

func newGateTestRuntime(l lock.Locker) *RuntimeServiceImpl {
	return &RuntimeServiceImpl{workflowEngine: &gateTestEngine{l: l}}
}

// 两副本共享同一把锁（模拟 Redis 锁跨进程互斥）：
// 副本 A 持锁期间副本 B 的 acquireDistExecGate 必须等待，A 释放后 B 才能获得。
func TestDistExecGate_CrossReplicaMutualExclusion(t *testing.T) {
	shared := lock.NewLocalLock()
	defer shared.Close()

	replicaA := newGateTestRuntime(shared)
	replicaB := newGateTestRuntime(shared)

	// A 先持有（模拟对端副本正在驱动该实例）
	unlockA := replicaA.acquireDistExecGate(context.Background(), "inst-1")
	require.NotNil(t, unlockA)

	acquired := make(chan struct{})
	var unlockB func()
	go func() {
		unlockB = replicaB.acquireDistExecGate(context.Background(), "inst-1")
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("副本 B 在 A 持锁期间不应拿到门闩（跨副本互斥失效）")
	case <-time.After(150 * time.Millisecond):
	}

	// A 释放后 B 应能获得
	unlockA()
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("A 释放后 B 未能获得门闩")
	}
	require.NotNil(t, unlockB)
	unlockB()
}

// 锁被他人长期持有（重试预算耗尽）：放行执行返回 nil（可用性优先，不卡死流程）。
func TestDistExecGate_BudgetExhausted_ProceedsWithoutGate(t *testing.T) {
	shared := lock.NewLocalLock()
	defer shared.Close()

	// 缩短重试预算，避免测试跑 15 秒
	oldInterval, oldRetries := distExecGateInterval, distExecGateRetries
	distExecGateInterval, distExecGateRetries = 5*time.Millisecond, 5
	defer func() { distExecGateInterval, distExecGateRetries = oldInterval, oldRetries }()

	// 占住锁不放（模拟对端副本驱动卡死，TTL 未到）
	_, err := shared.Lock(context.Background(), distExecGateKey("inst-2"), time.Minute)
	require.NoError(t, err)

	replica := newGateTestRuntime(shared)
	start := time.Now()
	unlock := replica.acquireDistExecGate(context.Background(), "inst-2")
	assert.Nil(t, unlock, "重试预算耗尽后应放行（返回 nil）")
	assert.Less(t, time.Since(start), 3*time.Second, "不应阻塞到默认 15s 预算")
}

// 锁空闲：立即获得，释放后可再次获得（正常生命周期）；nil Locker 引擎返回 nil。
func TestDistExecGate_Lifecycle(t *testing.T) {
	shared := lock.NewLocalLock()
	defer shared.Close()

	replica := newGateTestRuntime(shared)
	unlock := replica.acquireDistExecGate(context.Background(), "inst-3")
	require.NotNil(t, unlock)
	unlock()

	// 释放后可重入
	unlock2 := replica.acquireDistExecGate(context.Background(), "inst-3")
	require.NotNil(t, unlock2)
	unlock2()

	// 空 instanceID / nil Locker：no-op 放行
	assert.Nil(t, replica.acquireDistExecGate(context.Background(), ""))
	nilLocker := newGateTestRuntime(nil)
	assert.Nil(t, nilLocker.acquireDistExecGate(context.Background(), "inst-3"))
}
