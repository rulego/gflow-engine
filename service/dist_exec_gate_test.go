package service

import (
	"context"
	"sync"
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

// countingExtender 在本地锁之上计数续期调用的替身。lost 置 true 后 Extend 返回
// 持有权丢失，模拟锁 TTL 兜底转交他人。
type countingExtender struct {
	lock.Locker
	mu      sync.Mutex
	extends int
	lost    bool
}

func (c *countingExtender) Extend(_ context.Context, _, _ string, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.extends++
	return !c.lost, nil
}

func (c *countingExtender) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.extends
}

func shrinkWatchdogInterval(t *testing.T, d time.Duration) {
	t.Helper()
	old := distExecGateExtendInterval
	distExecGateExtendInterval = d
	t.Cleanup(func() { distExecGateExtendInterval = old })
}

// 持锁超过续期周期：看门狗按周期续期；释放后续期停止。
func TestDistExecGate_WatchdogExtendsWhileHeld(t *testing.T) {
	shared := lock.NewLocalLock()
	defer shared.Close()
	ext := &countingExtender{Locker: shared}
	shrinkWatchdogInterval(t, 20*time.Millisecond)

	replica := newGateTestRuntime(ext)
	unlock := replica.acquireDistExecGate(context.Background(), "inst-4")
	require.NotNil(t, unlock)

	time.Sleep(70 * time.Millisecond)
	assert.GreaterOrEqual(t, ext.count(), 2, "持锁期间应至少续期两次")

	unlock()
	base := ext.count()
	time.Sleep(60 * time.Millisecond)
	assert.Equal(t, base, ext.count(), "释放后看门狗应停止续期")
}

// 续期返回持有权丢失：看门狗退出。
func TestDistExecGate_WatchdogStopsOnOwnershipLost(t *testing.T) {
	shared := lock.NewLocalLock()
	defer shared.Close()
	ext := &countingExtender{Locker: shared, lost: true}
	shrinkWatchdogInterval(t, 20*time.Millisecond)

	replica := newGateTestRuntime(ext)
	unlock := replica.acquireDistExecGate(context.Background(), "inst-5")
	require.NotNil(t, unlock)
	defer unlock()

	time.Sleep(70 * time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	assert.Equal(t, 1, ext.count(), "持有权丢失后应只在首个周期续期一次即退出")
}
