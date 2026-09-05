/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrLockExists 锁已被其他持有者占用（Lock/TryLock 竞争失败时返回）。
var ErrLockExists = errors.New("lock already exists")

// Locker 键锁接口（value 为持有凭证，Unlock 时需原样传入）
type Locker interface {
	// Lock 阻塞获取锁，返回锁的值（用于释放锁）
	Lock(ctx context.Context, key string, expiration time.Duration) (string, error)

	// Unlock 释放锁（value 不匹配时返回错误）
	Unlock(ctx context.Context, key, value string) error

	// TryLock 非阻塞获取锁；未抢到时 acquired=false，ErrLockExists 不单独返回
	TryLock(ctx context.Context, key string, expiration time.Duration) (string, bool, error)

	// LockWithRetry 带重试间隔与次数上限的获取锁
	LockWithRetry(ctx context.Context, key string, expiration time.Duration, retryInterval time.Duration, maxRetries int) (string, error)
}

// LockExtender 可选的锁续期能力：凭证匹配时重置 TTL，供临界区执行超过 TTL 的
// 持有方周期性调用。跨进程实现应提供；进程内锁同步持有，无须实现。
type LockExtender interface {
	// Extend 凭证匹配时把 TTL 重置为 expiration。返回 false 表示锁不存在或
	// 凭证不匹配——持有权已丢失，调用方不应继续续期。
	Extend(ctx context.Context, key, value string, expiration time.Duration) (bool, error)
}

// LocalLock 本地键锁实现（用于单节点）
type LocalLock struct {
	locks  sync.Map // key: string, value: *lockInfo
	stopCh chan struct{}
}

type lockInfo struct {
	value     string
	expiredAt time.Time
	mutex     sync.RWMutex
}

// NewLocalLock 创建本地键锁实例。
// 返回具体类型以暴露 Close()：临时创建的实例不再使用时应 Close 释放后台清理
// goroutine；进程级单例（如 DefaultKeyLock）无需（也不应）关闭。
func NewLocalLock() *LocalLock {
	lock := &LocalLock{
		stopCh: make(chan struct{}),
	}

	// 启动清理过期锁的goroutine
	go lock.cleanupExpiredLocks()

	return lock
}

// Close 停止 LocalLock 的后台清理 goroutine。
// 该方法是幂等的，多次调用安全。仅对 NewLocalLock() 创建的实例有效。
// DefaultKeyLock 作为进程级单例不需要（也不应该）被关闭。
func (l *LocalLock) Close() {
	if l == nil {
		return
	}
	// 用 recover 兜底，防止对已关闭的 channel 调用 close 导致 panic
	defer func() {
		_ = recover()
	}()
	select {
	case <-l.stopCh:
		// 已经关闭过了
	default:
		close(l.stopCh)
	}
}

// Lock 获取本地锁
func (l *LocalLock) Lock(ctx context.Context, key string, expiration time.Duration) (string, error) {
	value := generateLockValue()
	expiredAt := time.Now().Add(expiration)

	for {
		if actual, loaded := l.locks.LoadOrStore(key, &lockInfo{
			value:     value,
			expiredAt: expiredAt,
		}); !loaded {
			// 成功获取锁
			return value, nil
		} else {
			// 锁已存在，检查是否过期
			info := actual.(*lockInfo)
			info.mutex.RLock()
			expired := time.Now().After(info.expiredAt)
			info.mutex.RUnlock()

			if expired {
				// 锁已过期：CAS 直接换成新锁。不能换成 nil——并发 Lock
				// 会 LoadOrStore 到 nil 并在类型断言处 panic。
				// expiredAt 重新计算：等待期间消耗的时间不应从新锁有效期里扣。
				if l.locks.CompareAndSwap(key, info, &lockInfo{
					value:     value,
					expiredAt: time.Now().Add(expiration),
				}) {
					return value, nil
				}
				continue
			}

			// 锁未过期，等待一段时间后重试
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}
	}
}

// Unlock 释放本地锁
func (l *LocalLock) Unlock(ctx context.Context, key, value string) error {
	if actual, ok := l.locks.Load(key); ok {
		info := actual.(*lockInfo)
		info.mutex.RLock()
		match := info.value == value
		info.mutex.RUnlock()
		if match {
			// 只删除自己校验过的那个 lockInfo：校验到删除之间锁可能过期并被
			// 其他 goroutine CAS 换新，无条件 Delete 会误删新持有者的锁
			l.locks.CompareAndDelete(key, info)
			return nil
		}
		return fmt.Errorf("lock value mismatch")
	}
	return fmt.Errorf("lock not found")
}

// TryLock 尝试获取本地锁（非阻塞）
func (l *LocalLock) TryLock(ctx context.Context, key string, expiration time.Duration) (string, bool, error) {
	value := generateLockValue()
	expiredAt := time.Now().Add(expiration)

	if actual, loaded := l.locks.LoadOrStore(key, &lockInfo{
		value:     value,
		expiredAt: expiredAt,
	}); !loaded {
		return value, true, nil
	} else {
		// 锁已存在，检查是否过期
		info := actual.(*lockInfo)
		info.mutex.RLock()
		expired := time.Now().After(info.expiredAt)
		info.mutex.RUnlock()

		if expired {
			// 锁已过期，尝试删除并重新获取
			if l.locks.CompareAndSwap(key, info, &lockInfo{
				value:     value,
				expiredAt: expiredAt,
			}) {
				return value, true, nil
			}
		}

		return "", false, nil
	}
}

// LockWithRetry 带重试的获取本地锁
func (l *LocalLock) LockWithRetry(ctx context.Context, key string, expiration time.Duration, retryInterval time.Duration, maxRetries int) (string, error) {
	for i := 0; i <= maxRetries; i++ {
		value, acquired, err := l.TryLock(ctx, key, expiration)
		if err != nil {
			return "", err
		}

		if acquired {
			return value, nil
		}

		if i < maxRetries {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(retryInterval):
				// 继续重试
			}
		}
	}

	return "", fmt.Errorf("failed to acquire lock after %d retries", maxRetries)
}

// cleanupExpiredLocks 清理过期的锁
// 监听 stopCh 通道，被关闭时优雅退出，避免 goroutine 泄漏
func (l *LocalLock) cleanupExpiredLocks() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopCh:
			return
		case <-ticker.C:
			now := time.Now()
			l.locks.Range(func(key, value interface{}) bool {
				info := value.(*lockInfo)
				info.mutex.RLock()
				expired := now.After(info.expiredAt)
				info.mutex.RUnlock()

				if expired {
					// CompareAndDelete 防止误删：检查到删除之间锁可能已被
					// 其他 goroutine 换成新锁
					l.locks.CompareAndDelete(key, info)
				}
				return true
			})
		}
	}
}

// generateLockValue 生成锁的值
func generateLockValue() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// DefaultKeyLock 默认键锁实例（本地模式）
var DefaultKeyLock = NewLocalLock()
