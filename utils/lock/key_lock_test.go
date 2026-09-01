package lock

import (
	"context"
	"testing"
	"time"
)

func TestNewLocalLock(t *testing.T) {
	l := NewLocalLock()
	if l == nil {
		t.Fatal("expected non-nil LocalLock")
	}
}

func TestLocalLock_BasicLockUnlock(t *testing.T) {
	l := NewLocalLock()
	ctx := context.Background()

	value, err := l.Lock(ctx, "test-key", 10*time.Second)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	if value == "" {
		t.Error("Lock returned empty value")
	}

	err = l.Unlock(ctx, "test-key", value)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func TestLocalLock_WrongValueUnlock(t *testing.T) {
	l := NewLocalLock()
	ctx := context.Background()

	_, _ = l.Lock(ctx, "key1", 10*time.Second)
	err := l.Unlock(ctx, "key1", "wrong-value")
	if err == nil {
		t.Error("expected error for wrong value")
	}
}

func TestLocalLock_UnlockUnknownKey(t *testing.T) {
	l := NewLocalLock()
	ctx := context.Background()

	err := l.Unlock(ctx, "unknown-key", "any-value")
	if err == nil {
		t.Error("expected error for unknown key")
	}
}

func TestLocalLock_TryLock(t *testing.T) {
	l := NewLocalLock()
	ctx := context.Background()

	value, ok, err := l.TryLock(ctx, "try-key", 10*time.Second)
	if err != nil {
		t.Fatalf("TryLock failed: %v", err)
	}
	if !ok {
		t.Error("TryLock should succeed on first attempt")
	}

	// Second TryLock on same key should fail
	_, ok, _ = l.TryLock(ctx, "try-key", 10*time.Second)
	if ok {
		t.Error("TryLock should fail when key is already locked")
	}

	// Unlock and try again
	_ = l.Unlock(ctx, "try-key", value)
	_, ok, _ = l.TryLock(ctx, "try-key", 10*time.Second)
	if !ok {
		t.Error("TryLock should succeed after unlock")
	}
}

func TestLocalLock_LockWithRetry(t *testing.T) {
	l := NewLocalLock()
	ctx := context.Background()

	// Lock the key
	value1, _ := l.Lock(ctx, "retry-key", 10*time.Second)

	// Try to acquire with retries - should fail since key is locked
	_, err := l.LockWithRetry(ctx, "retry-key", 10*time.Second, 10*time.Millisecond, 3)
	if err == nil {
		t.Error("expected error when key is locked")
	}

	// Unlock
	_ = l.Unlock(ctx, "retry-key", value1)

	// Now retry should succeed
	value2, err := l.LockWithRetry(ctx, "retry-key", 10*time.Second, 10*time.Millisecond, 3)
	if err != nil {
		t.Fatalf("LockWithRetry failed after unlock: %v", err)
	}
	if value2 == "" {
		t.Error("LockWithRetry returned empty value")
	}
}

func TestLocalLock_ExpiredLock(t *testing.T) {
	l := NewLocalLock()
	ctx := context.Background()

	// Lock with short expiration
	value, _ := l.Lock(ctx, "expire-key", 50*time.Millisecond)

	// Should be locked
	_, ok, _ := l.TryLock(ctx, "expire-key", 10*time.Second)
	if ok {
		t.Error("key should be locked")
	}

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Should be available now
	_, ok, _ = l.TryLock(ctx, "expire-key", 10*time.Second)
	if !ok {
		t.Error("key should be available after expiration")
	}

	// Clean up
	_ = l.Unlock(ctx, "expire-key", value)
}

func TestLocalLock_CancelledContext(t *testing.T) {
	l := NewLocalLock()
	ctx := context.Background()

	// Lock the key
	_, _ = l.Lock(ctx, "cancel-key", 10*time.Second)

	// Create a cancelled context
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	// Lock on already-locked key with cancelled context should fail
	_, err := l.Lock(cancelCtx, "cancel-key", 10*time.Second)
	if err == nil {
		t.Error("expected error with cancelled context")
	}
}

func TestLocalLock_DifferentKeys(t *testing.T) {
	l := NewLocalLock()
	ctx := context.Background()

	v1, err := l.Lock(ctx, "key-a", 10*time.Second)
	if err != nil {
		t.Fatalf("Lock key-a failed: %v", err)
	}
	v2, err := l.Lock(ctx, "key-b", 10*time.Second)
	if err != nil {
		t.Fatalf("Lock key-b failed: %v", err)
	}

	// Both should have different values
	if v1 == v2 {
		t.Error("different keys should have different values")
	}

	// Unlock both
	_ = l.Unlock(ctx, "key-a", v1)
	_ = l.Unlock(ctx, "key-b", v2)
}

func TestDefaultKeyLockIsNotNil(t *testing.T) {
	if DefaultKeyLock == nil {
		t.Error("DefaultKeyLock should not be nil")
	}
}
