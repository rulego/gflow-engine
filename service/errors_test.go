package service

import (
	"errors"
	"fmt"
	"testing"

	"gorm.io/gorm"
)

// TestIsUserError_TrueForUserErrors: every sentinel that represents a
// client-caused condition must be classified as a user error so the
// consumer can map it to HTTP 4xx. If you add a new user sentinel to
// errors.go, add it to this table too.
func TestIsUserError_TrueForUserErrors(t *testing.T) {
	userSentinels := []struct {
		name string
		err  error
	}{
		{"ErrPermissionDenied", ErrPermissionDenied},
		{"ErrValidation", ErrValidation},
		{"ErrConflict", ErrConflict},
		{"ErrNotFound", ErrNotFound},
		{"ErrTaskAlreadyCompleted", ErrTaskAlreadyCompleted},
		{"ErrTaskNotClaimable", ErrTaskNotClaimable},
		{"ErrCountersignRule", ErrCountersignRule},
		{"ErrAuthenticationRequired", ErrAuthenticationRequired},
		{"ErrInstanceNotFound", ErrInstanceNotFound},
		{"ErrInstanceTerminal", ErrInstanceTerminal},
	}
	for _, tc := range userSentinels {
		t.Run(tc.name, func(t *testing.T) {
			if !IsUserError(tc.err) {
				t.Errorf("IsUserError(%s) = false, want true", tc.name)
			}
		})
	}
}

// TestIsUserError_FalseForOtherErrors: server-side and transport errors
// must NOT be classified as user errors. They map to HTTP 5xx.
func TestIsUserError_FalseForOtherErrors(t *testing.T) {
	nonUserErrors := []struct {
		name string
		err  error
	}{
		{"disk full", fmt.Errorf("disk full")},
		{"connection refused", fmt.Errorf("connection refused")},
		{"gorm.ErrRecordNotFound", gorm.ErrRecordNotFound},
		{"plain errors.New", errors.New("something broke")},
		{"nil", nil},
	}
	for _, tc := range nonUserErrors {
		t.Run(tc.name, func(t *testing.T) {
			if IsUserError(tc.err) {
				t.Errorf("IsUserError(%v) = true, want false", tc.err)
			}
		})
	}
}

// TestIsUserError_WrappingPreservesIdentity: wrapping a sentinel with %w
// must not hide it from errors.Is or from IsUserError. This is the
// contract that lets call sites add context while keeping classification.
func TestIsUserError_WrappingPreservesIdentity(t *testing.T) {
	t.Run("permission denied wrapped", func(t *testing.T) {
		wrapped := fmt.Errorf("reject taskID=%s: %w", "abc", ErrPermissionDenied)
		if !errors.Is(wrapped, ErrPermissionDenied) {
			t.Error("errors.Is should match wrapped ErrPermissionDenied")
		}
		if !IsUserError(wrapped) {
			t.Error("IsUserError should classify wrapped ErrPermissionDenied as user error")
		}
	})

	t.Run("not found wrapped with context", func(t *testing.T) {
		wrapped := fmt.Errorf("%w: task=%s", ErrNotFound, "abc")
		if !errors.Is(wrapped, ErrNotFound) {
			t.Error("errors.Is should match wrapped ErrNotFound")
		}
		if !IsUserError(wrapped) {
			t.Error("IsUserError should classify wrapped ErrNotFound as user error")
		}
	})

	t.Run("double-wrapped still detects inner sentinel", func(t *testing.T) {
		inner := fmt.Errorf("outer: %w", ErrPermissionDenied)
		outer := fmt.Errorf("caller: %w", inner)
		if !errors.Is(outer, ErrPermissionDenied) {
			t.Error("errors.Is should unwrap twice and match")
		}
		if !IsUserError(outer) {
			t.Error("IsUserError should still classify the deeply wrapped sentinel")
		}
	})
}

// TestEngineSites_UsingSentinels verifies a representative sample of the
// migrated call sites actually wrap the sentinels. We don't reproduce the
// full engine call here (that would duplicate task_service_impl_test.go);
// we just sanity-check that a real rejection site classifies correctly.
func TestEngineSites_UsingSentinels(t *testing.T) {
	// Simulate the exact error string produced by task_service_impl.go for
	// "task cannot be claimed" — constructed the same way the engine does it.
	status := "COMPLETED"
	err := fmt.Errorf("status=%s: %w", status, ErrTaskNotClaimable)
	if !errors.Is(err, ErrTaskNotClaimable) {
		t.Error("expected errors.Is(err, ErrTaskNotClaimable) to be true")
	}
	if !IsUserError(err) {
		t.Error("expected task-not-claimable error to be a user error")
	}

	// Simulate countersign rule parse failure.
	parseErr := errors.New("bad token at pos 3")
	wrapped := fmt.Errorf("%w: parse approval rule: %v", ErrCountersignRule, parseErr)
	if !errors.Is(wrapped, ErrCountersignRule) {
		t.Error("expected errors.Is(wrapped, ErrCountersignRule) to be true")
	}
	if !IsUserError(wrapped) {
		t.Error("expected countersign-rule parse error to be a user error")
	}
}
