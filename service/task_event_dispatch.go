// This file provides the unified task-event dispatcher used across the
// service package. Centralising the dispatch logic solves two problems:
//
//  1. Goroutines that capture the transaction ctx may execute after the
//     transaction has been committed (or rolled back). The captured ctx can
//     be cancelled by then, leaking cancellation into the listener. We
//     therefore strip cancellation from the ctx before handing it to the
//     listener via context.WithoutCancel (Go 1.21+).
//
//  2. Listener panics must not crash the BPM main flow. Every dispatch goes
//     through a single recover+log helper so behaviour is consistent.

package service

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
)

// DispatchTaskEvent fires a TaskEvent at the given listener in a new goroutine.
//
// Contract:
//   - If listener is nil, the call is a no-op (no goroutine started).
//   - The goroutine uses a ctx derived from the caller via context.WithoutCancel,
//     so a subsequent transaction commit/rollback (which cancels the original
//     ctx) does NOT cancel the listener. The ctx still carries request-scoped
//     values (tenantID, userID, traceID) that listeners may read.
//   - listener panics are recovered and logged via logrus; they never propagate.
//   - evt.EventID is filled here when empty so every dispatched event carries
//     a unique id for upper-layer idempotency.
//   - evt is a value type, so it is safe to capture by pointer in the goroutine
//     even if the caller mutates its own copy afterwards.
//
// Callers MUST pre-fill evt.Timestamp if they want a deterministic value; this
// helper does not override it. When Timestamp is zero the helper stamps it at
// dispatch time so listeners always see a non-zero timestamp.
func DispatchTaskEvent(listener TaskEventListener, evt TaskEvent, ctx context.Context) {
	if listener == nil {
		return
	}
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now()
	}
	if evt.EventID == "" {
		evt.EventID = getDefaultIDGenerator().GenerateID()
	}
	// Strip cancellation chain: the original ctx may be the transaction ctx,
	// which gets cancelled on commit/rollback. We keep the values (tenant,
	// user, trace) but lose the cancellation signal so a late listener fire
	// does not observe ctx.Done().
	dispatchSafe("task", string(evt.Type), evt.EventID, ctx, func(safeCtx context.Context) {
		listener(safeCtx, evt)
	})
}

// DispatchCCEvent 异步派发 CC 事件，语义与 DispatchTaskEvent 一致
// （goroutine 异步 + 取消剥离 + panic recover）。
func DispatchCCEvent(listener CCTaskCreatedListener, evt CCEvent, ctx context.Context) {
	if listener == nil {
		return
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	dispatchSafe("cc", "ccCreated", evt.TaskID, ctx, func(safeCtx context.Context) {
		listener(safeCtx, evt)
	})
}

// dispatchSafe 在新 goroutine 中执行 fire：ctx 剥离取消链，panic recover。
func dispatchSafe(kind, eventType, refID string, ctx context.Context, fire func(context.Context)) {
	safeCtx := context.WithoutCancel(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logrus.WithFields(logrus.Fields{
					"eventKind": kind,
					"eventType": eventType,
					"refID":     refID,
				}).Errorf("event listener panicked: %v", r)
			}
		}()
		fire(safeCtx)
	}()
}

// uniqueStrings returns a new slice containing the input strings in their
// original order, with duplicates removed. Used for ToUsers deduplication.
func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
