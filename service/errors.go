package service

import "errors"

// Sentinel errors that consumers (such as gflow) can match with errors.Is.
//
// Wrap these with %w when adding context, for example:
//
//	fmt.Errorf("reject: %w", ErrPermissionDenied)
//
// Additional sentinels are declared in other files:
//   - ErrInstanceNotFound        (instance_lock.go) — IsUserError 识别
//   - ErrInstanceTerminal        (instance_lock.go) — IsUserError 识别
//   - ErrAuthenticationRequired  (calling_mode.go) — IsUserError 识别
//   - ErrUnsupportedForkTopology (fork_aware_resume.go) — IsUserError 识别
//   - ErrProcessDefinitionNotFound (instance_lock.go) — IsUserError 不识别，
//     消费方映射 404 需单独 errors.Is
//   - ErrInstanceLockTimeout     (instance_lock.go) — 服务端可重试错误，
//     IsUserError 不识别，消费方建议映射 HTTP 503/429
var (
	// ErrPermissionDenied — caller lacks required permission for the operation.
	// Map to HTTP 403 in the consumer.
	ErrPermissionDenied = errors.New("permission denied")

	// ErrValidation — request failed semantic validation (empty field, invalid state).
	// Map to HTTP 400.
	ErrValidation = errors.New("validation error")

	// ErrConflict — concurrent modification or duplicate-resource conflict.
	// Map to HTTP 409.
	ErrConflict = errors.New("conflict")

	// ErrNotFound — generic resource-not-found. Use ErrInstanceNotFound for
	// workflow instances. Map to HTTP 404.
	ErrNotFound = errors.New("not found")

	// ErrTaskAlreadyCompleted — Complete called on a task that is already Completed.
	ErrTaskAlreadyCompleted = errors.New("task already completed")

	// ErrTaskNotClaimable — Claim called on a task in a non-claimable state.
	ErrTaskNotClaimable = errors.New("task not claimable in current state")

	// ErrTaskTerminated — Complete called on a task that has been terminated
	// (or-sign sibling completion, countersign threshold reached, claim race,
	// instance teardown). Completing a terminated task would resurrect it in
	// history and re-trigger flow advancement. Map to HTTP 400 like
	// ErrTaskAlreadyCompleted.
	ErrTaskTerminated = errors.New("task is terminated")

	// ErrCountersignRule — countersign approval rule could not be parsed.
	ErrCountersignRule = errors.New("invalid countersign rule")

	// ErrNoSubTasks — countersign/vote parent has no sub tasks left (all
	// reduced via reduce-sign). Callers must not treat DAO or rule-parse
	// errors as this condition. Map to HTTP 400.
	ErrNoSubTasks = errors.New("no sub tasks")

	// ErrForceResumeActiveBranches — ForceResumeInstance called on an instance
	// whose fork branches still have non-completed tasks. ForceResume is meant
	// for the "last approve silently failed and the join never fired" scenario
	// where all branches ARE completed. If branches are still active, the
	// instance is not actually stuck — approve/reject those tasks normally.
	// Map to HTTP 409 (conflict between caller assumption and instance state).
	ErrForceResumeActiveBranches = errors.New("force resume blocked: branches still active")
)

// IsUserError reports whether err represents a client-caused condition
// (bad input, missing permission, conflict, not-found) versus a server-side
// or transient failure. Consumers use this to pick HTTP 4xx vs 5xx.
//
// The check uses errors.Is, so callers may freely wrap the sentinels with
// additional context via fmt.Errorf("...: %w", ErrX) and IsUserError still
// recognises the underlying kind.
func IsUserError(err error) bool {
	switch {
	case errors.Is(err, ErrPermissionDenied),
		errors.Is(err, ErrValidation),
		errors.Is(err, ErrConflict),
		errors.Is(err, ErrNotFound),
		errors.Is(err, ErrTaskAlreadyCompleted),
		errors.Is(err, ErrTaskNotClaimable),
		errors.Is(err, ErrTaskTerminated),
		errors.Is(err, ErrCountersignRule),
		errors.Is(err, ErrAuthenticationRequired),
		errors.Is(err, ErrInstanceNotFound),
		errors.Is(err, ErrInstanceTerminal),
		errors.Is(err, ErrForceResumeActiveBranches),
		errors.Is(err, ErrUnsupportedForkTopology):
		return true
	}
	return false
}
