package absurd

import (
	"errors"
	"fmt"
	"testing"
)

type fakeSQLStateError struct {
	state string
}

func (e *fakeSQLStateError) Error() string    { return "fake sql state error" }
func (e *fakeSQLStateError) SQLState() string { return e.state }

func TestMapTaskStateError(t *testing.T) {
	cancelled := mapTaskStateError(fmt.Errorf("wrapped: %w", &fakeSQLStateError{state: "AB001"}))
	if cancelled != errCancelled {
		t.Fatalf("expected errCancelled, got %v", cancelled)
	}

	failed := mapTaskStateError(fmt.Errorf("wrapped: %w", &fakeSQLStateError{state: "AB002"}))
	if failed != errFailedRun {
		t.Fatalf("expected errFailedRun, got %v", failed)
	}

	lost := mapTaskStateError(fmt.Errorf("wrapped: %w", &fakeSQLStateError{state: "AB004"}))
	var lostClaim *LostClaimError
	if !errors.As(lost, &lostClaim) {
		t.Fatalf("expected LostClaimError, got %v", lost)
	}
	var stateErr *fakeSQLStateError
	if !errors.As(lost, &stateErr) {
		t.Fatalf("expected LostClaimError to preserve its cause, got %v", lost)
	}

	original := fmt.Errorf("wrapped: %w", &fakeSQLStateError{state: "23505"})
	mapped := mapTaskStateError(original)
	if mapped != original {
		t.Fatalf("expected original error to be preserved, got %v", mapped)
	}
}

func TestSwallowTerminalTaskStateError(t *testing.T) {
	if err := swallowTerminalTaskStateError(errCancelled); err != nil {
		t.Fatalf("expected cancelled error to be swallowed, got %v", err)
	}

	if err := swallowTerminalTaskStateError(errFailedRun); err != nil {
		t.Fatalf("expected failed-run error to be swallowed, got %v", err)
	}

	if err := swallowTerminalTaskStateError(&LostClaimError{}); err != nil {
		t.Fatalf("expected lost-claim error to be swallowed, got %v", err)
	}

	original := fmt.Errorf("boom")
	if err := swallowTerminalTaskStateError(original); err != original {
		t.Fatalf("expected original error to be preserved, got %v", err)
	}
}

func TestDefaultWorkerIDsAreUnique(t *testing.T) {
	first := normalizeWorkBatchOptions(WorkBatchOptions{}).WorkerID
	second := normalizeWorkBatchOptions(WorkBatchOptions{}).WorkerID
	if first == "" || second == "" || first == second {
		t.Fatalf("expected unique non-empty worker IDs, got %q and %q", first, second)
	}
}
