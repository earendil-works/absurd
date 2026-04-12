package absurd

import (
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

	original := fmt.Errorf("boom")
	if err := swallowTerminalTaskStateError(original); err != original {
		t.Fatalf("expected original error to be preserved, got %v", err)
	}
}
