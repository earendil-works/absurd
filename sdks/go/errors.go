package absurd

import (
	"errors"
	"fmt"
)

var (
	ErrNoTaskContext = errors.New("absurd: no task context in context.Context")
	ErrTaskNotFound  = errors.New("absurd: task not found")
)

var (
	errSuspend   = errors.New("absurd: task suspended")
	errCancelled = errors.New("absurd: task cancelled")
	errFailedRun = errors.New("absurd: task already failed")
)

// TimeoutError indicates a durable wait timed out.
type TimeoutError struct {
	message string
}

func (e *TimeoutError) Error() string {
	return e.message
}

func (e *TimeoutError) Timeout() bool {
	return true
}

func newTimeoutError(format string, args ...any) error {
	return &TimeoutError{message: fmt.Sprintf(format, args...)}
}
