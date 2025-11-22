package absurd

import (
	"errors"
	"fmt"
)

var (
	ErrQueueNameEmpty          = errors.New("absurd: queue name must be a non-empty string")
	ErrQueueNameTooLong        = errors.New("absurd: queue names must not exceed 48 characters")
	ErrQueueNameCharsetInvalid = errors.New(
		"absurd: queue names must adhere to the charset [_0-9a-zA-Z]",
	)
	ErrNoPostgresConnectionConfigured = errors.New(
		"absurd: no postgres connection or uri was set",
	)
	ErrInvalidMaxAttempts           = errors.New("absurd: max attempts must be greater or equal 1")
	ErrFailedToSpawnTask            = errors.New("absurd: %s")
	ErrTaskNameEmpty                = errors.New("absurd: task name must be a non-empty string")
	ErrTaskIDEmpty                  = errors.New("absurd: task id must be a non-empty string")
	ErrEventNameEmpty               = errors.New("absurd: event name must be a non-empty string")
	ErrInvalidClaimTimeout          = errors.New("absurd: claim timeout must be greater or equal 1")
	ErrInvalidBatchSize             = errors.New("absurd: batch size must be greater or equal 1")
	ErrInvalidPollInterval          = errors.New("absurd: poll interval must be greater or equal 0")
	ErrUnknownTask                  = errors.New("absurd: unknown task")
	ErrWorkerAlreadyRunning         = errors.New("absurd: worker is already running")
	ErrWorkerNotRunning             = errors.New("absurd: worker is not running")
	ErrCheckpointNotFound           = errors.New("absurd: checkpoint was not found")
	ErrCheckpointHasUnexpectedState = errors.New("absurd: checkpoint has an unexpected state")
	ErrInvalidRetryBaseDuration     = errors.New("absurd: retry base duration must be greater 0")
	ErrInvalidRetryMaxDuration      = errors.New(
		"absurd: retry max duration must be greater than base duration",
	)
	ErrInvalidRetryFactor       = errors.New("absurd: retry factor must be greater 1")
	ErrInvalidRetryStrategyKind = errors.New(
		"absurd: retry strategy kind must be one of 'none', 'fixed' or 'exponential'",
	)
)

type TaskQueueMismatchError struct {
	Task            string
	Queue           string
	RegisteredQueue string
}

func (e TaskQueueMismatchError) Error() string {
	return fmt.Sprintf(
		"absurd: registered task '%s' has queue mismatch: expected='%s' provided='%s'",
		e.Task,
		e.RegisteredQueue,
		e.Queue,
	)
}
