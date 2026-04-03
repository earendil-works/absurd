package absurd

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

const maxQueueNameLength = 57

// Options configure a client.
type Options struct {
	DB                 *sql.DB
	DatabaseURL        string
	QueueName          string
	DefaultMaxAttempts int
	Logger             Logger
}

// Logger is the minimal logging interface used by the SDK.
type Logger interface {
	Printf(format string, args ...any)
}

// TaskHandler is a typed task handler.
type TaskHandler[P, R any] func(ctx context.Context, params P) (R, error)

// TaskOptions configure a registered task.
type TaskOptions struct {
	QueueName          string
	DefaultMaxAttempts int
}

// SpawnOptions configure spawning.
type SpawnOptions struct {
	QueueName   string
	MaxAttempts int
}

// AwaitEventOptions configure AwaitEvent.
type AwaitEventOptions struct {
	StepName string
	Timeout  time.Duration
}

// AwaitTaskResultOptions configure AwaitTaskResult.
type AwaitTaskResultOptions struct {
	QueueName string
	Timeout   time.Duration
}

// WorkBatchOptions configure a single worker batch.
type WorkBatchOptions struct {
	WorkerID     string
	ClaimTimeout time.Duration
	BatchSize    int
}

// WorkerOptions configure a continuous worker.
type WorkerOptions struct {
	WorkerID     string
	ClaimTimeout time.Duration
	BatchSize    int
	Concurrency  int
	PollInterval time.Duration
	OnError      func(error)
}

// SpawnResult identifies a spawned task and its current run.
type SpawnResult struct {
	TaskID  string
	RunID   string
	Attempt int
	Created bool
}

// TaskResultState describes the current task state.
type TaskResultState string

const (
	TaskPending   TaskResultState = "pending"
	TaskRunning   TaskResultState = "running"
	TaskSleeping  TaskResultState = "sleeping"
	TaskCompleted TaskResultState = "completed"
	TaskFailed    TaskResultState = "failed"
	TaskCancelled TaskResultState = "cancelled"
)

// TaskResultSnapshot is the raw task result view.
type TaskResultSnapshot struct {
	State   TaskResultState
	Result  json.RawMessage
	Failure json.RawMessage
}

func (s TaskResultSnapshot) DecodeResult(dst any) error {
	if len(s.Result) == 0 {
		return nil
	}
	return json.Unmarshal(s.Result, dst)
}

func (s TaskResultSnapshot) DecodeFailure(dst any) error {
	if len(s.Failure) == 0 {
		return nil
	}
	return json.Unmarshal(s.Failure, dst)
}

func (s TaskResultSnapshot) IsTerminal() bool {
	switch s.State {
	case TaskCompleted, TaskFailed, TaskCancelled:
		return true
	default:
		return false
	}
}

// StepHandle is returned by BeginStep.
type StepHandle[T any] struct {
	Name           string
	CheckpointName string
	Done           bool
	State          T
}

func (h StepHandle[T]) Complete(ctx context.Context, value T) (T, error) {
	if h.Done {
		return h.State, nil
	}
	task, err := requireTaskContext(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	if err := task.persistCheckpoint(ctx, h.CheckpointName, value); err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}
