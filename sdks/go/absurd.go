package absurd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/lib/pq"
)

const maxQueueNameLength = 57

// Errors
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

func (e *TimeoutError) Error() string { return e.message }
func (e *TimeoutError) Timeout() bool { return true }

func newTimeoutError(format string, args ...any) error {
	return &TimeoutError{message: fmt.Sprintf(format, args...)}
}

// Logger is the minimal logging interface used by the SDK.
type Logger interface {
	Printf(format string, args ...any)
}

// Options configure a client.
type Options struct {
	DB                 *sql.DB
	DatabaseURL        string
	QueueName          string
	DefaultMaxAttempts int
	Logger             Logger
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

// TaskDefinition is a typed task registration.
type TaskDefinition[P, R any] struct {
	name    string
	handler TaskHandler[P, R]
	options TaskOptions
}

// Task creates a typed task definition.
func Task[P, R any](name string, handler TaskHandler[P, R], options ...TaskOptions) TaskDefinition[P, R] {
	var opts TaskOptions
	if len(options) > 0 {
		opts = options[0]
	}
	return TaskDefinition[P, R]{name: name, handler: handler, options: opts}
}

func (t TaskDefinition[P, R]) Name() string { return t.name }

// Spawn uses the typed task name and parameter type.
func (t TaskDefinition[P, R]) Spawn(ctx context.Context, c *Client, params P, options ...SpawnOptions) (SpawnResult, error) {
	return c.Spawn(ctx, t.name, params, options...)
}

type taskRegistration interface {
	buildRegistered(defaultQueue string) (registeredTask, error)
}

func (t TaskDefinition[P, R]) buildRegistered(defaultQueue string) (registeredTask, error) {
	if t.name == "" {
		return registeredTask{}, fmt.Errorf("task registration requires a name")
	}
	if t.options.DefaultMaxAttempts < 0 {
		return registeredTask{}, fmt.Errorf("default max attempts must be at least 1")
	}
	queue := defaultQueue
	if t.options.QueueName != "" {
		queue = t.options.QueueName
	}
	queue, err := validateQueueName(queue)
	if err != nil {
		return registeredTask{}, err
	}
	return registeredTask{
		name:                  t.name,
		queueName:             queue,
		hasDefaultMaxAttempts: t.options.DefaultMaxAttempts > 0,
		defaultMaxAttempts:    t.options.DefaultMaxAttempts,
		handler: func(ctx context.Context, paramsRaw json.RawMessage) (any, error) {
			var params P
			if err := unmarshalJSON(paramsRaw, &params); err != nil {
				return nil, fmt.Errorf("unmarshal params for task %q: %w", t.name, err)
			}
			return t.handler(ctx, params)
		},
	}, nil
}

type registeredTask struct {
	name                  string
	queueName             string
	hasDefaultMaxAttempts bool
	defaultMaxAttempts    int
	handler               func(ctx context.Context, paramsRaw json.RawMessage) (any, error)
}

// Client is the Go Absurd SDK client.
type Client struct {
	db                 *sql.DB
	ownedDB            bool
	queueName          string
	defaultMaxAttempts int
	logger             Logger
	registry           map[string]registeredTask
}

// New constructs a client.
func New(options Options) (*Client, error) {
	queueName := options.QueueName
	if queueName == "" {
		queueName = "default"
	}
	validatedQueue, err := validateQueueName(queueName)
	if err != nil {
		return nil, err
	}
	defaultMaxAttempts := options.DefaultMaxAttempts
	if defaultMaxAttempts == 0 {
		defaultMaxAttempts = 5
	}
	if defaultMaxAttempts < 1 {
		return nil, fmt.Errorf("default max attempts must be at least 1")
	}
	logger := options.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	var db *sql.DB
	ownedDB := false
	if options.DB != nil {
		db = options.DB
	} else {
		dsn := options.DatabaseURL
		if dsn == "" {
			dsn = os.Getenv("ABSURD_DATABASE_URL")
		}
		if dsn == "" {
			dsn = "postgresql://localhost/absurd"
		}
		sqlDB, err := sql.Open("postgres", dsn)
		if err != nil {
			return nil, err
		}
		db = sqlDB
		ownedDB = true
	}

	return &Client{
		db:                 db,
		ownedDB:            ownedDB,
		queueName:          validatedQueue,
		defaultMaxAttempts: defaultMaxAttempts,
		logger:             logger,
		registry:           make(map[string]registeredTask),
	}, nil
}

func (c *Client) Close() error {
	if c.ownedDB && c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *Client) QueueName() string { return c.queueName }

func (c *Client) Register(task taskRegistration) error {
	registered, err := task.buildRegistered(c.queueName)
	if err != nil {
		return err
	}
	c.registry[registered.name] = registered
	return nil
}

func (c *Client) MustRegister(task taskRegistration) {
	if err := c.Register(task); err != nil {
		panic(err)
	}
}

func (c *Client) CreateQueue(ctx context.Context, queueName ...string) error {
	queue := c.queueName
	if len(queueName) > 0 && queueName[0] != "" {
		queue = queueName[0]
	}
	validated, err := validateQueueName(queue)
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, `SELECT absurd.create_queue($1)`, validated)
	return err
}

func (c *Client) DropQueue(ctx context.Context, queueName ...string) error {
	queue := c.queueName
	if len(queueName) > 0 && queueName[0] != "" {
		queue = queueName[0]
	}
	validated, err := validateQueueName(queue)
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, `SELECT absurd.drop_queue($1)`, validated)
	return err
}

func (c *Client) EmitEvent(ctx context.Context, eventName string, payload any, queueName ...string) error {
	if eventName == "" {
		return fmt.Errorf("event name must be a non-empty string")
	}
	queue := c.queueName
	if len(queueName) > 0 && queueName[0] != "" {
		queue = queueName[0]
	}
	validated, err := validateQueueName(queue)
	if err != nil {
		return err
	}
	raw, err := marshalJSON(payload)
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, `SELECT absurd.emit_event($1, $2, $3)`, validated, eventName, string(raw))
	return err
}

func (c *Client) Spawn(ctx context.Context, taskName string, params any, options ...SpawnOptions) (SpawnResult, error) {
	var opts SpawnOptions
	if len(options) > 0 {
		opts = options[0]
	}
	queue := c.queueName
	maxAttempts := c.defaultMaxAttempts

	if registration, ok := c.registry[taskName]; ok {
		queue = registration.queueName
		if opts.QueueName != "" && opts.QueueName != queue {
			return SpawnResult{}, fmt.Errorf("task %q is registered for queue %q but spawn requested queue %q", taskName, queue, opts.QueueName)
		}
		if registration.hasDefaultMaxAttempts {
			maxAttempts = registration.defaultMaxAttempts
		}
	} else if opts.QueueName != "" {
		queue = opts.QueueName
	}

	if opts.MaxAttempts > 0 {
		maxAttempts = opts.MaxAttempts
	}
	validatedQueue, err := validateQueueName(queue)
	if err != nil {
		return SpawnResult{}, err
	}
	if maxAttempts < 1 {
		return SpawnResult{}, fmt.Errorf("max attempts must be at least 1")
	}

	paramsRaw, err := marshalJSON(params)
	if err != nil {
		return SpawnResult{}, err
	}
	spawnOptsRaw, err := marshalJSON(map[string]any{
		"max_attempts": maxAttempts,
	})
	if err != nil {
		return SpawnResult{}, err
	}

	row := c.db.QueryRowContext(ctx, `SELECT task_id, run_id, attempt, created
       FROM absurd.spawn_task($1, $2, $3, $4)`, validatedQueue, taskName, string(paramsRaw), string(spawnOptsRaw))
	var result SpawnResult
	if err := row.Scan(&result.TaskID, &result.RunID, &result.Attempt, &result.Created); err != nil {
		return SpawnResult{}, err
	}
	return result, nil
}

func (c *Client) FetchTaskResult(ctx context.Context, taskID string, options ...AwaitTaskResultOptions) (*TaskResultSnapshot, error) {
	queue := c.queueName
	if len(options) > 0 && options[0].QueueName != "" {
		queue = options[0].QueueName
	}
	validatedQueue, err := validateQueueName(queue)
	if err != nil {
		return nil, err
	}
	return fetchTaskResultSnapshot(ctx, c.db, validatedQueue, taskID)
}

func (c *Client) AwaitTaskResult(ctx context.Context, taskID string, options ...AwaitTaskResultOptions) (TaskResultSnapshot, error) {
	var opts AwaitTaskResultOptions
	if len(options) > 0 {
		opts = options[0]
	}
	queue := c.queueName
	if opts.QueueName != "" {
		queue = opts.QueueName
	}
	validatedQueue, err := validateQueueName(queue)
	if err != nil {
		return TaskResultSnapshot{}, err
	}
	return awaitTaskResultWithBackoff(ctx, func(ctx context.Context) (*TaskResultSnapshot, error) {
		return fetchTaskResultSnapshot(ctx, c.db, validatedQueue, taskID)
	}, taskID, opts.Timeout)
}

// Internal helpers

type claimedTask struct {
	RunID      string
	TaskID     string
	TaskName   string
	Attempt    int
	ParamsRaw  json.RawMessage
	HeadersRaw json.RawMessage
	WakeEvent  string
	EventRaw   json.RawMessage
}

func fetchTaskResultSnapshot(ctx context.Context, db *sql.DB, queueName string, taskID string) (*TaskResultSnapshot, error) {
	row := db.QueryRowContext(ctx, `SELECT state, result, failure_reason
     FROM absurd.get_task_result($1, $2)`, queueName, taskID)
	var state string
	var result []byte
	var failure []byte
	if err := row.Scan(&state, &result, &failure); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &TaskResultSnapshot{
		State:   TaskResultState(state),
		Result:  cloneRawJSON(result),
		Failure: cloneRawJSON(failure),
	}, nil
}

func awaitTaskResultWithBackoff(ctx context.Context, fetch func(context.Context) (*TaskResultSnapshot, error), taskID string, timeout time.Duration) (TaskResultSnapshot, error) {
	startedAt := time.Now()
	delay := 50 * time.Millisecond
	for {
		snapshot, err := fetch(ctx)
		if err != nil {
			return TaskResultSnapshot{}, err
		}
		if snapshot == nil {
			return TaskResultSnapshot{}, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
		}
		if snapshot.IsTerminal() {
			return *snapshot, nil
		}
		if timeout > 0 && time.Since(startedAt) >= timeout {
			return TaskResultSnapshot{}, newTimeoutError(`timed out waiting for task %q`, taskID)
		}
		waitFor := delay
		if timeout > 0 {
			remaining := timeout - time.Since(startedAt)
			if remaining <= 0 {
				return TaskResultSnapshot{}, newTimeoutError(`timed out waiting for task %q`, taskID)
			}
			if waitFor > remaining {
				waitFor = remaining
			}
		}
		select {
		case <-ctx.Done():
			return TaskResultSnapshot{}, ctx.Err()
		case <-time.After(waitFor):
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}

func completeTaskRun(ctx context.Context, db *sql.DB, queueName string, runID string, result any) error {
	raw, err := marshalJSON(result)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `SELECT absurd.complete_run($1, $2, $3)`, queueName, runID, string(raw))
	return err
}

func failTaskRun(ctx context.Context, db *sql.DB, queueName string, runID string, err error) error {
	serialized, marshalErr := marshalJSON(map[string]any{
		"name":    fmt.Sprintf("%T", err),
		"message": err.Error(),
	})
	if marshalErr != nil {
		return marshalErr
	}
	_, execErr := db.ExecContext(ctx, `SELECT absurd.fail_run($1, $2, $3, $4)`, queueName, runID, string(serialized), nil)
	return execErr
}

// Utility functions

func validateQueueName(queueName string) (string, error) {
	if queueName == "" {
		return "", fmt.Errorf("queue name must be provided")
	}
	if len([]byte(queueName)) > maxQueueNameLength {
		return "", fmt.Errorf("queue name %q is too long (max %d bytes)", queueName, maxQueueNameLength)
	}
	return queueName, nil
}

func marshalJSON(value any) (json.RawMessage, error) {
	if value == nil {
		return json.RawMessage("null"), nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func unmarshalJSON(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		raw = json.RawMessage("null")
	}
	return json.Unmarshal(raw, dst)
}

func normalizeRawJSON(raw []byte) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("null")
	}
	return cloneRawJSON(raw)
}

func cloneRawJSON(raw []byte) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	cloned := make([]byte, len(raw))
	copy(cloned, raw)
	return json.RawMessage(cloned)
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func durationSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(math.Ceil(d.Seconds()))
}

func durationSecondsOrDefault(d time.Duration, fallback time.Duration) int {
	if d <= 0 {
		d = fallback
	}
	return durationSeconds(d)
}

func mapTaskStateError(err error) error {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		switch string(pqErr.Code) {
		case "AB001":
			return errCancelled
		case "AB002":
			return errFailedRun
		}
	}
	return err
}
