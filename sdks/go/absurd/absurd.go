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
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

const (
	maxQueueNameLength        = 57
	defaultQueueName          = "default"
	defaultDriverName         = "postgres"
	defaultClientMaxAttempts  = 5
	defaultClaimTimeout       = 120 * time.Second
	defaultBatchWorkerID      = "worker"
	defaultWorkerPollInterval = 250 * time.Millisecond
	minHeartbeatInterval      = 500 * time.Millisecond
	initialBackoff            = 50 * time.Millisecond
	maxBackoff                = time.Second
)

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
	DriverName         string
	DatabaseURL        string
	QueueName          string
	DefaultMaxAttempts int
	Logger             Logger
	Hooks              Hooks
}

// TaskHandler is a typed task handler.
type TaskHandler[P, R any] func(ctx context.Context, params P) (R, error)

// TaskOptions configure a registered task.
type TaskOptions struct {
	QueueName           string
	DefaultMaxAttempts  int
	DefaultCancellation *CancellationPolicy
}

// RetryStrategy configures task retries.
type RetryStrategy struct {
	Kind        string
	BaseSeconds float64
	Factor      float64
	MaxSeconds  float64
}

// CancellationPolicy configures task cancellation rules.
type CancellationPolicy struct {
	MaxDuration int64
	MaxDelay    int64
}

// SpawnOptions configure spawning.
type SpawnOptions struct {
	QueueName      string
	MaxAttempts    int
	RetryStrategy  *RetryStrategy
	Headers        map[string]any
	Cancellation   *CancellationPolicy
	IdempotencyKey string
}

type QueueStorageMode string

const (
	QueueStorageUnpartitioned QueueStorageMode = "unpartitioned"
	QueueStoragePartitioned   QueueStorageMode = "partitioned"
)

type QueueDetachMode string

const (
	QueueDetachNone  QueueDetachMode = "none"
	QueueDetachEmpty QueueDetachMode = "empty"
)

// QueuePolicyOptions configure queue maintenance policy updates.
type QueuePolicyOptions struct {
	PartitionLookahead string
	PartitionLookback  string
	CleanupTTLSeconds  *int
	CleanupLimit       *int
	DetachMode         QueueDetachMode
	DetachMinAge       string
}

// CreateQueueOptions configure queue creation mode and policy.
type CreateQueueOptions struct {
	StorageMode QueueStorageMode
	QueuePolicyOptions
}

// QueuePolicy is the persisted maintenance policy metadata for a queue.
type QueuePolicy struct {
	QueueName          string
	StorageMode        QueueStorageMode
	PartitionLookahead string
	PartitionLookback  string
	CleanupTTLSeconds  int
	CleanupLimit       int
	DetachMode         QueueDetachMode
	DetachMinAge       string
}

// RetryTaskOptions configure retrying a failed task.
type RetryTaskOptions struct {
	MaxAttempts int
	SpawnNew    bool
}

// BeforeSpawnHook can mutate spawn options before enqueueing a task.
type BeforeSpawnHook func(taskName string, params any, options SpawnOptions) (SpawnOptions, error)

// WrapTaskExecutionHook wraps task execution.
type WrapTaskExecutionHook func(ctx *TaskContext, execute func() (any, error)) (any, error)

// Hooks customize client behavior.
type Hooks struct {
	BeforeSpawn       BeforeSpawnHook
	WrapTaskExecution WrapTaskExecutionHook
}

// AwaitEventOptions configure AwaitEvent.
type AwaitEventOptions struct {
	StepName string
	Timeout  time.Duration
}

// AwaitTaskResultOptions configure AwaitTaskResult.
type AwaitTaskResultOptions struct {
	StepName string
	Timeout  time.Duration
}

// WorkBatchOptions configure a single worker batch.
type WorkBatchOptions struct {
	WorkerID     string
	ClaimTimeout time.Duration
	BatchSize    int
}

// WorkerOptions configure a continuous worker.
type WorkerOptions struct {
	WorkerID            string
	ClaimTimeout        time.Duration
	BatchSize           int
	Concurrency         int
	PollInterval        time.Duration
	OnError             func(error)
	FatalOnLeaseTimeout *bool
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
	State   TaskResultState `json:"state"`
	Result  json.RawMessage `json:"result,omitempty"`
	Failure json.RawMessage `json:"failure,omitempty"`
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

// CompleteStep persists the step result checkpoint and returns the value.
func (h StepHandle[T]) CompleteStep(ctx context.Context, value T) (T, error) {
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
	return TaskDefinition[P, R]{name: name, handler: handler, options: first(options)}
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
		hasDefaultMaxAttempts: t.options.DefaultMaxAttempts != 0,
		defaultMaxAttempts:    t.options.DefaultMaxAttempts,
		defaultCancellation:   cloneCancellationPolicy(t.options.DefaultCancellation),
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
	defaultCancellation   *CancellationPolicy
	handler               func(ctx context.Context, paramsRaw json.RawMessage) (any, error)
}

func cloneCancellationPolicy(policy *CancellationPolicy) *CancellationPolicy {
	if policy == nil {
		return nil
	}
	cloned := *policy
	return &cloned
}

func cloneRetryStrategy(strategy *RetryStrategy) *RetryStrategy {
	if strategy == nil {
		return nil
	}
	cloned := *strategy
	return &cloned
}

func cloneSpawnOptions(opts SpawnOptions) SpawnOptions {
	cloned := opts
	cloned.Headers = cloneMap(opts.Headers)
	cloned.RetryStrategy = cloneRetryStrategy(opts.RetryStrategy)
	cloned.Cancellation = cloneCancellationPolicy(opts.Cancellation)
	return cloned
}

func normalizeSpawnOptions(opts SpawnOptions) map[string]any {
	normalized := make(map[string]any)
	if opts.MaxAttempts != 0 {
		normalized["max_attempts"] = opts.MaxAttempts
	}
	if len(opts.Headers) > 0 {
		normalized["headers"] = opts.Headers
	}
	if opts.RetryStrategy != nil {
		retry := map[string]any{"kind": opts.RetryStrategy.Kind}
		if opts.RetryStrategy.BaseSeconds != 0 {
			retry["base_seconds"] = opts.RetryStrategy.BaseSeconds
		}
		if opts.RetryStrategy.Factor != 0 {
			retry["factor"] = opts.RetryStrategy.Factor
		}
		if opts.RetryStrategy.MaxSeconds != 0 {
			retry["max_seconds"] = opts.RetryStrategy.MaxSeconds
		}
		normalized["retry_strategy"] = retry
	}
	if opts.Cancellation != nil {
		cancellation := map[string]any{}
		if opts.Cancellation.MaxDuration != 0 {
			cancellation["max_duration"] = opts.Cancellation.MaxDuration
		}
		if opts.Cancellation.MaxDelay != 0 {
			cancellation["max_delay"] = opts.Cancellation.MaxDelay
		}
		if len(cancellation) > 0 {
			normalized["cancellation"] = cancellation
		}
	}
	if opts.IdempotencyKey != "" {
		normalized["idempotency_key"] = opts.IdempotencyKey
	}
	return normalized
}

// Client is the Go Absurd SDK client.
type Client struct {
	db                 *sql.DB
	ownedDB            bool
	queueName          string
	defaultMaxAttempts int
	logger             Logger
	hooks              Hooks
	registryMu         sync.RWMutex
	registry           map[string]registeredTask
}

// New constructs a client.
func New(options Options) (*Client, error) {
	queueName := orString(options.QueueName, defaultQueueName)
	validatedQueue, err := validateQueueName(queueName)
	if err != nil {
		return nil, err
	}
	defaultMaxAttempts := options.DefaultMaxAttempts
	if defaultMaxAttempts == 0 {
		defaultMaxAttempts = defaultClientMaxAttempts
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
		driverName := strings.TrimSpace(options.DriverName)
		if driverName == "" {
			driverName = defaultDriverName
		}
		dsn := resolveDatabaseURL(options.DatabaseURL)
		sqlDB, err := sql.Open(driverName, dsn)
		if err != nil {
			if strings.Contains(err.Error(), "sql: unknown driver") {
				return nil, fmt.Errorf("%w (set Options.DriverName and import a database/sql PostgreSQL driver, e.g. github.com/jackc/pgx/v5/stdlib)", err)
			}
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
		hooks:              options.Hooks,
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
	c.registryMu.Lock()
	c.registry[registered.name] = registered
	c.registryMu.Unlock()
	return nil
}

func (c *Client) getRegistration(taskName string) (registeredTask, bool) {
	c.registryMu.RLock()
	registration, ok := c.registry[taskName]
	c.registryMu.RUnlock()
	return registration, ok
}

func (c *Client) MustRegister(task taskRegistration) {
	if err := c.Register(task); err != nil {
		panic(err)
	}
}

func (c *Client) CreateQueue(ctx context.Context, queueName string, options ...CreateQueueOptions) error {
	if strings.TrimSpace(queueName) == "" {
		queueName = c.queueName
	}
	validated, err := validateQueueName(queueName)
	if err != nil {
		return err
	}

	opts := first(options)
	storageMode := string(opts.StorageMode)
	if storageMode == "" {
		storageMode = string(QueueStorageUnpartitioned)
	}
	if storageMode != string(QueueStorageUnpartitioned) && storageMode != string(QueueStoragePartitioned) {
		return fmt.Errorf("invalid queue storage mode: %s", storageMode)
	}

	if storageMode == string(QueueStorageUnpartitioned) {
		if _, err := c.db.ExecContext(ctx, `SELECT absurd.create_queue($1)`, validated); err != nil {
			return err
		}
	} else {
		if _, err := c.db.ExecContext(ctx, `SELECT absurd.create_queue($1, $2)`, validated, storageMode); err != nil {
			return err
		}
	}

	return c.SetQueuePolicy(ctx, validated, opts.QueuePolicyOptions)
}

func (c *Client) SetQueuePolicy(ctx context.Context, queueName string, options QueuePolicyOptions) error {
	if strings.TrimSpace(queueName) == "" {
		queueName = c.queueName
	}
	validated, err := validateQueueName(queueName)
	if err != nil {
		return err
	}

	payload, err := queuePolicyPayload(options)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}

	raw, err := marshalJSON(payload)
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, `SELECT absurd.set_queue_policy($1, $2::jsonb)`, validated, string(raw))
	return err
}

func (c *Client) GetQueuePolicy(ctx context.Context, queueName string) (*QueuePolicy, error) {
	if strings.TrimSpace(queueName) == "" {
		queueName = c.queueName
	}
	validated, err := validateQueueName(queueName)
	if err != nil {
		return nil, err
	}

	row := c.db.QueryRowContext(ctx, `SELECT
      queue_name,
      storage_mode,
      partition_lookahead::text,
      partition_lookback::text,
      cleanup_ttl_seconds,
      cleanup_limit,
      detach_mode,
      detach_min_age::text
    FROM absurd.get_queue_policy($1)`, validated)

	var policy QueuePolicy
	var storageMode string
	var detachMode string
	if err := row.Scan(
		&policy.QueueName,
		&storageMode,
		&policy.PartitionLookahead,
		&policy.PartitionLookback,
		&policy.CleanupTTLSeconds,
		&policy.CleanupLimit,
		&detachMode,
		&policy.DetachMinAge,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	policy.StorageMode = QueueStorageMode(storageMode)
	policy.DetachMode = QueueDetachMode(detachMode)
	return &policy, nil
}

func (c *Client) DropQueue(ctx context.Context, queueName string) error {
	validated, err := validateQueueName(queueName)
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, `SELECT absurd.drop_queue($1)`, validated)
	return err
}

func (c *Client) ListQueues(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT queue_name FROM absurd.list_queues()`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var queues []string
	for rows.Next() {
		var queue string
		if err := rows.Scan(&queue); err != nil {
			return nil, err
		}
		queues = append(queues, queue)
	}
	return queues, rows.Err()
}

func (c *Client) EmitEvent(ctx context.Context, queueName, eventName string, payload any) error {
	if eventName == "" {
		return fmt.Errorf("event name must be a non-empty string")
	}
	validated, err := validateQueueName(queueName)
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
	opts := cloneSpawnOptions(first(options))
	if c.hooks.BeforeSpawn != nil {
		var err error
		opts, err = c.hooks.BeforeSpawn(taskName, params, cloneSpawnOptions(opts))
		if err != nil {
			return SpawnResult{}, err
		}
	}
	validatedQueue, maxAttempts, cancellation, err := c.resolveSpawn(taskName, opts)
	if err != nil {
		return SpawnResult{}, err
	}

	paramsRaw, err := marshalJSON(params)
	if err != nil {
		return SpawnResult{}, err
	}
	spawnOptsRaw, err := marshalJSON(normalizeSpawnOptions(SpawnOptions{
		MaxAttempts:    maxAttempts,
		RetryStrategy:  cloneRetryStrategy(opts.RetryStrategy),
		Headers:        cloneMap(opts.Headers),
		Cancellation:   cancellation,
		IdempotencyKey: opts.IdempotencyKey,
	}))
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

func (c *Client) FetchTaskResult(ctx context.Context, queueName, taskID string) (*TaskResultSnapshot, error) {
	validatedQueue, err := validateQueueName(queueName)
	if err != nil {
		return nil, err
	}
	return fetchTaskResultSnapshot(ctx, c.db, validatedQueue, taskID)
}

func (c *Client) AwaitTaskResult(ctx context.Context, queueName, taskID string, options ...AwaitTaskResultOptions) (TaskResultSnapshot, error) {
	opts := first(options)
	validatedQueue, err := validateQueueName(queueName)
	if err != nil {
		return TaskResultSnapshot{}, err
	}
	return awaitTaskResultWithBackoff(ctx, func(ctx context.Context) (*TaskResultSnapshot, error) {
		return fetchTaskResultSnapshot(ctx, c.db, validatedQueue, taskID)
	}, taskID, opts.Timeout, nil)
}

func (c *Client) RetryTask(ctx context.Context, queueName, taskID string, options ...RetryTaskOptions) (SpawnResult, error) {
	opts := first(options)
	validatedQueue, err := validateQueueName(queueName)
	if err != nil {
		return SpawnResult{}, err
	}
	payload := map[string]any{}
	if opts.MaxAttempts != 0 {
		payload["max_attempts"] = opts.MaxAttempts
	}
	if opts.SpawnNew {
		payload["spawn_new"] = true
	}
	raw, err := marshalJSON(payload)
	if err != nil {
		return SpawnResult{}, err
	}
	row := c.db.QueryRowContext(ctx, `SELECT task_id, run_id, attempt, created
       FROM absurd.retry_task($1, $2, $3)`, validatedQueue, taskID, string(raw))
	var result SpawnResult
	if err := row.Scan(&result.TaskID, &result.RunID, &result.Attempt, &result.Created); err != nil {
		return SpawnResult{}, err
	}
	return result, nil
}

func (c *Client) CancelTask(ctx context.Context, queueName, taskID string) error {
	validatedQueue, err := validateQueueName(queueName)
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, `SELECT absurd.cancel_task($1, $2)`, validatedQueue, taskID)
	return err
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

func awaitTaskResultWithBackoff(ctx context.Context, fetch func(context.Context) (*TaskResultSnapshot, error), taskID string, timeout time.Duration, beforeSleep func() error) (TaskResultSnapshot, error) {
	startedAt := time.Now()
	delay := initialBackoff
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
		if timeout < 0 || (timeout > 0 && time.Since(startedAt) >= timeout) {
			return TaskResultSnapshot{}, newTimeoutError(`timed out waiting for task %q`, taskID)
		}
		if beforeSleep != nil {
			if err := beforeSleep(); err != nil {
				return TaskResultSnapshot{}, err
			}
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
		if delay < maxBackoff {
			delay *= 2
			if delay > maxBackoff {
				delay = maxBackoff
			}
		}
	}
}

func (c *Client) resolveSpawn(taskName string, opts SpawnOptions) (string, int, *CancellationPolicy, error) {
	queue := c.queueName
	maxAttempts := c.defaultMaxAttempts
	var cancellation *CancellationPolicy

	if registration, ok := c.getRegistration(taskName); ok {
		queue = registration.queueName
		if opts.QueueName != "" && opts.QueueName != queue {
			return "", 0, nil, fmt.Errorf("task %q is registered for queue %q but spawn requested queue %q", taskName, queue, opts.QueueName)
		}
		if registration.hasDefaultMaxAttempts {
			maxAttempts = registration.defaultMaxAttempts
		}
		cancellation = cloneCancellationPolicy(registration.defaultCancellation)
	} else {
		queue = orString(opts.QueueName, queue)
	}

	if opts.MaxAttempts != 0 {
		maxAttempts = opts.MaxAttempts
	}
	if opts.Cancellation != nil {
		cancellation = cloneCancellationPolicy(opts.Cancellation)
	}
	validatedQueue, err := validateQueueName(queue)
	if err != nil {
		return "", 0, nil, err
	}
	return validatedQueue, maxAttempts, cancellation, nil
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
	return failTaskRunWithTraceback(ctx, db, queueName, runID, fmt.Sprintf("%T", err), err.Error(), debug.Stack())
}

func failTaskRunWithTraceback(ctx context.Context, db *sql.DB, queueName string, runID string, name string, message string, traceback []byte) error {
	serialized, marshalErr := marshalJSON(map[string]any{
		"name":      name,
		"message":   message,
		"traceback": string(traceback),
	})
	if marshalErr != nil {
		return marshalErr
	}
	_, execErr := db.ExecContext(ctx, `SELECT absurd.fail_run($1, $2, $3, $4)`, queueName, runID, string(serialized), nil)
	return execErr
}

// Utility functions

func validateQueueName(queueName string) (string, error) {
	if strings.TrimSpace(queueName) == "" {
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

func zero[T any]() T {
	var zero T
	return zero
}

func first[T any](values []T) T {
	switch len(values) {
	case 0:
		return zero[T]()
	case 1:
		return values[0]
	default:
		panic(fmt.Sprintf("absurd: expected 0 or 1 optional arguments, got %d", len(values)))
	}
}

func queuePolicyPayload(options QueuePolicyOptions) (map[string]any, error) {
	payload := map[string]any{}
	if options.PartitionLookahead != "" {
		payload["partition_lookahead"] = options.PartitionLookahead
	}
	if options.PartitionLookback != "" {
		payload["partition_lookback"] = options.PartitionLookback
	}
	if options.CleanupTTLSeconds != nil {
		payload["cleanup_ttl_seconds"] = *options.CleanupTTLSeconds
	}
	if options.CleanupLimit != nil {
		payload["cleanup_limit"] = *options.CleanupLimit
	}
	if options.DetachMode != "" {
		if options.DetachMode != QueueDetachNone && options.DetachMode != QueueDetachEmpty {
			return nil, fmt.Errorf("invalid detach mode: %s", options.DetachMode)
		}
		payload["detach_mode"] = string(options.DetachMode)
	}
	if options.DetachMinAge != "" {
		payload["detach_min_age"] = options.DetachMinAge
	}
	return payload, nil
}

func orString(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func positiveOr(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func durationOr(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func dsnFromPGDatabase(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.Contains(value, "://") || strings.Contains(value, "=") {
		return value
	}
	return "dbname=" + value
}

func resolveDatabaseURL(explicit string) string {
	dsn := explicit
	if dsn == "" {
		dsn = os.Getenv("ABSURD_DATABASE_URL")
	}
	if dsn == "" {
		dsn = dsnFromPGDatabase(os.Getenv("PGDATABASE"))
	}
	if dsn == "" {
		dsn = "postgresql://localhost/absurd"
	}
	return dsn
}

func normalizeLeaseDuration(d time.Duration, fallback time.Duration) time.Duration {
	seconds := durationSecondsOrDefault(d, fallback)
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
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
	var stateErr interface{ SQLState() string }
	if errors.As(err, &stateErr) {
		switch stateErr.SQLState() {
		case "AB001":
			return errCancelled
		case "AB002":
			return errFailedRun
		}
	}
	return err
}
