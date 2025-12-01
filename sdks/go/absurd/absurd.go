package absurd

import (
	"cmp"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type Absurd struct {
	base BaseConfig

	// [mu] protects [handlers]. RWMutex should be fine, since the vast case of uses should register
	// tasks at the app initialization stage and the not dynamically register tasks during runtime.
	mu       sync.RWMutex
	handlers map[string]registredHandler

	runMu     sync.Mutex
	runCancel context.CancelFunc
}

// registeredHandler groups the task's default options as well as the [taskHandler].
type registredHandler struct {
	queue        string
	maxAttempts  int
	cancellation *CancellationPolicy
	handler      taskHandler
}

func New(opts ...BaseOption) (*Absurd, error) {
	c := defaultBaseConfig
	for _, opt := range opts {
		if err := opt(&c); err != nil {
			return nil, err
		}
	}

	return &Absurd{
		base: c,
	}, nil
}

// -------------------------
// Queue related operations
// -------------------------

func CreateQueue(ctx context.Context, a *Absurd, queue string) error {
	if err := checkQueueName(queue); err != nil {
		return err
	}
	conn, err := a.base.connect(ctx)
	if err != nil {
		return err
	}

	rows, err := conn.Query(ctx, `SELECT absurd.create_queue($1)`, queue)
	if err != nil {
		return err
	}
	rows.Close()

	return rows.Err()
}

func DropQueue(ctx context.Context, a *Absurd, queue string) error {
	if err := checkQueueName(queue); err != nil {
		return err
	}
	conn, err := a.base.connect(ctx)
	if err != nil {
		return err
	}

	rows, err := conn.Query(ctx, `SELECT absurd.drop_queue($1)`, queue)
	if err != nil {
		return err
	}
	rows.Close()

	return rows.Err()
}

func ListQueues(ctx context.Context, a *Absurd) ([]string, error) {
	conn, err := a.base.connect(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := conn.Query(ctx, `SELECT absurd.list_queues()`)
	if err != nil {
		return nil, err
	}

	var queues []string
	for rows.Next() {
		var queue string
		if err := rows.Scan(&queue); err != nil {
			return nil, err
		}

		queues = append(queues, queue)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return queues, nil
}

// -----------------------
// Task related operations
// -----------------------

// NOTE: Maybe this should be called SpawnTask, since we also have [CancelTask].
func Spawn[P any](
	ctx context.Context,
	a *Absurd,
	task string,
	params P,
	opts ...SpawnOption,
) (SpawnResult, error) {
	if err := checkTaskName(task); err != nil {
		return SpawnResult{}, err
	}

	c := defaultSpawnConfig
	for _, opt := range opts {
		if err := opt(&c); err != nil {
			return SpawnResult{}, err
		}
	}

	// If we have a registered handler, make sure that queue names match and extract registered
	// max attempts and cancellation policy.
	var registeredMaxAttempts int
	var registeredCancellation *CancellationPolicy
	var registeredQueue string
	{
		a.mu.RLock()
		registered, ok := a.handlers[task]
		if ok {
			if c.Queue != "" && c.Queue != registered.queue {
				return SpawnResult{}, TaskQueueMismatchError{task, c.Queue, registered.queue}
			}
			registeredMaxAttempts = registered.maxAttempts
			registeredCancellation = registered.cancellation
			registeredQueue = registered.queue
		}
		a.mu.RUnlock()
	}

	// 1. Parse correct optional values.
	queue := cmp.Or(c.Queue, registeredQueue, a.base.Queue)
	if queue == "" {
		// Panic on invariant.
		panic("absurd: default base queue not set")
	}

	maxAttempts := cmp.Or(c.MaxAttempts, registeredMaxAttempts, a.base.MaxAttempts)
	if maxAttempts == 0 {
		// Panic on invariant.
		panic("absurd: default base max attempts not set")
	}
	cancellation := cmp.Or(c.Cancellation, registeredCancellation)
	// TODO(hohmannr): Where else can RetryStrategy come from?
	retryStrategy := cmp.Or(c.RetryStrategy)

	// 2. Spawn task in postgres.
	conn, err := a.base.connect(ctx)
	if err != nil {
		return SpawnResult{}, err
	}
	row := conn.QueryRow(
		ctx,
		`SELECT task_id, run_id, attempt FROM absurd.spawn_task($1, $2, $3, $4)`,
		queue,
		task,
		params,
		SpawnOptions{
			MaxAttempts:   maxAttempts,
			RetryStrategy: retryStrategy.JSONB(),
			Cancellation:  cancellation,
		},
	)

	var res SpawnResult
	if err := row.Scan(&res.TaskID, &res.RunID, &res.Attempt); err != nil {
		// Join errors, since user can then check for `errors.Is(err, ErrFailedToSpawnTask)` easily.
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, pgx.ErrTooManyRows) {
			return SpawnResult{}, errors.Join(err, ErrFailedToSpawnTask)
		}
		// Do not join any network/database/scanning related errors.
		return SpawnResult{}, err
	}

	return res, nil
}

func CancelTask(ctx context.Context, a *Absurd, taskID string, opts ...CancelOption) error {
	if err := checkTaskID(taskID); err != nil {
		return err
	}

	c := defaultCancelConfig
	for _, opt := range opts {
		if err := opt(&c); err != nil {
			return err
		}
	}

	queue := cmp.Or(c.Queue, a.base.Queue)
	if queue == "" {
		// Panic on invariant.
		panic("absurd: default base queue not set")
	}

	conn, err := a.base.connect(ctx)
	if err != nil {
		return err
	}

	rows, err := conn.Query(ctx, `SELECT absurd.cancel_task($1, $2)`, queue, taskID)
	if err != nil {
		return err
	}
	rows.Close()

	return rows.Err()
}

func EmitEvent(
	ctx context.Context,
	a *Absurd,
	eventName string,
	payload any,
	opts ...EmitEventOption,
) error {
	if err := checkEventName(eventName); err != nil {
		return err
	}

	c := defaultEmitEventConfig
	for _, opt := range opts {
		if err := opt(&c); err != nil {
			return err
		}
	}

	// Parse correct optional values.
	queue := cmp.Or(c.Queue, a.base.Queue)
	if queue == "" {
		// Panic on invariant.
		panic("absurd: default base queue not set")
	}

	// Emit event to postgres.
	conn, err := a.base.connect(ctx)
	if err != nil {
		return err
	}

	rows, err := conn.Query(ctx, `SELECT absurd.emit_event($1, $2, $3)`, queue, eventName, payload)
	if err != nil {
		return err
	}
	rows.Close()

	return rows.Err()
}

// -------------------------
// Worker related operations
// -------------------------

// ClaimTasks claims up to batchSize tasks from the queue for processing.
func ClaimTasks(
	ctx context.Context,
	a *Absurd,
	batchSize int,
	claimTimeout time.Duration,
	workerID string,
) ([]ClaimedTask, error) {
	if batchSize < 1 {
		return nil, ErrInvalidBatchSize
	}
	claimTimeout = claimTimeout.Round(time.Second)
	if claimTimeout < time.Second {
		return nil, ErrInvalidClaimTimeout
	}

	queue := a.base.Queue
	if queue == "" {
		panic("absurd: default base queue not set")
	}

	conn, err := a.base.connect(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := conn.Query(
		ctx,
		`SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts,
		        headers, wake_event, event_payload
		 FROM absurd.claim_task($1, $2, $3, $4)`,
		queue,
		workerID,
		int(claimTimeout.Seconds()),
		batchSize,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []ClaimedTask
	for rows.Next() {
		var task ClaimedTask
		if err := rows.Scan(
			&task.RunID,
			&task.TaskID,
			&task.Attempt,
			&task.TaskName,
			&task.Params,
			&task.RetryStrategy,
			&task.MaxAttempts,
			&task.Headers,
			&task.WakeEvent,
			&task.EventPayload,
		); err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tasks, nil
}

// NOTE(hohmannr): This is the equivalent to `startWorker`. I renamed it, but we can always change
// it back.
func Run(ctx context.Context, a *Absurd, opts ...RunOption) error {
	var runningErrs []error

	c := defaultRunConfig
	for _, opt := range opts {
		if err := opt(&c); err != nil {
			return err
		}
	}

	// Initialize runner state and error if already running.
	a.runMu.Lock()
	if a.runCancel != nil {
		a.runMu.Unlock()
		return ErrWorkerAlreadyRunning
	}

	// Create context that can be cancelled via [Close].
	ctx, cancel := context.WithCancel(ctx)
	a.runCancel = cancel
	defer func() {
		a.runMu.Lock()
		a.runCancel = nil
		a.runMu.Unlock()
	}()

	// Main polling loop.
polling:
	for {
		// Block until we waited for poll interval or context was cancelled.
		select {
		case <-ctx.Done():
			break polling
		case <-time.After(c.PollInterval):
		}

		tasks, err := ClaimTasks(ctx, a, c.BatchSize, c.ClaimTimeout, c.WorkerID)
		if err != nil {
			// Distinguish between context errors and normal errors, since when the context was
			// cancelled we need to abort the main loop.
			if isContextError(err) {
				break polling
			}
			// TODO: Add logging or an user defined error callback handler.
			runningErrs = append(runningErrs, err)

			continue
		}

		// NOTE(hohmannr): I think we need to do sequential processing here due to race conditions
		// in the database, if not, we could add go routines here.
		for _, task := range tasks {
			// Check for cancellation or
			err := executeTask(ctx, a, task, c.ClaimTimeout)
			if err != nil {
				// Distinguish between context errors and normal errors, since when the context was
				// cancelled we need to abort the main loop.
				if isContextError(err) {
					break polling
				}

				// TODO: Add logging or an user defined error callback handler.
				runningErrs = append(runningErrs, err)

				continue
			}
		}
	}

	return errors.Join(append(runningErrs, ctx.Err())...)
}

// Close gracefully stops a running worker.
func Close(ctx context.Context, a *Absurd) error {
	// Stop the worker. This is no-op if the worker was not started yet.
	a.runMu.Lock()
	a.runCancel = nil
	a.runMu.Unlock()

	// Close the postgres connection.
	if a.base.Conn != nil {
		a.base.Conn.Close(ctx)
	}

	return nil
}

func executeTask(
	ctx context.Context,
	a *Absurd,
	task ClaimedTask,
	claimTimeout time.Duration,
) error {
	// Look up handler
	a.mu.RLock()
	registered, ok := a.handlers[task.TaskName]
	a.mu.RUnlock()

	// Check if task was registered and has the correct queue set.
	if !ok {
		err := failTaskRun(ctx, a, a.base.Queue, task.RunID, ErrUnknownTask)
		return errors.Join(err, ErrUnknownTask)
	}
	if registered.queue != a.base.Queue {
		mismatchErr := TaskQueueMismatchError{task.TaskName, a.base.Queue, registered.queue}
		err := failTaskRun(ctx, a, a.base.Queue, task.RunID, mismatchErr)
		return errors.Join(err, mismatchErr)
	}

	// Task step processing.
	conn, err := a.base.connect(ctx)
	if err != nil {
		return err
	}
	ictx := newInternalContext(ctx, task, conn, a.base.Queue, claimTimeout)

	result, err := registered.handler.handleAbsurdTask(ictx, task.Params)
	if err != nil {
		return err
	}

	return completeTaskRun(ctx, a, a.base.Queue, task.RunID, result)
}

// completeTaskRun marks a task run as completed.
func completeTaskRun(ctx context.Context, a *Absurd, queue, runID string, result any) error {
	conn, err := a.base.connect(ctx)
	if err != nil {
		return err
	}

	rows, err := conn.Query(ctx, `SELECT absurd.complete_run($1, $2, $3)`, queue, runID, result)
	if err != nil {
		return err
	}
	rows.Close()

	return rows.Err()
}

// failTaskRun marks a task run as failed.
func failTaskRun(ctx context.Context, a *Absurd, queue, runID string, taskErr error) error {
	conn, err := a.base.connect(ctx)
	if err != nil {
		return err
	}

	errorPayload := map[string]string{
		"message": taskErr.Error(),
	}

	rows, err := conn.Query(
		ctx,
		`SELECT absurd.fail_run($1, $2, $3, $4)`,
		queue,
		runID,
		errorPayload,
		nil,
	)
	if err != nil {
		return err
	}
	rows.Close()

	return rows.Err()
}

// NOTE: Handle is equivalent to [registerTask] in other SDKs, it feels more natural in go to use
// the Handler pattern that is widely spread, e.g. as in [http.Handler].
func Handle[P, R any](
	a *Absurd,
	task string,
	handler TaskHandler[P, R],
	opts ...HandleOption,
) error {
	if err := checkTaskName(task); err != nil {
		return err
	}

	c := defaultHandleConfig
	for _, opt := range opts {
		if err := opt(&c); err != nil {
			return err
		}
	}

	queue := cmp.Or(c.Queue, a.base.Queue)
	if queue == "" {
		// Panic on invariant.
		panic("absurd: default base queue not set")
	}
	maxAttempts := cmp.Or(c.MaxAttempts, a.base.MaxAttempts)
	if maxAttempts == 0 {
		// Panic on invariant.
		panic("absurd: default base max attempts not set")
	}

	registered := registredHandler{
		handler:      typeEraseTaskHandler(handler),
		queue:        queue,
		maxAttempts:  maxAttempts,
		cancellation: c.Cancellation,
	}
	a.handle(task, registered)

	return nil
}

// NOTE(hohmannr): Maybe it makes sense to let the user chose to use MustXxx functions, e.g.
// MustHandleTask when they are certain that all config is valid. We will check the config in these
// MustXxx functions and panic on error. This is a footgun, but gives more freedom to the user of
// the SDK.

// handle is the type erased version of [Handle] which registers a [taskHandler] with an [Absurd]
// instance. Registering multiple handlers with the same name results in the last registered task
// handler to be used on task execution.
func (a *Absurd) handle(task string, handler registredHandler) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.handlers[task] = handler
}

// checkQueueName checks if [name] is non-empty, adheres to the queue name charset, and is not too long.
func checkQueueName(name string) error {
	if name == "" {
		return ErrQueueNameEmpty
	}

	if len(name)+2 > 50 {
		return ErrQueueNameTooLong
	}

	const charset = "_0123456789abcdefghijklmnopqrstuvxyzABCDEFGHIJKLMNOPQRSTUVXYZ"
	for _, r := range name {
		if !strings.ContainsRune(charset, r) {
			return ErrQueueNameCharsetInvalid
		}
	}

	return nil
}

// checkTaskName checks if [name] is a non-empty string.
func checkTaskName(name string) error {
	if name == "" {
		return ErrTaskNameEmpty
	}

	// TODO: Add length and charset validation if needed.

	return nil
}

// checkTaskID checks if [id] is a non-empty string.
func checkTaskID(id string) error {
	if id == "" {
		return ErrTaskIDEmpty
	}

	// TODO: Add UUID format validation if needed.

	return nil
}

// checkEventName checks if [name] is a non-empty string.
func checkEventName(name string) error {
	if name == "" {
		return ErrEventNameEmpty
	}

	// TODO: Add length and charset validation if needed.

	return nil
}

// isDeepNil checks if an [any] type is nil or if the underlying type is itself nil.
func isDeepNil(v any) bool {
	// Fast path: interface itself is nil
	if v == nil {
		return true
	}

	// Slow path: Check if the underlying type is nil.
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return rv.IsNil()
	default:
		return false
	}
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
