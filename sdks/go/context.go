package absurd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type taskContextKey struct{}

// TaskContext carries metadata about the currently running task and also acts
// as a context.Context so it can be passed directly into normal Go APIs.
type TaskContext struct {
	context.Context

	client       *Client
	queueName    string
	taskID       string
	runID        string
	taskName     string
	attempt      int
	claimTimeout time.Duration

	headersRaw json.RawMessage
	wakeEvent  string
	eventRaw   json.RawMessage

	mu              sync.Mutex
	headersCache    map[string]any
	checkpointCache map[string]json.RawMessage
	stepNameCounter map[string]int
}

func newTaskContext(parent context.Context, client *Client, queueName string, task claimedTask, claimTimeout time.Duration) (*TaskContext, error) {
	taskCtx := &TaskContext{
		client:          client,
		queueName:       queueName,
		taskID:          task.TaskID,
		runID:           task.RunID,
		taskName:        task.TaskName,
		attempt:         task.Attempt,
		claimTimeout:    claimTimeout,
		headersRaw:      cloneRawJSON(task.HeadersRaw),
		wakeEvent:       task.WakeEvent,
		eventRaw:        cloneRawJSON(task.EventRaw),
		checkpointCache: make(map[string]json.RawMessage),
		stepNameCounter: make(map[string]int),
	}
	taskCtx.Context = context.WithValue(parent, taskContextKey{}, taskCtx)

	rows, err := client.db.QueryContext(taskCtx, `SELECT checkpoint_name, state
       FROM absurd.get_task_checkpoint_states($1, $2, $3)`, queueName, task.TaskID, task.RunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var checkpointName string
		var state []byte
		if err := rows.Scan(&checkpointName, &state); err != nil {
			return nil, err
		}
		taskCtx.checkpointCache[checkpointName] = normalizeRawJSON(state)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return taskCtx, nil
}

func (t *TaskContext) TaskID() string    { return t.taskID }
func (t *TaskContext) RunID() string     { return t.runID }
func (t *TaskContext) TaskName() string  { return t.taskName }
func (t *TaskContext) QueueName() string { return t.queueName }
func (t *TaskContext) Attempt() int      { return t.attempt }

func (t *TaskContext) Headers() map[string]any {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.headersCache != nil {
		return cloneMap(t.headersCache)
	}
	if len(t.headersRaw) == 0 || string(t.headersRaw) == "null" {
		t.headersCache = map[string]any{}
		return map[string]any{}
	}
	var headers map[string]any
	if err := json.Unmarshal(t.headersRaw, &headers); err != nil {
		t.headersCache = map[string]any{}
		return map[string]any{}
	}
	t.headersCache = headers
	return cloneMap(headers)
}

// TaskFromContext returns the active task context if one is present.
func TaskFromContext(ctx context.Context) (*TaskContext, bool) {
	if ctx == nil {
		return nil, false
	}
	taskCtx, ok := ctx.Value(taskContextKey{}).(*TaskContext)
	return taskCtx, ok
}

// MustTaskContext returns the active task context or panics.
func MustTaskContext(ctx context.Context) *TaskContext {
	taskCtx, ok := TaskFromContext(ctx)
	if !ok {
		panic(ErrNoTaskContext)
	}
	return taskCtx
}

func requireTaskContext(ctx context.Context) (*TaskContext, error) {
	taskCtx, ok := TaskFromContext(ctx)
	if !ok {
		return nil, ErrNoTaskContext
	}
	return taskCtx, nil
}

func (t *TaskContext) nextCheckpointName(name string) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	count := t.stepNameCounter[name] + 1
	t.stepNameCounter[name] = count
	if count == 1 {
		return name
	}
	return fmt.Sprintf("%s#%d", name, count)
}

func (t *TaskContext) lookupCheckpoint(ctx context.Context, checkpointName string) (json.RawMessage, bool, error) {
	t.mu.Lock()
	cached, ok := t.checkpointCache[checkpointName]
	t.mu.Unlock()
	if ok {
		return cloneRawJSON(cached), true, nil
	}

	row := t.client.db.QueryRowContext(ctx, `SELECT state
       FROM absurd.get_task_checkpoint_state($1, $2, $3)`, t.queueName, t.taskID, checkpointName)
	var state []byte
	if err := row.Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	raw := normalizeRawJSON(state)
	t.mu.Lock()
	t.checkpointCache[checkpointName] = cloneRawJSON(raw)
	t.mu.Unlock()
	return raw, true, nil
}

func (t *TaskContext) persistCheckpoint(ctx context.Context, checkpointName string, value any) error {
	raw, err := marshalJSON(value)
	if err != nil {
		return err
	}
	_, err = t.client.db.ExecContext(ctx, `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)`,
		t.queueName,
		t.taskID,
		checkpointName,
		string(raw),
		t.runID,
		durationSecondsOrDefault(t.claimTimeout, defaultClaimTimeout),
	)
	if err != nil {
		return mapTaskStateError(err)
	}
	t.mu.Lock()
	t.checkpointCache[checkpointName] = cloneRawJSON(raw)
	t.mu.Unlock()
	return nil
}

func (t *TaskContext) scheduleRun(ctx context.Context, wakeAt time.Time) error {
	_, err := t.client.db.ExecContext(ctx, `SELECT absurd.schedule_run($1, $2, $3)`, t.queueName, t.runID, wakeAt)
	return err
}

// AwaitTaskResult waits for another task to reach a terminal state.
func (t *TaskContext) AwaitTaskResult(ctx context.Context, queueName, taskID string, options ...AwaitTaskResultOptions) (TaskResultSnapshot, error) {
	opts := first(options)
	validatedQueue, err := validateQueueName(queueName)
	if err != nil {
		return TaskResultSnapshot{}, err
	}
	if validatedQueue == t.queueName {
		return TaskResultSnapshot{}, fmt.Errorf("TaskContext.AwaitTaskResult cannot wait on tasks in the same queue because this can deadlock workers. Spawn the child in a different queue")
	}
	heartbeatInterval := t.claimTimeout / 2
	if heartbeatInterval < minHeartbeatInterval {
		heartbeatInterval = minHeartbeatInterval
	}
	nextHeartbeatAt := time.Now().Add(heartbeatInterval)
	return awaitTaskResultWithBackoff(ctx, func(ctx context.Context) (*TaskResultSnapshot, error) {
		return fetchTaskResultSnapshot(ctx, t.client.db, validatedQueue, taskID)
	}, taskID, opts.Timeout, func() error {
		if time.Now().Before(nextHeartbeatAt) {
			return nil
		}
		nextHeartbeatAt = time.Now().Add(heartbeatInterval)
		return Heartbeat(ctx, 0)
	})
}

// Step runs an idempotent step whose result is checkpointed in Postgres.
func Step[T any](ctx context.Context, name string, fn func(context.Context) (T, error)) (T, error) {
	handle, err := BeginStep[T](ctx, name)
	if err != nil {
		return zero[T](), err
	}
	if handle.Done {
		return handle.State, nil
	}
	value, err := fn(ctx)
	if err != nil {
		return zero[T](), err
	}
	return handle.Complete(ctx, value)
}

// Do is Step for functions that do not return a value.
func Do(ctx context.Context, name string, fn func(context.Context) error) error {
	_, err := Step(ctx, name, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
	return err
}

// BeginStep starts a decomposed step and returns whether its state already exists.
func BeginStep[T any](ctx context.Context, name string) (StepHandle[T], error) {
	task, err := requireTaskContext(ctx)
	if err != nil {
		return zero[StepHandle[T]](), err
	}
	checkpointName := task.nextCheckpointName(name)
	raw, found, err := task.lookupCheckpoint(ctx, checkpointName)
	if err != nil {
		return zero[StepHandle[T]](), err
	}
	if !found {
		return StepHandle[T]{
			Name:           name,
			CheckpointName: checkpointName,
			Done:           false,
		}, nil
	}
	var value T
	if err := unmarshalJSON(raw, &value); err != nil {
		return zero[StepHandle[T]](), err
	}
	return StepHandle[T]{
		Name:           name,
		CheckpointName: checkpointName,
		Done:           true,
		State:          value,
	}, nil
}

// SleepFor suspends the task until the duration elapses.
func SleepFor(ctx context.Context, stepName string, d time.Duration) error {
	return SleepUntil(ctx, stepName, time.Now().UTC().Add(d))
}

// SleepUntil suspends the task until the given time.
func SleepUntil(ctx context.Context, stepName string, wakeAt time.Time) error {
	task, err := requireTaskContext(ctx)
	if err != nil {
		return err
	}
	checkpointName := task.nextCheckpointName(stepName)
	raw, found, err := task.lookupCheckpoint(ctx, checkpointName)
	if err != nil {
		return err
	}
	actualWakeAt := wakeAt
	if found {
		var wakeAtString string
		if err := unmarshalJSON(raw, &wakeAtString); err != nil {
			return err
		}
		if wakeAtString != "" {
			parsed, err := time.Parse(time.RFC3339Nano, wakeAtString)
			if err != nil {
				return err
			}
			actualWakeAt = parsed
		}
	} else {
		if err := task.persistCheckpoint(ctx, checkpointName, wakeAt.UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if time.Now().UTC().Before(actualWakeAt) {
		if err := task.scheduleRun(ctx, actualWakeAt); err != nil {
			return err
		}
		return errSuspend
	}
	return nil
}

// AwaitEvent durably waits for an event and returns its payload.
func AwaitEvent[T any](ctx context.Context, eventName string, options ...AwaitEventOptions) (T, error) {
	opts := first(options)
	task, err := requireTaskContext(ctx)
	if err != nil {
		return zero[T](), err
	}
	stepName := opts.StepName
	if stepName == "" {
		stepName = "$awaitEvent:" + eventName
	}
	checkpointName := task.nextCheckpointName(stepName)
	raw, found, err := task.lookupCheckpoint(ctx, checkpointName)
	if err != nil {
		return zero[T](), err
	}
	if found {
		var value T
		if err := unmarshalJSON(raw, &value); err != nil {
			return zero[T](), err
		}
		return value, nil
	}
	if task.wakeEvent == eventName && len(task.eventRaw) == 0 {
		task.wakeEvent = ""
		task.eventRaw = nil
		return zero[T](), newTimeoutError(`timed out waiting for event %q`, eventName)
	}

	var timeoutSeconds any = nil
	if opts.Timeout > 0 {
		timeoutSeconds = durationSeconds(opts.Timeout)
	}
	row := task.client.db.QueryRowContext(ctx, `SELECT should_suspend, payload
        FROM absurd.await_event($1, $2, $3, $4, $5, $6)`,
		task.queueName,
		task.taskID,
		task.runID,
		checkpointName,
		eventName,
		timeoutSeconds,
	)
	var shouldSuspend bool
	var payload []byte
	if err := row.Scan(&shouldSuspend, &payload); err != nil {
		return zero[T](), mapTaskStateError(err)
	}
	if shouldSuspend {
		return zero[T](), errSuspend
	}
	raw = normalizeRawJSON(payload)
	task.mu.Lock()
	task.checkpointCache[checkpointName] = cloneRawJSON(raw)
	task.eventRaw = nil
	task.mu.Unlock()
	var value T
	if err := unmarshalJSON(raw, &value); err != nil {
		return zero[T](), err
	}
	return value, nil
}

// Heartbeat extends the current run lease.
func Heartbeat(ctx context.Context, d time.Duration) error {
	task, err := requireTaskContext(ctx)
	if err != nil {
		return err
	}
	lease := d
	if lease <= 0 {
		lease = task.claimTimeout
	}
	_, err = task.client.db.ExecContext(ctx, `SELECT absurd.extend_claim($1, $2, $3)`, task.queueName, task.runID, durationSecondsOrDefault(lease, task.claimTimeout))
	if err != nil {
		return mapTaskStateError(err)
	}
	return nil
}

// EmitEvent emits an event on the current task's queue.
func EmitEvent(ctx context.Context, eventName string, payload any) error {
	if eventName == "" {
		return fmt.Errorf("event name must be a non-empty string")
	}
	task, err := requireTaskContext(ctx)
	if err != nil {
		return err
	}
	raw, err := marshalJSON(payload)
	if err != nil {
		return err
	}
	_, err = task.client.db.ExecContext(ctx, `SELECT absurd.emit_event($1, $2, $3)`, task.queueName, eventName, string(raw))
	return err
}
