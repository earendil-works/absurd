package absurd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

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

func (c *Client) QueueName() string {
	return c.queueName
}

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
