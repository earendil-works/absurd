# Go SDK

The Go SDK provides an idiomatic, typed client for building durable workflows
with Absurd.  It uses the standard library's `database/sql` package and is
driver-agnostic: use any Postgres `database/sql` driver (`pgx`, `pq`, etc.).

The API is intentionally Go-shaped:

- task handlers take `context.Context`
- task definitions are typed with `absurd.Task(...)`
- durable task operations are package-level helpers such as `absurd.Step()` and
  `absurd.AwaitEvent()`
- task metadata is available via `absurd.TaskFromContext()` /
  `absurd.MustTaskContext()`

## Installation

```bash
go get github.com/earendil-works/absurd/sdks/go/absurd@latest
```

Then import it:

```go
import "github.com/earendil-works/absurd/sdks/go/absurd"
```

Before using the SDK, initialize the Absurd schema in Postgres and create at
least one queue.  See **[Database Setup and Migrations](../database.md)**, the
**[Quickstart](../quickstart.md)**, and **[absurdctl](../tools/absurdctl.md)**.

## Creating a Client

### From a connection string

```go
package main

import (
    "log"
    "os"

    "github.com/earendil-works/absurd/sdks/go/absurd"
    _ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
    app, err := absurd.New(absurd.Options{
        DriverName:  "pgx",
        DatabaseURL: os.Getenv("ABSURD_DATABASE_URL"),
        QueueName:   "default",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer app.Close()
}
```

### From an existing `*sql.DB`

```go
package main

import (
    "database/sql"
    "log"
    "os"

    "github.com/earendil-works/absurd/sdks/go/absurd"
    _ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
    db, err := sql.Open("pgx", os.Getenv("PGDATABASE"))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    app, err := absurd.New(absurd.Options{
        DB:        db,
        QueueName: "default",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer app.Close()
}
```

### Minimal

```go
app, err := absurd.New(absurd.Options{})
if err != nil {
    log.Fatal(err)
}
defer app.Close()
```

This uses:

- `Options.DriverName` (default: `"postgres"`)
- `Options.DatabaseURL`, if provided
- otherwise `ABSURD_DATABASE_URL`
- otherwise `PGDATABASE`
- otherwise `postgresql://localhost/absurd`

The default queue is `"default"`.

If you do not pass `Options.DB`, ensure the corresponding
`database/sql` Postgres driver is imported in your program.

### `Options`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `DB` | `*sql.DB` | `nil` | Existing database handle. If set, it wins over `DatabaseURL`. |
| `DriverName` | `string` | `"postgres"` | `database/sql` driver name used with `sql.Open` when `DB` is not provided |
| `DatabaseURL` | `string` | `ABSURD_DATABASE_URL`, then `PGDATABASE`, then `postgresql://localhost/absurd` | Connection string used when `DB` is not provided |
| `QueueName` | `string` | `"default"` | Default queue for spawning and workers |
| `DefaultMaxAttempts` | `int` | `5` | Default retry limit |
| `Logger` | `Logger` | stdlib logger | Logger with a `Printf` method |
| `Hooks` | `Hooks` | zero value | Lifecycle hooks |

### Swapping PostgreSQL drivers (`pgx`, `pq`, others)

The SDK works with any Postgres driver that integrates with `database/sql`.

#### Option A: provide your own `*sql.DB` (recommended for apps with existing DB wiring)

```go
import (
    "database/sql"

    _ "github.com/jackc/pgx/v5/stdlib"
)

db, err := sql.Open("pgx", dsn)
if err != nil {
    return err
}

app, err := absurd.New(absurd.Options{
    DB:        db,
    QueueName: "default",
})
```

Swap to `lib/pq` by changing only driver import + name:

```go
import _ "github.com/lib/pq"

db, err := sql.Open("postgres", dsn)
```

#### Option B: let Absurd open the connection

```go
import _ "github.com/jackc/pgx/v5/stdlib"

app, err := absurd.New(absurd.Options{
    DriverName:  "pgx",
    DatabaseURL: dsn,
    QueueName:   "default",
})
```

For `lib/pq`, set `DriverName: "postgres"` and import `_ "github.com/lib/pq"`.

## Registering Tasks

Define a typed task with `absurd.Task(...)`, then register it with
`app.Register(...)` or `app.MustRegister(...)`.

```go
package main

import (
    "context"
    "log"

    "github.com/earendil-works/absurd/sdks/go/absurd"
    _ "github.com/jackc/pgx/v5/stdlib"
)

type SendEmailParams struct {
    To       string `json:"to"`
    Template string `json:"template"`
}

type SendEmailResult struct {
    Accepted []string `json:"accepted"`
    HTML     string   `json:"html"`
}

var sendEmailTask = absurd.Task(
    "send-email",
    func(
        ctx context.Context,
        params SendEmailParams,
    ) (SendEmailResult, error) {
        rendered, err := absurd.Step(
            ctx,
            "render",
            func(ctx context.Context) (string, error) {
                return "<h1>" + params.Template + "</h1>", nil
            },
        )
        if err != nil {
            return SendEmailResult{}, err
        }

        return absurd.Step(
            ctx,
            "send",
            func(ctx context.Context) (SendEmailResult, error) {
                return SendEmailResult{
                    Accepted: []string{params.To},
                    HTML:     rendered,
                }, nil
            },
        )
    },
)

func main() {
    app, err := absurd.New(absurd.Options{
        QueueName:  "default",
        DriverName: "pgx",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer app.Close()

    app.MustRegister(sendEmailTask)
}
```

### `TaskOptions`

Pass them as the optional third argument to `absurd.Task(...)`:

```go
absurd.Task(
    "send-email",
    handler,
    absurd.TaskOptions{
        QueueName:          "emails",
        DefaultMaxAttempts: 10,
        DefaultCancellation: &absurd.CancellationPolicy{
            MaxDuration: 3600,
        },
    },
)
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `QueueName` | `string` | Client queue | Queue this task belongs to |
| `DefaultMaxAttempts` | `int` | Client default | Default max attempts |
| `DefaultCancellation` | `*CancellationPolicy` | `nil` | Default cancellation policy |

## Spawning Tasks

```go
ctx := context.Background()

result, err := app.Spawn(
    ctx,
    "send-email",
    SendEmailParams{To: "user@example.com", Template: "welcome"},
    absurd.SpawnOptions{
        MaxAttempts: 10,
        RetryStrategy: &absurd.RetryStrategy{
            Kind:        "exponential",
            BaseSeconds: 2,
            Factor:      2,
            MaxSeconds:  300,
        },
        Headers: map[string]any{"trace_id": "..."},
        IdempotencyKey: "welcome:user-42",
    },
)
if err != nil {
    log.Fatal(err)
}

log.Printf(
    "task=%s run=%s created=%v",
    result.TaskID,
    result.RunID,
    result.Created,
)
```

If you already have a typed task definition, you can also spawn through it:

```go
result, err := sendEmailTask.Spawn(
    ctx,
    app,
    SendEmailParams{To: "user@example.com", Template: "welcome"},
)
```

### `SpawnOptions`

| Option | Type | Description |
|--------|------|-------------|
| `QueueName` | `string` | Target queue for unregistered tasks |
| `MaxAttempts` | `int` | Max retry attempts |
| `RetryStrategy` | `*RetryStrategy` | Backoff configuration |
| `Headers` | `map[string]any` | Metadata attached to the task |
| `Cancellation` | `*CancellationPolicy` | Auto-cancellation policy |
| `IdempotencyKey` | `string` | Dedup key |

### `SpawnResult`

| Field | Type | Description |
|-------|------|-------------|
| `TaskID` | `string` | Unique task identifier |
| `RunID` | `string` | Current run identifier |
| `Attempt` | `int` | Attempt number |
| `Created` | `bool` | `false` if an existing task was returned |

## Task Results

Queue-targeted control APIs in the Go SDK take the queue name explicitly.
Usually that means passing `app.QueueName()`.

### `app.FetchTaskResult(ctx, queueName, taskID)`

Returns a `*TaskResultSnapshot`, or `nil` if the task does not exist.

```go
snapshot, err := app.FetchTaskResult(ctx, app.QueueName(), taskID)
if err != nil {
    log.Fatal(err)
}
if snapshot != nil {
    log.Println(snapshot.State)
}
```

### `app.AwaitTaskResult(ctx, queueName, taskID, options...)`

Polls until the task reaches a terminal state (`completed`, `failed`,
`cancelled`). Returns a `TimeoutError` if the timeout is reached.

```go
final, err := app.AwaitTaskResult(
    ctx,
    app.QueueName(),
    taskID,
    absurd.AwaitTaskResultOptions{Timeout: 30 * time.Second},
)
if err != nil {
    log.Fatal(err)
}
```

If the task does not exist, the returned error wraps `absurd.ErrTaskNotFound`.

### `TaskResultSnapshot`

| Field | Type | Description |
|-------|------|-------------|
| `State` | `TaskResultState` | Current task state |
| `Result` | `json.RawMessage` | Final result payload |
| `Failure` | `json.RawMessage` | Failure payload |

Helper methods:

- `snapshot.DecodeResult(&dst)`
- `snapshot.DecodeFailure(&dst)`
- `snapshot.IsTerminal()`

## Task Context and Durable Operations

Inside a task handler, the active task metadata lives on the `context.Context`.
Use `absurd.TaskFromContext()` or `absurd.MustTaskContext()` when you need task
IDs, headers, or cross-queue task waits.

```go
task := absurd.MustTaskContext(ctx)
log.Println(task.TaskID(), task.RunID(), task.Attempt(), task.Headers())
```

### `absurd.Step(ctx, name, fn)`

Run an idempotent step.  The result is checkpointed in Postgres.

```go
result, err := absurd.Step(
    ctx,
    "fetch-data",
    func(ctx context.Context) (map[string]any, error) {
        return map[string]any{"ok": true, "source": "demo"}, nil
    },
)
```

### `absurd.BeginStep(ctx, name)` + `handle.CompleteStep(ctx, value)`

Advanced form of `Step()` when you need to split checkpoint handling across
multiple calls.

```go
handle, err := absurd.BeginStep[map[string]any](ctx, "agent-turn")
if err != nil {
    return err
}

if handle.Done {
    log.Println(handle.State)
} else {
    _, err = handle.CompleteStep(ctx, map[string]any{"message": "hello"})
}
```

`handle.CheckpointName` contains the concrete checkpoint key after automatic
step numbering (`name`, `name#2`, ...).

### `absurd.SleepFor(ctx, stepName, d)` / `absurd.SleepUntil(ctx, stepName, wakeAt)`

Suspend the task for a duration or until an absolute time.

```go
if err := absurd.SleepFor(ctx, "cooldown", time.Hour); err != nil {
    return err
}

if err := absurd.SleepUntil(
    ctx,
    "deadline",
    time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
); err != nil {
    return err
}
```

### `absurd.AwaitEvent(ctx, eventName, options...)`

Suspend until a named event is emitted.  Returns the event payload.

```go
type ShipmentEvent struct {
    TrackingNumber string `json:"tracking_number"`
}

shipment, err := absurd.AwaitEvent[ShipmentEvent](
    ctx,
    "order.shipped",
    absurd.AwaitEventOptions{Timeout: 24 * time.Hour},
)
if err != nil {
    return err
}
log.Println(shipment.TrackingNumber)
```

### `task.AwaitTaskResult(ctx, queueName, taskID, options...)`

Durably wait for another task's terminal result from inside a task handler.
This must target a **different queue** than the current task's queue.

```go
child, err := app.Spawn(
    ctx,
    "child-task",
    map[string]any{},
    absurd.SpawnOptions{QueueName: "child-workers"},
)
if err != nil {
    return err
}

task := absurd.MustTaskContext(ctx)
childResult, err := task.AwaitTaskResult(
    ctx,
    "child-workers",
    child.TaskID,
    absurd.AwaitTaskResultOptions{Timeout: time.Minute},
)
if err != nil {
    return err
}
log.Println(childResult.State)
```

### `absurd.Heartbeat(ctx, d)`

Extend the current run lease.

```go
if err := absurd.Heartbeat(ctx, 5*time.Minute); err != nil {
    return err
}
```

### `absurd.EmitEvent(ctx, eventName, payload)`

Emit an event on the current task's queue.

```go
if err := absurd.EmitEvent(
    ctx,
    "order.completed",
    map[string]any{"order_id": "42"},
); err != nil {
    return err
}
```

## Events

Emit events from outside a task handler:

```go
err := app.EmitEvent(
    context.Background(),
    app.QueueName(),
    "order.shipped",
    map[string]any{"tracking_number": "XYZ"},
)
```

## Cancellation

```go
err := app.CancelTask(ctx, app.QueueName(), taskID)
```

Running tasks notice cancellation at the next durable operation such as a
checkpoint write, event wait, or heartbeat.

## Retrying Failed Tasks

```go
result, err := app.RetryTask(
    ctx,
    app.QueueName(),
    taskID,
    absurd.RetryTaskOptions{
        MaxAttempts: 5,
        SpawnNew:    true,
    },
)
```

## Queue Management

```go
if err := app.CreateQueue(ctx, "emails"); err != nil {
    log.Fatal(err)
}
if err := app.DropQueue(ctx, "emails"); err != nil {
    log.Fatal(err)
}
queues, err := app.ListQueues(ctx)
```

## Running Workers

### Continuous worker

```go
workerCtx, cancel := context.WithCancel(context.Background())
defer cancel()

if err := app.RunWorker(workerCtx, absurd.WorkerOptions{
    WorkerID:     "web-1",
    ClaimTimeout: 2 * time.Minute,
    Concurrency:  4,
    BatchSize:    4,
    PollInterval: 250 * time.Millisecond,
    OnError: func(err error) {
        log.Printf("worker error: %v", err)
    },
}); err != nil && !errors.Is(err, context.Canceled) {
    log.Fatal(err)
}
```

Graceful shutdown is controlled by canceling the context you passed to
`RunWorker()`.

### Single-batch processing

```go
err := app.WorkBatch(ctx, absurd.WorkBatchOptions{
    WorkerID:     "cron-1",
    ClaimTimeout: 2 * time.Minute,
    BatchSize:    10,
})
```

## Hooks

### `BeforeSpawn`

Called before every `Spawn()`.  Use it to inject headers or modify options.

```go
app, err := absurd.New(absurd.Options{
    Hooks: absurd.Hooks{
        BeforeSpawn: func(
            taskName string,
            params any,
            options absurd.SpawnOptions,
        ) (absurd.SpawnOptions, error) {
            if options.Headers == nil {
                options.Headers = map[string]any{}
            }
            options.Headers["trace_id"] = "trace-123"
            return options, nil
        },
    },
})
```

### `WrapTaskExecution`

Wrap task execution for tracing, logging, or context propagation.

```go
app, err := absurd.New(absurd.Options{
    Hooks: absurd.Hooks{
        WrapTaskExecution: func(
            task *absurd.TaskContext,
            execute func() (any, error),
        ) (any, error) {
            traceID := task.Headers()["trace_id"]
            log.Printf("starting task %s trace=%v", task.TaskID(), traceID)
            return execute()
        },
    },
})
```

## Error Types

| Error | Description |
|-------|-------------|
| `absurd.ErrNoTaskContext` | No active task context in the provided `context.Context` |
| `absurd.ErrTaskNotFound` | Task result lookup could not find the task |
| `*absurd.TimeoutError` | Returned by `AwaitEvent()` / `AwaitTaskResult()` on timeout |

`TimeoutError` also implements `Timeout() bool`.

## Closing

```go
defer app.Close()
```

If the client opened its own database handle, `Close()` also closes it.

## Retry Strategies

| Field | Type | Description |
|-------|------|-------------|
| `Kind` | `string` | `"fixed"`, `"exponential"`, or `"none"` |
| `BaseSeconds` | `float64` | Base delay |
| `Factor` | `float64` | Exponential multiplier |
| `MaxSeconds` | `float64` | Maximum retry delay |

```go
&absurd.RetryStrategy{
    Kind:        "exponential",
    BaseSeconds: 1,
    Factor:      2,
    MaxSeconds:  300,
}
```

## Cancellation Policies

| Field | Type | Description |
|-------|------|-------------|
| `MaxDuration` | `int64` | Cancel task after N seconds total lifetime |
| `MaxDelay` | `int64` | Cancel task if no checkpoint is written for N seconds |
