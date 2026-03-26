# Python SDK

The Python SDK provides both **synchronous** (`Absurd`) and **asynchronous**
(`AsyncAbsurd`) clients for building durable workflows.  It uses
[psycopg](https://www.psycopg.org/) (v3) for database access.

## Installation

The SDK is currently used from source:

```bash
cd sdks/python
uv sync   # or: pip install -e .
```

## Creating a Client

### Synchronous

```python
from absurd_sdk import Absurd

# From a connection string (or ABSURD_DATABASE_URL env var)
app = Absurd("postgresql://user:pass@localhost:5432/mydb", queue_name="default")

# From an existing psycopg Connection
from psycopg import Connection
conn = Connection.connect("postgresql://...", autocommit=True)
app = Absurd(conn, queue_name="default")

# Minimal — uses ABSURD_DATABASE_URL and queue "default"
app = Absurd()
```

### Asynchronous

```python
from absurd_sdk import AsyncAbsurd

app = AsyncAbsurd("postgresql://user:pass@localhost:5432/mydb", queue_name="default")
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `conn_or_url` | `Connection \| str \| None` | `ABSURD_DATABASE_URL` or `postgresql://localhost/absurd` | Database connection or URL |
| `queue_name` | `str` | `"default"` | Default queue for operations |
| `default_max_attempts` | `int` | `5` | Default retry limit |
| `hooks` | `AbsurdHooks` | `None` | Lifecycle hooks |

## Registering Tasks

Use the `@register_task` decorator:

```python
@app.register_task("send-email")
def send_email(params, ctx):
    rendered = ctx.step("render", lambda: render_template(params["template"]))

    ctx.step("send", lambda: mailer.send(to=params["to"], html=rendered))
```

### Decorator Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | *required* | Task name |
| `queue` | `str` | Client queue | Queue this task belongs to |
| `default_max_attempts` | `int` | Client default | Default max attempts |
| `default_cancellation` | `CancellationPolicy` | `None` | Default cancellation policy |

### Async Tasks

```python
app = AsyncAbsurd("postgresql://...")

@app.register_task("send-email")
async def send_email(params, ctx):
    rendered = await ctx.step("render", render_template_async)
    await ctx.step("send", lambda: mailer.send(to=params["to"], html=rendered))
```

## Spawning Tasks

```python
result = app.spawn(
    "send-email",
    {"to": "user@example.com", "template": "welcome"},
    max_attempts=10,
    retry_strategy={"kind": "exponential", "base_seconds": 2, "factor": 2, "max_seconds": 300},
    headers={"trace_id": "..."},
    idempotency_key="welcome:user-42",
)
print(result["task_id"], result["run_id"])
```

### Spawn Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `task_name` | `str` | Name of the task to spawn |
| `params` | `Any` | JSON-serializable parameters |
| `max_attempts` | `int` | Max retry attempts |
| `retry_strategy` | `RetryStrategy` | Backoff configuration |
| `headers` | `dict` | Metadata attached to the task |
| `queue` | `str` | Target queue (must match registration if registered) |
| `cancellation` | `CancellationPolicy` | Auto-cancellation policy |
| `idempotency_key` | `str` | Dedup key |

### `SpawnResult`

A `TypedDict` with fields: `task_id`, `run_id`, `attempt`.

## Task Context

### Synchronous (`TaskContext`)

#### `ctx.task_id`

The unique identifier of the current task.

#### `ctx.headers`

Read-only mapping of headers attached to the task.

#### `ctx.step(name, fn)`

Run an idempotent step.  `fn` is a zero-argument callable whose return value
is cached.

```python
result = ctx.step("fetch-data", lambda: fetch_from_api())
```

#### `ctx.run_step`

Decorator form of `step()`.  The decorated function is immediately called and
replaced with its return value:

```python
@ctx.run_step
def payment():
    return stripe.charges.create(amount=params["amount"])

# `payment` is now the return value, not the function
print(payment)
```

With a custom name:

```python
@ctx.run_step("process-payment")
def payment():
    return stripe.charges.create(amount=params["amount"])
```

#### `ctx.sleep_for(step_name, seconds)`

Suspend the task for a duration.

```python
ctx.sleep_for("cooldown", 3600)
```

#### `ctx.sleep_until(step_name, wake_at)`

Suspend until an absolute time.  Accepts a `datetime`, or a UNIX timestamp
(`int`/`float`).

```python
from datetime import datetime, timezone
ctx.sleep_until("deadline", datetime(2025, 12, 31, tzinfo=timezone.utc))
```

#### `ctx.await_event(event_name, step_name=None, timeout=None)`

Suspend until a named event is emitted.  Returns the event payload.

```python
payload = ctx.await_event("order.shipped", timeout=86400)
```

Raises `TimeoutError` if the timeout expires.

#### `ctx.heartbeat(seconds=None)`

Extend the current run's lease.

```python
ctx.heartbeat(300)
```

#### `ctx.emit_event(event_name, payload=None)`

Emit an event on the current queue.

```python
ctx.emit_event("order.completed", {"order_id": "42"})
```

### Asynchronous (`AsyncTaskContext`)

The async context has the same methods, all `async`:

```python
result = await ctx.step("fetch-data", fetch_from_api_async)
await ctx.sleep_for("cooldown", 3600)
payload = await ctx.await_event("order.shipped")
await ctx.heartbeat(300)
await ctx.emit_event("order.completed", {"order_id": "42"})
```

## Events

```python
app.emit_event("order.shipped", {"tracking": "XYZ"})
app.emit_event("order.shipped", {"tracking": "XYZ"}, queue_name="orders")
```

## Cancellation

```python
app.cancel_task(task_id)
app.cancel_task(task_id, queue_name="other-queue")
```

## Retrying Failed Tasks

```python
result = app.retry_task(task_id, max_attempts=5, spawn_new=False)
```

Returns a `RetryTaskResult` with `task_id`, `run_id`, `attempt`, `created`.

## Queue Management

```python
app.create_queue("emails")
app.drop_queue("emails")
queues = app.list_queues()  # ["default", "emails"]
```

## Starting a Worker

### Synchronous (Blocking)

```python
app.start_worker(
    worker_id="web-1",
    claim_timeout=120,
    concurrency=1,
    poll_interval=0.25,
)
```

Call `app.stop_worker()` from a signal handler for graceful shutdown.

### Asynchronous

```python
await app.start_worker(
    worker_id="web-1",
    claim_timeout=120,
    concurrency=4,
    poll_interval=0.25,
)
```

### Single-Batch Processing

```python
app.work_batch(worker_id="cron-1", claim_timeout=120, batch_size=10)
```

## Context Variable

Access the current task context from anywhere in the call stack:

```python
from absurd_sdk import get_current_context

def helper():
    ctx = get_current_context()
    if ctx is not None:
        ctx.heartbeat(60)
```

Works for both sync and async contexts.

## Switching Between Sync and Async

```python
# Sync → Async
async_app = app.make_async()

# Async → Sync
sync_app = await async_app.make_sync()
```

Both share the same connection string but create independent connections.

## Hooks

### `before_spawn`

```python
def inject_trace(task_name, params, options):
    options["headers"] = {**(options.get("headers") or {}), "trace_id": get_trace_id()}
    return options

app = Absurd(hooks={"before_spawn": inject_trace})
```

### `wrap_task_execution`

```python
def with_tracing(ctx, execute):
    trace_id = ctx.headers.get("trace_id")
    with start_trace(trace_id):
        return execute()

app = Absurd(hooks={"wrap_task_execution": with_tracing})
```

Both hooks also accept async callables when used with `AsyncAbsurd`.

## Error Types

| Error | Description |
|-------|-------------|
| `SuspendTask` | Internal — task suspended. Never visible to user code. |
| `CancelledTask` | Internal — task was cancelled. |
| `FailedTask` | Internal — run already failed. |
| `TimeoutError` | Thrown by `await_event()` when timeout expires. |

## Closing

```python
# Sync
app.close()

# Async
await app.close()
```

Stops the worker and closes the connection if it was created by the client.
