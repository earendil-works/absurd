# TypeScript SDK

The TypeScript SDK provides a full-featured client for building durable workflows
with Absurd.  It uses the `pg` (node-postgres) library for database access.

## Installation

```bash
npm install absurd-sdk
```

Then import it:

```typescript
import { Absurd } from 'absurd-sdk';
```

## Creating a Client

```typescript
import { Absurd } from 'absurd-sdk';

// From a connection string (or ABSURD_DATABASE_URL env var)
const app = new Absurd({
  db: 'postgresql://user:pass@localhost:5432/mydb',
  queueName: 'default',
});

// From an existing pg.Pool
import * as pg from 'pg';
const pool = new pg.Pool({ connectionString: '...' });
const app = new Absurd({ db: pool, queueName: 'default' });

// Minimal — uses ABSURD_DATABASE_URL and queue "default"
const app = new Absurd();
```

### `AbsurdOptions`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db` | `pg.Pool \| string` | `ABSURD_DATABASE_URL` or `postgresql://localhost/absurd` | Database connection |
| `queueName` | `string` | `"default"` | Default queue for operations |
| `defaultMaxAttempts` | `number` | `5` | Default retry limit for spawned tasks |
| `log` | `Log` | `console` | Logger (must have `log`, `info`, `warn`, `error`) |
| `hooks` | `AbsurdHooks` | — | Lifecycle hooks for tracing / context propagation |

## Registering Tasks

```typescript
app.registerTask({ name: 'send-email' }, async (params, ctx) => {
  const rendered = await ctx.step('render', async () => {
    return renderTemplate(params.template, params.data);
  });

  await ctx.step('send', async () => {
    return await mailer.send({ to: params.to, html: rendered });
  });
});
```

### `TaskRegistrationOptions`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `name` | `string` | *required* | Task name (must match when spawning) |
| `queue` | `string` | Client queue | Queue this task belongs to |
| `defaultMaxAttempts` | `number` | Client default | Default max attempts |
| `defaultCancellation` | `CancellationPolicy` | — | Default cancellation policy |

## Spawning Tasks

```typescript
const { taskID, runID, attempt, created } = await app.spawn(
  'send-email',
  { to: 'user@example.com', template: 'welcome' },
  {
    maxAttempts: 10,
    retryStrategy: { kind: 'exponential', baseSeconds: 2, factor: 2, maxSeconds: 300 },
    headers: { traceId: '...' },
    idempotencyKey: 'welcome:user-42',
  }
);
```

### `SpawnOptions`

| Option | Type | Description |
|--------|------|-------------|
| `maxAttempts` | `number` | Max retry attempts |
| `retryStrategy` | `RetryStrategy` | Backoff configuration |
| `headers` | `JsonObject` | Metadata attached to the task |
| `queue` | `string` | Target queue (must match registration if registered) |
| `cancellation` | `CancellationPolicy` | Auto-cancellation policy |
| `idempotencyKey` | `string` | Dedup key (existing task returned if key matches) |

### `SpawnResult`

| Field | Type | Description |
|-------|------|-------------|
| `taskID` | `string` | Unique task identifier (UUIDv7) |
| `runID` | `string` | Current run identifier |
| `attempt` | `number` | Attempt number |
| `created` | `boolean` | `false` if an existing task was returned (idempotency) |

## Task Context (`TaskContext`)

The context object passed to every task handler.

### `ctx.taskID`

The unique identifier of the current task.

### `ctx.headers`

Read-only JSON object of headers attached to the task.

### `ctx.step(name, fn)`

Run an idempotent step.  The return value is cached in Postgres — on retries
the cached value is returned without calling `fn` again.

```typescript
const result = await ctx.step('fetch-data', async () => {
  return await fetchFromAPI();
});
```

The return value **must be JSON-serializable**.

### `ctx.sleepFor(stepName, seconds)`

Suspend the task for a duration.

```typescript
await ctx.sleepFor('cooldown', 3600); // sleep 1 hour
```

### `ctx.sleepUntil(stepName, date)`

Suspend the task until an absolute time.

```typescript
await ctx.sleepUntil('deadline', new Date('2025-12-31T00:00:00Z'));
```

### `ctx.awaitEvent(eventName, options?)`

Suspend until a named event is emitted.  Returns the event payload.

```typescript
const payload = await ctx.awaitEvent('order.shipped', {
  stepName: 'wait-for-shipment',  // optional custom checkpoint name
  timeout: 86400,                  // optional timeout in seconds
});
```

Throws `TimeoutError` if the timeout expires before the event arrives.

### `ctx.heartbeat(seconds?)`

Extend the current run's lease.  Useful in long-running steps that don't write
checkpoints frequently.

```typescript
await ctx.heartbeat(300); // extend lease by 5 minutes
```

### `ctx.emitEvent(eventName, payload?)`

Emit an event on the current queue.  First emit per name wins.

```typescript
await ctx.emitEvent('order.completed', { orderId: '42' });
```

## Events

Emit events from outside a task handler:

```typescript
await app.emitEvent('order.shipped', { trackingNumber: 'XYZ' });

// Emit to a specific queue
await app.emitEvent('order.shipped', { trackingNumber: 'XYZ' }, 'orders');
```

## Cancellation

```typescript
await app.cancelTask(taskID);
await app.cancelTask(taskID, 'other-queue');
```

Running tasks detect cancellation at the next `step()`, `heartbeat()`, or
`awaitEvent()` call.

## Retrying Failed Tasks

```typescript
const result = await app.retryTask(taskID, {
  maxAttempts: 5,       // increase attempt limit
  spawnNewTask: false,  // retry in-place (default) or spawn a new task
});
```

## Queue Management

```typescript
await app.createQueue('emails');
await app.dropQueue('emails');
const queues = await app.listQueues(); // ['default', 'emails']
```

## Starting a Worker

```typescript
const worker = await app.startWorker({
  concurrency: 4,        // parallel tasks (default: 1)
  claimTimeout: 120,     // lease duration in seconds (default: 120)
  batchSize: 4,          // tasks to claim per poll (default: concurrency)
  pollInterval: 0.25,    // seconds between idle polls (default: 0.25)
  workerId: 'web-1',     // identifier for tracking (default: hostname:pid)
  fatalOnLeaseTimeout: true, // exit process if task exceeds 2x lease (default: true)
  onError: (err) => console.error(err),
});

// Graceful shutdown
await worker.close();
```

### Single-Batch Processing

For cron-style or serverless workloads, process a single batch without a
long-lived worker:

```typescript
await app.workBatch('worker-1', 120, 10);
```

## Binding to a Connection

Use `bindToConnection()` to run Absurd operations on a specific database
connection (e.g., inside a transaction):

```typescript
const client = await pool.connect();
try {
  const bound = app.bindToConnection(client);
  await bound.spawn('my-task', { key: 'value' });
} finally {
  client.release();
}
```

## Hooks

Hooks let you integrate with tracing, logging, and context propagation systems.

### `beforeSpawn`

Called before every `spawn()`.  Modify options to inject headers:

```typescript
const app = new Absurd({
  hooks: {
    beforeSpawn: (taskName, params, options) => {
      const traceId = getCurrentTraceId();
      return {
        ...options,
        headers: { ...options.headers, traceId },
      };
    },
  },
});
```

### `wrapTaskExecution`

Wraps task handler execution.  Must call `execute()`:

```typescript
const app = new Absurd({
  hooks: {
    wrapTaskExecution: async (ctx, execute) => {
      const traceId = ctx.headers.traceId;
      return await runWithTraceContext(traceId, execute);
    },
  },
});
```

## Error Types

| Error | Description |
|-------|-------------|
| `SuspendTask` | Internal — task suspended (sleep/event). Never visible to user code. |
| `CancelledTask` | Internal — task was cancelled. Never visible to user code. |
| `FailedTask` | Internal — run already failed (e.g., claim expired). |
| `TimeoutError` | Thrown by `awaitEvent()` when the timeout expires. |

## Closing

```typescript
await app.close(); // stops worker, closes pool if owned
```

## Retry Strategies

| Kind | Description |
|------|-------------|
| `"fixed"` | Wait `baseSeconds` between each retry |
| `"exponential"` | Wait `baseSeconds * factor^attempt`, capped at `maxSeconds` |
| `"none"` | No automatic retries |

```typescript
{ kind: 'exponential', baseSeconds: 1, factor: 2, maxSeconds: 300 }
```

## Cancellation Policies

| Field | Type | Description |
|-------|------|-------------|
| `maxDuration` | `number` | Cancel task after N seconds total lifetime |
| `maxDelay` | `number` | Cancel task if no checkpoint written for N seconds |
