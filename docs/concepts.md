# Concepts

This page explains the core abstractions in Absurd and how they fit together.

## Tasks

A **task** is the top-level unit of work.  It has a name (like
`order-fulfillment`), JSON parameters, and is dispatched onto a queue.
Tasks can run for seconds, days, or years.

## Steps (Checkpoints)

Tasks are subdivided into **steps**.  Each step has a unique name and acts as a
checkpoint: once a step completes successfully, its return value is persisted in
Postgres and the step will never execute again — even across process restarts or
retries.

```typescript
const result = await ctx.step('process-payment', async () => {
  return await stripe.charges.create({ amount: params.amount });
});
```

Code **outside** steps may execute multiple times across retries.  Keep
side-effects inside steps.

## Runs

Every attempt to execute a task creates a **run**.  The first execution is
attempt 1.  If it fails, a new run (attempt 2) is created with backoff.  Runs
share the same checkpoints — completed steps are skipped on subsequent runs.

## Queues

A **queue** is a logical namespace for tasks.  Each queue creates its own set of
Postgres tables:

| Prefix | Purpose |
|--------|---------|
| `t_`   | Tasks — the logical work units |
| `r_`   | Runs — individual execution attempts |
| `c_`   | Checkpoints — persisted step results |
| `e_`   | Events — emitted signals |
| `w_`   | Wait registrations — tasks waiting for events |

Queues let you scale workers independently and isolate different workloads.

## Workers

A **worker** polls a queue for pending tasks, claims them with a time-limited
lease, and executes the registered handler.  The lease (claim timeout) is
automatically extended whenever a checkpoint is written.

If a worker crashes or fails to make progress before the lease expires, the task
becomes available for another worker to pick up.  This means brief overlapping
execution is possible — design your steps to tolerate it.

## Events

Tasks can **await events** by name.  Events are emitted with `emitEvent()` and
carry an optional JSON payload.  Event payloads are immutable: the first emit
for a given name wins, subsequent emits are ignored.

```typescript
// In a task handler — suspend until the event arrives
const shipment = await ctx.awaitEvent('shipment.packed:order-42');

// From anywhere (another task, an API handler, etc.)
await app.emitEvent('shipment.packed:order-42', { trackingNumber: 'XYZ' });
```

Events can also have **timeouts**.  If the event doesn't arrive before the
timeout expires, a `TimeoutError` is thrown.

## Sleep

Tasks can **sleep** for a duration or until an absolute time.  Sleep suspends
the task and schedules a future run.

```typescript
await ctx.sleepFor('wait-for-cooldown', 3600); // 1 hour
await ctx.sleepUntil('wait-for-deadline', new Date('2025-12-31'));
```

## Retries

Retries happen at the **task** level, not the step level.  When a task fails:

1. The current run is marked as failed
2. A new run is scheduled with backoff (configurable via retry strategy)
3. The new run replays completed checkpoints and continues from where it left off

Retry strategies can be `fixed`, `exponential`, or `none`:

```typescript
await app.spawn('my-task', params, {
  maxAttempts: 10,
  retryStrategy: {
    kind: 'exponential',
    baseSeconds: 2,
    factor: 2,
    maxSeconds: 300,
  },
});
```

## Cancellation

Tasks can be cancelled programmatically or via `absurdctl`.  Running tasks
detect cancellation at the next checkpoint write or heartbeat call and stop
gracefully.

Cancellation policies can also be set at spawn time:

- **`maxDuration`** — cancel the task if it has been alive longer than N seconds
- **`maxDelay`** — cancel the task if no checkpoint has been written for N seconds

## Idempotency Keys

When spawning tasks, you can provide an **idempotency key**.  If a task with the
same key already exists on the queue, the existing task is returned instead of
creating a new one.

```typescript
await app.spawn('send-email', { to: 'user@example.com' }, {
  idempotencyKey: 'welcome-email:user-42',
});
```

For a concrete cron scheduler recipe built on this, see
**[Cron Jobs With Deduplication Keys](./patterns/cron.md)**.

## Headers

Tasks can carry **headers** — a JSON object of metadata that travels with the
task.  Headers are useful for propagating context like trace IDs or correlation
IDs.  They are accessible in the task handler via `ctx.headers`.

## Pull-Based Architecture

Absurd is a **pull-based** system.  Workers pull tasks from Postgres as they
have capacity.  There is no push mechanism and no coordinator service.  This
keeps the system simple and avoids load-management complexity.

## Cleanup

By default data lives forever.  Use `absurdctl cleanup` or the stored functions
`absurd.cleanup_tasks` and `absurd.cleanup_events` to delete old completed,
failed, or cancelled tasks:

```bash
absurdctl cleanup default 7   # delete data older than 7 days
```
