# Concepts

This page explains what durable execution is and how the core abstractions in
Absurd fit together.

## What is Durable Execution?

Durable execution (or durable workflows) is a way to run long-lived, reliable
functions that can survive crashes, restarts, and network failures without
losing state or duplicating work.

You can think of it as a queue plus a state store that remembers what happened
most recently.  Instead of keeping all progress only in process memory, a
durable execution system breaks work into smaller pieces and persists the
important decisions along the way.

In Absurd, those smaller pieces are **steps**.  Each step acts as a checkpoint.
When a step completes successfully, its result is stored in Postgres.  If the
worker process crashes, the machine is restarted, or the task intentionally
suspends to sleep or wait for an event, Absurd can resume from the last saved
checkpoint instead of starting over.

That makes it possible to build systems for things like payments, order
processing, scheduled jobs, email flows, or AI agents without scattering retry
logic, bookkeeping, and resume state throughout your application code.

The practical promise is not magical perfection, but a much simpler programming
model for making forward progress safely.  Durable execution gives you something
close to "exactly once" behavior for the important logical steps in a workflow,
expressed as normal application code.

## How This Maps to Absurd

Absurd is a **Postgres-native** durable workflow system.  All workflow state
lives in Postgres tables and stored procedures, and workers simply pull tasks,
execute code, and write checkpoints back.

The rest of this page explains the building blocks that make that work:

## Tasks

A **task** is the top-level unit of work.  It has a name (like
`order-fulfillment`), JSON parameters, and is dispatched onto a queue.
Tasks can run for seconds, days, or years.

## Steps (Checkpoints)

Tasks are subdivided into **steps**.  Each step has a unique name and acts as a
checkpoint: once a step completes successfully, its return value is persisted in
Postgres and the step will never execute again — even across process restarts or
retries.

=== "TypeScript"

    ```typescript
    const result = await ctx.step('process-payment', async () => {
      return await stripe.charges.create({ amount: params.amount });
    });
    ```

=== "Python"

    ```python
    def process_payment():
        return stripe.charges.create(amount=params["amount"])


    result = ctx.step("process-payment", process_payment)
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

=== "TypeScript"

    ```typescript
    // In a task handler — suspend until the event arrives
    const shipment = await ctx.awaitEvent('shipment.packed:order-42');

    // From anywhere (another task, an API handler, etc.)
    await app.emitEvent('shipment.packed:order-42', { trackingNumber: 'XYZ' });
    ```

=== "Python"

    ```python
    # In a task handler — suspend until the event arrives
    shipment = ctx.await_event("shipment.packed:order-42")

    # From anywhere (another task, an API handler, etc.)
    app.emit_event("shipment.packed:order-42", {"tracking_number": "XYZ"})
    ```

Events can also have **timeouts**.  If the event doesn't arrive before the
timeout expires, a `TimeoutError` is thrown.

## Sleep

Tasks can **sleep** for a duration or until an absolute time.  Sleep suspends
the task and schedules a future run.

=== "TypeScript"

    ```typescript
    await ctx.sleepFor('wait-for-cooldown', 3600); // 1 hour
    await ctx.sleepUntil('wait-for-deadline', new Date('2025-12-31'));
    ```

=== "Python"

    ```python
    from datetime import datetime, timezone

    ctx.sleep_for("wait-for-cooldown", 3600)  # 1 hour
    ctx.sleep_until("wait-for-deadline", datetime(2025, 12, 31, tzinfo=timezone.utc))
    ```

## Retries

Retries happen at the **task** level, not the step level.  When a task fails:

1. The current run is marked as failed
2. A new run is scheduled with backoff (configurable via retry strategy)
3. The new run replays completed checkpoints and continues from where it left off

This task-level model also explains how worker crashes are handled.  A worker
claims a task for a limited amount of time, and that claim is extended whenever
the task writes a checkpoint.  If the worker crashes or stops making progress
before the claim expires, the task becomes available again and another worker
can pick it up.  That means brief overlapping execution is possible, so tasks
should make observable progress well within the claim timeout and leave ample
headroom.

Retry strategies can be `fixed`, `exponential`, or `none`:

=== "TypeScript"

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

=== "Python"

    ```python
    app.spawn(
        "my-task",
        params,
        max_attempts=10,
        retry_strategy={
            "kind": "exponential",
            "base_seconds": 2,
            "factor": 2,
            "max_seconds": 300,
        },
    )
    ```

## Cancellation

Tasks can be cancelled programmatically or via `absurdctl`.  Running tasks
detect cancellation at the next checkpoint write or heartbeat call and stop
gracefully.

Cancellation policies can also be set at spawn time:

- **`maxDuration`** — cancel the task if it has been alive longer than N seconds
- **`maxDelay`** — cancel the task if no checkpoint has been written for N seconds

## Idempotency Keys

There are two related idempotency patterns in Absurd:

1. **spawn-time deduplication** when creating tasks
2. **external idempotency** when calling systems such as payment APIs inside a step

### Spawn-time deduplication

When spawning tasks, you can provide an **idempotency key**.  If a task with the
same key already exists on the queue, the existing task is returned instead of
creating a new one.

=== "TypeScript"

    ```typescript
    await app.spawn('send-email', { to: 'user@example.com' }, {
      idempotencyKey: 'welcome-email:user-42',
    });
    ```

=== "Python"

    ```python
    app.spawn(
        "send-email",
        {"to": "user@example.com"},
        idempotency_key="welcome-email:user-42",
    )
    ```

This is useful whenever your scheduler or API endpoint might try to enqueue the
same logical work more than once.

### External idempotency inside steps

Completed steps are cached, but ordinary code around steps may still run again
across retries.  If a step calls an external system that supports its own
idempotency keys, derive one from the task identity.

=== "TypeScript"

    ```typescript
    const payment = await ctx.step('process-payment', async () => {
      const idempotencyKey = `${ctx.taskID}:payment`;
      return await stripe.charges.create({
        amount: params.amount,
        idempotencyKey,
      });
    });
    ```

=== "Python"

    ```python
    def process_payment():
        idempotency_key = f"{ctx.task_id}:payment"
        return stripe.charges.create(
            amount=params["amount"],
            idempotency_key=idempotency_key,
        )


    payment = ctx.step("process-payment", process_payment)
    ```

The important thing is that the key should come from something stable, such as
the task ID or a business identifier, not from the current time.

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

## Cleanup and Retention

By default data lives forever.  For retention policy design, direct SQL cleanup,
`absurdctl cleanup`, batching, and cron examples, see
**[Cleanup and Retention](./cleanup.md)**.
