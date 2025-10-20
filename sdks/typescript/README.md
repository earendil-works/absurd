# Absurd SDK

TypeScript SDK for [Absurd](../../) - PostgreSQL-based durable task execution.

## Installation

```bash
npm install absurd-sdk
```

## Quick Start

```typescript
import { Absurd } from "absurd-sdk";
import pg from "pg";

// Option 1: Use existing connection pool
const pool = new pg.Pool({ connectionString: "postgresql://..." });
const absurd = new Absurd(pool, "my-queue");

// Option 2: Pass connection string directly (SDK manages pool)
const absurd = new Absurd("postgresql://localhost/mydb");

// Register a task handler
absurd.registerTask("send-email", async (params, ctx) => {
  const sent = await ctx.step("send-attempt", async () => {
    // This code runs once, result is checkpointed
    await sendEmail(params.to, params.body);
    return true;
  });

  return { success: sent };
});

// Spawn a task
const { task_id, run_id } = await absurd.spawn("send-email", {
  to: "user@example.com",
  body: "Hello!",
});

// Process tasks (runs once)
await absurd.workOnce("worker-1");

// Or start a long-running worker
const shutdown = await absurd.startWorker({
  workerId: "worker-1",
  claimTimeout: 30,
  batchSize: 1,
  pollInterval: 1000,
  onError: (err) => console.error("Worker error:", err),
});

// Cleanup
process.on("SIGTERM", async () => {
  await shutdown();
  await absurd.close();
});
```

## API Reference

### `Absurd`

Main SDK class for task management.

#### Constructor

```typescript
new Absurd(poolOrUrl: pg.Pool | string, queueName?: string)
```

- `poolOrUrl`: Either a `pg.Pool` instance or PostgreSQL connection string
- `queueName`: Queue name (default: `'default'`)

#### Methods

**`registerTask<P, R>(name: string, handler: TaskHandler<P, R>): void`**

Register a task handler function.

**`spawn<P>(taskName: string, params: P, options?: SpawnOptions): Promise<SpawnResult>`**

Spawn a new task execution.

Options:

- `maxAttempts?: number` - Maximum retry attempts
- `retryStrategy?: RetryStrategy` - Retry configuration
- `headers?: JsonObject` - Custom headers

**`workOnce(workerId?: string, claimTimeout?: number, batchSize?: number): Promise<void>`**

Process one batch of tasks and return.

**`startWorker(options?: WorkerOptions): Promise<() => Promise<void>>`**

Start a long-running worker loop. Returns a shutdown function.

Options:

- `workerId?: string` - Worker identifier (default: `'worker'`)
- `claimTimeout?: number` - Claim timeout in seconds (default: `30`)
- `batchSize?: number` - Tasks per batch (default: `1`)
- `pollInterval?: number` - Poll interval in ms (default: `1000`)
- `onError?: (error: Error) => void` - Error handler

**`close(): Promise<void>`**

Close the SDK and clean up resources. If the SDK owns the connection pool, it will be closed.

### `TaskContext`

Execution context passed to task handlers.

#### Methods

**`step<T>(name: string, fn: () => Promise<T>, options?): Promise<T>`**

Execute a checkpointed step. If the step has already completed, returns the cached result.

Options:

- `ephemeral?: boolean` - Mark checkpoint as ephemeral
- `ttlSeconds?: number` - Checkpoint TTL in seconds

**`sleepFor(stepName: string, durationMs: number): Promise<never>`**

Sleep for a duration. The task will be resumed after the delay. Throws `SuspendTask`.

**`awaitEvent(stepName: string, eventName: string, payload?: JsonValue): Promise<never>`**

Wait for an event to be emitted. Throws `SuspendTask`.

**`emitEvent(eventName: string, payload?: JsonValue): Promise<void>`**

Emit an event to wake waiting tasks.

**`complete(result?: any): Promise<void>`**

Mark the task as successfully completed.

**`fail(err: unknown): Promise<void>`**

Mark the task as failed. Retries will be scheduled according to the retry strategy.

## Examples

### Durable Workflow with Retries

```typescript
absurd.registerTask("process-order", async (params, ctx) => {
  // Step 1: Reserve inventory (idempotent via checkpointing)
  const reservation = await ctx.step("reserve-inventory", async () => {
    return await reserveInventory(params.orderId);
  });

  // Step 2: Charge payment with retries
  let charged = false;
  for (let attempt = 0; attempt < 3; attempt++) {
    charged = await ctx.step(`charge-attempt-${attempt}`, async () => {
      try {
        await chargePayment(params.customerId, params.amount);
        return true;
      } catch (err) {
        return false;
      }
    });

    if (charged) break;

    // Wait 30 seconds before retry
    await ctx.sleepFor(`retry-delay-${attempt}`, 30_000);
  }

  if (!charged) {
    throw new Error("Payment failed after 3 attempts");
  }

  // Step 3: Ship order
  const tracking = await ctx.step("ship-order", async () => {
    return await shipOrder(params.orderId);
  });

  return { success: true, tracking };
});
```

### Event-Driven Coordination

```typescript
// Task 1: Wait for approval
absurd.registerTask("wait-for-approval", async (params, ctx) => {
  await ctx.step("request-approval", async () => {
    await sendApprovalRequest(params.userId);
  });

  // Suspend until approval event
  await ctx.awaitEvent("approval-wait", `approval:${params.requestId}`);

  // Continue after event
  return { approved: true };
});

// Task 2: Approve request
absurd.registerTask("approve-request", async (params, ctx) => {
  await ctx.emitEvent(`approval:${params.requestId}`, {
    approvedBy: params.approver,
    timestamp: new Date(),
  });
});
```

### Retry Strategies

```typescript
// Exponential backoff
await absurd.spawn("flaky-task", params, {
  maxAttempts: 5,
  retryStrategy: {
    kind: "exponential",
    baseSeconds: 2,
    factor: 2,
    maxSeconds: 300, // Cap at 5 minutes
  },
});

// Fixed delay
await absurd.spawn("simple-task", params, {
  maxAttempts: 3,
  retryStrategy: {
    kind: "fixed",
    baseSeconds: 10,
  },
});

// No retries
await absurd.spawn("one-shot-task", params, {
  retryStrategy: { kind: "none" },
});
```

## License

ISC
