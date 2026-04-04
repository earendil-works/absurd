# Absurd SDK for TypeScript

TypeScript SDK for [Absurd](https://github.com/earendil-works/absurd): a PostgreSQL-based durable task execution system.

Absurd is the simplest durable execution workflow system you can think of. It's entirely based on Postgres and nothing else. It's almost as easy to use as a queue, but it handles scheduling and retries, and it does all of that without needing any other services to run in addition to Postgres.

**Warning:** _this is an early experiment and should not be used in production._

## What is Durable Execution?

Durable execution (or durable workflows) is a way to run long-lived, reliable functions that can survive crashes, restarts, and network failures without losing state or duplicating work. Instead of running your logic in memory, a durable execution system decomposes a task into smaller pieces (step functions) and records every step and decision.

## Installation

```bash
npm install absurd-sdk
```

Examples in this README are intended to run directly on modern Node.js with
native TypeScript type stripping. No transpilation step is required.

## Prerequisites

Before using the SDK, you need to initialize Absurd in your PostgreSQL database:

```bash
# One-off usage
uvx absurdctl init -d your-database-name
uvx absurdctl create-queue -d your-database-name default

# Or install it once
uv tool install absurdctl
absurdctl init -d your-database-name
absurdctl create-queue -d your-database-name default
```

See the [absurdctl docs](https://earendil-works.github.io/absurd/tools/absurdctl/) for installation details and
the full CLI reference, including
[`uvx`](https://docs.astral.sh/uv/guides/tools/) usage.

## Quick Start

If you omit `db`, the client uses `ABSURD_DATABASE_URL`, then `PGDATABASE`,
then `postgresql://localhost/absurd`.

```typescript
import { Absurd } from "absurd-sdk";

const app = new Absurd({ db: "postgresql://localhost/mydb" });

// Register a task
app.registerTask({ name: "order-fulfillment" }, async (params, ctx) => {
  // Each step is checkpointed, so if the process crashes, we resume
  // from the last completed step
  const payment = await ctx.step("process-payment", async () => {
    return { paymentId: `pay-${params.orderId}`, amount: params.amount };
  });

  const inventory = await ctx.step("reserve-inventory", async () => {
    return { reservedItems: params.items };
  });

  // Wait for an event - the task suspends until the event arrives
  const shipment = await ctx.awaitEvent(`shipment.packed:${params.orderId}`);

  await ctx.step("send-notification", async () => {
    return { sentTo: params.email, trackingNumber: shipment.trackingNumber };
  });

  return { orderId: params.orderId, payment, inventory, trackingNumber: shipment.trackingNumber };
});

// Start a worker that pulls tasks from Postgres
await app.startWorker();
```

## Spawning Tasks

```typescript
// Spawn a task - it will be executed durably with automatic retries
await app.spawn("order-fulfillment", {
  orderId: "42",
  amount: 9999,
  items: ["widget-1", "gadget-2"],
  email: "customer@example.com",
});
```

## Task Result Snapshots

You can inspect or wait for a task's terminal result:

```typescript
const snapshot = await app.fetchTaskResult(taskID);
// { state: "pending" } | { state: "running" } | { state: "sleeping" }
// { state: "completed", result: ... }
// { state: "failed", failure: ... }
// { state: "cancelled" }

const final = await app.awaitTaskResult(taskID, { timeout: 30 });
```

Inside a task, you can also wait for child tasks durably:

```typescript
const child = await app.spawn("child-task", {}, { queue: "child-workers" });
const childResult = await ctx.awaitTaskResult(child.taskID, {
  queue: "child-workers",
  timeout: 30,
});
```

## Emitting Events

```typescript
// Emit an event that a suspended task might be waiting for
await app.emitEvent("shipment.packed:42", {
  trackingNumber: "TRACK123",
});
```

## Idempotency Keys

Use the task ID to derive idempotency keys for external APIs:

```typescript
const payment = await ctx.step("process-payment", async () => {
  const idempotencyKey = `${ctx.taskID}:payment`;
  return { idempotencyKey, amount: params.amount };
});
```

## Decomposed Steps

If you need explicit before/after control, split `step()` into two calls:

```typescript
const handle = await ctx.beginStep<{ messages: any[] }>("agent-turn");

let messages: any[];
if (handle.done) {
  // `handle.state` is fully typed when done=true.
  messages = handle.state.messages;
} else {
  messages = [{ role: "assistant", content: "hello" }];
  await ctx.completeStep(handle, { messages });
}
```

This is useful when integrating with event-driven loops (for example agent
runtimes) where the checkpoint boundary is not a single inline callback.

## License and Links

- [Examples](https://github.com/earendil-works/absurd/tree/main/sdks/typescript/examples)
- [Issue Tracker](https://github.com/earendil-works/absurd/issues)
- License: [Apache-2.0](https://github.com/earendil-works/absurd/blob/main/LICENSE)
