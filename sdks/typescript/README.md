# Absurd SDK for TypeScript

TypeScript SDK for [Absurd](https://github.com/earendil-works/absurd): a PostgreSQL-based durable task execution system.

Absurd is the simplest durable execution workflow system you can think of.  It's entirely based on Postgres and nothing else.  It's almost as easy to use as a queue, but it handles scheduling and retries, and it does all of that without needing any other services to run in addition to Postgres.

**Warning:** *this is an early experiment and should not be used in production.*

## What is Durable Execution?

Durable execution (or durable workflows) is a way to run long-lived, reliable functions that can survive crashes, restarts, and network failures without losing state or duplicating work. Instead of running your logic in memory, a durable execution system decomposes a task into smaller pieces (step functions) and records every step and decision.

## Installation

```bash
npm install absurd-sdk
```

## Prerequisites

Before using the SDK, you need to initialize Absurd in your PostgreSQL database:

```bash
# Install absurdctl from https://github.com/earendil-works/absurd/releases
absurdctl init -d your-database-name
absurdctl create-queue -d your-database-name default
```

## Quick Start

```typescript
import { Absurd } from 'absurd-sdk';

const app = new Absurd({
  connectionString: 'postgresql://localhost/mydb'
});

// Register a task
app.registerTask({ name: 'order-fulfillment' }, async (params, ctx) => {
  // Each step is checkpointed, so if the process crashes, we resume
  // from the last completed step
  const payment = await ctx.step('process-payment', async () => {
    return await processPayment(params.amount);
  });

  const inventory = await ctx.step('reserve-inventory', async () => {
    return await reserveItems(params.items);
  });

  // Wait for an event - the task suspends until the event arrives
  const shipment = await ctx.awaitEvent(`shipment.packed:${params.orderId}`);

  await ctx.step('send-notification', async () => {
    return await sendEmail(params.email, shipment);
  });

  return { orderId: payment.id, trackingNumber: shipment.trackingNumber };
});

// Start a worker that pulls tasks from Postgres
await app.startWorker();
```

## Spawning Tasks

```typescript
// Spawn a task - it will be executed durably with automatic retries
await app.spawn('order-fulfillment', {
  orderId: '42',
  amount: 9999,
  items: ['widget-1', 'gadget-2'],
  email: 'customer@example.com'
});
```

## Emitting Events

```typescript
// Emit an event that a suspended task might be waiting for
await app.emitEvent('shipment.packed:42', {
  trackingNumber: 'TRACK123'
});
```

## Idempotency Keys

Use the task ID to derive idempotency keys for external APIs:

```typescript
const payment = await ctx.step('process-payment', async () => {
  const idempotencyKey = `${ctx.taskID}:payment`;
  return await stripe.charges.create({
    amount: params.amount,
    idempotencyKey,
  });
});
```

## Documentation

For more information, examples, and documentation, visit:

- [Main Repository](https://github.com/earendil-works/absurd)
- [Examples](https://github.com/earendil-works/absurd/tree/main/sdks/typescript/examples)
- [Issue Tracker](https://github.com/earendil-works/absurd/issues)

## License

Apache-2.0
