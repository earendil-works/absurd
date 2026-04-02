<div style="text-align: center" align="center">
  <img src="images/logo.jpg" width="350" alt="Une photo d'un éléphant avec le titre : « Ceci n'est pas un éléphant »">
</div>

# Absurd

Absurd is a Postgres-native durable workflow system.  It moves the complexity of
durable execution into the database layer via stored procedures, keeping SDKs
lightweight and language-agnostic.  The core principle is to handle tasks that
may run for minutes, days, or years without losing state.

All you need is a Postgres database and the single
[`absurd.sql`](https://github.com/earendil-works/absurd/blob/main/sql/absurd.sql)
schema file.  No extra services, no message brokers, no coordination layer.
SDKs stay [simple too](https://github.com/earendil-works/absurd/blob/main/sdks/typescript/src/index.ts).

*… because it's absurd how much you can over-design such a simple thing.*

## How It Works

A **task** dispatches onto a **queue** from where a **worker** picks it up.
Tasks are subdivided into **steps** that act as checkpoints.  Once a step
completes successfully its return value is persisted and the step won't execute
again.  If a task fails, it retries from the last checkpoint.

Tasks can also **sleep** (suspend until a time) or **await events** (suspend
until a named event is emitted).  Events are cached — first emit wins — making
them race-free.

```typescript
import { Absurd } from 'absurd-sdk';

const app = new Absurd();

app.registerTask({ name: 'order-fulfillment' }, async (params, ctx) => {
  const payment = await ctx.step('process-payment', async () => {
    return { paymentId: `pay-${params.orderId}`, amount: params.amount };
  });

  const shipment = await ctx.awaitEvent(`shipment.packed:${params.orderId}`);

  await ctx.step('send-notification', async () => {
    return { sentTo: params.email, trackingNumber: shipment.trackingNumber };
  });

  return { orderId: params.orderId, payment, trackingNumber: shipment.trackingNumber };
});

await app.startWorker();
```

## Quick Links

- **[Quickstart](./quickstart.md)** — install the schema, create a queue, run your first task
- **[Working With Agents](./agents.md)** — install the bundled Absurd skill for pi or other coding agents
- **[Database Setup and Migrations](./database.md)** — initialize the schema, upgrade releases, and generate SQL for your own migration system
- **[Concepts](./concepts.md)** — what durable execution is, plus tasks, steps, runs, events, and retry semantics
- **[Cleanup and Retention](./cleanup.md)** — set retention policies and automate cleanup with SQL, `absurdctl`, or cron
- **[Comparison](./comparison.md)** — where Absurd fits relative to PGMQ, Cadence, Temporal, Inngest, and DBOS
- **[Patterns](./patterns/)** — practical recipes for common workflow and scheduling setups

## SDKs

- **[TypeScript SDK](./sdk-typescript.md)** — full API reference for Node.js / TypeScript
- **[Python SDK](./sdk-python.md)** — sync and async clients using psycopg

## Tools

- **[absurdctl](./absurdctl.md)** — CLI for schema management, queue operations, and task inspection
- **[Habitat](./habitat.md)** — web dashboard for monitoring tasks, runs, and events
