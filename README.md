<div style="text-align: center" align="center">
  <img src="logo.jpg" width="350" alt="Une photo d'un éléphant avec le titre : « Ceci n'est pas un éléphant »">
  <br><br>
</div>

# Absurd

Absurd is the simplest durable execution workflow system you can think of.
It's entirely based on Postgres and nothing else.  It's almost as easy to use as
a queue, but it handles scheduling and retries, and it does all of that without
needing any other services to run in addition to Postgres.

*… because it's absurd how much you can over-design such a simple thing.*

**Warning:** *this is an early experiment and should not be used in production.
It's an exploration of whether such a system can be built in a way that the majority
of the complexity sits with the database and not the client SDKs.*

## What is Durable Execution?

Durable execution (or durable workflows) is a way to run long-lived, reliable
functions that can survive crashes, restarts, and network failures without losing
state or duplicating work.  Durable execution can be thought of as the combination
of a queue system and a state store that remembers the most recently seen state.

Instead of running your logic in memory, a durable execution system decomposes
a task into smaller pieces (step functions) and records every step and decision.
When the process stops (fails, intentionally suspends, or a machine dies), the
engine can replay those events to restore the exact state and continue where it
left off, as if nothing happened.

In practice, that makes it possible to build dependable systems for things like
LLM-based agents, payments, email scheduling, order processing--really anything
that spans minutes, days, or even years.  Rather than bolting on ad-hoc retry
logic and database checkpoints, durable workflows give you one consistent model
for ensuring progress without double execution.  It's the promise of
"exactly-once" semantics in distributed systems, but expressed as code you can
read and reason about.

## Installation

Absurd just needs a single `.sql` file ([`absurd.sql`](sql/absurd.sql)) which
needs to be applied to a database of your choice.  You can plug it into your
favorite migration system of choice.  Additionally if that file changes, we
also release [migrations](sql/migrations) which should make upgrading easy.

## Comparison

Absurd wants to be absurdly simple.  There are many systems on the market you
might want to look at if you think you need more:

* [pgmq](https://github.com/pgmq/pgmq) is a lightweight message queue built on
  top of just Postgres.  Absurd has been heavily influenced by it.
* [Cadence](https://github.com/cadence-workflow/cadence) in many ways is the
  OG of durable execution systems.  It was built at Uber and has inspired many
  systems since.
* [Temporal](https://temporal.io/) was built by the Cadence authors to be a
  more modern interpretation of it.  What sets it apart is that it takes strong
  control of the runtime environment within the target language to help you build
  deterministic workflows.
* [Inngest](https://www.inngest.com/) is an event-driven workflow system.  It's
  self-hostable and can run locally, but it's licensed under a fair-source-inspired
  license.
* [DBOS](https://docs.dbos.dev/) is also attempting to implement durable workflows
  on top of Postgres.

## Push vs Pull

Absurd is a pull-based system, which means that your code pulls tasks from
Postgres as it has capacity.  It does not support push at all, which would
require a coordinator to run and call HTTP endpoints or similar.  Push systems
have the inherent disadvantage that you need to take greater care of system load
constraints.  If you need this, you can write yourself a simple service that
consumes messages and makes HTTP requests.

## High-Level Operations

Absurd's goal is to move the complexity of SDKs into the underlying stored
functions.  The SDKs then try to make the system convenient by abstracting the
low-level operations in a way that leverages the ergonomics of the language you
are working with.

A *task* dispatches onto a given *queue* from where a *worker* picks it up
to work on.  Tasks are subdivided into *steps*, which are executed in sequence
by the worker.  Tasks can be suspended or fail, and when that happens, they
execute again (a *run*).  The result of a step is stored in the database (a
*checkpoint*).  To not repeat work, checkpoints are automatically loaded from
the state storage in Postgres again.

Additionally, tasks can *sleep* or *suspend for events*.  Events are cached,
which means they are race-free.

## Components

Absurd comes with two basic tools that help you work with it.  One is
called [`absurdctl`](absurdctl) which allows you to create, drop, and list
queues, as well as spawn tasks.  The other is [habitat](habitat) which is
a Go application that serves up a web UI to show you the current state of
running and executed tasks.

```bash
absurdctl init -d database-name
absurdctl create-queue -d database-name default
```

Right now, there is only a TypeScript SDK, which isn't published yet, so you
need to use the SDK from the repository.  You can run `npm run build`
to get a JS-only build or use the TypeScript code right away.

<div style="text-align: center" align="center">
  <img src="habitat/screenshot.png" width="550" alt="Screenshot of habitat">
</div>

## Example

Here's what that looks like in TypeScript.

```typescript
import { Absurd } from 'absurd';

const app = new Absurd();

// A task represents a series of operations.  It can be decomposed into
// steps, which act as checkpoints.  Once a step has been passed
// successfully, the return value is retained and it won't execute again.
// If it fails, the entire task is retried.  Code that runs outside of
// steps will potentially be executed multiple times.
app.registerTask({ name: 'order-fulfillment' }, async (params, ctx) => {

  // Each step is checkpointed, so if the process crashes, we resume
  // from the last completed step
  const payment = await ctx.step('process-payment', async () => {
    // If you need an idempotency key, you can derive one from ctx.taskID.
    return await stripe.charges.create({ amount: params.amount, ... });
  });

  const inventory = await ctx.step('reserve-inventory', async () => {
    return await db.reserveItems(params.items);
  });

  // Wait indefinitely for a warehouse event - the task suspends
  // until the event arrives.  Events are cached like step checkpoints,
  // which means this is race-free.
  const shipment = await ctx.awaitEvent(`shipment.packed:${params.orderId}`);

  // Ready to send a notification!
  await ctx.step('send-notification', async () => {
    return await sendEmail(params.email, shipment);
  });

  return { orderId: payment.id, trackingNumber: shipment.trackingNumber };
});

myWebApp.post("/api/shipment/pack/{orderId}", async (req) => {
  const trackingNumber = ...;
  await app.emitEvent(`shipment.packed:${req.params.orderId}`, {
    trackingNumber,
  });
});

// Start a worker that pulls tasks from Postgres
await app.startWorker();
```

Spawn a task:

```typescript
// Spawn a task - it will be executed durably with automatic retries.  If
// triggered from within a task, you can also await it.
app.spawn('order-fulfillment', {
  orderId: '42',
  amount: 9999,
  items: ['widget-1', 'gadget-2'],
  email: 'customer@example.com'
});
```

## Idempotency Keys

Because steps have their return values cached, for all intents and purposes
they simulate "exactly-once" semantics.  However, all the code outside of steps
will run multiple times.  Sometimes you want to integrate into systems that
themselves have some sort of idempotency mechanism (like the `idempotency-key`
HTTP header).  In that case, it's recommended to use `taskID` from the
context to derive one:

```typescript
const payment = await ctx.step('process-payment', async () => {
  const idempotencyKey = `${ctx.taskID}:payment`;
  return await somesdk.charges.create({
    amount: params.amount,
    idempotencyKey,
  });
});
```

## Living with Code Changes

One of the fun perks of working with durable execution systems is that your
tasks might keep running for... well, geological timescales.  The codebase
evolves but that workflow you kicked off six months ago?  If you sleep long
enough, that is still ticking.  There's no magic fix for this, just awareness.

In practice, it means that if you change what a step returns, some future code
might suddenly receive old results from a bygone era.  That's part of the
deal.  So what can you do? Two options:

1. Rename the step (the old state is never going to be seen again)
2. Handle the relics gracefully when they come crawling back.

Both work. The first is cleaner, the second is braver.

## Retries

Unlike some other durable execution systems, Absurd only knows about tasks. Tasks
are the unit that gets retried, and they happen to be composed of steps that act
as checkpoints. Retries therefore happen at the task level. When a worker starts
on a task it "claims" it, reserving the task for a configured duration. Each time
the task stores a checkpoint, the claim is extended. If the worker crashes or does
not make progress before the claim times out, the task resets and is handed to
another worker. This overlap can lead to two workers running the same task, so
write tasks so they always make observable progress well inside the claim timeout,
with ample headroom.

## Cleanup

By default, data will live forever, which is unlikely to be what you want.  Currently,
there is no support for time-based partitioning.  To get rid of data, you can run
the cleanup functions (`absurd.cleanup_tasks` and `absurd.cleanup_events`)
manually or use the `absurdctl cleanup` command which lets you define a queue name
and a TTL in days.  For instance, the following command deletes runs older than 7
days on the default queue:

```
absurdctl cleanup default 7
```

## Working With Agents

Absurd is built so that agents such as Claude Code can efficiently work with the
state in the database.  You can either point them straight at Postgres and hope
that they pry the information out, but the better idea is to make `absurdctl`
available on `PATH` and give them some ideas of what to do with it.  `absurdctl`
can output some agent-specific help that you can put into your `AGENTS.md` /
`CLAUDE.md` files:

```
absurdctl agent-help >> AGENTS.md
```

You might have to tweak the outputs afterwards to work best for your setup.

## AI Use Disclaimer

This codebase has been built with a lot of support of AI.  A combination of hand
written code, Codex and Claude Code was used to create this repository.  To the
extent to which it can be copyrighted, the Apache 2.0 license should be assumed.

## License and Links

- [Examples](https://github.com/earendil-works/absurd/tree/main/sdks/typescript/examples)
- [Issue Tracker](https://github.com/earendil-works/absurd/issues)
- License: [Apache-2.0](https://github.com/earendil-works/absurd/blob/main/LICENSE)
