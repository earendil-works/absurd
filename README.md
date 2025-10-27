<div style="text-align: center" align="center">
  <img src="logo.jpg" width="350" alt="Une photo d'un éléphant avec le titre : « Ceci n'est pas un éléphant »">
  <br><br>
</div>

# Absurd

Absurd is the most simple durable execution workflow system you can think of.
It's entirely based on Postgres and nothing else.  It's almost as easy to use as
a queue, but it handles scheduling and retries, and it does all of that without
needing any other services to run in addition to Postgres.

*… because it's absurd how much you can over-design such a simple thing.*

**Warning:** *this is an early experiment and should not be used in production.
It's an exploration of if such a system can be built in a way that the majority
of the complexity sits with the database and not the client SDKs.*

## What is Durable Execution?

Durable execution (or durable workflows) is a way to run long-lived, reliable
functions that can survive crashes, restarts, and network failures without losing
state or duplicating work.  Durable execution can be thought of as the combination
of a queue system and a state store that remembers the most recently seen state.

Instead of running your logic in memory, a durable execution system decomposes
a task into smaller pieces (step functions) and record every step and decision.
When the process stops (fails, intentionally suspends or a machine dies), the
engine can replay those events to restore the exact state and continue where it
left off, as if nothing happened.

In practice, that makes it possible to build dependable systems for things like
LLM basd agents, payments, email scheduling, order processing, really anything
that spans minutes, days, or even years.  Rather than bolting on ad-hoc retry
logic and database checkpoints, durable workflows give you one consistent model
for ensuring progress without double execution.  It's the promise of
"exactly-once" semantics in distributed systems, but expressed as code you can
read and reason about.

## Comparison

Absurd wants to be absurdly simple.  There are many systems on the market you
might want to look at if you think you need more:

* [Cadence](https://github.com/cadence-workflow/cadence) in many ways is the
  OG of durable execution systems.  It was built at Uber and has inspired many
  systems since.
* [Temporal](https://temporal.io/) was built by the Cadence authors to be a
  more modern interpreation of it.  What sets it apart, is that it takes strong
  control of the runtime environment within the target language to help you build
  deterministic workflows.
* [Inngest](https://www.inngest.com/) is an event driven workflow system.  It's
  self hostable and can run locally, but it's licensed under a fair-source
  inspired license.

## Push vs Pull

Absurd is a pull based system which means that your code pulls tasks from
Postgres as it has capacity.  It does not support push at all, which would
require a coordinator to run and call HTTP endpoints or similar.  Push systems
have the inherent disadvantage that you need to take greater care of system load
constraints.  If you need this, you can write yourself a simple service that
consumes messages and makes HTTP requests.

## Highlevel Operations

Absurd's goal is to move the complexity of SDKs into the underlying stored
functions.  The goal os the SDKs is that they make the system convenient by
abstracting over the lowlevel operations in a way that leverages the ergonomics
of the language you are working with.

A *task* is dispatches onto a given *queue* from where a *worker* picks it up
to work on.  Tasks are subdivided into *steps* which are excuted in sequence
by the worker.  Tasks can be suspended or fail, and when that happens they
execute again (a *run*).  The result of a step is stored in the database (a
*checkpoint*).  To not repeat work, checkpoints are automatically loaded from
the state storage in Postgres again.

Additionally tasks can *sleep* or *suspend for events*.  Events are cached
which means that they are race-free.

## Example

This demonstrates this for TypeScript.

```typescript
import { Absurd } from 'absurd';

const app = new Absurd();

// A task represents a series of operations.  It can be decomposed into
// steps which act as checkpoints.  Once a step has been passed
// successfully the return value is retained and it won't execute again.
// if it fails, the entire task is retried.  Code that runs outside of
// steps will potentially be executed multiple times.
app.registerTask({ name: 'order-fulfillment' }, async (params, ctx) => {

  // Each step is checkpointed - if the process crashes, we resume
  // from the last completed step
  const payment = await ctx.step('process-payment', async () => {
    // if you need an idempotency-key you can derive one from ctx.taskID.
    return await stripe.charges.create({ amount: params.amount, ... });
  });

  const inventory = await ctx.step('reserve-inventory', async () => {
    return await db.reserveItems(params.items);
  });

  // Wait indefinitely for a warehouse event - the task suspends
  // until the event arrives.  Events are cached like step checkpoints
  // which means that this is race-free.
  const shipment = await ctx.awaitEvent(`shipment.packed:${params.orderId}`);

  // ready to send a notification!
  await ctx.step('send-notification', async () => {
    return await sendEmail(params.email, shipment);
  });

  return { orderId: payment.id, trackingNumber: shipment.trackingNumber };
});

myWebApp.post("/api/shipment/pack/{orderId}", async (req) => {
  const trackingNumber = ...;
  app.emitEvent(`shipment.packed:${req.params.orderId}`, {
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

Because steps have their return values cached, for all intends and purposes
it simulate "exact-once" semantics.  However all the code outside of steps
will run multiple times.  Sometimes you want to integrate into systems that
themselves have some sort of idempotency mechanism (like the `idempotency-key`
HTTP header).  In that case it's recommended to use use `taskID` from the
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