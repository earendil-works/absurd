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

More about it can be found [in the announcement post](https://lucumr.pocoo.org/2025/11/3/absurd-workflows/).

You can read in the docs [how Absurd compares to PGMQ, Cadence, Temporal,
Inngest, and DBOS](docs/comparison.md).

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
See the [quickstart guide](docs/quickstart.md) for a short tutorial and the
[documentation index](docs/index.md) for everything else.

## Client SDKs

Currently SDKs exist for the following languages:

* [TypeScript](sdks/typescript) (and JavaScript)
* [Python](sdks/python)
* [Go](sdks/go/absurd) *(experimental bootstrap)*

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

Additionally, tasks can *sleep* or *suspend for events*.  Events are cached
(first emit wins), which means they are race-free.

## Components

Absurd comes with two basic tools that help you work with it.  One is
called [`absurdctl`](docs/absurdctl.md) which allows you to initialize and
migrate the schema, inspect schema versions, create/drop/list queues, and spawn
or retry tasks.  The other is [habitat](habitat) which is a Go application that
serves up a web UI to show you the current state of running and executed tasks.

```bash
uvx absurdctl init -d database-name
uvx absurdctl schema-version -d database-name
uvx absurdctl migrate -d database-name
uvx absurdctl create-queue -d database-name default
```

Use [`uvx`](https://docs.astral.sh/uv/guides/tools/) `absurdctl ...` for
one-off commands, or install it persistently with `uv tool install absurdctl`.
If you prefer a standalone file, you can also download `absurdctl` from GitHub
Releases and put it on your `PATH`:

```bash
curl -fsSL \
  https://github.com/earendil-works/absurd/releases/latest/download/absurdctl \
  -o absurdctl
chmod +x absurdctl
install -m 755 absurdctl ~/.local/bin/absurdctl
```

Install the SDK for your language of choice:

```bash
# TypeScript / JavaScript
npm install absurd-sdk

# Python
uv add absurd-sdk

# Go
go get github.com/earendil-works/absurd/sdks/go/absurd@latest
```

<div style="text-align: center" align="center">
  <img src="habitat/screenshot.png" width="550" alt="Screenshot of habitat">
</div>

## Example

Here's what that looks like in TypeScript.

```typescript
import { Absurd } from 'absurd-sdk';

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
    return {
      paymentId: `pay-${params.orderId}`,
      amount: params.amount,
    };
  });

  const inventory = await ctx.step('reserve-inventory', async () => {
    return { reservedItems: params.items };
  });

  // Wait indefinitely for a warehouse event - the task suspends
  // until the event arrives.  Events are cached like step checkpoints
  // (first emit wins), which means this is race-free.
  const shipment = await ctx.awaitEvent(`shipment.packed:${params.orderId}`);

  // Ready to send a notification!
  await ctx.step('send-notification', async () => {
    return {
      sentTo: params.email,
      trackingNumber: shipment.trackingNumber,
    };
  });

  return {
    orderId: params.orderId,
    payment,
    inventory,
    trackingNumber: shipment.trackingNumber,
  };
});

// Start a worker that pulls tasks from Postgres
const worker = await app.startWorker();

// Spawn a task - it will be executed durably with automatic retries.  If
// triggered from within a task, you can also await it.
const { taskID } = await app.spawn('order-fulfillment', {
  orderId: '42',
  amount: 9999,
  items: ['widget-1', 'gadget-2'],
  email: 'customer@example.com'
});

await app.emitEvent('shipment.packed:42', {
  trackingNumber: 'TRACK123',
});

console.log(await app.awaitTaskResult(taskID, { timeout: 10 }));

await worker.close();
await app.close();
```

## Documentation

More detail lives in the docs:

- [Quickstart](docs/quickstart.md)
- [TypeScript SDK](docs/sdk-typescript.md)
- [Python SDK](docs/sdk-python.md)
- [Go SDK](docs/sdk-go.md) *(experimental)*
- [Database Setup and Migrations](docs/database.md)
- [Concepts](docs/concepts.md) — includes retry semantics, worker claims, and idempotency keys
- [Living with Code Changes](docs/patterns/living-with-code-changes.md)
- [Cleanup and Retention](docs/cleanup.md)

## Working With Agents

Absurd is built so that agents such as Claude Code or pi can efficiently work
with the state in the database.  The easiest setup is to install the bundled
Absurd skill into a project or user skills directory:

```
absurdctl install-skill              # installs to .agents/skills
absurdctl install-skill .pi/skills   # if you want a different path
```

See [Working With Agents](docs/agents.md) for the recommended setup.

## AI Use Disclaimer

This codebase has been built with a lot of support of AI.  A combination of hand
written code, Codex and Claude Code was used to create this repository.  To the
extent to which it can be copyrighted, the Apache 2.0 license should be assumed.

## License and Links

- [Examples](https://github.com/earendil-works/absurd/tree/main/sdks/typescript/examples)
- [Issue Tracker](https://github.com/earendil-works/absurd/issues)
- License: [Apache-2.0](https://github.com/earendil-works/absurd/blob/main/LICENSE)
