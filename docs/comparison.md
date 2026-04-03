# Comparison

Absurd is not trying to win on feature count.  It is trying to make durable
execution feel as close to "just use Postgres" as possible.

That means the most useful comparison is not "which system is best?" but
"what is each system optimizing for?"

The notes below are intentionally opinionated and are based on the public docs
and READMEs of these projects as of April 2026.

## What Absurd Is Optimizing For

Absurd deliberately optimizes for a small surface area:

- **one SQL schema as the engine**
- **Postgres as the only required service**
- **pull-based workers** instead of push orchestration
- **thin SDKs** with most of the durable behavior in stored procedures
- a small set of primitives: **tasks, steps, retries, sleep, and events**

That buys a lot of simplicity, but it also means Absurd intentionally does not
try to match the breadth of a full workflow platform.

## PGMQ

[PGMQ](https://github.com/pgmq/pgmq) is the closest thing to a pure queue.  It
is explicitly positioned as a lightweight Postgres message queue, with SQS-like
concepts such as visibility timeouts, explicit delete or archive, FIFO queues,
and topic routing.

That makes it an important comparison point because Absurd is heavily inspired
by the idea that Postgres can do more than people give it credit for.

### How it differs from Absurd

PGMQ intentionally stops at the queue layer.  Absurd starts where that leaves
off.

With PGMQ, your application is responsible for turning messages into larger
processes which means that you need to build durable execution on top of the
queue yourself.

Absurd bakes those ideas into the model.  A task is already a durable unit of
work; a step is already a checkpoint.

## Cadence

[Cadence](https://github.com/cadence-workflow/cadence) is one of the seminal
workflow systems in this space.  It was built at Uber for large-scale,
long-running workflow orchestration and comes with a much larger operational and
conceptual surface than Absurd.

Cadence is not just a schema and some workers.  It is a backend made up of
multiple services, workers, task lists, domains, operational tooling, and
production deployment guidance.

For all intends and purposes we consider Cadence to be the precursor to Temporal.

## Temporal

[Temporal](https://temporal.io/) is the clearest "full durable execution
platform" comparison for Absurd.  It grew out of the Cadence lineage, but with
its own ecosystem, server, tooling, and SDK model.

Temporal's biggest philosophical difference is that it takes much stronger
control of the workflow runtime.  In practice, that means a much richer and
more structured model around deterministic workflow execution, activities,
signals, timers, child workflows, task queues, and replay.

### How it differs from Absurd

Temporal gives you more, but asks for more:

- you run **Temporal Server**, not just a database schema
- the SDK runtime is more opinionated about how workflow code executes
- the system exposes more first-class workflow concepts
- the ecosystem, tooling, and battle-tested patterns are broader

Absurd is intentionally less invasive.  It does **not** try to turn your code
into a deterministic workflow runtime.  Instead, it relies on explicit step
boundaries and persisted step results.  This results in much simpler SDKs.
Temporal's Python SDK is 170.000 lines of code, Absurd's is under 2000.

That difference is important:

- in Temporal, the system leans harder on workflow replay and deterministic execution
- in Absurd, the system replays a task by **reusing checkpoints** and re-running ordinary code around them

That makes Absurd easier to explain and easier to bolt onto an existing system,
but it also means you get fewer built-in guarantees and fewer high-level
primitives.

## Inngest

[Inngest](https://www.inngest.com/) approaches the problem from a different
angle: it is fundamentally **event-driven**.  Its model revolves around
triggers, durable steps, flow control, and HTTP-based function invocation.

Compared to Absurd, Inngest feels much more like an application platform for
background and event-driven execution than a Postgres-native task engine.

### How it differs from Absurd

Inngest has a number of built-in concepts that Absurd deliberately does not try
to own:

- event and cron triggers as first-class entry points
- HTTP-based invocation and syncing of functions
- flow control primitives like concurrency limits, throttling, debouncing, rate limiting, prioritization, and batching
- an integrated local development and dashboard experience
- self-hosted and hosted deployment shapes outside of plain Postgres

Absurd is much narrower:

- workers pull from Postgres directly
- there is no push model or HTTP control plane in the core system
- events are a durable waiting primitive, not the center of the whole platform
- the operational footprint is smaller, but so is the feature set

There is also a licensing difference worth noting: Inngest's repository uses an
SSPL-based license with an Apache 2.0 future-license clause, whereas Absurd is
Apache 2.0 today.  That may matter for some self-hosting or commercial use
cases.

## DBOS

[DBOS](https://docs.dbos.dev/) is probably the closest project on this list in
spirit.  It also builds durable execution on top of Postgres and tries to avoid
forcing you into a separate orchestration cluster.

Like Absurd, DBOS is trying to keep durability close to the application and the
database rather than building a giant external workflow brain.

In particular, Absurd pushes more of the durable behavior into stored procedures
and keeps the SDKs relatively light.  DBOS, by contrast, has rather beefy SDKs
in comparison that try to do more.  For instance the Python SDK clocks in at
40.000 lines of code.
