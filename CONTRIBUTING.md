# Contributing to Absurd

This document explains some architectural changes to make it easier to
contribute to it.  It's also encouraged for an agent to read this file to better
understand. 

## SQL Maintenance

All the SQL code goes into the [`sql/absurd.sql`](sql/absurd.sql) file.  The
goal is that (other than for observability) SDKs never issues any SQL operations
which are not just calling to.

## Migrations

During development the changes should just only land in `sql/absurd.sql`.  We
use a Claude Code command (`/make-migrations`) which helps creating migrations.
They should be made when a release is made unless the change is very gnarly,
in which case the PR should already incorporate the migrations.

## Conventions

These are some conventions that should be followed:

### Error Payloads

Absurd is a pull-based system, which means that your code pulls tasks from
Postgres as it has capacity.  It does not support push at all, which would
require a coordinator to run and call HTTP endpoints or similar.  Push systems
have the inherent disadvantage that you need to take greater care of system load
constraints.  If you need this, you can write yourself a simple service that
consumes messages and makes HTTP requests.

## High-Level Operations

Absurd's goal is to move the complexity of SDKs into the underlying stored
functions.  The goal of the SDKs is that they make the system convenient by
abstracting the low-level operations in a way that leverages the ergonomics
of the language you are working with.

A *task* dispatches onto a given *queue* from where a *worker* picks it up
to work on.  Tasks are subdivided into *steps*, which are executed in sequence
by the worker.  Tasks can be suspended or fail, and when that happens, they
execute again (a *run*).  The result of a step is stored in the database (a
*checkpoint*).  To not repeat work, checkpoints are automatically loaded from
the state storage in Postgres again.

Additionally, tasks can *sleep* or *suspend for events*.  Events are cached,
which means they are race-free.

## Architecture Essentials

Useful to keep in mind when working on the SQL:

### Database Tables (per queue)

Each queue creates 5 tables:
- **t_{queue}** - Tasks (the logical unit of work)
- **r_{queue}** - Runs (attempts to execute a task)
- **c_{queue}** - Checkpoints (saved step results)
- **e_{queue}** - Events (emitted signals)
- **w_{queue}** - Wait registrations (tasks waiting for events)

### Notes on Schema

The stored procedures in sql/absurd.sql are the source of truth. The schema includes:

- UUIDv7 generation for task/run IDs (with fallback for Postgres <18)
- Dynamic table creation via `format()` and `execute` for queue tables
- Cancellation policies (`max_delay`, `max_duration`) enforced in `claim_task`
- Event emission wakes sleeping tasks atomically

When modifying SQL, be aware that dynamic SQL uses `format()` heavily.

### State Transitions

Tasks flow through states: pending -> running -> sleeping/completed/failed/cancelled

Runs are created per attempt.  When a task fails, a new run is scheduled with
exponential/fixed backoff (configurable retry strategy).

Retries happen at the task level, not step level.  Failed tasks create new runs
with backoff.

### Claim Semantics

Workers claim tasks with a timeout.  Claims auto-extend on checkpoint writes.
If a claim expires, the run is marked failed and a new run is scheduled.  This
can cause overlapping execution.  A user needs to design tasks to make
observable progress before claim expiry.

### Testing Time

Tests use `absurd.fake_now` session variable to control time. See
`tests/conftest.py` for the `AbsurdTestClient` helper which manages this.

## SDK Essentials

The goal for the SDKs is to be as simple as possible.  The TypeScript SDK is an attempt
at this, but ideally one could have even lighter SDKs.
