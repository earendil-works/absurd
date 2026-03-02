# Recurring / Scheduled Tasks Design

**Date:** 2026-03-02
**Status:** Approved

## Overview

Add recurring/scheduled task support to Absurd at the database level, consistent
with the "complexity in the DB" philosophy. Schedules are per-queue data stored
in a new `s_{queue}` table, managed through stored procedures, and ticked
automatically when workers poll for work.

## Schedule Table: `s_{queue}`

Each queue gets a schedules table alongside its existing 5 tables:

| Column | Type | Notes |
|--------|------|-------|
| `schedule_name` | text PK | Human-readable identifier |
| `task_name` | text not null | Task handler to spawn |
| `params` | jsonb not null default '{}' | Params passed to spawned tasks |
| `headers` | jsonb | Inherited by spawned tasks |
| `retry_strategy` | jsonb | Inherited by spawned tasks |
| `max_attempts` | integer | Inherited by spawned tasks |
| `cancellation` | jsonb | Inherited by spawned tasks |
| `schedule_expr` | text not null | Cron expression, shorthand, or `@every N` |
| `enabled` | boolean not null default true | Pause without deleting |
| `catchup_policy` | text not null default 'skip' | `'skip'` or `'all'` |
| `last_triggered_at` | timestamptz | When last spawned |
| `next_run_at` | timestamptz not null | Pre-computed next fire time |
| `created_at` | timestamptz not null | Creation timestamp |

## Schedule Expression Format

### 5-field cron (standard Unix)

Format: `minute hour day-of-month month day-of-week`

Supported features:
- Exact values: `30 9 * * *` (daily at 9:30)
- Wildcards: `* * * * *` (every minute)
- Steps: `*/5 * * * *` (every 5 minutes)
- Ranges: `0 9-17 * * *` (hourly 9am-5pm)
- Lists: `0 9 * * 1,3,5` (MWF at 9am)
- Range+step: `0 9-17/2 * * *` (every 2h, 9am-5pm)
- Day-of-week names: MON-FRI
- Month names: JAN-DEC

### Shorthands

- `@yearly` / `@annually` = `0 0 1 1 *`
- `@monthly` = `0 0 1 * *`
- `@weekly` = `0 0 * * 0`
- `@daily` / `@midnight` = `0 0 * * *`
- `@hourly` = `0 * * * *`

### Fixed interval

- `@every N` where N is seconds (e.g., `@every 300` = every 5 minutes)

### Not supported

- Seconds field (6-field cron)
- Year field (7-field Quartz)
- Special characters: L, W, # (Quartz)

## New Stored Procedures

### Cron engine

- **`next_cron_time(expr text, after timestamptz) -> timestamptz`** — Parses
  expression and returns next matching timestamp after the given time. Works in
  UTC internally to avoid DST ambiguity.
- **`parse_cron_field(field text, min int, max int) -> int[]`** — Expands a
  single cron field into an array of matching integers.

### Schedule management

- **`create_schedule(queue, schedule_name, task_name, params, schedule_expr, options)`**
  — Inserts into `s_{queue}`, computes initial `next_run_at`.
- **`update_schedule(queue, schedule_name, options)`** — Updates schedule fields.
  Recomputes `next_run_at` if expression changes.
- **`delete_schedule(queue, schedule_name)`** — Removes the schedule.
- **`list_schedules(queue)`** — Returns all schedules with state.
- **`get_schedule(queue, schedule_name)`** — Single schedule detail.

### Tick engine

- **`tick_schedules(queue)`** — Core function called inside `claim_task`.

Tick logic per enabled schedule where `next_run_at <= now`:

- If `catchup_policy = 'skip'`: spawn 1 task, jump `next_run_at` to next
  future time.
- If `catchup_policy = 'all'`: spawn up to 5 tasks per tick, advance
  `next_run_at` incrementally. Remaining catch-up happens on subsequent polls.

Concurrency safety: `FOR UPDATE SKIP LOCKED` on schedule rows prevents
double-ticking from concurrent workers.

Idempotency: spawned tasks use key `sched:{schedule_name}:{next_run_at_iso}`
so double-ticking is harmless.

### Strict calendar firing

Schedules fire on a strict calendar. The next occurrence spawns regardless of
whether the previous task has completed. No dependency between spawned tasks.

## Modified Existing Procedures

- **`create_queue`** — Also creates `s_{queue}` table.
- **`drop_queue`** — Also drops `s_{queue}`.
- **`ensure_queue_tables`** — Includes `s_{queue}` DDL.
- **`claim_task`** — Calls `tick_schedules` before claiming work.

## Catch-Up Behavior

Default (`catchup_policy = 'skip'`): If a worker was down and missed
occurrences, spawn 1 task and skip ahead to the next future `next_run_at`.
This avoids thundering herds and is correct for most use cases (syncs, polls,
health checks).

Opt-in (`catchup_policy = 'all'`): Drip-feed missed runs at 5 per tick.
Since workers poll at ~250ms intervals, this naturally rate-limits catch-up.
Useful for schedules where each occurrence matters (reports, notifications).

Re-enabling a disabled schedule does not catch up missed runs; it sets
`next_run_at` to the next future occurrence from now.

## SDK Changes (TypeScript + Python)

Thin wrappers around the new stored procedures:

- `app.createSchedule(name, { taskName, params, schedule, ... })`
- `app.updateSchedule(name, { ... })`
- `app.deleteSchedule(name)`
- `app.listSchedules()`
- `app.getSchedule(name)`

No changes to `registerTask`, `startWorker`, or `TaskContext`. Scheduled tasks
are regular tasks once spawned.

## absurdctl Commands

- `create-schedule <queue> <name> <task> --cron "expr"` or `--every N`
- `update-schedule <queue> <name> [--cron ...] [--params ...] [--enable] [--disable]`
- `delete-schedule <queue> <name>`
- `list-schedules <queue>`

## Habitat UI

- New "Schedules" view per queue.
- Shows schedule_expr, next_run_at, last_triggered_at, enabled status.
- Link to spawned tasks via idempotency key pattern.

## Migration

New file `sql/migrations/0.0.7-0.0.8.sql`:
- Add `s_{queue}` table to all existing queues.
- Install new stored procedures (cron engine, schedule management, tick engine).
- Modify `claim_task` to call `tick_schedules`.
