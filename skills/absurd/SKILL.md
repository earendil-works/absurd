---
name: absurd
description: Debug and operate Absurd durable workflows with absurdctl. Use when working with Absurd queues, tasks, runs, events, retries, schema setup, or when a user wants to inspect, spawn, retry, cancel, or wake workflows.
license: Apache-2.0
---

# Absurd

Use this skill when the project uses **Absurd**, the Postgres-native durable workflow engine, or when the user mentions **`absurdctl`**, queues, durable tasks, runs, retries, sleeping tasks, or events.

## Tiny mental model

- A **queue** is a namespace of Absurd tables (`t_`, `r_`, `c_`, `e_`, `w_`).
- A **task** is the durable workflow instance.
- A **run** is one execution attempt of a task.
- A **step** is a checkpoint. Completed step results are stored as JSON.
- **Sleeping** tasks are usually waiting for time or an event.
- **Events** wake waiting tasks. Event payloads are cached; first emit wins.

Important distinction:

- `task_id` = the whole workflow across all attempts
- `run_id` = one specific execution attempt

## First: make sure `absurdctl` works

If `absurdctl` is not on `PATH`, check whether you are inside a repo checkout and, if so, use:

```bash
export PATH="$PWD:$PATH"
```

Absurd connection precedence is:

```text
--database > ABSURD_DATABASE_URL > PGDATABASE > postgresql://localhost/absurd
```

For non-URI connections, `PGHOST`, `PGPORT`, `PGUSER`, and `PGPASSWORD` are also honored.

## Default debugging workflow

Prefer **`absurdctl` state inspection before source inspection**. Usually you do **not** need to read application code first.

If the user explicitly asks you to use **`absurdctl`** to inspect or fix a workflow, do that first instead of starting with `rg` / source browsing.

When the user wants to debug a task, start with these commands in order:

### 1) Discover queues

```bash
absurdctl list-queues
```

### 2) Inspect recent activity in the likely queue

```bash
absurdctl list-tasks --queue=default --limit=20
```

Notes:
- `list-tasks` defaults to 50 rows if `--limit` is omitted.
- Useful statuses: `pending`, `running`, `sleeping`, `completed`, `failed`, `cancelled`.

### 3) Focus on failures or sleepers

```bash
absurdctl list-tasks --queue=default --status=failed --limit=20
absurdctl list-tasks --queue=default --status=sleeping --limit=20
```

### 4) Inspect one workflow or one attempt in detail

```bash
absurdctl dump-task --task-id=<task-id>
absurdctl dump-task --run-id=<run-id>
```

`dump-task` is the most important inspection command. It shows things like:
- task name, params, and headers
- retry settings and attempts
- checkpointed step state
- waits / events / sleep state
- final result or failure

## How to reason about common states

### If a task is `failed`

1. `dump-task --task-id=<task-id>`
2. Read the failure and the last successful checkpoints.
3. If needed, inspect the most recent attempt with `--run-id`.
4. Search the code for the task implementation by task name.
5. Only then decide whether to retry.

### If a task is `sleeping`

1. `dump-task --task-id=<task-id>`
2. Look for the wait reason:
   - sleeping until a timestamp
   - waiting for an event name
3. If it is waiting for an event and the user wants it resumed, emit that event.

### If a task is `running`

1. `dump-task --task-id=<task-id>`
2. Look at existing checkpoints to see how far it got.
3. If the user suspects a stuck worker, inspect the worker process / application logs too.

### About workers

Do not assume you need to start or modify a worker.

- If a spawned task moves from `pending` to `sleeping`, `running`, or `completed`, a worker is already active.
- If tasks remain `pending`, then investigate whether a worker for that queue is actually running.
- Only inspect Python / TypeScript runtime details when the task state suggests a worker problem or the user asks for code changes.

### If you need the implementation

After you know the task name, search the codebase for its registration.

TypeScript / JavaScript:

```bash
rg -n "registerTask\(|name:\s*['\"]<task-name>['\"]" .
```

Python:

```bash
rg -n "register_task\(|@.*register_task|['\"]<task-name>['\"]" .
```

If the task is waiting for an event, also search for the event name.

## Common actions

### Spawn work

Use `-P key=value` for strings and `-P key:=json` for typed JSON values.

```bash
absurdctl spawn-task my-task -q default -P foo=bar
absurdctl spawn-task my-task -q default -P count:=42 -P enabled:=true
absurdctl spawn-task my-task -q default -P user.name=Alice -P user.age:=30
```

Use `--params` when the user already has a JSON object:

```bash
absurdctl spawn-task my-task -q default --params '{"foo":"bar","count":42}'
```

### Retry failed work

```bash
absurdctl retry-task <task-id>
absurdctl retry-task <task-id> --max-attempts 5
absurdctl retry-task -q default <task-id> --spawn-new
```

Guidance:
- plain `retry-task` retries the existing task
- `--spawn-new` creates a brand-new task with the original inputs
- prefer understanding the failure before retrying

### Cancel work

```bash
absurdctl cancel-task <task-id>
absurdctl cancel-task -q default <task-id>
```

### Wake waiting tasks by emitting an event

```bash
absurdctl emit-event order.completed -q default -P orderId=123
absurdctl emit-event approval.granted:42 -q default -P approved:=true
```

If the event payload should be structured JSON:

```bash
absurdctl emit-event shipment.packed:42 -q default --payload '{"trackingNumber":"XYZ"}'
```

### Schema setup / migrations

Use these on a blank or controlled database, or when the user explicitly asks:

```bash
absurdctl init
absurdctl schema-version
absurdctl migrate
absurdctl create-queue default
```

## Safe operating rules

Be careful with state-changing commands. Unless the user clearly wants them, avoid running these blindly on a shared or production database:

- `init`
- `migrate`
- `create-queue`
- `drop-queue`
- `cleanup`
- `cancel-task`
- `retry-task`
- `emit-event`
- `spawn-task`

If the environment is ambiguous, ask which database / queue is safe to operate on.

## Good copy-paste playbooks

### Debug the latest failures in `default`

```bash
absurdctl list-queues
absurdctl list-tasks --queue=default --status=failed --limit=20
absurdctl dump-task --task-id=<task-id>
```

### Find sleepers and wake one

```bash
absurdctl list-tasks --queue=default --status=sleeping --limit=20
absurdctl dump-task --task-id=<task-id>
absurdctl emit-event <event-name> -q default -P key=value
```

### Reproduce by spawning a task, then inspect it

```bash
absurdctl spawn-task my-task -q default -P foo=bar
absurdctl list-tasks --queue=default --task-name=my-task --limit=5
absurdctl dump-task --task-id=<task-id>
```

Fast path when the user says “spawn a task and debug it”:

```bash
absurdctl spawn-task my-task -q default -P foo=bar
absurdctl list-tasks --queue=default --task-name=my-task --limit=5
absurdctl dump-task --task-id=<task-id>
# then either:
absurdctl emit-event <event-name> -q default -P key=value
# or:
absurdctl retry-task <task-id>
```

## Extra reference

- Use `absurdctl <command> --help` for full options.
- `dump-task --task-id` is usually the best starting point once you know the task.
- Checkpointed step results are durable JSON state; code outside steps may execute multiple times across retries.
