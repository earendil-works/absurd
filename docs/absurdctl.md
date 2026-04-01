# absurdctl

`absurdctl` is a Python CLI tool for managing Absurd schemas, queues, tasks,
and events.  It talks directly to Postgres using standard `PG*` environment
variables.

## Installation

`absurdctl` is available from
[GitHub Releases](https://github.com/earendil-works/absurd/releases).  Download
it and place it on your `PATH`.  It requires Python 3 and `psql`.

```bash
# Set your database connection
export PGDATABASE="postgresql://user:pass@localhost:5432/mydb"
```

You can also pass connection details as flags (`-d`, `-h`, `-p`, `-U`) to each
command.

## Connection Options

All commands accept these flags:

| Flag | Env Var | Description |
|------|---------|-------------|
| `-d` | `PGDATABASE` | Database name or full connection URI |
| `-h` | `PGHOST` | Database host |
| `-p` | `PGPORT` | Database port |
| `-U` | `PGUSER` | Database user |
| — | `PGPASSWORD` | Database password |

Using a full URI is the simplest approach:

```bash
export PGDATABASE="postgresql://user:pass@localhost:5432/mydb"
```

## Schema Management

For a full guide to fresh installs, upgrades, and generating SQL for your own
migration system, see **[Database Setup and Migrations](./database.md)**.

### `init`

Apply the Absurd schema (`absurd.sql`) to the database.  Safe to run on a fresh
database.

```bash
absurdctl init
absurdctl init -d mydb
absurdctl init --ref 0.2.0
```

Options:

| Flag | Description |
|------|-------------|
| `--ref REF` | Install `absurd.sql` from a specific Git ref or release tag instead of `main` |

### `schema-version`

Display the currently installed schema version.

```bash
absurdctl schema-version
```

### `migrate`

Apply incremental migrations to bring the schema up to the target version.
Migrations are bundled into built copies of `absurdctl` (via
`scripts/build-absurdctl`) or fetched from GitHub.

```bash
absurdctl migrate
absurdctl migrate -d mydb
absurdctl migrate --dry-run
absurdctl migrate --to 0.2.0
absurdctl migrate --from 0.1.0 --to main --dump-sql > absurd-migrations.sql
```

For production, a good pattern is to generate SQL with `--dump-sql`, check that
file into your application's migration directory, and let your usual migration
system apply it.

Options:

| Flag | Description |
|------|-------------|
| `--from VERSION` | Starting schema version (overrides value recorded in DB) |
| `--to VERSION` | Target schema version (default: `main`) |
| `--dry-run` | Show migration plan without applying SQL |
| `--dump-sql` | Print one combined SQL script for all migrations in range and exit (requires `--from`; does not connect to Postgres) |

## Queue Management

### `create-queue`

Create a new queue.  This creates the per-queue tables (`t_`, `r_`, `c_`, `e_`, `w_`).

```bash
absurdctl create-queue default
absurdctl create-queue emails -d mydb
```

### `drop-queue`

Drop a queue and all its data.  Requires `--yes` for confirmation.

```bash
absurdctl drop-queue old-queue --yes
```

### `list-queues`

List all existing queues.

```bash
absurdctl list-queues
```

## Task Operations

### `spawn-task`

Enqueue a new task.  Parameters are passed with `-P key=value` (strings) or
`-P key:=json` (typed JSON).

```bash
absurdctl spawn-task my-task -P name=Alice -P count:=42
absurdctl spawn-task --queue reports generate-report -P month=january
```

Options:

| Flag | Description |
|------|-------------|
| `--queue` | Target queue (default: `default`) |
| `-P key=value` | String parameter |
| `-P key:=json` | JSON parameter (numbers, booleans, objects, arrays) |

### `list-tasks`

List tasks with optional filtering.

```bash
absurdctl list-tasks
absurdctl list-tasks --queue=default --status=failed
absurdctl list-tasks --status=running --limit=20
```

Options:

| Flag | Description |
|------|-------------|
| `--queue` | Filter by queue |
| `--status` | Filter by status (`pending`, `running`, `sleeping`, `completed`, `failed`, `cancelled`) |
| `--limit` | Maximum number of tasks to show |

### `dump-task`

Show detailed information about a task or run, including checkpoints.

```bash
absurdctl dump-task --task-id=019a32d3-8425-7ae2-a5af-2f17a6707666
absurdctl dump-task --run-id=019a32d3-8425-7ae2-a5af-2f17a6707667
```

### `cancel-task`

Cancel a running or pending task.

```bash
absurdctl cancel-task 019a32d3-8425-7ae2-a5af-2f17a6707666
absurdctl cancel-task 019a32d3-8425-7ae2-a5af-2f17a6707666 --queue=emails
```

### `retry-task`

Retry a failed task, optionally increasing the attempt limit.

```bash
absurdctl retry-task 019a32d3-8425-7ae2-a5af-2f17a6707666
absurdctl retry-task 019a32d3-8425-7ae2-a5af-2f17a6707666 --max-attempts 10
```

## Events

### `emit-event`

Emit an event to wake tasks waiting for it.  Parameters become the event payload.

```bash
absurdctl emit-event order.completed -P orderId=123
absurdctl emit-event --queue=orders shipment.packed -P tracking:='"XYZ"'
```

## Cleanup

### `cleanup`

Delete old completed, failed, or cancelled tasks and events.  Takes a queue name
and a TTL in days.

```bash
absurdctl cleanup default 7     # delete data older than 7 days
absurdctl cleanup emails 30     # 30-day retention
```

For batching, direct SQL usage, and cron/job examples, see
**[Cleanup and Retention](./cleanup.md)**.

## Agent Help

### `agent-help`

Print guidance tailored for AI coding agents.  Append it to your `AGENTS.md` or
`CLAUDE.md`:

```bash
absurdctl agent-help >> AGENTS.md
```

## Bundled Builds

For deployment, `scripts/build-absurdctl` creates a self-contained copy with
the schema SQL and migrations embedded:

```bash
make build-absurdctl
```

The bundled version works without network access for `init` and `migrate`.

## Examples

```bash
# Full setup from scratch
export PGDATABASE="postgresql://user:pass@localhost:5432/mydb"
absurdctl init
absurdctl create-queue default
absurdctl spawn-task hello-world -P name=Alice
absurdctl list-tasks --status=pending
absurdctl dump-task --task-id=<id>
absurdctl cleanup default 7
```
