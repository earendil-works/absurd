# Cleanup and Retention

Absurd keeps task history and event data in Postgres until you delete it.

That is useful for debugging and inspection, but it also means you need a
retention plan.  Otherwise old runs, checkpoints, waits, and events will simply
accumulate forever.

This page explains what cleanup removes, how to do it with
[`absurdctl`](./absurdctl.md), how to call the stored procedures directly, and
how to automate retention with real cron jobs or with an Absurd task of your
own.

## What Cleanup Removes

Absurd exposes two cleanup functions in SQL:

- `absurd.cleanup_tasks(queue, ttl_seconds, limit)`
- `absurd.cleanup_events(queue, ttl_seconds, limit)`

### Task cleanup

`absurd.cleanup_tasks` deletes **terminal** tasks older than the TTL:

- `completed`
- `failed`
- `cancelled`

It also deletes their related:

- wait registrations
- checkpoints
- runs
- the task row itself

It does **not** delete tasks that are still pending, running, or sleeping.

### Event cleanup

`absurd.cleanup_events` deletes emitted events older than the TTL.

## The Easiest Option: `absurdctl cleanup`

If you just want a simple retention job, use `absurdctl cleanup`.
It cleans up both old terminal tasks and old events for the target queue.

```bash
absurdctl cleanup default 7
```

That means:

- queue: `default`
- retention: `7` days

A couple more examples:

```bash
absurdctl cleanup emails 30
absurdctl cleanup reports 90
```

This is the best choice when:

- you already have shell access to the machine
- you want one simple command in cron
- "days" is good enough as your retention unit

## Direct SQL Cleanup

If you want finer control, call the stored procedures directly.

Unlike `absurdctl cleanup`, the SQL functions take TTL in **seconds** and let
you control the **batch size** explicitly.

```sql
select absurd.cleanup_tasks('default', 7 * 86400, 1000);
select absurd.cleanup_events('default', 7 * 86400, 1000);
```

The third argument is the maximum number of rows to delete in one call.

This is useful when:

- you want to run cleanup from your own application code
- you want second-level retention control
- you want to batch large deletions more carefully

## Batching Large Cleanups

If you have a lot of old data, it can be better to delete it in chunks.

For example, in `psql`:

```sql
select absurd.cleanup_tasks('default', 30 * 86400, 1000);
select absurd.cleanup_events('default', 30 * 86400, 1000);
```

Then run those repeatedly until they return `0`.

A simple shell loop looks like this:

```bash
#!/usr/bin/env bash
set -euo pipefail

QUEUE="default"
TTL_SECONDS=$((30 * 86400))
LIMIT=1000

while true; do
  deleted_tasks=$(psql "$PGDATABASE" -Atqc \
    "select absurd.cleanup_tasks('${QUEUE}', ${TTL_SECONDS}, ${LIMIT})")
  deleted_events=$(psql "$PGDATABASE" -Atqc \
    "select absurd.cleanup_events('${QUEUE}', ${TTL_SECONDS}, ${LIMIT})")

  echo "deleted tasks=${deleted_tasks} events=${deleted_events}"

  if [ "$deleted_tasks" = "0" ] && [ "$deleted_events" = "0" ]; then
    break
  fi

done
```

That pattern is handy when you are cleaning up a backlog for the first time.

## Real Cron Jobs with `absurdctl`

For most deployments, the simplest production setup is an ordinary OS cron job.

Run every day at 03:17:

```cron
17 3 * * * PGDATABASE=postgresql://user:pass@db/app absurdctl cleanup default 30 >> /var/log/absurd-cleanup.log 2>&1
```

For multiple queues, use a wrapper script:

```bash
#!/usr/bin/env bash
set -euo pipefail

export PGDATABASE="postgresql://user:pass@db/app"

absurdctl cleanup default 30
absurdctl cleanup emails 90
absurdctl cleanup reports 14
```

And then schedule that script:

```cron
17 3 * * * /srv/app/bin/absurd-retention.sh >> /var/log/absurd-cleanup.log 2>&1
```

This is usually the right answer if you do not need cleanup itself to be a
workflow.

## Using an Absurd Task for Cleanup

If you want cleanup to be observable in Habitat, retryable, and recorded like
other work, you can wrap it in an Absurd task.

A practical way to do that is:

1. register a `cleanup-retention` task
2. call the SQL cleanup functions inside steps using your normal Postgres client
3. schedule that task from cron or another scheduler
4. use a daily idempotency key so duplicate cron runs collapse into one task

### Example: cleanup task

=== "TypeScript"

    ```typescript
    import * as pg from 'pg';
    import { Absurd } from 'absurd-sdk';

    const pool = new pg.Pool({ connectionString: process.env.PGDATABASE });
    const app = new Absurd({ db: pool, queueName: 'ops' });

    app.registerTask({ name: 'cleanup-retention' }, async (params, ctx) => {
      const ttlSeconds = params.ttlDays * 86400;
      const limit = params.limit ?? 1000;

      const deletedTasks = await ctx.step('cleanup-tasks', async () => {
        const result = await pool.query(
          'select absurd.cleanup_tasks($1, $2, $3) as deleted',
          [params.targetQueue, ttlSeconds, limit],
        );
        return result.rows[0].deleted as number;
      });

      const deletedEvents = await ctx.step('cleanup-events', async () => {
        const result = await pool.query(
          'select absurd.cleanup_events($1, $2, $3) as deleted',
          [params.targetQueue, ttlSeconds, limit],
        );
        return result.rows[0].deleted as number;
      });

      return {
        targetQueue: params.targetQueue,
        ttlDays: params.ttlDays,
        deletedTasks,
        deletedEvents,
      };
    });

    await app.startWorker();
    ```

=== "Python"

    ```python
    import os

    from psycopg import Connection

    from absurd_sdk import Absurd

    conn = Connection.connect(os.environ["PGDATABASE"], autocommit=True)
    app = Absurd(conn, queue_name="ops")


    @app.register_task(name="cleanup-retention")
    def cleanup_retention(params, ctx):
        ttl_seconds = params["ttl_days"] * 86400
        limit = params.get("limit", 1000)

        def cleanup_tasks():
            return conn.execute(
                "select absurd.cleanup_tasks(%s, %s, %s)",
                (params["target_queue"], ttl_seconds, limit),
            ).fetchone()[0]

        deleted_tasks = ctx.step("cleanup-tasks", cleanup_tasks)

        def cleanup_events():
            return conn.execute(
                "select absurd.cleanup_events(%s, %s, %s)",
                (params["target_queue"], ttl_seconds, limit),
            ).fetchone()[0]

        deleted_events = ctx.step("cleanup-events", cleanup_events)

        return {
            "target_queue": params["target_queue"],
            "ttl_days": params["ttl_days"],
            "deleted_tasks": deleted_tasks,
            "deleted_events": deleted_events,
        }


    app.start_worker()
    ```

=== "Go"

    ```go
    package main

    import (
        "context"
        "database/sql"
        "log"
        "os"

        absurd "github.com/earendil-works/absurd/sdks/go"
    )

    type CleanupParams struct {
        TargetQueue string `json:"target_queue"`
        TTLDays     int    `json:"ttl_days"`
        Limit       int    `json:"limit"`
    }

    type CleanupResult struct {
        TargetQueue   string `json:"target_queue"`
        TTLDays       int    `json:"ttl_days"`
        DeletedTasks  int    `json:"deleted_tasks"`
        DeletedEvents int    `json:"deleted_events"`
    }

    func main() {
        db, err := sql.Open("postgres", os.Getenv("PGDATABASE"))
        if err != nil {
            log.Fatal(err)
        }
        defer db.Close()

        app, err := absurd.New(absurd.Options{DB: db, QueueName: "ops"})
        if err != nil {
            log.Fatal(err)
        }
        defer app.Close()

        app.MustRegister(absurd.Task(
            "cleanup-retention",
            func(ctx context.Context, params CleanupParams) (CleanupResult, error) {
                ttlSeconds := params.TTLDays * 86400
                limit := params.Limit
                if limit == 0 {
                    limit = 1000
                }

                deletedTasks, err := absurd.Step(ctx, "cleanup-tasks", func(ctx context.Context) (int, error) {
                    var deleted int
                    err := db.QueryRowContext(
                        ctx,
                        "select absurd.cleanup_tasks($1, $2, $3)",
                        params.TargetQueue,
                        ttlSeconds,
                        limit,
                    ).Scan(&deleted)
                    return deleted, err
                })
                if err != nil {
                    return CleanupResult{}, err
                }

                deletedEvents, err := absurd.Step(ctx, "cleanup-events", func(ctx context.Context) (int, error) {
                    var deleted int
                    err := db.QueryRowContext(
                        ctx,
                        "select absurd.cleanup_events($1, $2, $3)",
                        params.TargetQueue,
                        ttlSeconds,
                        limit,
                    ).Scan(&deleted)
                    return deleted, err
                })
                if err != nil {
                    return CleanupResult{}, err
                }

                return CleanupResult{
                    TargetQueue:   params.TargetQueue,
                    TTLDays:       params.TTLDays,
                    DeletedTasks:  deletedTasks,
                    DeletedEvents: deletedEvents,
                }, nil
            },
        ))

        if err := app.RunWorker(context.Background()); err != nil {
            log.Fatal(err)
        }
    }
    ```

And a small script that enqueues it once per day:

=== "TypeScript"

    ```typescript
    import * as pg from 'pg';
    import { Absurd } from 'absurd-sdk';

    const pool = new pg.Pool({ connectionString: process.env.PGDATABASE });
    const app = new Absurd({ db: pool, queueName: 'ops' });

    const day = new Date().toISOString().slice(0, 10);

    await app.spawn(
      'cleanup-retention',
      {
        targetQueue: 'default',
        ttlDays: 30,
        limit: 1000,
      },
      {
        idempotencyKey: `cleanup:default:${day}`,
      },
    );

    await app.close();
    ```

=== "Python"

    ```python
    import os
    from datetime import date

    from psycopg import Connection

    from absurd_sdk import Absurd

    conn = Connection.connect(os.environ["PGDATABASE"], autocommit=True)
    app = Absurd(conn, queue_name="ops")

    day = date.today().isoformat()

    app.spawn(
        "cleanup-retention",
        {
            "target_queue": "default",
            "ttl_days": 30,
            "limit": 1000,
        },
        idempotency_key=f"cleanup:default:{day}",
    )

    app.close()
    conn.close()
    ```

=== "Go"

    ```go
    package main

    import (
        "context"
        "database/sql"
        "fmt"
        "log"
        "os"
        "time"

        absurd "github.com/earendil-works/absurd/sdks/go"
    )

    type CleanupParams struct {
        TargetQueue string `json:"target_queue"`
        TTLDays     int    `json:"ttl_days"`
        Limit       int    `json:"limit"`
    }

    func main() {
        db, err := sql.Open("postgres", os.Getenv("PGDATABASE"))
        if err != nil {
            log.Fatal(err)
        }
        defer db.Close()

        app, err := absurd.New(absurd.Options{DB: db, QueueName: "ops"})
        if err != nil {
            log.Fatal(err)
        }
        defer app.Close()

        day := time.Now().UTC().Format("2006-01-02")

        _, err = app.Spawn(
            context.Background(),
            "cleanup-retention",
            CleanupParams{
                TargetQueue: "default",
                TTLDays:     30,
                Limit:       1000,
            },
            absurd.SpawnOptions{
                IdempotencyKey: fmt.Sprintf("cleanup:default:%s", day),
            },
        )
        if err != nil {
            log.Fatal(err)
        }
    }
    ```

Then run that enqueue script from cron:

=== "TypeScript"

    ```cron
    17 3 * * * PGDATABASE=postgresql://user:pass@db/app node /srv/app/bin/spawn-cleanup.js
    ```

=== "Python"

    ```cron
    17 3 * * * PGDATABASE=postgresql://user:pass@db/app uv run /srv/app/bin/spawn-cleanup.py
    ```

=== "Go"

    ```cron
    17 3 * * * PGDATABASE=postgresql://user:pass@db/app /srv/app/bin/spawn-cleanup
    ```

This example handles one cleanup batch per task run.  For steady-state daily
retention that is often enough.  If you are draining a large backlog, prefer the
SQL batching loop above or enqueue multiple cleanup tasks until the deleted
counts reach zero.

That gives you a nice hybrid:

- cron decides **when** cleanup should happen
- Absurd records **that** cleanup happened and retries it if needed

## Which Approach Should You Pick?

A good rule of thumb:

- use **`absurdctl cleanup`** if you want the simplest operational setup
- use the **SQL functions directly** if you need tighter batching or want to integrate with existing app/database tooling
- use an **Absurd cleanup task** if you want retention work to be tracked and retried like any other task

## Suggested Retention Strategy

Different queues often deserve different TTLs.

For example:

- `default`: 30 days
- `emails`: 90 days
- `reports`: 14 days
- agent/debug-heavy queues: longer, if you rely on history for investigation

The important part is not choosing the perfect number on day one.  The important
part is choosing **some** number and automating it.
