# Cleanup and Retention

Absurd keeps task history and events in Postgres until you remove them.

Cleanup works by setting a retention policy per queue which is then used by
the maintenance commands.  It works like this:

1. set retention policy per queue
2. run policy-aware cleanup for all queues
3. (for partitioned queues) combine row cleanup with partition lifecycle jobs (see below)

This page gives the high-level operating model and links to command details.

## Cleanup Is Policy-Driven Per Queue

Each queue has retention policy fields in `absurd.queues`, including:

- `cleanup_ttl`
- `cleanup_limit`

You usually manage these with:

```bash
absurdctl queue-policy <queue> --cleanup-ttl '30 days' --cleanup-limit 1000
```

Once policy is set, cleanup tasks will honor it:

```sql
select * from absurd.cleanup_all_queues();
```

That function applies each queue's own policy, so different queues can keep
history for different durations without separate scripts per queue.

See also:

- **[Storage](./storage.md)** for full policy fields (including partition policy)
- **[absurdctl](./tools/absurdctl.md)** for queue-policy command options

## Queue-Specific Cleanup

Queue-targeted cleanup is available for ad-hoc use:

- `absurdctl cleanup <queue> <ttl_days>`
- `absurd.cleanup_tasks(queue, ttl_seconds, limit)`
- `absurd.cleanup_events(queue, ttl_seconds, limit)`

## Partitioned Queue Lifecycle

For partitioned queues, cleanup is only one piece of retention.

A complete lifecycle is:

1. **Provision partitions ahead of time** with `absurd.ensure_partitions(...)`
2. **Delete old rows** with policy-driven cleanup (`absurd.cleanup_all_queues(...)`)
3. **Plan and execute detach/drop** for old empty partitions

Partition detach/drop is controlled by queue policy (`detach_mode`,
`detach_min_age`) and is operationally separate from row deletion with the
help of `DETACH PARTITION ... CONCURRENTLY`.

For details, see:

- **[Storage](./storage.md#detaching-old-empty-partitions)**
- **[absurdctl](./tools/absurdctl.md#partition-detach-operations)**

## Cron-Driven Maintenance

If `pg_cron` is available, let Absurd manage recurring maintenance jobs:

```bash
# all queues
absurdctl cron --enable

# queue-scoped schedules
absurdctl cron --enable --queue jobs \
  --partition-schedule '*/15 * * * *' \
  --cleanup-schedule '7 * * * *' \
  --detach-schedule '29 * * * *'
```

Under the hood, this schedules:

- partition provisioning (`ensure_partitions`)
- policy-driven cleanup (`cleanup_all_queues`)
- detach planning (`schedule_detach_jobs`)

This gives one coherent database-native maintenance loop for both
unpartitioned and partitioned queues.

## Using an Absurd Task for Cleanup

If you want cleanup to be visible in Habitat and retried like regular work,
you can wrap cleanup calls in an Absurd task.

This is also a good option when you do not use `pg_cron`: you can trigger
this task from any external scheduler (OS cron, systemd timer, CI, Kubernetes
CronJob, etc.) or run a one-off manual trigger script.

### Example cleanup task

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

### Manual trigger script (no `pg_cron` required)

You can enqueue one cleanup task per day with an idempotency key.

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

Then schedule that script in your existing scheduler, for example:

```cron
PGDATABASE=postgresql://user:pass@db/app
17 3 * * * uv run /srv/app/bin/spawn-cleanup.py
```

This model gives a simple split of responsibilities:

- external scheduler decides **when** cleanup should run
- Absurd records **that** cleanup ran, including retries and step history

## Practical Model by Storage Mode

- **Unpartitioned queues**: set `cleanup_ttl`/`cleanup_limit`, then run cron
  cleanup (`cleanup_all_queues`).
- **Partitioned queues**: same cleanup policy, plus partition provisioning and
  detach/drop planning.

If you only need one-off deletion, `absurdctl cleanup` is fine.
If you need stable production retention, use queue policy + Absurd-managed cron.
