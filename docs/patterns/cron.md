# Scheduling Tasks with Cron

Absurd does not include a built-in scheduler, but scheduling is straightforward.
There are two common approaches:

1. **Application-side scheduler + idempotency keys**
2. **Database-side scheduler with `pg_cron`**

## Application-side Scheduler

Run a small scheduler process (or serverless job) that evaluates cron
expressions and calls `spawn`.

Here you should use an idempotency key derived from:

1. task name
2. cron expression
3. computed execution slot (UTC minute)

That ensures duplicate scheduler runs (deploy overlap, crash restart, multiple
replicas) collapse into a single Absurd task.

=== "TypeScript"

    ```typescript
    import { createHash } from "node:crypto";

    import { Absurd } from "absurd-sdk";
    import { CronExpressionParser } from "cron-parser";

    const app = new Absurd({ queueName: "default" });

    const CRONTAB: Array<[expr: string, taskName: string]> = [
      ["*/5 * * * *", "send-report"],
      ["0 2 * * *", "rebuild-search-index"],
    ];

    function dedupKey(taskName: string, expr: string, nextAt: Date): string {
      const slot = nextAt.toISOString().slice(0, 16); // minute precision, UTC
      const raw = `${taskName}|${expr}|${slot}`;
      return `cron:${createHash("sha256")
        .update(raw)
        .digest("hex")
        .slice(0, 24)}`;
    }

    const now = new Date();
    now.setUTCSeconds(0, 0);

    for (const [expr, taskName] of CRONTAB) {
      const nextAt = CronExpressionParser.parse(expr, {
        currentDate: now,
        tz: "UTC",
      })
        .next()
        .toDate();

      await app.spawn(
        taskName,
        { scheduledFor: nextAt.toISOString() },
        { idempotencyKey: dedupKey(taskName, expr, nextAt) },
      );
    }

    await app.close();
    ```

=== "Python"

    ```python
    from datetime import datetime, timezone
    from hashlib import sha256

    from absurd_sdk import Absurd
    from croniter import croniter

    app = Absurd(queue_name="default")

    CRONTAB = [
        ("*/5 * * * *", "send-report"),
        ("0 2 * * *", "rebuild-search-index"),
    ]


    def dedup_key(task_name: str, expr: str, next_at: datetime) -> str:
        slot = next_at.astimezone(timezone.utc).isoformat(timespec="minutes")
        raw = f"{task_name}|{expr}|{slot}"
        return "cron:" + sha256(raw.encode()).hexdigest()[:24]


    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    for expr, task_name in CRONTAB:
        next_at = croniter(expr, now).get_next(datetime)
        app.spawn(
            task_name,
            {"scheduled_for": next_at.isoformat()},
            idempotency_key=dedup_key(task_name, expr, next_at),
        )
    ```

=== "Go"

    ```go
    package main

    import (
        "context"
        "crypto/sha256"
        "encoding/hex"
        "log"
        "time"

        "github.com/earendil-works/absurd/sdks/go/absurd"
        "github.com/robfig/cron/v3"
    )

    var crontab = [][2]string{
        {"*/5 * * * *", "send-report"},
        {"0 2 * * *", "rebuild-search-index"},
    }

    func dedupKey(taskName, expr string, nextAt time.Time) string {
        slot := nextAt.UTC().Format("2006-01-02T15:04")
        raw := taskName + "|" + expr + "|" + slot
        sum := sha256.Sum256([]byte(raw))
        return "cron:" + hex.EncodeToString(sum[:])[:24]
    }

    func main() {
        app, err := absurd.New(absurd.Options{QueueName: "default"})
        if err != nil {
            log.Fatal(err)
        }
        defer app.Close()

        parser := cron.NewParser(
            cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
        )
        now := time.Now().UTC().Truncate(time.Minute)
        ctx := context.Background()

        for _, entry := range crontab {
            expr := entry[0]
            taskName := entry[1]

            schedule, err := parser.Parse(expr)
            if err != nil {
                log.Fatal(err)
            }

            nextAt := schedule.Next(now)
            _, err = app.Spawn(
                ctx,
                taskName,
                map[string]string{
                    "scheduled_for": nextAt.Format(time.RFC3339),
                },
                absurd.SpawnOptions{
                    IdempotencyKey: dedupKey(taskName, expr, nextAt),
                },
            )
            if err != nil {
                log.Fatal(err)
            }
        }
    }
    ```

## Postgres-side Scheduler With `pg_cron`

If you already run Postgres with [`pg_cron`](https://github.com/citusdata/pg_cron),
you can schedule `absurd.spawn_task(...)` directly inside the database.

This is often simpler operationally: no separate scheduler process, and one
`pg_cron` job entry maps to one schedule.

### Setup

```sql
create extension if not exists pg_cron;
```

> Depending on your Postgres setup, `pg_cron` may require
> `shared_preload_libraries = 'pg_cron'` and a restart.

### Schedule a Task Spawn

```sql
select cron.schedule(
  'absurd-send-report-every-5m',
  '*/5 * * * *',
  $$
  select absurd.spawn_task(
    'default',
    'send-report',
    jsonb_build_object('scheduled_for', now())
  );
  $$
);
```

### Daily Job Example

```sql
select cron.schedule(
  'absurd-rebuild-search-index-daily',
  '0 2 * * *',
  $$
  select absurd.spawn_task(
    'default',
    'rebuild-search-index',
    jsonb_build_object('scheduled_for', now())
  );
  $$
);
```
