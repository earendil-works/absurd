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


## Absurd Cron Job Registry

For schedules that should live with Absurd itself, use the declarative cron job
registry.  The registry keeps the schedule, target queue, target task, params,
and task options in `absurd.cron_jobs`.  `pg_cron` remains optional: when it is
available, it is an adapter that calls `absurd.run_cron_job(...)` for a registry
row.

This gives agents and operators a single Absurd-owned catalog to inspect without
making `cron.job` the source of truth.

```sql
select *
from absurd.register_cron_job(
  p_job_name => 'daily-report',
  p_queue_name => 'default',
  p_task_name => 'send-report',
  p_schedule => '0 8 * * *',
  p_params => '{"kind":"daily"}'::jsonb,
  p_task_options => '{"headers":{"owner":"ops"}}'::jsonb,
  p_description => 'Send the daily report',
  p_metadata => '{"team":"growth"}'::jsonb
);
```

To execute a due slot from an application-side scheduler:

```sql
select * from absurd.run_cron_job('daily-report');
```

`run_cron_job` derives an idempotency key from the job name and UTC minute slot,
then calls `spawn_task`.  Running the same job twice in the same minute returns
the same task instead of duplicating work.

If `pg_cron` is installed, Absurd can install, replace, and uninstall the adapter
job for you:

```sql
-- Register and install in one call.
select *
from absurd.register_cron_job(
  'daily-report',
  'default',
  'send-report',
  '0 8 * * *',
  '{"kind":"daily"}'::jsonb,
  '{}'::jsonb,
  true,
  'Send the daily report',
  '{}'::jsonb,
  true
);

-- Or install/uninstall separately.
select * from absurd.install_cron_job('daily-report');
select * from absurd.uninstall_cron_job('daily-report');
select * from absurd.unregister_cron_job('daily-report');
```

### Registry Shape

`absurd.cron_jobs` contains:

- `job_name` — stable logical name and primary key.
- `queue_name` — target Absurd queue; cascades when the queue is dropped.
- `task_name` — task to spawn.
- `schedule` — cron expression (validated by the scheduler adapter when
  installed).
- `params` — JSON object passed to the task unchanged.
- `task_options` — JSON object passed to `spawn_task`, except
  `idempotency_key` is forbidden because Absurd derives it per schedule slot.
- `enabled` — disabled jobs remain cataloged but `run_cron_job` does not spawn.
- `description` and `metadata` — operator/agent-readable context.
- `pgcron_job_id` — last installed `pg_cron` adapter job ID, when installed.

`run_cron_job` adds scheduling context to spawn headers instead of mutating task
params:

- `absurd_cron_job`
- `absurd_cron_schedule`
- `absurd_cron_scheduled_for`

### Adversarial Design Notes

The registry is deliberately a small **module** with a narrow **interface**:
register a schedule, install an optional adapter, run a due slot, and unregister
it.  The implementation hides the messy parts that would otherwise leak to every
caller.

Important failure modes and mitigations:

1. **Duplicate scheduler runs** — multiple scheduler processes or overlapping
   `pg_cron` executions can call `run_cron_job` for the same UTC minute.  The
   derived idempotency key collapses those calls into one task.
2. **Registry/adapter drift** — `pg_cron` is treated as an adapter, not source of
   truth.  Reinstalling a job unschedules existing adapter rows with the same
   deterministic adapter name before scheduling the replacement.
3. **Command injection** — `install_cron_job` quotes the logical job name when it
   builds the adapter command.
4. **User-controlled idempotency** — registry task options reject
   `idempotency_key`; otherwise one schedule could accidentally suppress another
   slot forever.
5. **Queue deletion** — `drop_queue` unregisters queue-scoped cron jobs before
   deleting queue tables, avoiding orphaned `pg_cron` jobs that fail forever.
6. **Scheduler portability** — application-side schedulers can call
   `run_cron_job`; Postgres-side schedulers can use `install_cron_job`.  The
   registry does not require `pg_cron` to exist.
7. **Cron expression validation** — plain Postgres does not parse cron syntax.
   Application-side schedulers should validate expressions before calling the
   registry; `pg_cron` installations validate by attempting `cron.schedule`.

### Candidate Jobs From a Dexter/Hermes Audit

The following operator-specific jobs were identified while designing the
registry.  They are **candidates for registry rows**, not defaults installed by
Absurd:

| Job | Schedule | Target task idea | Notes |
| --- | --- | --- | --- |
| Daily Sentry top unresolved one-shot | `0 7 * * *` | `diggy.sentry.top-unresolved-one-shot` | Triage the highest-frequency unresolved Diggy Sentry issue and start the one-shot remediation workflow. |
| Diggy weekly mining news to second brain | `0 8 * * 1` | `diggy.marketing.weekly-mining-news` | Collect weekly mining/field-ops news into the Diggy second brain. |
| Diggy prod daily usability analysis report | `0 8 * * *` | `diggy.prod.usability-analysis` | Produce anonymized production usability insights from PostHog/Sentry. |
| Daily architecture improvement with Effect best practices | `0 9 * * *` | `diggy.architecture.daily-review` | Run a read-only architecture review using the local Effect source. |
| CMO blog implementation shepherd watchdog | `every 10m` | `diggy.workstream.shepherd` | Temporary bounded workstream watchdog. Convert to cron syntax if persisted in Postgres. |
| DIG-5234 auth bootstrap one-shot watchdog | `every 10m` | `diggy.one-shot.watchdog` | Temporary bounded watchdog for a Diggy one-shot. |
| DIG-5236 asset detail one-shot watchdog | `every 15m` | `diggy.one-shot.watchdog` | Temporary bounded watchdog for a Diggy one-shot. |
| Diggy P0/P1 performance architecture shepherd | `every 15m` | `diggy.performance.shepherd` | Temporary bounded shepherd for performance work. |
| Diggy performance PR adversarial merge/deploy shepherd | `every 10m` | `diggy.pr.merge-deploy-shepherd` | Temporary bounded PR/deploy shepherd. |
| Absurd queued/running task monitor | operator-defined | `absurd.ops.task-monitor` | Mentioned as a useful monitor for queued/running Absurd tasks. |
| Scheduled/autonomous Sentry remediation | operator-defined | `diggy.sentry.autonomous-remediation` | Broader recurring remediation idea; the daily top unresolved job is the concrete instance. |
