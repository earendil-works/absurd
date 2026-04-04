# Cron Jobs With Deduplication Keys

It's quite common that you need to run tasks on a schedule.  Usually that is expressed
in the form of [cron rules](https://en.wikipedia.org/wiki/Cron).  With absurd running
cronjobs is pretty simple even though it does not have a scheduler itself.

The only tricky is to ensure that each cron only runs once.  If your scheduler
runs twice (deploy overlap, crash restart, two replicas), you can still
guarantee each cron slot is enqueued only once.

The trick: derive an idempotency key from:

1. task name
2. cron expression
3. computed next execution slot (normalized to UTC minute)

If two scheduler processes compute the same slot, they produce the same key, and
Absurd returns the already-existing task instead of creating a duplicate.

## Example

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
