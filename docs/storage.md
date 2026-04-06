# Storage

Absurd supports two queue storage modes:

- **`unpartitioned`** (default)
- **`partitioned`** (weekly range partitions)

This page explains when to use each mode, what gets created in Postgres, and
how to automate partition creation and cleanup with `absurdctl` and `pg_cron`.

## Quick Recommendation

- Start with **unpartitioned** for small/medium workloads or when you want the
  simplest operations.
- Use **partitioned** when task history volume is high and you want easier,
  safer retention operations over time windows.

## What Each Mode Creates

For every queue, Absurd creates queue-local tables in the `absurd` schema:

- `t_<queue>` tasks
- `r_<queue>` runs
- `c_<queue>` checkpoints
- `e_<queue>` evente
- `w_<queue>` waits

For **partitioned** queues:

- `t_`, `r_`, `c_`, `w_` are declarative partitioned parent tables
- weekly child partitions are created with a suffix like `<YWW>` (ISO year + ISO week)
- default catch-all partitions are created with `_d` suffix
- an additional `i_<queue>` table is created for idempotency-key mapping

Both `i_<queue>` and `e_<queue>` remain unpartitioned.

## Creating Queues

Queues need to be created manually before usage though the SDKs can create them for you
as well.

### Unpartitioned

```bash
absurdctl create-queue jobs
```

Equivalent SQL:

```sql
select absurd.create_queue('jobs');
```

### Partitioned

```bash
absurdctl create-queue jobs --storage-mode partitioned
```

Equivalent SQL:

```sql
select absurd.create_queue('jobs', 'partitioned');
```

`create_queue` is idempotent if the queue already exists with the same storage
mode.  If you try to recreate with a different mode, Absurd raises an error.

## Queue Storage Policy

Each queue has policy fields in `absurd.queues`:

- `partition_lookahead` (default `28 days`)
- `partition_lookback` (default `1 day`)
- `cleanup_ttl` (default `30 days`)
- `cleanup_limit` (default `1000`)
- `detach_mode` (`none` or `empty`, default `none`)
- `detach_min_age` (default `30 days`)

Read/update policy with `absurdctl`:

```bash
# show
absurdctl queue-policy jobs

# update
absurdctl queue-policy jobs \
  --partition-lookahead '42 days' \
  --partition-lookback '2 days' \
  --cleanup-ttl '30 days' \
  --cleanup-limit 2000 \
  --detach-mode empty \
  --detach-min-age '30 days'
```

Equivalent SQL:

```sql
select absurd.set_queue_policy(
  'jobs',
  '{
    "partition_lookahead": "42 days",
    "partition_lookback": "2 days",
    "cleanup_ttl": "30 days",
    "cleanup_limit": 2000,
    "detach_mode": "empty",
    "detach_min_age": "30 days"
  }'::jsonb
);
```

## Partition Provisioning (Creating Partitions Ahead of Time)

Partitioned queues use `absurd.ensure_partitions(...)`.

For each partitioned queue, it creates partitions over this window:

- `start = week_bucket_utc(now - partition_lookback)`
- `end   = week_bucket_utc(now + partition_lookahead)`

Run manually:

```sql
-- one queue
select absurd.ensure_partitions('jobs');

-- all partitioned queues
select absurd.ensure_partitions();
```

This is safe to run repeatedly.

## Cleanup Retention

Cleanup is policy-driven per queue.

Run for all queues at once:

```sql
select * from absurd.cleanup_all_queues();
```

You can still run queue-specific cleanup directly:

```sql
select absurd.cleanup_tasks('jobs', 30 * 86400, 1000);
select absurd.cleanup_events('jobs', 30 * 86400, 1000);
```

See also:

- **[Cleanup and Retention](./cleanup.md)** for the policy-driven model
- **[absurdctl](./tools/absurdctl.md)** for operational commands

## Detaching Old Empty Partitions

For partitioned queues, Absurd supports policy-based detach planning:

1. `absurd.list_detach_candidates(...)` finds old, empty weekly partitions.
2. Those partitions can be detached with
   `ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY`.
3. Detached tables can then be dropped.

Why this is split: `DETACH ... CONCURRENTLY` must run as top-level SQL.

### Manual flow with `absurdctl`

```bash
absurdctl list-detach-candidates --queue jobs
absurdctl detach-candidate --queue jobs <partition_table>
absurdctl detach-candidate --queue jobs <partition_table> --drop
```

## Automating with `pg_cron`

If `pg_cron` is available, Absurd can manage cron jobs for you.

### One command via absurdctl

```bash
# Global jobs for all queues
absurdctl cron --enable

# Queue-scoped jobs with custom schedules
absurdctl cron --enable --queue jobs \
  --partition-schedule '*/15 * * * *' \
  --cleanup-schedule '7 * * * *' \
  --detach-schedule '29 * * * *'
```

Disable again:

```bash
absurdctl cron --disable
absurdctl cron --disable --queue jobs
```

### What gets scheduled

`absurd.enable_cron(...)` installs three recurring jobs:

1. **partition provisioning** (`absurd.ensure_partitions(...)`)
2. **cleanup** (`absurd.cleanup_all_queues(...)`)
3. **detach planning** (`absurd.schedule_detach_jobs(...)`)

Detach planning then creates per-partition detach/drop jobs as needed.

- detach jobs run top-level `DETACH PARTITION ... CONCURRENTLY`
- drop jobs call `absurd.drop_detached_partition(...)` until the table is safe to drop
- both unschedule themselves when done

## Practical Operating Model

A common setup for high-throughput queues:

1. Create queue as `partitioned`.
2. Set policy (`queue-policy`) for lookahead/lookback and retention.
3. Enable cron (`absurdctl cron --enable --queue ...`).
4. Periodically inspect:
   - `absurdctl queue-policy <queue>`
   - `absurdctl list-detach-candidates --queue <queue>`
   - `select * from cron.job` (if you want direct DB visibility)

For lower-volume queues, keep `unpartitioned` and only configure cleanup.

## Notes and Caveats

- Partitioning is currently based on UUIDv7 time ranges and weekly buckets.
- Default (`_d`) partitions are intentionally kept as a safety net for rows
  outside pre-created weekly windows.
- `detach_mode=empty` only detaches partitions that are both old enough and
  empty.
- `pg_cron` integration requires `cron.job` plus `cron.schedule` and
  `cron.unschedule` functions to exist.
