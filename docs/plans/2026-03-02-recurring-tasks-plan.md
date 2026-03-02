# Recurring / Scheduled Tasks Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add recurring/scheduled task support via per-queue `s_{queue}` tables, a PL/pgSQL cron parser, and a tick engine that fires inside `claim_task`.

**Architecture:** New `s_{queue}` table per queue stores schedule definitions with cron expressions. A `tick_schedules` function, called at the start of every `claim_task`, finds due schedules and spawns tasks idempotently. A `next_cron_time` function (pure PL/pgSQL) parses standard 5-field cron expressions + shorthands + `@every N`.

**Tech Stack:** PL/pgSQL (all core logic), Python psycopg (tests), TypeScript pg (SDK wrappers)

---

## Task 1: Cron Field Parser — `parse_cron_field`

This is the lowest-level building block. It expands a single cron field string (e.g., `*/5`, `1,3,5`, `9-17/2`) into a sorted integer array of matching values.

**Files:**
- Modify: `sql/absurd.sql` (append after line 53, before `ensure_queue_tables`)
- Create: `tests/test_schedules.py`

**Step 1: Write the failing test**

Add `tests/test_schedules.py`:

```python
from datetime import datetime, timedelta, timezone

from psycopg.sql import SQL


def test_parse_cron_field_wildcard(client):
    """Wildcard '*' expands to all values in range."""
    queue = "cron-field"
    client.create_queue(queue)
    result = client.conn.execute(
        "select absurd.parse_cron_field('*', 0, 59)"
    ).fetchone()[0]
    assert result == list(range(0, 60))


def test_parse_cron_field_exact(client):
    """Single value '5' returns [5]."""
    result = client.conn.execute(
        "select absurd.parse_cron_field('5', 0, 59)"
    ).fetchone()[0]
    assert result == [5]


def test_parse_cron_field_step(client):
    """Step '*/15' over 0-59 gives [0, 15, 30, 45]."""
    result = client.conn.execute(
        "select absurd.parse_cron_field('*/15', 0, 59)"
    ).fetchone()[0]
    assert result == [0, 15, 30, 45]


def test_parse_cron_field_range(client):
    """Range '9-12' gives [9, 10, 11, 12]."""
    result = client.conn.execute(
        "select absurd.parse_cron_field('9-12', 0, 23)"
    ).fetchone()[0]
    assert result == [9, 10, 11, 12]


def test_parse_cron_field_range_step(client):
    """Range with step '9-17/2' gives [9, 11, 13, 15, 17]."""
    result = client.conn.execute(
        "select absurd.parse_cron_field('9-17/2', 0, 23)"
    ).fetchone()[0]
    assert result == [9, 11, 13, 15, 17]


def test_parse_cron_field_list(client):
    """List '1,3,5' gives [1, 3, 5]."""
    result = client.conn.execute(
        "select absurd.parse_cron_field('1,3,5', 0, 6)"
    ).fetchone()[0]
    assert result == [1, 3, 5]


def test_parse_cron_field_list_with_range(client):
    """Mixed list '1,3-5,7' gives [1, 3, 4, 5, 7]."""
    result = client.conn.execute(
        "select absurd.parse_cron_field('1,3-5,7', 0, 10)"
    ).fetchone()[0]
    assert result == [1, 3, 4, 5, 7]


def test_parse_cron_field_day_names(client):
    """Day names 'MON-FRI' expanded to [1, 2, 3, 4, 5]."""
    result = client.conn.execute(
        "select absurd.parse_cron_field('MON-FRI', 0, 6)"
    ).fetchone()[0]
    assert result == [1, 2, 3, 4, 5]


def test_parse_cron_field_month_names(client):
    """Month names 'JAN,JUN,DEC' expanded to [1, 6, 12]."""
    result = client.conn.execute(
        "select absurd.parse_cron_field('JAN,JUN,DEC', 1, 12)"
    ).fetchone()[0]
    assert result == [1, 6, 12]
```

**Step 2: Run tests to verify they fail**

Run: `cd tests && uv run pytest test_schedules.py -v -x`
Expected: FAIL — `absurd.parse_cron_field` does not exist

**Step 3: Implement `parse_cron_field`**

Add to `sql/absurd.sql` after the `current_time` function (after line 53), before `ensure_queue_tables`:

```sql
-- Expands a single cron field (e.g., '*/5', '1,3,5', '9-17/2', 'MON-FRI')
-- into a sorted array of integers within [p_min..p_max].
create function absurd.parse_cron_field (
  p_field text,
  p_min integer,
  p_max integer
)
  returns integer[]
  language plpgsql
  immutable
as $$
declare
  v_field text := upper(trim(p_field));
  v_parts text[];
  v_part text;
  v_result integer[] := '{}';
  v_range_parts text[];
  v_lo integer;
  v_hi integer;
  v_step integer;
  v_val integer;
  v_base text;
  v_step_str text;
  v_names text[][] := array[
    array['SUN','0'],array['MON','1'],array['TUE','2'],array['WED','3'],
    array['THU','4'],array['FRI','5'],array['SAT','6'],
    array['JAN','1'],array['FEB','2'],array['MAR','3'],array['APR','4'],
    array['MAY','5'],array['JUN','6'],array['JUL','7'],array['AUG','8'],
    array['SEP','9'],array['OCT','10'],array['NOV','11'],array['DEC','12']
  ];
  v_i integer;
begin
  -- Replace named constants (MON, JAN, etc.)
  for v_i in 1..array_length(v_names, 1) loop
    v_field := replace(v_field, v_names[v_i][1], v_names[v_i][2]);
  end loop;

  -- Split by comma for lists
  v_parts := string_to_array(v_field, ',');

  foreach v_part in array v_parts loop
    v_part := trim(v_part);

    -- Split by '/' for step values
    if position('/' in v_part) > 0 then
      v_base := split_part(v_part, '/', 1);
      v_step := split_part(v_part, '/', 2)::integer;
    else
      v_base := v_part;
      v_step := 1;
    end if;

    -- Determine range
    if v_base = '*' then
      v_lo := p_min;
      v_hi := p_max;
    elsif position('-' in v_base) > 0 then
      v_lo := split_part(v_base, '-', 1)::integer;
      v_hi := split_part(v_base, '-', 2)::integer;
    else
      -- Exact value
      v_lo := v_base::integer;
      v_hi := v_lo;
    end if;

    -- Clamp to bounds
    v_lo := greatest(v_lo, p_min);
    v_hi := least(v_hi, p_max);

    -- Expand range with step
    v_val := v_lo;
    while v_val <= v_hi loop
      if not (v_val = any(v_result)) then
        v_result := v_result || v_val;
      end if;
      v_val := v_val + v_step;
    end loop;
  end loop;

  -- Sort
  select array_agg(x order by x) into v_result from unnest(v_result) as x;

  return v_result;
end;
$$;
```

**Step 4: Run tests to verify they pass**

Run: `cd tests && uv run pytest test_schedules.py -v`
Expected: All 9 tests PASS

**Step 5: Commit**

```bash
git add sql/absurd.sql tests/test_schedules.py
git commit -m "feat: add parse_cron_field PL/pgSQL function

Expands a single cron field (wildcards, ranges, steps, lists, named
days/months) into a sorted integer array. Foundation for the cron
parser needed by recurring schedules."
```

---

## Task 2: Cron Expression Parser — `next_cron_time`

Takes a full schedule expression and a reference timestamp, returns the next matching time.

**Files:**
- Modify: `sql/absurd.sql` (append after `parse_cron_field`)
- Modify: `tests/test_schedules.py` (append tests)

**Step 1: Write the failing tests**

Append to `tests/test_schedules.py`:

```python
def test_next_cron_time_every_minute(client):
    """'* * * * *' from 09:30:45 → 09:31:00."""
    result = client.conn.execute(
        "select absurd.next_cron_time('* * * * *', '2024-06-15 09:30:45+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 15, 9, 31, tzinfo=timezone.utc)


def test_next_cron_time_specific_time(client):
    """'30 9 * * *' from 09:00 → same day 09:30; from 09:31 → next day 09:30."""
    result = client.conn.execute(
        "select absurd.next_cron_time('30 9 * * *', '2024-06-15 09:00:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 15, 9, 30, tzinfo=timezone.utc)

    result = client.conn.execute(
        "select absurd.next_cron_time('30 9 * * *', '2024-06-15 09:31:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 16, 9, 30, tzinfo=timezone.utc)


def test_next_cron_time_step(client):
    """'*/15 * * * *' from 09:07 → 09:15."""
    result = client.conn.execute(
        "select absurd.next_cron_time('*/15 * * * *', '2024-06-15 09:07:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 15, 9, 15, tzinfo=timezone.utc)


def test_next_cron_time_day_of_week(client):
    """'0 9 * * 1' (Monday 9am). 2024-06-15 is Saturday → Monday 2024-06-17."""
    result = client.conn.execute(
        "select absurd.next_cron_time('0 9 * * 1', '2024-06-15 10:00:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 17, 9, 0, tzinfo=timezone.utc)


def test_next_cron_time_shorthand_daily(client):
    """'@daily' from 00:01 → next day 00:00."""
    result = client.conn.execute(
        "select absurd.next_cron_time('@daily', '2024-06-15 00:01:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 16, 0, 0, tzinfo=timezone.utc)


def test_next_cron_time_shorthand_hourly(client):
    """'@hourly' from 09:15 → 10:00."""
    result = client.conn.execute(
        "select absurd.next_cron_time('@hourly', '2024-06-15 09:15:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 15, 10, 0, tzinfo=timezone.utc)


def test_next_cron_time_every_interval(client):
    """'@every 300' from 09:10:00 → 09:15:00."""
    result = client.conn.execute(
        "select absurd.next_cron_time('@every 300', '2024-06-15 09:10:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 15, 9, 15, tzinfo=timezone.utc)


def test_next_cron_time_month_boundary(client):
    """'0 0 1 * *' (first of month) from Jan 2 → Feb 1."""
    result = client.conn.execute(
        "select absurd.next_cron_time('0 0 1 * *', '2024-01-02 00:00:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 2, 1, 0, 0, tzinfo=timezone.utc)


def test_next_cron_time_year_boundary(client):
    """'0 0 1 1 *' (Jan 1 midnight) from Dec 2024 → Jan 1 2025."""
    result = client.conn.execute(
        "select absurd.next_cron_time('0 0 1 1 *', '2024-12-15 00:00:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
```

**Step 2: Run tests to verify they fail**

Run: `cd tests && uv run pytest test_schedules.py::test_next_cron_time_every_minute -v -x`
Expected: FAIL — `absurd.next_cron_time` does not exist

**Step 3: Implement `next_cron_time`**

Add to `sql/absurd.sql` after `parse_cron_field`:

```sql
-- Returns the next timestamp that matches a schedule expression, strictly
-- after p_after.
--
-- Supported expressions:
--   5-field cron:  "minute hour dom month dow" (standard Unix)
--   Shorthands:    @yearly, @annually, @monthly, @weekly, @daily, @midnight, @hourly
--   Interval:      @every N  (N in seconds, advances from p_after)
create function absurd.next_cron_time (
  p_expr text,
  p_after timestamptz
)
  returns timestamptz
  language plpgsql
  immutable
as $$
declare
  v_expr text := trim(p_expr);
  v_fields text[];
  v_minutes int[];
  v_hours int[];
  v_doms int[];
  v_months int[];
  v_dows int[];
  v_has_dow boolean;
  v_has_dom boolean;
  v_candidate timestamptz;
  v_year int;
  v_month int;
  v_dom int;
  v_hour int;
  v_minute int;
  v_max_dom int;
  v_dow int;
  v_after_utc timestamptz;
  v_safety int := 0;
begin
  v_after_utc := p_after at time zone 'UTC';

  -- Handle @every N (interval in seconds)
  if v_expr ~* '^@every\s+' then
    return v_after_utc + (regexp_replace(v_expr, '^@every\s+', '', 'i')::integer * interval '1 second');
  end if;

  -- Handle shorthands
  case lower(v_expr)
    when '@yearly', '@annually' then v_expr := '0 0 1 1 *';
    when '@monthly'             then v_expr := '0 0 1 * *';
    when '@weekly'              then v_expr := '0 0 * * 0';
    when '@daily', '@midnight'  then v_expr := '0 0 * * *';
    when '@hourly'              then v_expr := '0 * * * *';
    else null;
  end case;

  -- Parse 5 fields
  v_fields := regexp_split_to_array(v_expr, '\s+');
  if array_length(v_fields, 1) <> 5 then
    raise exception 'Invalid cron expression: expected 5 fields, got %', array_length(v_fields, 1);
  end if;

  v_minutes := absurd.parse_cron_field(v_fields[1], 0, 59);
  v_hours   := absurd.parse_cron_field(v_fields[2], 0, 23);
  v_doms    := absurd.parse_cron_field(v_fields[3], 1, 31);
  v_months  := absurd.parse_cron_field(v_fields[4], 1, 12);
  v_dows    := absurd.parse_cron_field(v_fields[5], 0, 6);

  v_has_dom := v_fields[3] <> '*';
  v_has_dow := v_fields[5] <> '*';

  -- Start scanning from the minute after p_after (truncated to minute)
  v_candidate := date_trunc('minute', v_after_utc) + interval '1 minute';
  v_year := extract(year from v_candidate)::int;

  -- Scan forward (capped at 4 years to avoid infinite loops on impossible expressions)
  while v_safety < 366 * 24 * 60 * 4 loop
    v_month  := extract(month from v_candidate)::int;
    v_dom    := extract(day from v_candidate)::int;
    v_hour   := extract(hour from v_candidate)::int;
    v_minute := extract(minute from v_candidate)::int;
    v_dow    := extract(dow from v_candidate)::int;

    -- Check month
    if not (v_month = any(v_months)) then
      -- Skip to first day of next matching month
      v_candidate := date_trunc('month', v_candidate) + interval '1 month';
      while not (extract(month from v_candidate)::int = any(v_months)) loop
        v_candidate := v_candidate + interval '1 month';
        v_safety := v_safety + 28 * 24 * 60;
        if v_safety >= 366 * 24 * 60 * 4 then
          raise exception 'Could not find next cron time within 4 years';
        end if;
      end loop;
      continue;
    end if;

    -- Check day (DOM and DOW interaction per cron standard:
    -- if both are restricted, match either; if only one is restricted, match that one)
    if v_has_dom and v_has_dow then
      if not ((v_dom = any(v_doms)) or (v_dow = any(v_dows))) then
        v_candidate := date_trunc('day', v_candidate) + interval '1 day';
        v_safety := v_safety + 24 * 60;
        continue;
      end if;
    elsif v_has_dom then
      if not (v_dom = any(v_doms)) then
        v_candidate := date_trunc('day', v_candidate) + interval '1 day';
        v_safety := v_safety + 24 * 60;
        continue;
      end if;
    elsif v_has_dow then
      if not (v_dow = any(v_dows)) then
        v_candidate := date_trunc('day', v_candidate) + interval '1 day';
        v_safety := v_safety + 24 * 60;
        continue;
      end if;
    end if;

    -- Check hour
    if not (v_hour = any(v_hours)) then
      v_candidate := date_trunc('hour', v_candidate) + interval '1 hour';
      v_safety := v_safety + 60;
      continue;
    end if;

    -- Check minute
    if not (v_minute = any(v_minutes)) then
      v_candidate := v_candidate + interval '1 minute';
      v_safety := v_safety + 1;
      continue;
    end if;

    -- All fields match
    return v_candidate;
  end loop;

  raise exception 'Could not find next cron time within 4 years';
end;
$$;
```

**Step 4: Run tests to verify they pass**

Run: `cd tests && uv run pytest test_schedules.py -v`
Expected: All tests PASS (both parse_cron_field and next_cron_time tests)

**Step 5: Commit**

```bash
git add sql/absurd.sql tests/test_schedules.py
git commit -m "feat: add next_cron_time PL/pgSQL function

Parses 5-field cron expressions, shorthands (@daily, @hourly, etc.),
and @every N interval syntax. Returns the next matching timestamp
after a given reference time."
```

---

## Task 3: Schedule Table — `s_{queue}`

Add the schedule table to queue creation/deletion.

**Files:**
- Modify: `sql/absurd.sql` — `ensure_queue_tables` (line ~60), `drop_queue` (line ~190)
- Modify: `tests/test_schedules.py` (append tests)

**Step 1: Write the failing test**

Append to `tests/test_schedules.py`:

```python
def test_schedule_table_created_with_queue(client):
    """create_queue should also create the s_{queue} table."""
    queue = "sched-tbl"
    client.create_queue(queue)

    # The s_ table should exist
    result = client.conn.execute(
        "select count(*) from information_schema.tables "
        "where table_schema = 'absurd' and table_name = %s",
        (f"s_{queue}",)
    ).fetchone()[0]
    assert result == 1


def test_schedule_table_dropped_with_queue(client):
    """drop_queue should also drop the s_{queue} table."""
    queue = "sched-drop"
    client.create_queue(queue)
    client.drop_queue(queue)

    result = client.conn.execute(
        "select count(*) from information_schema.tables "
        "where table_schema = 'absurd' and table_name = %s",
        (f"s_{queue}",)
    ).fetchone()[0]
    assert result == 0
```

**Step 2: Run tests to verify they fail**

Run: `cd tests && uv run pytest test_schedules.py::test_schedule_table_created_with_queue -v -x`
Expected: FAIL — table `s_sched-tbl` does not exist

**Step 3: Add `s_{queue}` to `ensure_queue_tables` and `drop_queue`**

In `ensure_queue_tables` (after the `w_` table creation and indexes, around line 158), add:

```sql
  execute format(
    'create table if not exists absurd.%I (
        schedule_name text primary key,
        task_name text not null,
        params jsonb not null default ''{}''::jsonb,
        headers jsonb,
        retry_strategy jsonb,
        max_attempts integer,
        cancellation jsonb,
        schedule_expr text not null,
        enabled boolean not null default true,
        catchup_policy text not null default ''skip''
            check (catchup_policy in (''skip'', ''all'')),
        last_triggered_at timestamptz,
        next_run_at timestamptz not null,
        created_at timestamptz not null default absurd.current_time()
     )',
    's_' || p_queue_name
  );
```

In `drop_queue`, add before the existing `w_` drop (around line 205):

```sql
  execute format('drop table if exists absurd.%I cascade', 's_' || p_queue_name);
```

**Step 4: Run tests to verify they pass**

Run: `cd tests && uv run pytest test_schedules.py -v`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add sql/absurd.sql tests/test_schedules.py
git commit -m "feat: add s_{queue} schedule table to queue lifecycle

create_queue now also creates s_{queue} for schedule definitions.
drop_queue drops it alongside the other per-queue tables."
```

---

## Task 4: Schedule CRUD — `create_schedule`, `get_schedule`, `list_schedules`, `delete_schedule`, `update_schedule`

**Files:**
- Modify: `sql/absurd.sql` (append new functions)
- Modify: `tests/test_schedules.py` (append tests)
- Modify: `tests/conftest.py` (add helper methods to `AbsurdTestClient`)

**Step 1: Write the failing tests**

First, add helpers to `tests/conftest.py` (append to `AbsurdTestClient` class):

```python
    def create_schedule(self, queue, schedule_name, task_name, schedule_expr,
                        params=None, options=None):
        opts = options or {}
        if params is not None:
            opts["params"] = params
        result = self.conn.execute(
            "select * from absurd.create_schedule(%s, %s, %s, %s, %s)",
            (queue, schedule_name, task_name, schedule_expr, Jsonb(opts)),
        )
        return _fetchone_dict(result)

    def get_schedule(self, queue, schedule_name):
        result = self.conn.execute(
            "select * from absurd.get_schedule(%s, %s)",
            (queue, schedule_name),
        )
        return _fetchone_dict(result)

    def list_schedules(self, queue):
        result = self.conn.execute(
            "select * from absurd.list_schedules(%s)",
            (queue,),
        )
        return _fetchall_dicts(result)

    def delete_schedule(self, queue, schedule_name):
        self.conn.execute(
            "select absurd.delete_schedule(%s, %s)",
            (queue, schedule_name),
        )

    def update_schedule(self, queue, schedule_name, options):
        self.conn.execute(
            "select absurd.update_schedule(%s, %s, %s)",
            (queue, schedule_name, Jsonb(options)),
        )
```

Then append to `tests/test_schedules.py`:

```python
def test_create_and_get_schedule(client):
    """Create a schedule and retrieve it."""
    queue = "sched-crud"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    result = client.create_schedule(
        queue, "my-job", "process-data", "*/5 * * * *"
    )
    assert result["schedule_name"] == "my-job"
    assert result["next_run_at"] is not None

    sched = client.get_schedule(queue, "my-job")
    assert sched["task_name"] == "process-data"
    assert sched["schedule_expr"] == "*/5 * * * *"
    assert sched["enabled"] is True
    assert sched["catchup_policy"] == "skip"
    # next_run_at should be 10:05 (next */5 after 10:00)
    assert sched["next_run_at"] == datetime(2024, 7, 1, 10, 5, tzinfo=timezone.utc)


def test_create_schedule_with_options(client):
    """Create a schedule with custom params and options."""
    queue = "sched-opts"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(
        queue, "daily-report", "gen-report", "@daily",
        params={"format": "pdf"},
        options={
            "params": {"format": "pdf"},
            "headers": {"trace-id": "abc"},
            "max_attempts": 3,
            "catchup_policy": "all",
        },
    )
    sched = client.get_schedule(queue, "daily-report")
    assert sched["params"] == {"format": "pdf"}
    assert sched["headers"] == {"trace-id": "abc"}
    assert sched["max_attempts"] == 3
    assert sched["catchup_policy"] == "all"


def test_list_schedules(client):
    """list_schedules returns all schedules for a queue."""
    queue = "sched-list"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "job-a", "task-a", "@hourly")
    client.create_schedule(queue, "job-b", "task-b", "@daily")

    schedules = client.list_schedules(queue)
    names = [s["schedule_name"] for s in schedules]
    assert "job-a" in names
    assert "job-b" in names


def test_delete_schedule(client):
    """delete_schedule removes the schedule."""
    queue = "sched-del"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "temp-job", "task-x", "@hourly")
    assert client.get_schedule(queue, "temp-job") is not None

    client.delete_schedule(queue, "temp-job")
    assert client.get_schedule(queue, "temp-job") is None


def test_update_schedule_expression(client):
    """update_schedule changes expression and recomputes next_run_at."""
    queue = "sched-upd"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "upd-job", "task-y", "*/5 * * * *")
    sched_before = client.get_schedule(queue, "upd-job")
    assert sched_before["next_run_at"] == datetime(2024, 7, 1, 10, 5, tzinfo=timezone.utc)

    client.update_schedule(queue, "upd-job", {"schedule_expr": "@hourly"})
    sched_after = client.get_schedule(queue, "upd-job")
    assert sched_after["schedule_expr"] == "@hourly"
    assert sched_after["next_run_at"] == datetime(2024, 7, 1, 11, 0, tzinfo=timezone.utc)


def test_update_schedule_disable(client):
    """update_schedule can disable a schedule."""
    queue = "sched-dis"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "dis-job", "task-z", "@hourly")
    client.update_schedule(queue, "dis-job", {"enabled": False})

    sched = client.get_schedule(queue, "dis-job")
    assert sched["enabled"] is False
```

**Step 2: Run tests to verify they fail**

Run: `cd tests && uv run pytest test_schedules.py::test_create_and_get_schedule -v -x`
Expected: FAIL — `absurd.create_schedule` does not exist

**Step 3: Implement the CRUD functions**

Append to `sql/absurd.sql` (after `next_cron_time`, before `ensure_queue_tables`):

```sql
-- Creates a schedule in a queue.
create function absurd.create_schedule (
  p_queue_name text,
  p_schedule_name text,
  p_task_name text,
  p_schedule_expr text,
  p_options jsonb default '{}'::jsonb
)
  returns table (
    schedule_name text,
    next_run_at timestamptz
  )
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_next_run timestamptz;
  v_params jsonb;
  v_headers jsonb;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_cancellation jsonb;
  v_catchup_policy text;
  v_enabled boolean;
begin
  if p_schedule_name is null or length(trim(p_schedule_name)) = 0 then
    raise exception 'schedule_name must be provided';
  end if;
  if p_task_name is null or length(trim(p_task_name)) = 0 then
    raise exception 'task_name must be provided';
  end if;

  v_next_run := absurd.next_cron_time(p_schedule_expr, v_now);
  v_params := coalesce(p_options->'params', '{}'::jsonb);
  v_headers := p_options->'headers';
  v_retry_strategy := p_options->'retry_strategy';
  if p_options ? 'max_attempts' then
    v_max_attempts := (p_options->>'max_attempts')::int;
  end if;
  v_cancellation := p_options->'cancellation';
  v_catchup_policy := coalesce(p_options->>'catchup_policy', 'skip');
  v_enabled := coalesce((p_options->>'enabled')::boolean, true);

  execute format(
    'insert into absurd.%I (schedule_name, task_name, params, headers,
       retry_strategy, max_attempts, cancellation, schedule_expr,
       enabled, catchup_policy, last_triggered_at, next_run_at, created_at)
     values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, null, $11, $12)',
    's_' || p_queue_name
  ) using p_schedule_name, p_task_name, v_params, v_headers,
          v_retry_strategy, v_max_attempts, v_cancellation, p_schedule_expr,
          v_enabled, v_catchup_policy, v_next_run, v_now;

  return query select p_schedule_name, v_next_run;
end;
$$;

-- Returns a single schedule by name, or no rows if not found.
create function absurd.get_schedule (
  p_queue_name text,
  p_schedule_name text
)
  returns table (
    schedule_name text,
    task_name text,
    params jsonb,
    headers jsonb,
    retry_strategy jsonb,
    max_attempts integer,
    cancellation jsonb,
    schedule_expr text,
    enabled boolean,
    catchup_policy text,
    last_triggered_at timestamptz,
    next_run_at timestamptz,
    created_at timestamptz
  )
  language plpgsql
as $$
begin
  return query execute format(
    'select schedule_name, task_name, params, headers,
            retry_strategy, max_attempts, cancellation, schedule_expr,
            enabled, catchup_policy, last_triggered_at, next_run_at, created_at
       from absurd.%I
      where schedule_name = $1',
    's_' || p_queue_name
  ) using p_schedule_name;
end;
$$;

-- Lists all schedules in a queue.
create function absurd.list_schedules (
  p_queue_name text
)
  returns table (
    schedule_name text,
    task_name text,
    schedule_expr text,
    enabled boolean,
    catchup_policy text,
    last_triggered_at timestamptz,
    next_run_at timestamptz
  )
  language plpgsql
as $$
begin
  return query execute format(
    'select schedule_name, task_name, schedule_expr, enabled,
            catchup_policy, last_triggered_at, next_run_at
       from absurd.%I
      order by schedule_name',
    's_' || p_queue_name
  );
end;
$$;

-- Deletes a schedule.
create function absurd.delete_schedule (
  p_queue_name text,
  p_schedule_name text
)
  returns void
  language plpgsql
as $$
begin
  execute format(
    'delete from absurd.%I where schedule_name = $1',
    's_' || p_queue_name
  ) using p_schedule_name;
end;
$$;

-- Updates a schedule. Recomputes next_run_at if schedule_expr changes.
create function absurd.update_schedule (
  p_queue_name text,
  p_schedule_name text,
  p_options jsonb
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_new_expr text;
  v_next_run timestamptz;
  v_current_enabled boolean;
  v_new_enabled boolean;
begin
  -- Get current state
  execute format(
    'select enabled from absurd.%I where schedule_name = $1 for update',
    's_' || p_queue_name
  ) into v_current_enabled using p_schedule_name;

  if v_current_enabled is null then
    raise exception 'Schedule "%" not found in queue "%"', p_schedule_name, p_queue_name;
  end if;

  -- Update fields that are present in options
  if p_options ? 'schedule_expr' then
    v_new_expr := p_options->>'schedule_expr';
    v_next_run := absurd.next_cron_time(v_new_expr, v_now);
    execute format(
      'update absurd.%I set schedule_expr = $2, next_run_at = $3 where schedule_name = $1',
      's_' || p_queue_name
    ) using p_schedule_name, v_new_expr, v_next_run;
  end if;

  if p_options ? 'params' then
    execute format(
      'update absurd.%I set params = $2 where schedule_name = $1',
      's_' || p_queue_name
    ) using p_schedule_name, (p_options->'params');
  end if;

  if p_options ? 'headers' then
    execute format(
      'update absurd.%I set headers = $2 where schedule_name = $1',
      's_' || p_queue_name
    ) using p_schedule_name, (p_options->'headers');
  end if;

  if p_options ? 'max_attempts' then
    execute format(
      'update absurd.%I set max_attempts = $2 where schedule_name = $1',
      's_' || p_queue_name
    ) using p_schedule_name, (p_options->>'max_attempts')::integer;
  end if;

  if p_options ? 'retry_strategy' then
    execute format(
      'update absurd.%I set retry_strategy = $2 where schedule_name = $1',
      's_' || p_queue_name
    ) using p_schedule_name, (p_options->'retry_strategy');
  end if;

  if p_options ? 'cancellation' then
    execute format(
      'update absurd.%I set cancellation = $2 where schedule_name = $1',
      's_' || p_queue_name
    ) using p_schedule_name, (p_options->'cancellation');
  end if;

  if p_options ? 'catchup_policy' then
    execute format(
      'update absurd.%I set catchup_policy = $2 where schedule_name = $1',
      's_' || p_queue_name
    ) using p_schedule_name, (p_options->>'catchup_policy');
  end if;

  if p_options ? 'enabled' then
    v_new_enabled := (p_options->>'enabled')::boolean;
    execute format(
      'update absurd.%I set enabled = $2 where schedule_name = $1',
      's_' || p_queue_name
    ) using p_schedule_name, v_new_enabled;

    -- Re-enabling: skip to next future run from now
    if v_new_enabled and not v_current_enabled then
      execute format(
        'select schedule_expr from absurd.%I where schedule_name = $1',
        's_' || p_queue_name
      ) into v_new_expr using p_schedule_name;
      v_next_run := absurd.next_cron_time(v_new_expr, v_now);
      execute format(
        'update absurd.%I set next_run_at = $2 where schedule_name = $1',
        's_' || p_queue_name
      ) using p_schedule_name, v_next_run;
    end if;
  end if;
end;
$$;
```

**Step 4: Run tests to verify they pass**

Run: `cd tests && uv run pytest test_schedules.py -v`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add sql/absurd.sql tests/test_schedules.py tests/conftest.py
git commit -m "feat: add schedule CRUD stored procedures

create_schedule, get_schedule, list_schedules, delete_schedule,
update_schedule. Supports all schedule options including params,
headers, retry strategy, catchup policy. Re-enabling a schedule
skips to next future run."
```

---

## Task 5: Tick Engine — `tick_schedules`

The core function that spawns tasks from due schedules.

**Files:**
- Modify: `sql/absurd.sql` (add `tick_schedules`, modify `claim_task`)
- Modify: `tests/test_schedules.py` (append tests)

**Step 1: Write the failing tests**

Append to `tests/test_schedules.py`:

```python
def test_tick_spawns_task_when_due(client):
    """tick_schedules spawns a task when next_run_at has passed."""
    queue = "tick-basic"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "tick-job", "my-task", "*/5 * * * *",
                           params={"key": "value"})

    # Advance past next_run_at (10:05)
    client.set_fake_now(base + timedelta(minutes=6))

    # tick_schedules should spawn a task
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))

    # Verify a task was spawned
    tasks = client.conn.execute(
        SQL("select task_name, params, idempotency_key from absurd.{table}").format(
            table=client.get_table("t", queue)
        )
    ).fetchall()
    assert len(tasks) == 1
    assert tasks[0][0] == "my-task"
    assert tasks[0][1] == {"key": "value"}
    assert "sched:tick-job:" in tasks[0][2]

    # Verify next_run_at advanced
    sched = client.get_schedule(queue, "tick-job")
    assert sched["next_run_at"] == datetime(2024, 7, 1, 10, 10, tzinfo=timezone.utc)
    assert sched["last_triggered_at"] == datetime(2024, 7, 1, 10, 5, tzinfo=timezone.utc)


def test_tick_skips_disabled_schedule(client):
    """tick_schedules ignores disabled schedules."""
    queue = "tick-disabled"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "off-job", "task-off", "*/5 * * * *")
    client.update_schedule(queue, "off-job", {"enabled": False})

    client.set_fake_now(base + timedelta(minutes=10))
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))

    assert client.count_tasks(queue) == 0


def test_tick_skip_catchup_policy(client):
    """With catchup_policy='skip', only one task is spawned even if many runs missed."""
    queue = "tick-skip"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "skip-job", "task-skip", "*/5 * * * *")

    # Skip ahead 30 minutes — 6 runs missed
    client.set_fake_now(base + timedelta(minutes=30))
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))

    assert client.count_tasks(queue) == 1

    sched = client.get_schedule(queue, "skip-job")
    # next_run_at should be in the future (next */5 after 10:30)
    assert sched["next_run_at"] == datetime(2024, 7, 1, 10, 35, tzinfo=timezone.utc)


def test_tick_all_catchup_policy_drip(client):
    """With catchup_policy='all', spawns up to 5 per tick."""
    queue = "tick-all"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "all-job", "task-all", "*/5 * * * *",
                           options={"catchup_policy": "all"})

    # Skip ahead 60 minutes — 12 runs missed
    client.set_fake_now(base + timedelta(minutes=60))

    # First tick: spawns 5
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))
    assert client.count_tasks(queue) == 5

    # Second tick: spawns 5 more
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))
    assert client.count_tasks(queue) == 10

    # Third tick: spawns remaining 2
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))
    assert client.count_tasks(queue) == 12

    # Schedule should now be current
    sched = client.get_schedule(queue, "all-job")
    assert sched["next_run_at"] == datetime(2024, 7, 1, 11, 5, tzinfo=timezone.utc)


def test_tick_idempotent(client):
    """Calling tick_schedules twice at the same time doesn't double-spawn."""
    queue = "tick-idempotent"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "idem-job", "task-idem", "*/5 * * * *")

    client.set_fake_now(base + timedelta(minutes=6))
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))

    assert client.count_tasks(queue) == 1


def test_tick_every_interval(client):
    """@every N schedule works with tick."""
    queue = "tick-every"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "every-job", "task-every", "@every 300")

    # next_run_at should be base + 300s = 10:05
    sched = client.get_schedule(queue, "every-job")
    assert sched["next_run_at"] == datetime(2024, 7, 1, 10, 5, tzinfo=timezone.utc)

    client.set_fake_now(base + timedelta(minutes=6))
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))

    assert client.count_tasks(queue) == 1
    sched = client.get_schedule(queue, "every-job")
    assert sched["next_run_at"] == datetime(2024, 7, 1, 10, 10, tzinfo=timezone.utc)
```

**Step 2: Run tests to verify they fail**

Run: `cd tests && uv run pytest test_schedules.py::test_tick_spawns_task_when_due -v -x`
Expected: FAIL — `absurd.tick_schedules` does not exist

**Step 3: Implement `tick_schedules`**

Append to `sql/absurd.sql` (after the schedule CRUD functions):

```sql
-- Ticks all due schedules in a queue, spawning tasks as needed.
--
-- For each enabled schedule where next_run_at <= now:
--   catchup_policy='skip': spawn 1 task, jump next_run_at to next future time
--   catchup_policy='all': spawn up to 5 tasks per tick, advance incrementally
--
-- Uses FOR UPDATE SKIP LOCKED to prevent double-ticking from concurrent workers.
-- Spawned tasks use idempotency keys for safety.
create function absurd.tick_schedules (
  p_queue_name text
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_sched record;
  v_spawned integer;
  v_next timestamptz;
  v_idem_key text;
  v_spawn_options jsonb;
  v_max_per_tick integer := 5;
begin
  for v_sched in
    execute format(
      'select schedule_name, task_name, params, headers,
              retry_strategy, max_attempts, cancellation,
              schedule_expr, catchup_policy, next_run_at
         from absurd.%I
        where enabled = true
          and next_run_at <= $1
        for update skip locked',
      's_' || p_queue_name
    ) using v_now
  loop
    v_spawned := 0;
    v_next := v_sched.next_run_at;

    if v_sched.catchup_policy = 'skip' then
      -- Spawn one task for the current next_run_at
      v_idem_key := 'sched:' || v_sched.schedule_name || ':' || v_next::text;
      v_spawn_options := jsonb_strip_nulls(jsonb_build_object(
        'idempotency_key', v_idem_key,
        'headers', v_sched.headers,
        'retry_strategy', v_sched.retry_strategy,
        'max_attempts', v_sched.max_attempts,
        'cancellation', v_sched.cancellation
      ));
      perform absurd.spawn_task(p_queue_name, v_sched.task_name, v_sched.params, v_spawn_options);

      -- Jump to next future run
      v_next := absurd.next_cron_time(v_sched.schedule_expr, v_next);
      while v_next <= v_now loop
        v_next := absurd.next_cron_time(v_sched.schedule_expr, v_next);
      end loop;

    elsif v_sched.catchup_policy = 'all' then
      -- Drip-feed: spawn up to max_per_tick
      while v_next <= v_now and v_spawned < v_max_per_tick loop
        v_idem_key := 'sched:' || v_sched.schedule_name || ':' || v_next::text;
        v_spawn_options := jsonb_strip_nulls(jsonb_build_object(
          'idempotency_key', v_idem_key,
          'headers', v_sched.headers,
          'retry_strategy', v_sched.retry_strategy,
          'max_attempts', v_sched.max_attempts,
          'cancellation', v_sched.cancellation
        ));
        perform absurd.spawn_task(p_queue_name, v_sched.task_name, v_sched.params, v_spawn_options);
        v_next := absurd.next_cron_time(v_sched.schedule_expr, v_next);
        v_spawned := v_spawned + 1;
      end loop;
    end if;

    -- Update schedule state
    execute format(
      'update absurd.%I
          set last_triggered_at = $2,
              next_run_at = $3
        where schedule_name = $1',
      's_' || p_queue_name
    ) using v_sched.schedule_name,
            case when v_sched.catchup_policy = 'skip'
                 then v_sched.next_run_at
                 else absurd.next_cron_time(v_sched.schedule_expr,
                        v_next - (absurd.next_cron_time(v_sched.schedule_expr, v_sched.next_run_at) - v_sched.next_run_at))
            end,
            v_next;
  end loop;
end;
$$;
```

Note: The `last_triggered_at` tracking for the `all` policy needs care. A simpler approach:

```sql
    -- Update schedule state
    execute format(
      'update absurd.%I
          set last_triggered_at = $2,
              next_run_at = $3
        where schedule_name = $1',
      's_' || p_queue_name
    ) using v_sched.schedule_name, v_sched.next_run_at, v_next;
```

This sets `last_triggered_at` to the `next_run_at` that was due when the tick started. For `skip` policy this is the single trigger time; for `all` it's the first trigger time in this tick batch. The implementer should verify the exact value through test assertions and adjust.

**Step 4: Modify `claim_task` to call `tick_schedules`**

In `sql/absurd.sql`, in the `claim_task` function body, add as the first statement after variable declarations (after line 349, before the cancellation check):

```sql
  -- Tick schedules before claiming work
  perform absurd.tick_schedules(p_queue_name);
```

**Step 5: Run tests to verify they pass**

Run: `cd tests && uv run pytest test_schedules.py -v`
Expected: All tests PASS

**Step 6: Run full test suite to verify no regressions**

Run: `cd tests && uv run pytest -v`
Expected: All existing tests still pass

**Step 7: Commit**

```bash
git add sql/absurd.sql tests/test_schedules.py
git commit -m "feat: add tick_schedules engine and integrate with claim_task

tick_schedules spawns tasks from due schedules with support for
skip (default) and all catchup policies. Called automatically at
the start of every claim_task poll. Uses FOR UPDATE SKIP LOCKED
and idempotency keys for concurrent safety."
```

---

## Task 6: Claim-Tick Integration Test

Verify that `claim_task` now automatically ticks schedules so spawned tasks are immediately claimable.

**Files:**
- Modify: `tests/test_schedules.py` (append tests)

**Step 1: Write the test**

Append to `tests/test_schedules.py`:

```python
def test_claim_task_ticks_schedules(client):
    """claim_task automatically calls tick_schedules, spawning and returning scheduled tasks."""
    queue = "claim-tick"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "auto-job", "auto-task", "*/5 * * * *",
                           params={"source": "schedule"})

    # Advance past first run time
    client.set_fake_now(base + timedelta(minutes=6))

    # claim_task should tick schedules internally, spawning the task,
    # then claim and return it
    claimed = client.claim_tasks(queue, worker="sched-worker")
    assert len(claimed) == 1
    assert claimed[0]["task_name"] == "auto-task"
    assert claimed[0]["params"] == {"source": "schedule"}


def test_claim_task_ticks_then_claims_mixed(client):
    """claim_task handles both scheduled and manually spawned tasks."""
    queue = "claim-mixed"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    # Manual task
    client.spawn_task(queue, "manual-task", {"source": "manual"})

    # Schedule
    client.create_schedule(queue, "sched-mix", "sched-task", "*/5 * * * *",
                           params={"source": "schedule"})

    # Advance past schedule
    client.set_fake_now(base + timedelta(minutes=6))

    claimed = client.claim_tasks(queue, worker="w", qty=5)
    task_names = [c["task_name"] for c in claimed]
    assert "manual-task" in task_names
    assert "sched-task" in task_names
```

**Step 2: Run tests**

Run: `cd tests && uv run pytest test_schedules.py::test_claim_task_ticks_schedules test_schedules.py::test_claim_task_ticks_then_claims_mixed -v`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/test_schedules.py
git commit -m "test: verify claim_task automatically ticks schedules"
```

---

## Task 7: TypeScript SDK — Schedule Methods

Add schedule management methods to the `Absurd` class.

**Files:**
- Modify: `sdks/typescript/src/index.ts` (add interfaces + methods to `Absurd` class)
- Modify: `sdks/typescript/test/` (add schedule tests if vitest suite exists)

**Step 1: Add TypeScript interfaces and methods**

In `sdks/typescript/src/index.ts`, add interfaces after the existing interfaces (after `AbsurdOptions`, around line 167):

```typescript
export interface ScheduleOptions {
  params?: JsonValue;
  headers?: JsonObject;
  retryStrategy?: RetryStrategy;
  maxAttempts?: number;
  cancellation?: CancellationPolicy;
  catchupPolicy?: "skip" | "all";
  enabled?: boolean;
}

export interface Schedule {
  scheduleName: string;
  taskName: string;
  params: JsonValue;
  headers: JsonObject | null;
  retryStrategy: JsonValue;
  maxAttempts: number | null;
  cancellation: JsonValue;
  scheduleExpr: string;
  enabled: boolean;
  catchupPolicy: string;
  lastTriggeredAt: Date | null;
  nextRunAt: Date;
  createdAt: Date;
}

export interface ScheduleSummary {
  scheduleName: string;
  taskName: string;
  scheduleExpr: string;
  enabled: boolean;
  catchupPolicy: string;
  lastTriggeredAt: Date | null;
  nextRunAt: Date;
}
```

Then add methods to the `Absurd` class (after `cancelTask`, around line 676):

```typescript
  async createSchedule(
    scheduleName: string,
    taskName: string,
    scheduleExpr: string,
    options: ScheduleOptions = {},
    queueName?: string,
  ): Promise<{ scheduleName: string; nextRunAt: Date }> {
    const queue = queueName ?? this.queueName;
    const normalizedOptions: JsonObject = {};
    if (options.params !== undefined) normalizedOptions.params = options.params as JsonValue;
    if (options.headers !== undefined) normalizedOptions.headers = options.headers;
    if (options.maxAttempts !== undefined) normalizedOptions.max_attempts = options.maxAttempts;
    if (options.retryStrategy) normalizedOptions.retry_strategy = serializeRetryStrategy(options.retryStrategy);
    if (options.cancellation) {
      const c = normalizeCancellation(options.cancellation);
      if (c) normalizedOptions.cancellation = c;
    }
    if (options.catchupPolicy !== undefined) normalizedOptions.catchup_policy = options.catchupPolicy;
    if (options.enabled !== undefined) normalizedOptions.enabled = options.enabled;

    const result = await this.con.query<{ schedule_name: string; next_run_at: Date }>(
      `SELECT schedule_name, next_run_at FROM absurd.create_schedule($1, $2, $3, $4, $5)`,
      [queue, scheduleName, taskName, scheduleExpr, JSON.stringify(normalizedOptions)],
    );
    const row = result.rows[0];
    return { scheduleName: row.schedule_name, nextRunAt: row.next_run_at };
  }

  async getSchedule(scheduleName: string, queueName?: string): Promise<Schedule | null> {
    const queue = queueName ?? this.queueName;
    const result = await this.con.query(
      `SELECT * FROM absurd.get_schedule($1, $2)`,
      [queue, scheduleName],
    );
    if (result.rows.length === 0) return null;
    const r = result.rows[0] as any;
    return {
      scheduleName: r.schedule_name,
      taskName: r.task_name,
      params: r.params,
      headers: r.headers,
      retryStrategy: r.retry_strategy,
      maxAttempts: r.max_attempts,
      cancellation: r.cancellation,
      scheduleExpr: r.schedule_expr,
      enabled: r.enabled,
      catchupPolicy: r.catchup_policy,
      lastTriggeredAt: r.last_triggered_at,
      nextRunAt: r.next_run_at,
      createdAt: r.created_at,
    };
  }

  async listSchedules(queueName?: string): Promise<ScheduleSummary[]> {
    const queue = queueName ?? this.queueName;
    const result = await this.con.query(
      `SELECT * FROM absurd.list_schedules($1)`,
      [queue],
    );
    return result.rows.map((r: any) => ({
      scheduleName: r.schedule_name,
      taskName: r.task_name,
      scheduleExpr: r.schedule_expr,
      enabled: r.enabled,
      catchupPolicy: r.catchup_policy,
      lastTriggeredAt: r.last_triggered_at,
      nextRunAt: r.next_run_at,
    }));
  }

  async deleteSchedule(scheduleName: string, queueName?: string): Promise<void> {
    const queue = queueName ?? this.queueName;
    await this.con.query(`SELECT absurd.delete_schedule($1, $2)`, [queue, scheduleName]);
  }

  async updateSchedule(
    scheduleName: string,
    options: Partial<ScheduleOptions> & { scheduleExpr?: string },
    queueName?: string,
  ): Promise<void> {
    const queue = queueName ?? this.queueName;
    const normalizedOptions: JsonObject = {};
    if (options.params !== undefined) normalizedOptions.params = options.params as JsonValue;
    if (options.headers !== undefined) normalizedOptions.headers = options.headers;
    if (options.maxAttempts !== undefined) normalizedOptions.max_attempts = options.maxAttempts;
    if (options.retryStrategy) normalizedOptions.retry_strategy = serializeRetryStrategy(options.retryStrategy);
    if (options.cancellation) {
      const c = normalizeCancellation(options.cancellation);
      if (c) normalizedOptions.cancellation = c;
    }
    if (options.catchupPolicy !== undefined) normalizedOptions.catchup_policy = options.catchupPolicy;
    if (options.enabled !== undefined) normalizedOptions.enabled = options.enabled;
    if (options.scheduleExpr !== undefined) normalizedOptions.schedule_expr = options.scheduleExpr;

    await this.con.query(`SELECT absurd.update_schedule($1, $2, $3)`, [
      queue, scheduleName, JSON.stringify(normalizedOptions),
    ]);
  }
```

**Step 2: Build the SDK**

Run: `cd sdks/typescript && npm run build`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add sdks/typescript/src/index.ts
git commit -m "feat: add schedule management methods to TypeScript SDK

createSchedule, getSchedule, listSchedules, deleteSchedule,
updateSchedule. Thin wrappers over the SQL stored procedures."
```

---

## Task 8: Update Header Comment in `absurd.sql`

Update the top-of-file documentation comment to mention schedules and the `s_` prefix.

**Files:**
- Modify: `sql/absurd.sql` (lines 1-30)

**Step 1: Update the comment**

Add `s_` to the list of table prefixes in the header comment (around line 8-12):

```sql
-- Each queue is materialized as its own set of tables that share a prefix:
-- * `t_` for tasks (what is to be run)
-- * `r_` for runs (attempts to run a task)
-- * `c_` for checkpoints (saved states)
-- * `e_` for emitted events
-- * `w_` for wait registrations
-- * `s_` for schedules (recurring task definitions)
```

Also add a line in the overview paragraph about schedules:

```sql
-- Recurring work is managed through schedule definitions (`create_schedule`,
-- `delete_schedule`, `list_schedules`, etc.) that are automatically ticked
-- during `claim_task`, spawning tasks on cron or interval cadences.
```

**Step 2: Commit**

```bash
git add sql/absurd.sql
git commit -m "docs: update absurd.sql header comment to mention schedules"
```

---

## Task 9: Migration File

Create the migration from 0.0.7 to 0.0.8.

**Files:**
- Create: `sql/migrations/0.0.7-0.0.8.sql`

**Step 1: Write the migration**

The migration must:
1. Add `parse_cron_field` and `next_cron_time` functions
2. Add schedule CRUD functions
3. Add `tick_schedules` function
4. Add `s_{queue}` table to all existing queues
5. Replace `claim_task` with the version that calls `tick_schedules`

Create `sql/migrations/0.0.7-0.0.8.sql`. This file should contain all the new `CREATE FUNCTION` statements and a DO block that loops over existing queues to add the `s_` table:

```sql
-- Migration from 0.0.7 to 0.0.8
--
-- Adds recurring/scheduled task support:
-- - parse_cron_field: cron field expansion
-- - next_cron_time: cron expression parser
-- - s_{queue} table for schedule definitions
-- - Schedule CRUD: create_schedule, get_schedule, list_schedules,
--   delete_schedule, update_schedule
-- - tick_schedules: spawns tasks from due schedules
-- - claim_task: now calls tick_schedules before claiming

-- [paste all new CREATE FUNCTION statements here]

-- Add s_ table to all existing queues
do $$
declare
  v_queue text;
begin
  for v_queue in select queue_name from absurd.queues loop
    execute format(
      'create table if not exists absurd.%I (
          schedule_name text primary key,
          -- [full DDL here]
       )',
      's_' || v_queue
    );
  end loop;
end;
$$;

-- Replace claim_task with version that calls tick_schedules
-- [paste the full create or replace function absurd.claim_task ...]
```

The implementer should copy the exact function bodies from `absurd.sql` into the migration, using `CREATE OR REPLACE FUNCTION` for all functions.

**Step 2: Commit**

```bash
git add sql/migrations/0.0.7-0.0.8.sql
git commit -m "feat: add migration 0.0.7 to 0.0.8 for recurring schedules"
```

---

## Task 10: Full Integration Test

End-to-end test combining schedule creation, ticking via claim, and task execution.

**Files:**
- Modify: `tests/test_schedules.py` (append test)

**Step 1: Write the integration test**

```python
def test_full_schedule_lifecycle(client):
    """Full lifecycle: create schedule, tick via claim, complete task, tick again."""
    queue = "lifecycle-sched"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    # Create schedule
    client.create_schedule(queue, "heartbeat", "ping", "@every 300",
                           params={"target": "db"})

    sched = client.get_schedule(queue, "heartbeat")
    assert sched["next_run_at"] == base + timedelta(seconds=300)

    # Advance past first run
    client.set_fake_now(base + timedelta(minutes=6))

    # claim_task ticks and returns the scheduled task
    claimed = client.claim_tasks(queue)
    assert len(claimed) == 1
    assert claimed[0]["task_name"] == "ping"
    assert claimed[0]["params"] == {"target": "db"}

    # Complete the task
    client.complete_run(queue, claimed[0]["run_id"], {"status": "pong"})

    # Verify schedule advanced
    sched = client.get_schedule(queue, "heartbeat")
    assert sched["next_run_at"] == base + timedelta(seconds=600)

    # Advance past second run
    client.set_fake_now(base + timedelta(minutes=11))

    # Second tick via claim
    claimed2 = client.claim_tasks(queue)
    assert len(claimed2) == 1
    assert claimed2[0]["task_name"] == "ping"

    # Delete schedule
    client.delete_schedule(queue, "heartbeat")
    assert client.get_schedule(queue, "heartbeat") is None

    # No more tasks spawned
    client.complete_run(queue, claimed2[0]["run_id"])
    client.set_fake_now(base + timedelta(minutes=20))
    claimed3 = client.claim_tasks(queue)
    assert len(claimed3) == 0
```

**Step 2: Run full test suite**

Run: `cd tests && uv run pytest -v`
Expected: All tests PASS (existing + new schedule tests)

**Step 3: Commit**

```bash
git add tests/test_schedules.py
git commit -m "test: add full schedule lifecycle integration test"
```

---

## Summary of Implementation Order

| Task | Description | Dependencies |
|------|-------------|--------------|
| 1 | `parse_cron_field` function + tests | None |
| 2 | `next_cron_time` function + tests | Task 1 |
| 3 | `s_{queue}` table in queue lifecycle | None (parallel with 1-2) |
| 4 | Schedule CRUD functions + tests | Tasks 2, 3 |
| 5 | `tick_schedules` + `claim_task` integration | Task 4 |
| 6 | Claim-tick integration tests | Task 5 |
| 7 | TypeScript SDK schedule methods | Task 4 |
| 8 | Update SQL header comment | Task 5 |
| 9 | Migration file 0.0.7-0.0.8 | Task 5 |
| 10 | Full lifecycle integration test | Task 5 |

Tasks 7-10 can be done in parallel after Task 5 is complete.
