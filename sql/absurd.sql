-- Absurd installs a Postgres-native durable workflow system that can be dropped
-- into an existing database.
--
-- It bootstraps the `absurd` schema and required extensions so that jobs, runs,
-- checkpoints, and workflow events all live alongside application data without
-- external services.
--
-- Each queue is materialized as its own set of tables that share a prefix:
-- * `t_` for tasks (what is to be run)
-- * `r_` for runs (attempts to run a task)
-- * `c_` for checkpoints (saved states)
-- * `e_` for emitted events
-- * `w_` for wait registrations
-- * `s_` for schedules (recurring task definitions)
--
-- `create_queue`, `drop_queue`, and `list_queues` provide the management
-- surface for provisioning queues safely.
--
-- Task execution flows through `spawn_task`, which records the logical task and
-- its first run, and `claim_task`, which hands work to workers with leasing
-- semantics, state transitions, and cancellation checks.  Runtime routines
-- such as `complete_run`, `schedule_run`, and `fail_run` advance or retry work,
-- enforce attempt accounting, and keep the task and run tables synchronized.
--
-- Long-running or event-driven workflows rely on lightweight persistence
-- primitives.  Checkpoint helpers (`set_task_checkpoint_state`,
-- `get_task_checkpoint_state`, `get_task_checkpoint_states`) write arbitrary
-- JSON payloads keyed by task and step, while `await_event` and `emit_event`
-- coordinate sleepers and external signals so that tasks can suspend and resume
-- without losing context.  Events are uniquely indexed and can only be fired
-- once per name.
--
-- Recurring work is managed through schedule definitions (`create_schedule`,
-- `delete_schedule`, `list_schedules`, etc.) that are automatically ticked
-- during `claim_task`, spawning tasks on cron or interval cadences.

create extension if not exists "uuid-ossp";

create schema if not exists absurd;

-- Returns either the actual current timestamp or a fake one if
-- the session sets `absurd.fake_now`.  This lets tests control time.
create function absurd.current_time ()
  returns timestamptz
  language plpgsql
  volatile
as $$
declare
  v_fake text;
begin
  v_fake := current_setting('absurd.fake_now', true);
  if v_fake is not null and length(trim(v_fake)) > 0 then
    return v_fake::timestamptz;
  end if;

  return clock_timestamp();
end;
$$;

-- Expands a single cron field expression into a sorted, deduplicated
-- integer array.  Supports *, exact values, ranges (N-M), steps (*/N,
-- N-M/S), comma-separated lists, and named days/months.
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
  v_field text;
  v_parts text[];
  v_part text;
  v_result integer[] := '{}';
  v_range_parts text[];
  v_step integer;
  v_start integer;
  v_end integer;
  v_val integer;
  v_i integer;
begin
  -- Normalize: upper-case for name matching, strip whitespace
  v_field := upper(trim(p_field));

  -- Replace named days (must come before month names to avoid conflicts)
  v_field := replace(v_field, 'SUN', '0');
  v_field := replace(v_field, 'MON', '1');
  v_field := replace(v_field, 'TUE', '2');
  v_field := replace(v_field, 'WED', '3');
  v_field := replace(v_field, 'THU', '4');
  v_field := replace(v_field, 'FRI', '5');
  v_field := replace(v_field, 'SAT', '6');

  -- Replace named months
  v_field := replace(v_field, 'JAN', '1');
  v_field := replace(v_field, 'FEB', '2');
  v_field := replace(v_field, 'MAR', '3');
  v_field := replace(v_field, 'APR', '4');
  v_field := replace(v_field, 'MAY', '5');
  v_field := replace(v_field, 'JUN', '6');
  v_field := replace(v_field, 'JUL', '7');
  v_field := replace(v_field, 'AUG', '8');
  v_field := replace(v_field, 'SEP', '9');
  v_field := replace(v_field, 'OCT', '10');
  v_field := replace(v_field, 'NOV', '11');
  v_field := replace(v_field, 'DEC', '12');

  -- Split by comma to handle lists
  v_parts := string_to_array(v_field, ',');

  foreach v_part in array v_parts loop
    v_part := trim(v_part);

    if v_part = '*' then
      -- Wildcard: all values in range
      for v_i in p_min..p_max loop
        v_result := v_result || v_i;
      end loop;

    elsif v_part ~ '^\*/[0-9]+$' then
      -- Step from min: */N
      v_step := split_part(v_part, '/', 2)::integer;
      if v_step > 0 then
        v_i := p_min;
        while v_i <= p_max loop
          v_result := v_result || v_i;
          v_i := v_i + v_step;
        end loop;
      end if;

    elsif v_part ~ '^[0-9]+-[0-9]+/[0-9]+$' then
      -- Range with step: N-M/S
      v_range_parts := string_to_array(split_part(v_part, '/', 1), '-');
      v_start := greatest(v_range_parts[1]::integer, p_min);
      v_end := least(v_range_parts[2]::integer, p_max);
      v_step := split_part(v_part, '/', 2)::integer;
      if v_step > 0 then
        v_i := v_start;
        while v_i <= v_end loop
          v_result := v_result || v_i;
          v_i := v_i + v_step;
        end loop;
      end if;

    elsif v_part ~ '^[0-9]+-[0-9]+$' then
      -- Simple range: N-M (supports wrap-around, e.g. FRI-MON -> 5-1)
      v_range_parts := string_to_array(v_part, '-');
      v_start := greatest(v_range_parts[1]::integer, p_min);
      v_end := least(v_range_parts[2]::integer, p_max);
      if v_start <= v_end then
        for v_i in v_start..v_end loop
          v_result := v_result || v_i;
        end loop;
      else
        -- Wrap-around: e.g. 5-1 expands to 5,6,0,1
        for v_i in v_start..p_max loop
          v_result := v_result || v_i;
        end loop;
        for v_i in p_min..v_end loop
          v_result := v_result || v_i;
        end loop;
      end if;

    elsif v_part ~ '^[0-9]+$' then
      -- Exact value: N
      v_val := v_part::integer;
      if v_val >= p_min and v_val <= p_max then
        v_result := v_result || v_val;
      end if;

    end if;
  end loop;

  -- Sort and deduplicate
  select array_agg(distinct val order by val)
    into v_result
    from unnest(v_result) as val;

  return coalesce(v_result, '{}');
end;
$$;

-- Given a cron expression and a reference timestamp, returns the next
-- matching time strictly after p_after.  Handles standard 5-field cron,
-- common shorthands (@daily, @hourly, etc.), and @every <seconds>.
create function absurd.next_cron_time (
  p_expr text,
  p_after timestamptz
)
  returns timestamptz
  language plpgsql
  immutable
as $$
declare
  v_expr text;
  v_fields text[];
  v_minutes integer[];
  v_hours integer[];
  v_doms integer[];
  v_months integer[];
  v_dows integer[];
  v_dom_restricted boolean;
  v_dow_restricted boolean;
  v_candidate timestamptz;
  v_year integer;
  v_month integer;
  v_day integer;
  v_hour integer;
  v_minute integer;
  v_max_day integer;
  v_dow integer;
  v_found boolean;
  v_limit_ts timestamptz;
  v_val integer;
begin
  v_expr := trim(p_expr);

  -- Handle @every <seconds> shorthand
  if v_expr ~* '^@every\s+' then
    declare
      v_secs_text text := regexp_replace(v_expr, '^@every\s+', '', 'i');
    begin
      if v_secs_text !~ '^\d+$' then
        raise exception '@every requires a plain integer number of seconds, got: "%"', v_secs_text;
      end if;
      return p_after + (v_secs_text::integer * interval '1 second');
    end;
  end if;

  -- Handle named shorthands
  if v_expr ~* '^@yearly$' or v_expr ~* '^@annually$' then
    v_expr := '0 0 1 1 *';
  elsif v_expr ~* '^@monthly$' then
    v_expr := '0 0 1 * *';
  elsif v_expr ~* '^@weekly$' then
    v_expr := '0 0 * * 0';
  elsif v_expr ~* '^@daily$' or v_expr ~* '^@midnight$' then
    v_expr := '0 0 * * *';
  elsif v_expr ~* '^@hourly$' then
    v_expr := '0 * * * *';
  end if;

  -- Split into 5 fields: minute hour dom month dow
  v_fields := regexp_split_to_array(v_expr, '\s+');
  if array_length(v_fields, 1) <> 5 then
    raise exception 'Invalid cron expression: expected 5 fields, got %', array_length(v_fields, 1);
  end if;

  v_minutes := absurd.parse_cron_field(v_fields[1], 0, 59);
  v_hours   := absurd.parse_cron_field(v_fields[2], 0, 23);
  v_doms    := absurd.parse_cron_field(v_fields[3], 1, 31);
  v_months  := absurd.parse_cron_field(v_fields[4], 1, 12);
  v_dows    := absurd.parse_cron_field(v_fields[5], 0, 6);

  -- Determine if dom/dow are restricted (not wildcards).
  -- When both are restricted, we use union (OR) semantics per cron standard.
  v_dom_restricted := (v_fields[3] <> '*');
  v_dow_restricted := (v_fields[5] <> '*');

  -- Start scanning from the minute after p_after, truncated to minute boundary
  v_candidate := date_trunc('minute', p_after at time zone 'UTC') + interval '1 minute';
  -- Safety limit: ~4 years from p_after
  v_limit_ts := p_after + interval '4 years';

  <<scan>>
  loop
    if v_candidate > v_limit_ts then
      raise exception 'next_cron_time: no match found within 4 years for expression "%"', p_expr;
    end if;

    v_year   := extract(year from v_candidate);
    v_month  := extract(month from v_candidate);
    v_day    := extract(day from v_candidate);
    v_hour   := extract(hour from v_candidate);
    v_minute := extract(minute from v_candidate);

    -- Check month
    if not v_month = any(v_months) then
      -- Advance to the next matching month
      v_found := false;
      foreach v_val in array v_months loop
        if v_val > v_month then
          v_candidate := make_timestamptz(v_year, v_val, 1, 0, 0, 0, 'UTC');
          v_found := true;
          exit;
        end if;
      end loop;
      if not v_found then
        -- Wrap to first matching month of next year
        v_candidate := make_timestamptz(v_year + 1, v_months[1], 1, 0, 0, 0, 'UTC');
      end if;
      continue scan;
    end if;

    -- Check day (dom/dow logic)
    -- Calculate max days in this month
    v_max_day := extract(day from
      (make_timestamptz(v_year, v_month, 1, 0, 0, 0, 'UTC') + interval '1 month' - interval '1 day')
    );

    if v_day > v_max_day then
      -- Invalid day for this month, advance to next month
      v_candidate := make_timestamptz(v_year, v_month, 1, 0, 0, 0, 'UTC') + interval '1 month';
      continue scan;
    end if;

    -- Day-of-week: 0=Sunday in cron. Postgres extract(dow) is also 0=Sunday.
    v_dow := extract(dow from v_candidate);

    declare
      v_day_match boolean := false;
    begin
      if v_dom_restricted and v_dow_restricted then
        -- Both restricted: match EITHER (union)
        v_day_match := (v_day = any(v_doms) and v_day <= v_max_day) or (v_dow = any(v_dows));
      elsif v_dom_restricted then
        v_day_match := (v_day = any(v_doms) and v_day <= v_max_day);
      elsif v_dow_restricted then
        v_day_match := (v_dow = any(v_dows));
      else
        -- Neither restricted: any day matches
        v_day_match := true;
      end if;

      if not v_day_match then
        -- Advance to next day, reset hour/minute
        v_candidate := make_timestamptz(v_year, v_month, v_day, 0, 0, 0, 'UTC') + interval '1 day';
        continue scan;
      end if;
    end;

    -- Check hour
    if not v_hour = any(v_hours) then
      v_found := false;
      foreach v_val in array v_hours loop
        if v_val > v_hour then
          v_candidate := make_timestamptz(v_year, v_month, v_day, v_val, 0, 0, 'UTC');
          v_found := true;
          exit;
        end if;
      end loop;
      if not v_found then
        -- No matching hour left today, advance to next day
        v_candidate := make_timestamptz(v_year, v_month, v_day, 0, 0, 0, 'UTC') + interval '1 day';
      end if;
      continue scan;
    end if;

    -- Check minute
    if not v_minute = any(v_minutes) then
      v_found := false;
      foreach v_val in array v_minutes loop
        if v_val > v_minute then
          v_candidate := make_timestamptz(v_year, v_month, v_day, v_hour, v_val, 0, 'UTC');
          v_found := true;
          exit;
        end if;
      end loop;
      if not v_found then
        -- No matching minute left this hour, advance to next hour
        v_candidate := make_timestamptz(v_year, v_month, v_day, v_hour, 0, 0, 'UTC') + interval '1 hour';
      end if;
      continue scan;
    end if;

    -- All fields match
    return v_candidate;
  end loop;
end;
$$;

-- Creates a new schedule in the given queue.
-- Computes next_run_at from the cron expression and stores it alongside
-- the schedule metadata.
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

-- Retrieves a single schedule by name from the given queue.
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

-- Lists all schedules in the given queue, ordered by name.
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

-- Deletes a schedule by name from the given queue.
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

-- Updates an existing schedule.  Only the keys present in p_options are
-- modified; absent keys are left untouched.  When the schedule expression
-- changes, next_run_at is recomputed.  Re-enabling a disabled schedule
-- also fast-forwards next_run_at to the next future occurrence.
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
  execute format(
    'select enabled from absurd.%I where schedule_name = $1 for update',
    's_' || p_queue_name
  ) into v_current_enabled using p_schedule_name;

  if v_current_enabled is null then
    raise exception 'Schedule "%" not found in queue "%"', p_schedule_name, p_queue_name;
  end if;

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

-- Ticks all due schedules in a queue, spawning tasks as needed.
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
  v_trigger_at timestamptz;
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
      v_trigger_at := v_next;
      v_idem_key := 'sched:' || v_sched.schedule_name || ':' || extract(epoch from v_trigger_at)::bigint::text;
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

      -- Update schedule
      execute format(
        'update absurd.%I
            set last_triggered_at = $2,
                next_run_at = $3
          where schedule_name = $1',
        's_' || p_queue_name
      ) using v_sched.schedule_name, v_trigger_at, v_next;

    elsif v_sched.catchup_policy = 'all' then
      -- Drip-feed: spawn up to max_per_tick
      while v_next <= v_now and v_spawned < v_max_per_tick loop
        v_trigger_at := v_next;
        v_idem_key := 'sched:' || v_sched.schedule_name || ':' || extract(epoch from v_trigger_at)::bigint::text;
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

      -- Update schedule: last_triggered_at = last spawned time
      execute format(
        'update absurd.%I
            set last_triggered_at = $2,
                next_run_at = $3
          where schedule_name = $1',
        's_' || p_queue_name
      ) using v_sched.schedule_name, v_trigger_at, v_next;
    end if;
  end loop;
end;
$$;

create table if not exists absurd.queues (
  queue_name text primary key,
  created_at timestamptz not null default absurd.current_time()
);

create function absurd.ensure_queue_tables (p_queue_name text)
  returns void
  language plpgsql
as $$
begin
  execute format(
    'create table if not exists absurd.%I (
        task_id uuid primary key,
        task_name text not null,
        params jsonb not null,
        headers jsonb,
        retry_strategy jsonb,
        max_attempts integer,
        cancellation jsonb,
        enqueue_at timestamptz not null default absurd.current_time(),
        first_started_at timestamptz,
        state text not null check (state in (''pending'', ''running'', ''sleeping'', ''completed'', ''failed'', ''cancelled'')),
        attempts integer not null default 0,
        last_attempt_run uuid,
        completed_payload jsonb,
        cancelled_at timestamptz,
        idempotency_key text unique
     ) with (fillfactor=70)',
    't_' || p_queue_name
  );

  execute format(
    'create table if not exists absurd.%I (
        run_id uuid primary key,
        task_id uuid not null,
        attempt integer not null,
        state text not null check (state in (''pending'', ''running'', ''sleeping'', ''completed'', ''failed'', ''cancelled'')),
        claimed_by text,
        claim_expires_at timestamptz,
        available_at timestamptz not null,
        wake_event text,
        event_payload jsonb,
        started_at timestamptz,
        completed_at timestamptz,
        failed_at timestamptz,
        result jsonb,
        failure_reason jsonb,
        created_at timestamptz not null default absurd.current_time()
     ) with (fillfactor=70)',
    'r_' || p_queue_name
  );

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid not null,
        checkpoint_name text not null,
        state jsonb,
        status text not null default ''committed'',
        owner_run_id uuid,
        updated_at timestamptz not null default absurd.current_time(),
        primary key (task_id, checkpoint_name)
     ) with (fillfactor=70)',
    'c_' || p_queue_name
  );

  execute format(
    'create table if not exists absurd.%I (
        event_name text primary key,
        payload jsonb,
        emitted_at timestamptz not null default absurd.current_time()
     )',
    'e_' || p_queue_name
  );

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid not null,
        run_id uuid not null,
        step_name text not null,
        event_name text not null,
        timeout_at timestamptz,
        created_at timestamptz not null default absurd.current_time(),
        primary key (run_id, step_name)
     )',
    'w_' || p_queue_name
  );

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

  execute format(
    'create index if not exists %I on absurd.%I (state, available_at)',
    ('r_' || p_queue_name) || '_sai',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (task_id)',
    ('r_' || p_queue_name) || '_ti',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (event_name)',
    ('w_' || p_queue_name) || '_eni',
    'w_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (enabled, next_run_at)',
    ('s_' || p_queue_name) || '_enri',
    's_' || p_queue_name
  );
end;
$$;

-- Creates the queue with the given name.
--
-- If the table already exists, the function returns silently.
create function absurd.create_queue (p_queue_name text)
  returns void
  language plpgsql
as $$
begin
  if p_queue_name is null or length(trim(p_queue_name)) = 0 then
    raise exception 'Queue name must be provided';
  end if;

  if length(p_queue_name) + 2 > 50 then
    raise exception 'Queue name "%" is too long', p_queue_name;
  end if;

  begin
    insert into absurd.queues (queue_name)
    values (p_queue_name);
  exception when unique_violation then
    return;
  end;

  perform absurd.ensure_queue_tables(p_queue_name);
end;
$$;

-- Drop a queue if it exists.
create function absurd.drop_queue (p_queue_name text)
  returns void
  language plpgsql
as $$
declare
  v_existing_queue text;
begin
  select queue_name into v_existing_queue
  from absurd.queues
  where queue_name = p_queue_name;

  if v_existing_queue is null then
    return;
  end if;

  execute format('drop table if exists absurd.%I cascade', 's_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'w_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'e_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'c_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'r_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 't_' || p_queue_name);

  delete from absurd.queues where queue_name = p_queue_name;
end;
$$;

-- Lists all queues that currently exist.
create function absurd.list_queues ()
  returns table (queue_name text)
  language sql
as $$
  select queue_name from absurd.queues order by queue_name;
$$;

-- Spawns a given task in a queue.
--
-- If an idempotency_key is provided in p_options, the function will check if a task
-- with that key already exists. If so, it returns the existing task_id with run_id
-- and attempt set to NULL to signal "already exists". This is race-safe via
-- INSERT ... ON CONFLICT DO NOTHING.
create function absurd.spawn_task (
  p_queue_name text,
  p_task_name text,
  p_params jsonb,
  p_options jsonb default '{}'::jsonb
)
  returns table (
    task_id uuid,
    run_id uuid,
    attempt integer,
    created boolean
  )
  language plpgsql
as $$
declare
  v_task_id uuid := absurd.portable_uuidv7();
  v_run_id uuid := absurd.portable_uuidv7();
  v_attempt integer := 1;
  v_headers jsonb;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_cancellation jsonb;
  v_idempotency_key text;
  v_existing_task_id uuid;
  v_row_count integer;
  v_now timestamptz := absurd.current_time();
  v_params jsonb := coalesce(p_params, 'null'::jsonb);
begin
  if p_task_name is null or length(trim(p_task_name)) = 0 then
    raise exception 'task_name must be provided';
  end if;

  if p_options is not null then
    v_headers := p_options->'headers';
    v_retry_strategy := p_options->'retry_strategy';
    if p_options ? 'max_attempts' then
      v_max_attempts := (p_options->>'max_attempts')::int;
      if v_max_attempts is not null and v_max_attempts < 1 then
        raise exception 'max_attempts must be >= 1';
      end if;
    end if;
    v_cancellation := p_options->'cancellation';
    v_idempotency_key := p_options->>'idempotency_key';
  end if;

  -- If idempotency_key is provided, use INSERT ... ON CONFLICT DO NOTHING
  if v_idempotency_key is not null then
    execute format(
      'insert into absurd.%I (task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, enqueue_at, first_started_at, state, attempts, last_attempt_run, completed_payload, cancelled_at, idempotency_key)
       values ($1, $2, $3, $4, $5, $6, $7, $8, null, ''pending'', $9, $10, null, null, $11)
       on conflict (idempotency_key) do nothing',
      't_' || p_queue_name
    )
    using v_task_id, p_task_name, v_params, v_headers, v_retry_strategy, v_max_attempts, v_cancellation, v_now, v_attempt, v_run_id, v_idempotency_key;

    get diagnostics v_row_count = row_count;

    if v_row_count = 0 then
      -- Task already exists, look up existing task info
      execute format(
        'select task_id, last_attempt_run, attempts from absurd.%I where idempotency_key = $1',
        't_' || p_queue_name
      )
      into v_existing_task_id, v_run_id, v_attempt
      using v_idempotency_key;

      return query select v_existing_task_id, v_run_id, v_attempt, false;
      return;
    end if;
  else
    -- No idempotency key, insert normally
    execute format(
      'insert into absurd.%I (task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, enqueue_at, first_started_at, state, attempts, last_attempt_run, completed_payload, cancelled_at, idempotency_key)
       values ($1, $2, $3, $4, $5, $6, $7, $8, null, ''pending'', $9, $10, null, null, null)',
      't_' || p_queue_name
    )
    using v_task_id, p_task_name, v_params, v_headers, v_retry_strategy, v_max_attempts, v_cancellation, v_now, v_attempt, v_run_id;
  end if;

  execute format(
    'insert into absurd.%I (run_id, task_id, attempt, state, available_at, wake_event, event_payload, result, failure_reason)
     values ($1, $2, $3, ''pending'', $4, null, null, null, null)',
    'r_' || p_queue_name
  )
  using v_run_id, v_task_id, v_attempt, v_now;

  return query select v_task_id, v_run_id, v_attempt, true;
end;
$$;

-- Workers call this to reserve a task from a given queue
-- for a given reservation period in seconds.
create function absurd.claim_task (
  p_queue_name text,
  p_worker_id text,
  p_claim_timeout integer default 30,
  p_qty integer default 1
)
  returns table (
    run_id uuid,
    task_id uuid,
    attempt integer,
    task_name text,
    params jsonb,
    retry_strategy jsonb,
    max_attempts integer,
    headers jsonb,
    wake_event text,
    event_payload jsonb
  )
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_claim_timeout integer := greatest(coalesce(p_claim_timeout, 30), 0);
  v_worker_id text := coalesce(nullif(p_worker_id, ''), 'worker');
  v_qty integer := greatest(coalesce(p_qty, 1), 1);
  v_claim_until timestamptz := null;
  v_sql text;
  v_expired_run record;
begin
  -- Tick schedules before claiming work
  perform absurd.tick_schedules(p_queue_name);

  if v_claim_timeout > 0 then
    v_claim_until := v_now + make_interval(secs => v_claim_timeout);
  end if;

  -- Apply cancellation rules before claiming.
  execute format(
    'with limits as (
        select task_id,
               (cancellation->>''max_delay'')::bigint as max_delay,
               (cancellation->>''max_duration'')::bigint as max_duration,
               enqueue_at,
               first_started_at,
               state
          from absurd.%I
        where state in (''pending'', ''sleeping'', ''running'')
     ),
     to_cancel as (
        select task_id
          from limits
         where
           (
             max_delay is not null
             and first_started_at is null
             and extract(epoch from ($1 - enqueue_at)) >= max_delay
           )
           or
           (
             max_duration is not null
             and first_started_at is not null
             and extract(epoch from ($1 - first_started_at)) >= max_duration
           )
     )
     update absurd.%I t
        set state = ''cancelled'',
            cancelled_at = coalesce(t.cancelled_at, $1)
      where t.task_id in (select task_id from to_cancel)',
    't_' || p_queue_name,
    't_' || p_queue_name
  ) using v_now;

  for v_expired_run in
    execute format(
      'select run_id,
              claimed_by,
              claim_expires_at,
              attempt
         from absurd.%I
        where state = ''running''
          and claim_expires_at is not null
          and claim_expires_at <= $1
        for update skip locked',
      'r_' || p_queue_name
    )
  using v_now
  loop
    perform absurd.fail_run(
      p_queue_name,
      v_expired_run.run_id,
      jsonb_strip_nulls(jsonb_build_object(
        'name', '$ClaimTimeout',
        'message', 'worker did not finish task within claim interval',
        'workerId', v_expired_run.claimed_by,
        'claimExpiredAt', v_expired_run.claim_expires_at,
        'attempt', v_expired_run.attempt
      )),
      null
    );
  end loop;

  execute format(
    'update absurd.%I r
        set state = ''cancelled'',
            claimed_by = null,
            claim_expires_at = null,
            available_at = $1,
            wake_event = null
      where task_id in (select task_id from absurd.%I where state = ''cancelled'')
        and r.state <> ''cancelled''',
    'r_' || p_queue_name,
    't_' || p_queue_name
  ) using v_now;

  v_sql := format(
    'with candidate as (
        select r.run_id
          from absurd.%1$I r
          join absurd.%2$I t on t.task_id = r.task_id
         where r.state in (''pending'', ''sleeping'')
           and t.state in (''pending'', ''sleeping'', ''running'')
           and r.available_at <= $1
         order by r.available_at, r.run_id
         limit $2
         for update skip locked
     ),
     updated as (
        update absurd.%1$I r
           set state = ''running'',
               claimed_by = $3,
               claim_expires_at = $4,
               started_at = $1,
               available_at = $1
         where run_id in (select run_id from candidate)
         returning r.run_id, r.task_id, r.attempt
     ),
     task_upd as (
        update absurd.%2$I t
           set state = ''running'',
               attempts = greatest(t.attempts, u.attempt),
               first_started_at = coalesce(t.first_started_at, $1),
               last_attempt_run = u.run_id
          from updated u
         where t.task_id = u.task_id
         returning t.task_id
     ),
     wait_cleanup as (
        delete from absurd.%3$I w
         using updated u
        where w.run_id = u.run_id
          and w.timeout_at is not null
          and w.timeout_at <= $1
        returning w.run_id
     )
     select
       u.run_id,
       u.task_id,
       u.attempt,
       t.task_name,
       t.params,
       t.retry_strategy,
       t.max_attempts,
      t.headers,
      r.wake_event,
      r.event_payload
     from updated u
     join absurd.%1$I r on r.run_id = u.run_id
     join absurd.%2$I t on t.task_id = u.task_id
     order by r.available_at, u.run_id',
    'r_' || p_queue_name,
    't_' || p_queue_name,
    'w_' || p_queue_name
  );

  return query execute v_sql using v_now, v_qty, v_worker_id, v_claim_until;
end;
$$;

-- Markes a run as completed
create function absurd.complete_run (
  p_queue_name text,
  p_run_id uuid,
  p_state jsonb default null
)
  returns void
  language plpgsql
as $$
declare
  v_task_id uuid;
  v_state text;
  v_now timestamptz := absurd.current_time();
begin
  execute format(
    'select task_id, state
       from absurd.%I
      where run_id = $1
      for update',
    'r_' || p_queue_name
  )
  into v_task_id, v_state
  using p_run_id;

  if v_task_id is null then
    raise exception 'Run "%" not found in queue "%"', p_run_id, p_queue_name;
  end if;

  if v_state <> 'running' then
    raise exception 'Run "%" is not currently running in queue "%"', p_run_id, p_queue_name;
  end if;

  execute format(
    'update absurd.%I
        set state = ''completed'',
            completed_at = $2,
            result = $3
      where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id, v_now, p_state;

  execute format(
    'update absurd.%I
        set state = ''completed'',
            completed_payload = $2,
            last_attempt_run = $3
      where task_id = $1',
    't_' || p_queue_name
  ) using v_task_id, p_state, p_run_id;

  execute format(
    'delete from absurd.%I where run_id = $1',
    'w_' || p_queue_name
  ) using p_run_id;
end;
$$;

create function absurd.schedule_run (
  p_queue_name text,
  p_run_id uuid,
  p_wake_at timestamptz
)
  returns void
  language plpgsql
as $$
declare
  v_task_id uuid;
begin
  execute format(
    'select task_id
       from absurd.%I
      where run_id = $1
        and state = ''running''
      for update',
    'r_' || p_queue_name
  )
  into v_task_id
  using p_run_id;

  if v_task_id is null then
    raise exception 'Run "%" is not currently running in queue "%"', p_run_id, p_queue_name;
  end if;

  execute format(
    'update absurd.%I
        set state = ''sleeping'',
            claimed_by = null,
            claim_expires_at = null,
            available_at = $2,
            wake_event = null
      where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id, p_wake_at;

  execute format(
    'update absurd.%I
        set state = ''sleeping''
      where task_id = $1',
    't_' || p_queue_name
  ) using v_task_id;
end;
$$;

create function absurd.fail_run (
  p_queue_name text,
  p_run_id uuid,
  p_reason jsonb,
  p_retry_at timestamptz default null
)
  returns void
  language plpgsql
as $$
declare
  v_task_id uuid;
  v_attempt integer;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_now timestamptz := absurd.current_time();
  v_next_attempt integer;
  v_delay_seconds double precision := 0;
  v_next_available timestamptz;
  v_retry_kind text;
  v_base double precision;
  v_factor double precision;
  v_max_seconds double precision;
  v_first_started timestamptz;
  v_cancellation jsonb;
  v_max_duration bigint;
  v_task_state text;
  v_task_cancel boolean := false;
  v_new_run_id uuid;
  v_task_state_after text;
  v_recorded_attempt integer;
  v_last_attempt_run uuid := p_run_id;
  v_cancelled_at timestamptz := null;
begin
  execute format(
    'select r.task_id, r.attempt
       from absurd.%I r
      where r.run_id = $1
        and r.state in (''running'', ''sleeping'')
      for update',
    'r_' || p_queue_name
  )
  into v_task_id, v_attempt
  using p_run_id;

  if v_task_id is null then
    raise exception 'Run "%" cannot be failed in queue "%"', p_run_id, p_queue_name;
  end if;

  execute format(
    'select retry_strategy, max_attempts, first_started_at, cancellation, state
       from absurd.%I
      where task_id = $1
      for update',
    't_' || p_queue_name
  )
  into v_retry_strategy, v_max_attempts, v_first_started, v_cancellation, v_task_state
  using v_task_id;

  execute format(
    'update absurd.%I
        set state = ''failed'',
            wake_event = null,
            failed_at = $2,
            failure_reason = $3
      where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id, v_now, p_reason;

  v_next_attempt := v_attempt + 1;
  v_task_state_after := 'failed';
  v_recorded_attempt := v_attempt;

  if v_max_attempts is null or v_next_attempt <= v_max_attempts then
    if p_retry_at is not null then
      v_next_available := p_retry_at;
    else
      v_retry_kind := coalesce(v_retry_strategy->>'kind', 'none');
      if v_retry_kind = 'fixed' then
        v_base := coalesce((v_retry_strategy->>'base_seconds')::double precision, 60);
        v_delay_seconds := v_base;
      elsif v_retry_kind = 'exponential' then
        v_base := coalesce((v_retry_strategy->>'base_seconds')::double precision, 30);
        v_factor := coalesce((v_retry_strategy->>'factor')::double precision, 2);
        v_delay_seconds := v_base * power(v_factor, greatest(v_attempt - 1, 0));
        v_max_seconds := (v_retry_strategy->>'max_seconds')::double precision;
        if v_max_seconds is not null then
          v_delay_seconds := least(v_delay_seconds, v_max_seconds);
        end if;
      else
        v_delay_seconds := 0;
      end if;
      v_next_available := v_now + (v_delay_seconds * interval '1 second');
    end if;

    if v_next_available < v_now then
      v_next_available := v_now;
    end if;

    if v_cancellation is not null then
      v_max_duration := (v_cancellation->>'max_duration')::bigint;
      if v_max_duration is not null and v_first_started is not null then
        if extract(epoch from (v_next_available - v_first_started)) >= v_max_duration then
          v_task_cancel := true;
        end if;
      end if;
    end if;

    if not v_task_cancel then
      v_task_state_after := case when v_next_available > v_now then 'sleeping' else 'pending' end;
      v_new_run_id := absurd.portable_uuidv7();
      v_recorded_attempt := v_next_attempt;
      v_last_attempt_run := v_new_run_id;
      execute format(
        'insert into absurd.%I (run_id, task_id, attempt, state, available_at, wake_event, event_payload, result, failure_reason)
         values ($1, $2, $3, %L, $4, null, null, null, null)',
        'r_' || p_queue_name,
        v_task_state_after
      )
      using v_new_run_id, v_task_id, v_next_attempt, v_next_available;
    end if;
  end if;

  if v_task_cancel then
    v_task_state_after := 'cancelled';
    v_cancelled_at := v_now;
    v_recorded_attempt := greatest(v_recorded_attempt, v_attempt);
    v_last_attempt_run := p_run_id;
  end if;

  execute format(
    'update absurd.%I
        set state = %L,
            attempts = greatest(attempts, $3),
            last_attempt_run = $4,
            cancelled_at = coalesce(cancelled_at, $5)
      where task_id = $1',
    't_' || p_queue_name,
    v_task_state_after
  ) using v_task_id, v_task_state_after, v_recorded_attempt, v_last_attempt_run, v_cancelled_at;

  execute format(
    'delete from absurd.%I where run_id = $1',
    'w_' || p_queue_name
  ) using p_run_id;
end;
$$;

create function absurd.set_task_checkpoint_state (
  p_queue_name text,
  p_task_id uuid,
  p_step_name text,
  p_state jsonb,
  p_owner_run uuid,
  p_extend_claim_by integer default null
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_new_attempt integer;
  v_existing_attempt integer;
  v_existing_owner uuid;
  v_task_state text;
begin
  if p_step_name is null or length(trim(p_step_name)) = 0 then
    raise exception 'step_name must be provided';
  end if;

  execute format(
    'select r.attempt, t.state
       from absurd.%I r
       join absurd.%I t on t.task_id = r.task_id
      where r.run_id = $1',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_new_attempt, v_task_state
  using p_owner_run;

  if v_new_attempt is null then
    raise exception 'Run "%" not found for checkpoint', p_owner_run;
  end if;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  -- Extend the claim if requested
  if p_extend_claim_by is not null and p_extend_claim_by > 0 then
    execute format(
      'update absurd.%I
          set claim_expires_at = $2 + make_interval(secs => $3)
        where run_id = $1
          and state = ''running''
          and claim_expires_at is not null',
      'r_' || p_queue_name
    )
    using p_owner_run, v_now, p_extend_claim_by;
  end if;

  execute format(
    'select c.owner_run_id,
            r.attempt
       from absurd.%I c
       left join absurd.%I r on r.run_id = c.owner_run_id
      where c.task_id = $1
        and c.checkpoint_name = $2',
    'c_' || p_queue_name,
    'r_' || p_queue_name
  )
  into v_existing_owner, v_existing_attempt
  using p_task_id, p_step_name;

  if v_existing_owner is null or v_existing_attempt is null or v_new_attempt >= v_existing_attempt then
    execute format(
      'insert into absurd.%I (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
       values ($1, $2, $3, ''committed'', $4, $5)
       on conflict (task_id, checkpoint_name)
       do update set state = excluded.state,
                     status = excluded.status,
                     owner_run_id = excluded.owner_run_id,
                     updated_at = excluded.updated_at',
      'c_' || p_queue_name
    ) using p_task_id, p_step_name, p_state, p_owner_run, v_now;
  end if;
end;
$$;

create function absurd.extend_claim (
  p_queue_name text,
  p_run_id uuid,
  p_extend_by integer
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_extend_by integer;
  v_claim_timeout integer;
  v_rows_updated integer;
  v_task_state text;
begin
  execute format(
    'select t.state
       from absurd.%I r
       join absurd.%I t on t.task_id = r.task_id
      where r.run_id = $1',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_task_state
  using p_run_id;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  execute format(
    'update absurd.%I
        set claim_expires_at = $2 + make_interval(secs => $3)
      where run_id = $1
        and state = ''running''
        and claim_expires_at is not null',
    'r_' || p_queue_name
  )
  using p_run_id, v_now, p_extend_by;
end;
$$;

create function absurd.get_task_checkpoint_state (
  p_queue_name text,
  p_task_id uuid,
  p_step_name text,
  p_include_pending boolean default false
)
  returns table (
    checkpoint_name text,
    state jsonb,
    status text,
    owner_run_id uuid,
    updated_at timestamptz
  )
  language plpgsql
as $$
begin
  return query execute format(
    'select checkpoint_name, state, status, owner_run_id, updated_at
       from absurd.%I
      where task_id = $1
        and checkpoint_name = $2',
    'c_' || p_queue_name
  ) using p_task_id, p_step_name;
end;
$$;

create function absurd.get_task_checkpoint_states (
  p_queue_name text,
  p_task_id uuid,
  p_run_id uuid
)
  returns table (
    checkpoint_name text,
    state jsonb,
    status text,
    owner_run_id uuid,
    updated_at timestamptz
  )
  language plpgsql
as $$
begin
  return query execute format(
    'select checkpoint_name, state, status, owner_run_id, updated_at
       from absurd.%I
      where task_id = $1
      order by updated_at asc',
    'c_' || p_queue_name
  ) using p_task_id;
end;
$$;

create function absurd.await_event (
  p_queue_name text,
  p_task_id uuid,
  p_run_id uuid,
  p_step_name text,
  p_event_name text,
  p_timeout integer default null
)
  returns table (
    should_suspend boolean,
    payload jsonb
  )
  language plpgsql
as $$
declare
  v_run_state text;
  v_existing_payload jsonb;
  v_event_payload jsonb;
  v_checkpoint_payload jsonb;
  v_resolved_payload jsonb;
  v_timeout_at timestamptz;
  v_available_at timestamptz;
  v_now timestamptz := absurd.current_time();
  v_task_state text;
  v_wake_event text;
begin
  if p_event_name is null or length(trim(p_event_name)) = 0 then
    raise exception 'event_name must be provided';
  end if;

  if p_timeout is not null then
    if p_timeout < 0 then
      raise exception 'timeout must be non-negative';
    end if;
    v_timeout_at := v_now + (p_timeout::double precision * interval '1 second');
  end if;

  v_available_at := coalesce(v_timeout_at, 'infinity'::timestamptz);

  execute format(
    'select state
       from absurd.%I
      where task_id = $1
        and checkpoint_name = $2',
    'c_' || p_queue_name
  )
  into v_checkpoint_payload
  using p_task_id, p_step_name;

  if v_checkpoint_payload is not null then
    return query select false, v_checkpoint_payload;
    return;
  end if;

  -- Ensure a row exists for this event so we can take a row-level lock.
  --
  -- We use payload IS NULL as the sentinel for "not emitted yet".  emit_event
  -- always writes a non-NULL payload (at minimum JSON null).
  --
  -- Lock ordering is important to avoid deadlocks: await_event locks the event
  -- row first (FOR SHARE) and then the run row (FOR UPDATE).  emit_event
  -- naturally locks the event row via its UPSERT before touching waits/runs.
  execute format(
    'insert into absurd.%I (event_name, payload, emitted_at)
     values ($1, null, ''epoch''::timestamptz)
     on conflict (event_name) do nothing',
    'e_' || p_queue_name
  ) using p_event_name;

  execute format(
    'select 1
       from absurd.%I
      where event_name = $1
      for share',
    'e_' || p_queue_name
  ) using p_event_name;

  execute format(
    'select r.state, r.event_payload, r.wake_event, t.state
       from absurd.%I r
       join absurd.%I t on t.task_id = r.task_id
      where r.run_id = $1
      for update',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_run_state, v_existing_payload, v_wake_event, v_task_state
  using p_run_id;

  if v_run_state is null then
    raise exception 'Run "%" not found while awaiting event', p_run_id;
  end if;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  execute format(
    'select payload
       from absurd.%I
      where event_name = $1',
    'e_' || p_queue_name
  )
  into v_event_payload
  using p_event_name;

  if v_existing_payload is not null then
    execute format(
      'update absurd.%I
          set event_payload = null
        where run_id = $1',
      'r_' || p_queue_name
    ) using p_run_id;

    if v_event_payload is not null and v_event_payload = v_existing_payload then
      v_resolved_payload := v_existing_payload;
    end if;
  end if;

  if v_run_state <> 'running' then
    raise exception 'Run "%" must be running to await events', p_run_id;
  end if;

  if v_resolved_payload is null and v_event_payload is not null then
    v_resolved_payload := v_event_payload;
  end if;

  if v_resolved_payload is not null then
    execute format(
      'insert into absurd.%I (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
       values ($1, $2, $3, ''committed'', $4, $5)
       on conflict (task_id, checkpoint_name)
       do update set state = excluded.state,
                     status = excluded.status,
                     owner_run_id = excluded.owner_run_id,
                     updated_at = excluded.updated_at',
      'c_' || p_queue_name
    ) using p_task_id, p_step_name, v_resolved_payload, p_run_id, v_now;
    return query select false, v_resolved_payload;
    return;
  end if;

  -- Detect if we resumed due to timeout: wake_event matches and payload is null
  if v_resolved_payload is null and v_wake_event = p_event_name and v_existing_payload is null then
    -- Resumed due to timeout; don't re-sleep and don't create a new wait
    execute format(
      'update absurd.%I set wake_event = null where run_id = $1',
      'r_' || p_queue_name
    ) using p_run_id;
    return query select false, null::jsonb;
    return;
  end if;

  execute format(
    'insert into absurd.%I (task_id, run_id, step_name, event_name, timeout_at, created_at)
     values ($1, $2, $3, $4, $5, $6)
     on conflict (run_id, step_name)
     do update set event_name = excluded.event_name,
                   timeout_at = excluded.timeout_at,
                   created_at = excluded.created_at',
    'w_' || p_queue_name
  ) using p_task_id, p_run_id, p_step_name, p_event_name, v_timeout_at, v_now;

  execute format(
    'update absurd.%I
        set state = ''sleeping'',
            claimed_by = null,
            claim_expires_at = null,
            available_at = $3,
            wake_event = $2,
            event_payload = null
      where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id, p_event_name, v_available_at;

  execute format(
    'update absurd.%I
        set state = ''sleeping''
      where task_id = $1',
    't_' || p_queue_name
  ) using p_task_id;

  return query select true, null::jsonb;
  return;
end;
$$;

create function absurd.emit_event (
  p_queue_name text,
  p_event_name text,
  p_payload jsonb default null
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_payload jsonb := coalesce(p_payload, 'null'::jsonb);
begin
  if p_event_name is null or length(trim(p_event_name)) = 0 then
    raise exception 'event_name must be provided';
  end if;

  execute format(
    'insert into absurd.%I (event_name, payload, emitted_at)
     values ($1, $2, $3)
     on conflict (event_name)
     do update set payload = excluded.payload,
                   emitted_at = excluded.emitted_at',
    'e_' || p_queue_name
  ) using p_event_name, v_payload, v_now;

  execute format(
    'with expired_waits as (
        delete from absurd.%1$I w
         where w.event_name = $1
           and w.timeout_at is not null
           and w.timeout_at <= $2
         returning w.run_id
     ),
     affected as (
        select run_id, task_id, step_name
          from absurd.%1$I
         where event_name = $1
           and (timeout_at is null or timeout_at > $2)
     ),
     updated_runs as (
        update absurd.%2$I r
           set state = ''pending'',
               available_at = $2,
               wake_event = null,
               event_payload = $3,
               claimed_by = null,
               claim_expires_at = null
         where r.run_id in (select run_id from affected)
           and r.state = ''sleeping''
         returning r.run_id, r.task_id
     ),
     checkpoint_upd as (
        insert into absurd.%3$I (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
        select a.task_id, a.step_name, $3, ''committed'', a.run_id, $2
          from affected a
          join updated_runs ur on ur.run_id = a.run_id
        on conflict (task_id, checkpoint_name)
        do update set state = excluded.state,
                      status = excluded.status,
                      owner_run_id = excluded.owner_run_id,
                      updated_at = excluded.updated_at
     ),
     updated_tasks as (
        update absurd.%4$I t
           set state = ''pending''
         where t.task_id in (select task_id from updated_runs)
         returning task_id
     )
     delete from absurd.%5$I w
      where w.event_name = $1
        and w.run_id in (select run_id from updated_runs)',
    'w_' || p_queue_name,
    'r_' || p_queue_name,
    'c_' || p_queue_name,
    't_' || p_queue_name,
    'w_' || p_queue_name
  ) using p_event_name, v_now, v_payload;
end;
$$;

-- Manually cancels a task by its task_id.
-- Sets the task state to 'cancelled' and prevents any future runs.
-- Currently running code will detect cancellation at the next checkpoint or heartbeat.
create function absurd.cancel_task (
  p_queue_name text,
  p_task_id uuid
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_task_state text;
begin
  execute format(
    'select state
       from absurd.%I
      where task_id = $1
      for update',
    't_' || p_queue_name
  )
  into v_task_state
  using p_task_id;

  if v_task_state is null then
    raise exception 'Task "%" not found in queue "%"', p_task_id, p_queue_name;
  end if;

  if v_task_state in ('completed', 'failed', 'cancelled') then
    return;
  end if;

  execute format(
    'update absurd.%I
        set state = ''cancelled'',
            cancelled_at = coalesce(cancelled_at, $2)
      where task_id = $1',
    't_' || p_queue_name
  ) using p_task_id, v_now;

  execute format(
    'update absurd.%I
        set state = ''cancelled'',
            claimed_by = null,
            claim_expires_at = null
      where task_id = $1
        and state not in (''completed'', ''failed'', ''cancelled'')',
    'r_' || p_queue_name
  ) using p_task_id;

  execute format(
    'delete from absurd.%I where task_id = $1',
    'w_' || p_queue_name
  ) using p_task_id;
end;
$$;

-- Cleans up old completed, failed, or cancelled tasks and their related data.
-- Deletes tasks whose terminal timestamp (completed_at, failed_at, or cancelled_at)
-- is older than the specified TTL in seconds.
--
-- Returns the number of tasks deleted.
create function absurd.cleanup_tasks (
  p_queue_name text,
  p_ttl_seconds integer,
  p_limit integer default 1000
)
  returns integer
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_cutoff timestamptz;
  v_deleted_count integer;
begin
  if p_ttl_seconds is null or p_ttl_seconds < 0 then
    raise exception 'TTL must be a non-negative number of seconds';
  end if;

  v_cutoff := v_now - (p_ttl_seconds * interval '1 second');

  -- Delete in order: wait registrations, checkpoints, runs, then tasks
  -- Use a CTE to find eligible tasks and delete their related data
  execute format(
    'with eligible_tasks as (
        select t.task_id,
               case
                 when t.state = ''completed'' then r.completed_at
                 when t.state = ''failed'' then r.failed_at
                 when t.state = ''cancelled'' then t.cancelled_at
                 else null
               end as terminal_at
          from absurd.%1$I t
          left join absurd.%2$I r on r.run_id = t.last_attempt_run
         where t.state in (''completed'', ''failed'', ''cancelled'')
     ),
     to_delete as (
        select task_id
          from eligible_tasks
         where terminal_at is not null
           and terminal_at < $1
         order by terminal_at
         limit $2
     ),
     del_waits as (
        delete from absurd.%3$I w
         where w.task_id in (select task_id from to_delete)
     ),
     del_checkpoints as (
        delete from absurd.%4$I c
         where c.task_id in (select task_id from to_delete)
     ),
     del_runs as (
        delete from absurd.%2$I r
         where r.task_id in (select task_id from to_delete)
     ),
     del_tasks as (
        delete from absurd.%1$I t
         where t.task_id in (select task_id from to_delete)
         returning 1
     )
     select count(*) from del_tasks',
    't_' || p_queue_name,
    'r_' || p_queue_name,
    'w_' || p_queue_name,
    'c_' || p_queue_name
  )
  into v_deleted_count
  using v_cutoff, p_limit;

  return v_deleted_count;
end;
$$;

-- Cleans up old emitted events.
-- Deletes events whose emitted_at timestamp is older than the specified TTL in seconds.
--
-- Returns the number of events deleted.
create function absurd.cleanup_events (
  p_queue_name text,
  p_ttl_seconds integer,
  p_limit integer default 1000
)
  returns integer
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_cutoff timestamptz;
  v_deleted_count integer;
begin
  if p_ttl_seconds is null or p_ttl_seconds < 0 then
    raise exception 'TTL must be a non-negative number of seconds';
  end if;

  v_cutoff := v_now - (p_ttl_seconds * interval '1 second');

  execute format(
    'with to_delete as (
        select event_name
          from absurd.%I
         where emitted_at < $1
         order by emitted_at
         limit $2
     ),
     del_events as (
        delete from absurd.%I e
         where e.event_name in (select event_name from to_delete)
         returning 1
     )
     select count(*) from del_events',
    'e_' || p_queue_name,
    'e_' || p_queue_name
  )
  into v_deleted_count
  using v_cutoff, p_limit;

  return v_deleted_count;
end;
$$;

-- utility function to generate a uuidv7 even for older postgres versions.
create function absurd.portable_uuidv7 ()
  returns uuid
  language plpgsql
  volatile
as $$
declare
  v_server_num integer := current_setting('server_version_num')::int;
  ts_ms bigint;
  b bytea;
  rnd bytea;
  i int;
begin
  if v_server_num >= 180000 then
    return uuidv7 ();
  end if;
  ts_ms := floor(extract(epoch from absurd.current_time()) * 1000)::bigint;
  rnd := uuid_send(uuid_generate_v4 ());
  b := repeat(E'\\000', 16)::bytea;
  for i in 0..5 loop
    b := set_byte(b, i, ((ts_ms >> ((5 - i) * 8)) & 255)::int);
  end loop;
  for i in 6..15 loop
    b := set_byte(b, i, get_byte(rnd, i));
  end loop;
  b := set_byte(b, 6, ((get_byte(b, 6) & 15) | (7 << 4)));
  b := set_byte(b, 8, ((get_byte(b, 8) & 63) | 128));
  return encode(b, 'hex')::uuid;
end;
$$;
