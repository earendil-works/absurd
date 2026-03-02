-- Migration from 0.0.7 to 0.0.8
--
-- Adds recurring/scheduled task support:
-- - parse_cron_field: cron field expansion helper
-- - next_cron_time: cron expression parser
-- - s_{queue} table for schedule definitions
-- - Schedule CRUD: create_schedule, get_schedule, list_schedules,
--   delete_schedule, update_schedule
-- - tick_schedules: spawns tasks from due schedules
-- - claim_task: now calls tick_schedules before claiming

-- Expands a single cron field expression into a sorted, deduplicated
-- integer array.  Supports *, exact values, ranges (N-M), steps (*/N,
-- N-M/S), comma-separated lists, and named days/months.
create or replace function absurd.parse_cron_field (
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
      -- Simple range: N-M
      v_range_parts := string_to_array(v_part, '-');
      v_start := greatest(v_range_parts[1]::integer, p_min);
      v_end := least(v_range_parts[2]::integer, p_max);
      for v_i in v_start..v_end loop
        v_result := v_result || v_i;
      end loop;

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
create or replace function absurd.next_cron_time (
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
    return p_after + (regexp_replace(v_expr, '^@every\s+', '', 'i')::integer * interval '1 second');
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
create or replace function absurd.create_schedule (
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
create or replace function absurd.get_schedule (
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
create or replace function absurd.list_schedules (
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
create or replace function absurd.delete_schedule (
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
create or replace function absurd.update_schedule (
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
create or replace function absurd.tick_schedules (
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

-- Add s_ table to all existing queues
do $$
declare
  v_queue text;
begin
  for v_queue in select queue_name from absurd.queues loop
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
      's_' || v_queue
    );
  end loop;
end;
$$;

-- Replace claim_task with the version that calls tick_schedules
create or replace function absurd.claim_task (
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

-- Update ensure_queue_tables so new queues created after migration get the s_ table
create or replace function absurd.ensure_queue_tables (p_queue_name text)
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
end;
$$;

-- Update drop_queue so dropping queues cleans up s_ table
create or replace function absurd.drop_queue (p_queue_name text)
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
