------------------------------------------------------------
-- Durable task functions
------------------------------------------------------------
create function absurd.spawn_task (p_queue_name text, p_task_name text, p_params jsonb, p_options jsonb default '{}'::jsonb)
  returns table (
    task_id uuid,
    run_id uuid,
    attempt integer
  )
  as $$
declare
  v_task_id uuid := absurd.portable_uuidv7 ();
  v_run_id uuid := absurd.portable_uuidv7 ();
  v_attempt integer := 1;
  v_headers jsonb;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_raw_cancellation jsonb;
  v_cancellation jsonb;
  v_max_duration_ms bigint;
  v_max_delay_ms bigint;
  v_message jsonb;
  v_raw_options jsonb := coalesce(p_options, '{}'::jsonb);
  v_sanitized_options jsonb;
  v_qtable text := absurd.format_table_name (p_queue_name, 'q');
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_now timestamptz := clock_timestamp();
begin
  v_headers := v_raw_options -> 'headers';
  v_retry_strategy := v_raw_options -> 'retry_strategy';
  v_raw_cancellation := v_raw_options -> 'cancellation';
  if v_raw_options ? 'max_attempts' then
    v_max_attempts := (v_raw_options ->> 'max_attempts')::integer;
    if v_max_attempts < 1 then
      raise exception 'max_attempts must be at least 1';
    end if;
  else
    v_max_attempts := null;
  end if;
  if v_raw_cancellation is not null and jsonb_typeof(v_raw_cancellation) = 'object' then
    v_cancellation := '{}'::jsonb;
    if v_raw_cancellation ? 'max_duration_ms' then
      v_max_duration_ms := (v_raw_cancellation ->> 'max_duration_ms')::bigint;
      if v_max_duration_ms <= 0 then
        raise exception 'max_duration_ms must be greater than 0';
      end if;
      v_cancellation := v_cancellation || jsonb_build_object('max_duration_ms', v_max_duration_ms);
    end if;
    if v_raw_cancellation ? 'max_delay_ms' then
      v_max_delay_ms := (v_raw_cancellation ->> 'max_delay_ms')::bigint;
      if v_max_delay_ms <= 0 then
        raise exception 'max_delay_ms must be greater than 0';
      end if;
      v_cancellation := v_cancellation || jsonb_build_object('max_delay_ms', v_max_delay_ms);
    end if;
    if v_cancellation = '{}'::jsonb then
      v_cancellation := null;
    end if;
  else
    v_cancellation := null;
  end if;
  v_sanitized_options := v_raw_options;
  if v_max_attempts is not null then
    v_sanitized_options := v_sanitized_options || jsonb_build_object('max_attempts', v_max_attempts);
  else
    v_sanitized_options := v_sanitized_options - 'max_attempts';
  end if;
  if v_retry_strategy is not null then
    v_sanitized_options := v_sanitized_options || jsonb_build_object('retry_strategy', v_retry_strategy);
  else
    v_sanitized_options := v_sanitized_options - 'retry_strategy';
  end if;
  if v_headers is not null then
    v_sanitized_options := v_sanitized_options || jsonb_build_object('headers', v_headers);
  else
    v_sanitized_options := v_sanitized_options - 'headers';
  end if;
  if v_cancellation is not null then
    v_sanitized_options := v_sanitized_options || jsonb_build_object('cancellation', v_cancellation);
  else
    v_sanitized_options := v_sanitized_options - 'cancellation';
  end if;
  v_sanitized_options := jsonb_strip_nulls(v_sanitized_options);
  if v_sanitized_options is null then
    v_sanitized_options := '{}'::jsonb;
  end if;
  v_message := jsonb_build_object('task_id', v_task_id, 'run_id', v_run_id, 'attempt', v_attempt);
  perform
    absurd.send (p_queue_name, v_run_id, v_message, v_headers);
  execute format($fmt$
    insert into absurd.%I (
      task_id,
      run_id,
      attempt,
      task_name,
      params,
      status,
      created_at,
      updated_at,
      options
    )
    values ($1, $2, $3, $4, $5, 'pending', $6, $6, $7)
  $fmt$, v_rtable)
  using v_task_id, v_run_id, v_attempt, p_task_name, p_params, v_now, v_sanitized_options;
  return query
  select
    v_task_id,
    v_run_id,
    v_attempt;
end;
$$
language plpgsql;

create function absurd.claim_task (p_queue_name text, p_worker_id text, p_claim_timeout integer default 30, p_qty integer default 1)
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
  as $$
declare
  v_claimed_at timestamptz := clock_timestamp();
  v_lease_expires timestamptz := v_claimed_at + make_interval(secs => p_claim_timeout);
  v_qtable text := absurd.format_table_name (p_queue_name, 'q');
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_wtable text := absurd.format_table_name (p_queue_name, 'w');
  v_sql text;
begin
  v_sql := format($fmt$
    with candidate as (
      select
        q.msg_id
      from
        absurd.%I as q
      where
        q.vt <= $2
      order by
        q.vt asc, q.msg_id asc
      limit $1
      for update
        skip locked
    ),
    -- we want to clean up any orphaned queue entries that have no corresponding run
    -- this should not happen, but if it does, we want to avoid clogging the queue
    deleted_orphans as (
      delete from absurd.%I as q
      using candidate c
      where
        q.msg_id = c.msg_id
        and not exists (
          select
            1
          from
            absurd.%I as r
          where
            r.run_id = c.msg_id)
      returning
        q.msg_id
    ),
    claimable as (
      select
        c.msg_id,
        r.task_id,
        r.attempt,
        r.task_name,
        r.params,
        r.options,
        r.created_at,
        r.started_at,
        w.wake_event,
        w.payload
      from
        candidate c
        join absurd.%I as r
          on r.run_id = c.msg_id
        left join lateral (
          select
            w.wake_event,
            w.payload
          from
            absurd.%I as w
          where
            w.run_id = c.msg_id
          order by
            w.updated_at desc
          limit 1
        ) as w on true
    ),
    to_cancel as (
      select
        cl.msg_id,
        cl.task_id,
        cl.attempt,
        cancel.reason,
        case cancel.reason
          when 'max_delay_exceeded' then cancel.max_delay_ms
          when 'max_duration_exceeded' then cancel.max_duration_ms
          else null
        end as limit_ms,
        case cancel.reason
          when 'max_delay_exceeded' then floor(extract(epoch from ($4 - cl.created_at)) * 1000)::bigint
          when 'max_duration_exceeded' then floor(extract(epoch from ($4 - cl.started_at)) * 1000)::bigint
          else null
        end as elapsed_ms,
        case cancel.reason
          when 'max_delay_exceeded' then 'startup'
          when 'max_duration_exceeded' then 'runtime'
          else null
        end as phase
      from
        claimable cl
        cross join lateral (
          select
            case
              when opt.cancellation is null then null
              when opt.cancellation ? 'max_delay_ms'
                and cl.started_at is null
                and cl.created_at + (interval '1 millisecond' * (opt.cancellation ->> 'max_delay_ms')::bigint) <= $4 then 'max_delay_exceeded'
              when opt.cancellation ? 'max_duration_ms'
                and cl.started_at is not null
                and cl.started_at + (interval '1 millisecond' * (opt.cancellation ->> 'max_duration_ms')::bigint) <= $4 then 'max_duration_exceeded'
              else null
            end as reason,
            (opt.cancellation ->> 'max_delay_ms')::bigint as max_delay_ms,
            (opt.cancellation ->> 'max_duration_ms')::bigint as max_duration_ms
          from
            (select cl.options -> 'cancellation' as cancellation) as opt
        ) as cancel
      where
        cancel.reason is not null
    ),
    cancel_runs as (
      update
        absurd.%I as r
      set
        status = 'cancelled',
        updated_at = $4,
        state = jsonb_build_object(
          'status', 'cancelled',
          'reason', t.reason,
          'phase', t.phase,
          'limit_ms', t.limit_ms,
          'elapsed_ms', t.elapsed_ms,
          'cancelled_at', $4,
          'attempt', r.attempt
        )
      from
        to_cancel t
      where
        r.run_id = t.msg_id
      returning
        r.run_id
    ),
    cancel_queue as (
      delete from absurd.%I as q
      using to_cancel t
      where
        q.msg_id = t.msg_id
      returning
        q.msg_id
    ),
    cancel_waits as (
      delete from absurd.%I as w
      using to_cancel t
      where
        w.run_id = t.msg_id
    ),
    active_claimable as (
      select
        c.*
      from
        claimable c
        left join to_cancel t
          on t.msg_id = c.msg_id
      where
        t.msg_id is null
    ),
    update_queue as (
      update
        absurd.%I as q
      set
        vt = $3,
        read_ct = q.read_ct + 1
      from
        active_claimable c
      where
        q.msg_id = c.msg_id
      returning
        q.msg_id
    ),
    update_runs as (
      update
        absurd.%I as r
      set
        status = 'running',
        started_at = coalesce(r.started_at, $4),
        updated_at = $4
      from
        active_claimable c
      where
        r.run_id = c.msg_id
      returning
        r.run_id,
        r.task_id,
        r.attempt,
        r.task_name,
        r.params,
        r.options as options
    ),
    delete_waits as (
      delete from absurd.%I as w
      using active_claimable c
      where
        w.run_id = c.msg_id
    )
    select
      c.msg_id as run_id,
      u.task_id,
      u.attempt,
      u.task_name,
      u.params,
      u.options -> 'retry_strategy' as retry_strategy,
      case when u.options ? 'max_attempts' then (u.options ->> 'max_attempts')::integer else null end as max_attempts,
      u.options -> 'headers' as headers,
      c.wake_event,
      c.payload as event_payload
    from
      active_claimable c
      join update_runs u
        on u.run_id = c.msg_id
      left join update_queue
        on update_queue.msg_id = c.msg_id
      order by
        c.msg_id asc
  $fmt$, v_qtable, v_qtable, v_rtable, v_rtable, v_wtable, v_rtable, v_qtable, v_wtable, v_qtable, v_rtable, v_wtable);
  return query execute v_sql
  using p_qty, v_claimed_at, v_lease_expires, v_claimed_at;
end;
$$
language plpgsql;

create function absurd.complete_run (p_queue_name text, p_run_id uuid, p_state jsonb default null)
  returns void
  as $$
declare
  v_task_id uuid;
  v_status text;
  v_now timestamptz := clock_timestamp();
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_wtable text := absurd.format_table_name (p_queue_name, 'w');
  v_rowcount integer;
begin
  execute format($fmt$
    select
      task_id,
      status
    from
      absurd.%I
    where
      run_id = $1
  $fmt$, v_rtable)
  using p_run_id
  into v_task_id,
  v_status;
  get diagnostics v_rowcount = row_count;
  if v_rowcount = 0 then
    raise exception 'run % not found for queue %', p_run_id, p_queue_name;
  end if;
  if v_status = 'cancelled' then
    raise exception 'run % has been cancelled and cannot be completed', p_run_id;
  end if;
  if v_status <> 'running' then
    raise exception 'run % cannot be completed from status %', p_run_id, v_status;
  end if;
  perform
    absurd.delete (p_queue_name, p_run_id);
  execute format($fmt$
    update absurd.%I
    set
      status = 'completed',
      updated_at = $2,
      completed_at = $2,
      state = $3
    where
      run_id = $1
  $fmt$, v_rtable)
  using p_run_id, v_now, p_state;
  execute format($fmt$
    delete from absurd.%I
    where
      run_id = $1
  $fmt$, v_wtable)
  using p_run_id;
end;
$$
language plpgsql;

create function absurd.fail_run (p_queue_name text, p_run_id uuid, p_reason jsonb, p_retry_at timestamptz default null)
  returns void
  as $$
declare
  v_task_id uuid;
  v_attempt integer;
  v_max_attempts integer;
  v_retry_strategy jsonb;
  v_options jsonb;
  v_should_retry boolean;
  v_next_attempt integer;
  v_new_run_id uuid;
  v_effective_retry_at timestamptz;
  v_now timestamptz := clock_timestamp();
  v_qtable text := absurd.format_table_name (p_queue_name, 'q');
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_wtable text := absurd.format_table_name (p_queue_name, 'w');
  v_new_status text;
  v_headers jsonb;
  v_strategy_kind text;
  v_base_seconds double precision;
  v_factor double precision;
  v_max_seconds double precision;
  v_delay_seconds double precision;
  v_task_name text;
  v_params jsonb;
  v_rowcount integer;
  v_status text;
begin
  execute format($fmt$
    select
      task_id,
      attempt,
      options,
      task_name,
      params,
      status
    from
      absurd.%I
    where
      run_id = $1
  $fmt$, v_rtable)
  using p_run_id
  into v_task_id,
  v_attempt,
  v_options,
  v_task_name,
  v_params,
  v_status;
  get diagnostics v_rowcount = row_count;
  if v_rowcount = 0 then
    raise exception 'run % not found for queue %', p_run_id, p_queue_name;
  end if;
  if v_status = 'cancelled' then
    raise exception 'run % has been cancelled and cannot fail', p_run_id;
  end if;
  if v_status <> 'running' then
    raise exception 'run % cannot be failed from status %', p_run_id, v_status;
  end if;
  v_headers := v_options -> 'headers';
  if v_options ? 'max_attempts' then
    v_max_attempts := (v_options ->> 'max_attempts')::integer;
  else
    v_max_attempts := null;
  end if;
  v_retry_strategy := v_options -> 'retry_strategy';
  execute format($fmt$
    update absurd.%I
    set
      status = 'failed',
      updated_at = $2,
      state = $3
    where
      run_id = $1
  $fmt$, v_rtable)
  using p_run_id, v_now, p_reason;
  execute format($fmt$
    delete from absurd.%I
    where
      run_id = $1
  $fmt$, v_wtable)
  using p_run_id;
  perform
    absurd.delete (p_queue_name, p_run_id);
  v_should_retry := v_max_attempts is null
    or v_attempt < v_max_attempts;
  if v_should_retry then
    v_next_attempt := v_attempt + 1;
    if p_retry_at is not null then
      v_effective_retry_at := p_retry_at;
    else
      v_effective_retry_at := v_now;
      if v_retry_strategy is not null then
        v_strategy_kind := lower(v_retry_strategy ->> 'kind');
        if v_strategy_kind in ('fixed', 'exponential') then
          v_base_seconds := coalesce(
            (v_retry_strategy ->> 'base_seconds')::double precision,
            5::double precision
          );
          if v_strategy_kind = 'exponential' then
            v_factor := coalesce(
              (v_retry_strategy ->> 'factor')::double precision,
              2::double precision
            );
            v_delay_seconds := v_base_seconds * power(v_factor, greatest(v_attempt - 1, 0));
          else
            v_delay_seconds := v_base_seconds;
          end if;
          v_max_seconds := (v_retry_strategy ->> 'max_seconds')::double precision;
          if v_max_seconds is not null then
            v_delay_seconds := least(v_delay_seconds, v_max_seconds);
          end if;
          if v_delay_seconds is not null and v_delay_seconds > 0 then
            v_effective_retry_at := v_now + make_interval(secs => v_delay_seconds);
          end if;
        end if;
      end if;
    end if;
    if v_effective_retry_at is null then
      v_effective_retry_at := v_now;
    end if;
    v_new_status := case when v_effective_retry_at > v_now then
      'sleeping'
    else
      'pending'
    end;
    v_new_run_id := absurd.portable_uuidv7();
    perform
      absurd.send (p_queue_name, v_new_run_id, jsonb_build_object('task_id', v_task_id, 'run_id', v_new_run_id, 'attempt', v_next_attempt), v_headers, v_effective_retry_at);
    execute format($fmt$
      insert into absurd.%I (
        task_id,
        run_id,
        attempt,
        task_name,
        params,
        status,
        created_at,
        updated_at,
        options
      )
      values (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $7,
        $8
      )
    $fmt$, v_rtable)
    using v_task_id, v_new_run_id, v_next_attempt, v_task_name, v_params, v_new_status, v_now, v_options;
    if v_effective_retry_at > v_now then
      perform
        absurd.schedule_run (p_queue_name, v_new_run_id, v_effective_retry_at, true);
    end if;
  end if;
end;
$$
language plpgsql;

create function absurd.schedule_run (p_queue_name text, p_run_id uuid, p_wake_at timestamptz, p_suspend boolean default true)
  returns void
  as $$
declare
  v_task_id uuid;
  v_rowcount integer;
  v_now timestamptz := clock_timestamp();
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_wtable text := absurd.format_table_name (p_queue_name, 'w');
  v_created_at timestamptz;
  v_started_at timestamptz;
  v_options jsonb;
  v_raw_cancellation jsonb;
  v_max_duration_ms bigint;
  v_max_delay_ms bigint;
  v_cancel_at timestamptz;
  v_candidate timestamptz;
  v_effective_wake_at timestamptz;
begin
  execute format($fmt$
    update absurd.%I
    set
      status = case when $3 then
        'sleeping'
      else
        status
      end,
      updated_at = $4
    where
      run_id = $1
    returning
      task_id,
      created_at,
      started_at,
      options
  $fmt$, v_rtable)
  using p_run_id, p_wake_at, p_suspend, v_now
  into v_task_id,
  v_created_at,
  v_started_at,
  v_options;
  get diagnostics v_rowcount = row_count;
  if v_rowcount = 0 then
    raise exception 'run % not found for queue %', p_run_id, p_queue_name;
  end if;
  v_raw_cancellation := v_options -> 'cancellation';
  v_cancel_at := null;
  if v_raw_cancellation is not null and jsonb_typeof(v_raw_cancellation) = 'object' then
    if v_raw_cancellation ? 'max_delay_ms' then
      v_max_delay_ms := (v_raw_cancellation ->> 'max_delay_ms')::bigint;
      if v_max_delay_ms is not null and v_max_delay_ms > 0 and v_started_at is null then
        v_candidate := v_created_at + (interval '1 millisecond' * v_max_delay_ms);
        if v_cancel_at is null or v_candidate < v_cancel_at then
          v_cancel_at := v_candidate;
        end if;
      end if;
    end if;
    if v_raw_cancellation ? 'max_duration_ms' then
      v_max_duration_ms := (v_raw_cancellation ->> 'max_duration_ms')::bigint;
      if v_max_duration_ms is not null and v_max_duration_ms > 0 and v_started_at is not null then
        v_candidate := v_started_at + (interval '1 millisecond' * v_max_duration_ms);
        if v_cancel_at is null or v_candidate < v_cancel_at then
          v_cancel_at := v_candidate;
        end if;
      end if;
    end if;
  end if;
  if p_suspend then
    execute format($fmt$
      insert into absurd.%I (task_id, run_id, wait_type, wake_at, updated_at)
      values ($1, $2, 'sleep', $3, $4)
      on conflict (task_id, run_id, wait_type)
      do update set
        wake_at = excluded.wake_at,
        updated_at = excluded.updated_at
    $fmt$, v_wtable)
    using v_task_id, p_run_id, p_wake_at, v_now;
  else
    execute format($fmt$
      delete from absurd.%I
      where
        run_id = $1
        and wait_type = 'sleep'
    $fmt$, v_wtable)
    using p_run_id;
  end if;
  execute format($fmt$
    delete from absurd.%I
    where
      run_id = $1
      and wait_type = 'event'
  $fmt$, v_wtable)
  using p_run_id;
  v_effective_wake_at := p_wake_at;
  if v_cancel_at is not null then
    if v_effective_wake_at is null or v_effective_wake_at > v_cancel_at then
      v_effective_wake_at := v_cancel_at;
    end if;
  end if;
  if v_effective_wake_at is null then
    v_effective_wake_at := v_now;
  end if;
  if v_effective_wake_at < v_now then
    v_effective_wake_at := v_now;
  end if;
  perform
    absurd.set_vt_at (p_queue_name, p_run_id, v_effective_wake_at);
end;
$$
language plpgsql;
