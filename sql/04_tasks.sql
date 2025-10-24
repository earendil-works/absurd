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
  v_message jsonb;
  v_options jsonb := coalesce(p_options, '{}'::jsonb);
  v_qtable text := absurd.format_table_name (p_queue_name, 'q');
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_now timestamptz := clock_timestamp();
begin
  v_headers := v_options -> 'headers';
  v_retry_strategy := v_options -> 'retry_strategy';
  if v_options ? 'max_attempts' then
    v_max_attempts := (v_options ->> 'max_attempts')::integer;
    if v_max_attempts < 1 then
      raise exception 'max_attempts must be at least 1';
    end if;
  else
    v_max_attempts := null;
  end if;
  v_message := jsonb_build_object('task_id', v_task_id, 'run_id', v_run_id, 'attempt', v_attempt);
  perform
    absurd.send (p_queue_name, v_run_id, v_message, v_headers);
  execute format($fmt$
    insert into absurd.%I (
      queue_name,
      task_id,
      run_id,
      attempt,
      task_name,
      params,
      status,
      max_attempts,
      retry_strategy,
      created_at,
      updated_at,
      headers
    )
    values ($1, $2, $3, $4, $5, $6, 'pending', $7, $8, $9, $9, $10)
  $fmt$, v_rtable)
  using p_queue_name, v_task_id, v_run_id, v_attempt, p_task_name, p_params, v_max_attempts, v_retry_strategy, v_now, v_headers;
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
    lease_expires_at timestamptz,
    wake_event text,
    event_payload jsonb
  )
  as $$
declare
  v_claimed_at timestamptz := clock_timestamp();
  v_lease_expires timestamptz := v_claimed_at + make_interval(secs => p_claim_timeout);
  v_qtable text := absurd.format_table_name (p_queue_name, 'q');
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_stable text := absurd.format_table_name (p_queue_name, 's');
  v_sql text;
begin
  v_sql := format($fmt$
    with candidate as (
      select
        q.msg_id,
        q.headers as queue_headers
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
        c.queue_headers,
        r.task_id,
        r.attempt,
        r.task_name,
        r.params,
        r.retry_strategy,
        r.max_attempts,
        r.headers as run_headers,
        w.wake_event,
        w.payload
      from
        candidate c
        join absurd.%I as r
          on r.run_id = c.msg_id
        left join lateral (
          select
            s.wake_event,
            s.payload
          from
            absurd.%I as s
          where
            s.item_type = 'wait'
            and s.run_id = c.msg_id
          order by
            s.updated_at desc
          limit 1
        ) as w on true
    ),
    update_queue as (
      update
        absurd.%I as q
      set
        vt = $3,
        read_ct = q.read_ct + 1
      from
        claimable c
      where
        q.msg_id = c.msg_id
      returning
        q.msg_id,
        q.headers as queue_headers
    ),
    update_runs as (
      update
        absurd.%I as r
      set
        status = 'running',
        claimed_by = $4,
        last_claimed_at = $5,
        lease_expires_at = $6,
        next_wake_at = null,
        wake_event = null,
        updated_at = $5
      from
        claimable c
      where
        r.run_id = c.msg_id
      returning
        r.run_id,
        r.task_id,
        r.attempt,
        r.task_name,
        r.params,
        r.retry_strategy,
        r.max_attempts,
        r.headers as run_headers
    ),
    delete_waits as (
      delete from absurd.%I as s
      using claimable c
      where
        s.run_id = c.msg_id
        and s.item_type = 'wait'
    )
    select
      c.msg_id as run_id,
      u.task_id,
      u.attempt,
      u.task_name,
      u.params,
      u.retry_strategy,
      u.max_attempts,
      coalesce(update_queue.queue_headers, u.run_headers, c.queue_headers) as headers,
      $6 as lease_expires_at,
      c.wake_event,
      c.payload as event_payload
    from
      claimable c
      join update_runs u
        on u.run_id = c.msg_id
      left join update_queue
        on update_queue.msg_id = c.msg_id
      order by
        c.msg_id asc
  $fmt$, v_qtable, v_qtable, v_rtable, v_rtable, v_stable, v_qtable, v_rtable, v_stable);
  return query execute v_sql
  using p_qty, v_claimed_at, v_lease_expires, p_worker_id, v_claimed_at, v_lease_expires;
end;
$$
language plpgsql;

create function absurd.complete_run (p_queue_name text, p_run_id uuid, p_final_state jsonb default null, p_archive boolean default false)
  returns void
  as $$
declare
  v_task_id uuid;
  v_now timestamptz := clock_timestamp();
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_stable text := absurd.format_table_name (p_queue_name, 's');
  v_rowcount integer;
begin
  execute format($fmt$
    select
      task_id
    from
      absurd.%I
    where
      run_id = $1
  $fmt$, v_rtable)
  using p_run_id
  into v_task_id;
  get diagnostics v_rowcount = row_count;
  if v_rowcount = 0 then
    raise exception 'run % not found for queue %', p_run_id, p_queue_name;
  end if;
  perform
    absurd.delete (p_queue_name, p_run_id);
  execute format($fmt$
    update absurd.%I
    set
      status = 'completed',
      updated_at = $2,
      lease_expires_at = null,
      claimed_by = null,
      next_wake_at = null,
      wake_event = null,
      completed_at = $2,
      final_status = 'completed',
      final_state = case when $4 then $3 else final_state end
    where
      run_id = $1
  $fmt$, v_rtable)
  using p_run_id, v_now, p_final_state, p_archive;
  execute format($fmt$
    update absurd.%I
    set
      final_status = 'completed',
      completed_at = coalesce(completed_at, $2)
    where
      task_id = $1
  $fmt$, v_rtable)
  using v_task_id, v_now;
  execute format($fmt$
    delete from absurd.%I
    where
      item_type = 'wait'
      and run_id = $1
  $fmt$, v_stable)
  using p_run_id;
end;
$$
language plpgsql;

create function absurd.fail_run (p_queue_name text, p_run_id uuid, p_reason text, p_retry_at timestamptz default null)
  returns void
  as $$
declare
  v_task_id uuid;
  v_attempt integer;
  v_max_attempts integer;
  v_retry_strategy jsonb;
  v_should_retry boolean;
  v_next_attempt integer;
  v_new_run_id uuid;
  v_effective_retry_at timestamptz;
  v_now timestamptz := clock_timestamp();
  v_qtable text := absurd.format_table_name (p_queue_name, 'q');
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_stable text := absurd.format_table_name (p_queue_name, 's');
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
begin
  execute format($fmt$
    select
      task_id,
      attempt,
      max_attempts,
      retry_strategy,
      headers,
      task_name,
      params
    from
      absurd.%I
    where
      run_id = $1
  $fmt$, v_rtable)
  using p_run_id
  into v_task_id,
  v_attempt,
  v_max_attempts,
  v_retry_strategy,
  v_headers,
  v_task_name,
  v_params;
  get diagnostics v_rowcount = row_count;
  if v_rowcount = 0 then
    raise exception 'run % not found for queue %', p_run_id, p_queue_name;
  end if;
  execute format($fmt$
    update absurd.%I
    set
      status = 'failed',
      updated_at = $2,
      lease_expires_at = null,
      claimed_by = null,
      next_wake_at = null,
      wake_event = null
    where
      run_id = $1
  $fmt$, v_rtable)
  using p_run_id, v_now;
  execute format($fmt$
    delete from absurd.%I
    where
      item_type = 'wait'
      and run_id = $1
  $fmt$, v_stable)
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
        queue_name,
        task_id,
        run_id,
        attempt,
        task_name,
        params,
        status,
        max_attempts,
        retry_strategy,
        next_wake_at,
        created_at,
        updated_at,
        headers
      )
      values (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        case when $7 = 'sleeping' then $10 else null end,
        $11,
        $11,
        $12
      )
    $fmt$, v_rtable)
    using p_queue_name, v_task_id, v_new_run_id, v_next_attempt, v_task_name, v_params, v_new_status, v_max_attempts, v_retry_strategy, v_effective_retry_at, v_now, v_headers;
    execute format($fmt$
      update absurd.%I
      set
        final_status = 'pending',
        completed_at = null
      where
        task_id = $1
    $fmt$, v_rtable)
    using v_task_id;
    if v_effective_retry_at > v_now then
      perform
        absurd.schedule_run (p_queue_name, v_new_run_id, v_effective_retry_at, true);
    end if;
  else
    execute format($fmt$
      update absurd.%I
      set
        final_status = 'failed',
        completed_at = coalesce(completed_at, $2)
      where
        task_id = $1
    $fmt$, v_rtable)
    using v_task_id, v_now;
  end if;
end;
$$
language plpgsql;

create function absurd.schedule_run (p_queue_name text, p_run_id uuid, p_wake_at timestamptz, p_suspend boolean default true)
  returns void
  as $$
declare
  v_task_id uuid;
  v_status text;
  v_rowcount integer;
  v_now timestamptz := clock_timestamp();
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_stable text := absurd.format_table_name (p_queue_name, 's');
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
  into v_task_id, v_status;
  get diagnostics v_rowcount = row_count;
  if v_rowcount = 0 then
    raise exception 'run % not found for queue %', p_run_id, p_queue_name;
  end if;
  if p_suspend then
    execute format($fmt$
      insert into absurd.%I (item_type, task_id, run_id, wait_type, wake_at, created_at, updated_at)
      values ('wait', $1, $2, 'sleep', $3, $4, $4)
      on conflict (task_id, run_id, wait_type)
        where item_type = 'wait'
      do update set
        wake_at = excluded.wake_at,
        updated_at = excluded.updated_at
    $fmt$, v_stable)
    using v_task_id, p_run_id, p_wake_at, v_now;
  else
    execute format($fmt$
      delete from absurd.%I
      where
        item_type = 'wait'
        and run_id = $1
        and wait_type = 'sleep'
    $fmt$, v_stable)
    using p_run_id;
  end if;
  execute format($fmt$
    delete from absurd.%I
    where
      item_type = 'wait'
      and run_id = $1
      and wait_type = 'event'
  $fmt$, v_stable)
  using p_run_id;
  execute format($fmt$
    update absurd.%I
    set
      status = case when $3 then
        'sleeping'
      else
        $5
      end,
      next_wake_at = $2,
      wake_event = null,
      updated_at = $4,
      lease_expires_at = case when $3 then
        null
      else
        lease_expires_at
      end,
      claimed_by = case when $3 then
        null
      else
        claimed_by
      end
    where
      run_id = $1
  $fmt$, v_rtable)
  using p_run_id, p_wake_at, p_suspend, v_now, v_status;
  perform
    absurd.set_vt_at (p_queue_name, p_run_id, p_wake_at);
end;
$$
language plpgsql;
