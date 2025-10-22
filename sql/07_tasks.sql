------------------------------------------------------------
-- Durable task catalog and helpers
------------------------------------------------------------
create table absurd.run_catalog (
  run_id uuid primary key,
  task_id uuid not null,
  queue_name text not null,
  attempt integer not null,
  created_at timestamptz not null default now()
);

create index run_catalog_task_idx on absurd.run_catalog (task_id, attempt desc);
create index run_catalog_queue_idx on absurd.run_catalog (queue_name);

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
  v_run_id uuid;
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
  select
    s.msg_id into v_run_id
  from
    absurd.send (p_queue_name, jsonb_build_object('task_id', v_task_id, 'attempt', v_attempt), v_headers) as s (msg_id)
  limit 1;
  if v_run_id is null then
    raise exception 'failed to enqueue task % for queue %', p_task_name, p_queue_name;
  end if;
  v_message := jsonb_build_object('task_id', v_task_id, 'run_id', v_run_id, 'attempt', v_attempt);
  execute format($fmt$update absurd.%I set message = $2 where msg_id = $1$fmt$, v_qtable)
  using v_run_id, v_message;
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
  insert into absurd.run_catalog (run_id, task_id, queue_name, attempt, created_at)
    values (v_run_id, v_task_id, p_queue_name, v_attempt, v_now);
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
  v_message absurd.message_record;
  v_run record;
  v_claimed_at timestamptz;
  v_lease_expires timestamptz;
  v_wait_event text;
  v_wait_payload jsonb;
  v_qtable text := absurd.format_table_name (p_queue_name, 'q');
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_stable text := absurd.format_table_name (p_queue_name, 's');
  v_rowcount integer;
begin
  for v_message in
  select
    *
  from
    absurd.read (p_queue_name, p_claim_timeout, p_qty)
    loop
      v_claimed_at := clock_timestamp();
      v_lease_expires := v_claimed_at + make_interval(secs => p_claim_timeout);
      execute format($fmt$
        update absurd.%I
        set
          status = 'running',
          claimed_by = $1,
          last_claimed_at = $2,
          lease_expires_at = $3,
          next_wake_at = null,
          wake_event = null,
          updated_at = $2
        where
          run_id = $4
        returning
          task_id,
          attempt,
          task_name,
          params,
          retry_strategy,
          max_attempts,
          headers
      $fmt$, v_rtable)
      using p_worker_id, v_claimed_at, v_lease_expires, v_message.msg_id
      into v_run;
      get diagnostics v_rowcount = row_count;
      if v_rowcount = 0 then
        perform
          absurd.delete (p_queue_name, v_message.msg_id);
        continue;
      end if;
      execute format($fmt$
        select
          wake_event,
          payload
        from
          absurd.%I
        where
          item_type = 'wait'
          and run_id = $1
        order by
          updated_at desc
        limit 1
      $fmt$, v_stable)
      using v_message.msg_id
      into v_wait_event,
      v_wait_payload;
      execute format($fmt$
        delete from absurd.%I
        where
          item_type = 'wait'
          and run_id = $1
      $fmt$, v_stable)
      using v_message.msg_id;
      run_id := v_message.msg_id;
      task_id := v_run.task_id;
      attempt := v_run.attempt;
      task_name := v_run.task_name;
      params := v_run.params;
      retry_strategy := v_run.retry_strategy;
      max_attempts := v_run.max_attempts;
      headers := coalesce(v_message.headers, v_run.headers);
      lease_expires_at := v_lease_expires;
      wake_event := v_wait_event;
      event_payload := v_wait_payload;
      v_wait_event := null;
      v_wait_payload := null;
      return next;
    end loop;
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
    select
      s.msg_id into v_new_run_id
    from
      absurd.send (p_queue_name, jsonb_build_object('task_id', v_task_id, 'attempt', v_next_attempt), v_headers, v_effective_retry_at) as s (msg_id)
    limit 1;
    if v_new_run_id is null then
      raise exception 'failed to enqueue retry for run %', p_run_id;
    end if;
    execute format($fmt$update absurd.%I set message = $2 where msg_id = $1$fmt$, v_qtable)
    using v_new_run_id, jsonb_build_object('task_id', v_task_id, 'run_id', v_new_run_id, 'attempt', v_next_attempt);
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
    insert into absurd.run_catalog (run_id, task_id, queue_name, attempt, created_at)
      values (v_new_run_id, v_task_id, p_queue_name, v_next_attempt, v_now);
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
        absurd.schedule_run (v_new_run_id, v_effective_retry_at, true);
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

create function absurd.schedule_run (p_run_id uuid, p_wake_at timestamptz, p_suspend boolean default true)
  returns void
  as $$
declare
  v_queue_name text;
  v_task_id uuid;
  v_status text;
  v_now timestamptz := clock_timestamp();
  v_rtable text;
  v_stable text;
begin
  select
    rc.queue_name,
    rc.task_id into v_queue_name,
    v_task_id
  from
    absurd.run_catalog rc
  where
    rc.run_id = p_run_id;
  if v_queue_name is null then
    raise exception 'run % not found', p_run_id;
  end if;
  v_rtable := absurd.format_table_name (v_queue_name, 'r');
  v_stable := absurd.format_table_name (v_queue_name, 's');
  execute format($fmt$
    select
      status
    from
      absurd.%I
    where
      run_id = $1
  $fmt$, v_rtable)
  using p_run_id
  into v_status;
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
    absurd.set_vt_at (v_queue_name, p_run_id, p_wake_at);
end;
$$
language plpgsql;

create function absurd.await_event (p_run_id uuid, p_step_name text, p_event_name text, p_payload jsonb default null)
  returns table (
    should_suspend boolean,
    payload jsonb
  )
  as $$
declare
  v_queue_name text;
  v_task_id uuid;
  v_now timestamptz := clock_timestamp();
  v_event_payload jsonb;
  v_rtable text;
  v_stable text;
begin
  if p_event_name is null then
    raise exception 'await_event requires a non-null event name';
  end if;
  select
    rc.queue_name,
    rc.task_id into v_queue_name,
    v_task_id
  from
    absurd.run_catalog rc
  where
    rc.run_id = p_run_id;
  if v_queue_name is null then
    raise exception 'run % not found', p_run_id;
  end if;
  v_rtable := absurd.format_table_name (v_queue_name, 'r');
  v_stable := absurd.format_table_name (v_queue_name, 's');
  execute format($fmt$
    select
      payload
    from
      absurd.%I
    where
      item_type = 'event'
      and event_name = $1
  $fmt$, v_stable)
  using p_event_name
  into v_event_payload;
  if found then
    should_suspend := false;
    payload := v_event_payload;
    return next;
    return;
  end if;
  execute format($fmt$
    delete from absurd.%I
    where
      item_type = 'wait'
      and run_id = $1
      and wait_type = 'sleep'
  $fmt$, v_stable)
  using p_run_id;
  execute format($fmt$
    insert into absurd.%I (item_type, task_id, run_id, wait_type, wake_event, payload, step_name, created_at, updated_at)
    values ('wait', $1, $2, 'event', $3, $4, $5, $6, $6)
    on conflict (task_id, run_id, wait_type)
      where item_type = 'wait'
    do update set
      wake_event = excluded.wake_event,
      payload = excluded.payload,
      step_name = excluded.step_name,
      updated_at = excluded.updated_at
  $fmt$, v_stable)
  using v_task_id, p_run_id, p_event_name, p_payload, p_step_name, v_now;
  execute format($fmt$
    update absurd.%I
    set
      status = 'sleeping',
      wake_event = $2,
      next_wake_at = null,
      lease_expires_at = null,
      claimed_by = null,
      updated_at = $3
    where
      run_id = $1
  $fmt$, v_rtable)
  using p_run_id, p_event_name, v_now;
  perform
    absurd.set_vt_at (v_queue_name, p_run_id, 'infinity'::timestamptz);
  should_suspend := true;
  payload := null;
  return next;
  return;
end;
$$
language plpgsql;

create function absurd.emit_event (p_queue_name text, p_event_name text, p_payload jsonb default null)
  returns void
  as $$
declare
  v_wait record;
  v_now timestamptz := clock_timestamp();
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_stable text := absurd.format_table_name (p_queue_name, 's');
begin
  if p_event_name is null then
    raise exception 'emit_event requires a non-null event name';
  end if;
  execute format($fmt$
    insert into absurd.%I (item_type, event_name, payload, emitted_at, updated_at)
    values ('event', $1, $2, $3, $3)
    on conflict (event_name)
      where item_type = 'event'
    do update set
      payload = excluded.payload,
      emitted_at = excluded.emitted_at,
      updated_at = excluded.updated_at
  $fmt$, v_stable)
  using p_event_name, p_payload, v_now;
  for v_wait in
  execute format($fmt$
    select
      task_id,
      run_id,
      step_name
    from
      absurd.%I
    where
      item_type = 'wait'
      and wait_type = 'event'
      and wake_event = $1
  $fmt$, v_stable)
  using p_event_name
  loop
    execute format($fmt$
      update absurd.%I
      set
        payload = $3,
        updated_at = $4
      where
        item_type = 'wait'
        and task_id = $1
        and run_id = $2
        and wait_type = 'event'
    $fmt$, v_stable)
    using v_wait.task_id, v_wait.run_id, p_payload, v_now;
    if v_wait.step_name is not null then
      perform
        absurd.set_task_checkpoint_state (v_wait.task_id, v_wait.step_name, p_payload, v_wait.run_id, true, null);
    end if;
    execute format($fmt$
      update absurd.%I
      set
        status = 'pending',
        wake_event = null,
        next_wake_at = null,
        updated_at = $2,
        lease_expires_at = null,
        claimed_by = null
      where
        run_id = $1
    $fmt$, v_rtable)
    using v_wait.run_id, v_now;
    perform
      absurd.set_vt_at (p_queue_name, v_wait.run_id, v_now);
  end loop;
end;
$$
language plpgsql;

create function absurd.set_task_checkpoint_state (p_task_id uuid, p_step_name text, p_state jsonb, p_owner_run uuid, p_ephemeral boolean default false, p_ttl_seconds integer default null)
  returns void
  as $$
declare
  v_queue_name text;
  v_stable text;
  v_expires_at timestamptz;
  v_now timestamptz := clock_timestamp();
begin
  if p_owner_run is not null then
    select
      queue_name into v_queue_name
    from
      absurd.run_catalog
    where
      run_id = p_owner_run;
  end if;
  if v_queue_name is null then
    select
      queue_name into v_queue_name
    from
      absurd.run_catalog
    where
      task_id = p_task_id
    order by
      attempt desc
    limit 1;
  end if;
  if v_queue_name is null then
    raise exception 'task % not found in catalog', p_task_id;
  end if;
  v_stable := absurd.format_table_name (v_queue_name, 's');
  if p_ttl_seconds is not null then
    v_expires_at := v_now + make_interval(secs => p_ttl_seconds);
  else
    v_expires_at := null;
  end if;
  execute format($fmt$
    insert into absurd.%I (item_type, task_id, step_name, owner_run_id, status, state, ephemeral, expires_at, created_at, updated_at)
    values ('checkpoint', $1, $2, $3, 'complete', $4, $5, $6, $7, $7)
    on conflict (task_id, step_name)
      where item_type = 'checkpoint'
    do update set
      owner_run_id = excluded.owner_run_id,
      status = excluded.status,
      state = excluded.state,
      ephemeral = excluded.ephemeral,
      expires_at = excluded.expires_at,
      updated_at = excluded.updated_at
  $fmt$, v_stable)
  using p_task_id, p_step_name, p_owner_run, p_state, p_ephemeral, v_expires_at, v_now;
  execute format($fmt$
    delete from absurd.%I
    where
      item_type = 'checkpoint_read'
      and task_id = $1
      and step_name = $2
  $fmt$, v_stable)
  using p_task_id, p_step_name;
end;
$$
language plpgsql;

create function absurd.get_task_checkpoint_state (p_task_id uuid, p_step_name text, p_include_pending boolean default false)
  returns table (
    checkpoint_name text,
    state jsonb,
    status text,
    owner_run_id uuid,
    ephemeral boolean,
    expires_at timestamptz,
    updated_at timestamptz
  )
  as $$
declare
  v_queue_name text;
  v_stable text;
begin
  select
    queue_name into v_queue_name
  from
    absurd.run_catalog
  where
    task_id = p_task_id
  order by
    attempt desc
  limit 1;
  if v_queue_name is null then
    return;
  end if;
  v_stable := absurd.format_table_name (v_queue_name, 's');
  return query
  execute format($fmt$
    select
      step_name,
      state,
      status,
      owner_run_id,
      coalesce(ephemeral, false),
      expires_at,
      updated_at
    from
      absurd.%I
    where
      item_type = 'checkpoint'
      and task_id = $1
      and step_name = $2
      and (status = 'complete'
        or $3)
      and (expires_at is null
        or expires_at > clock_timestamp())
  $fmt$, v_stable)
  using p_task_id, p_step_name, p_include_pending;
end;
$$
language plpgsql;

create function absurd.get_task_checkpoint_states (p_task_id uuid, p_run_id uuid)
  returns table (
    checkpoint_name text,
    state jsonb,
    status text,
    owner_run_id uuid,
    ephemeral boolean,
    expires_at timestamptz,
    updated_at timestamptz
  )
  as $$
declare
  v_queue_name text;
  v_stable text;
  v_row record;
  v_now timestamptz := clock_timestamp();
begin
  select
    queue_name into v_queue_name
  from
    absurd.run_catalog
  where
    task_id = p_task_id
  order by
    attempt desc
  limit 1;
  if v_queue_name is null then
    return;
  end if;
  v_stable := absurd.format_table_name (v_queue_name, 's');
  for v_row in
  execute format($fmt$
    select
      step_name,
      state,
      status,
      owner_run_id,
      coalesce(ephemeral, false) as ephemeral,
      expires_at,
      updated_at
    from
      absurd.%I
    where
      item_type = 'checkpoint'
      and task_id = $1
      and status = 'complete'
      and (expires_at is null
        or expires_at > $2)
  $fmt$, v_stable)
  using p_task_id, v_now
  loop
    checkpoint_name := v_row.step_name;
    state := v_row.state;
    status := v_row.status;
    owner_run_id := v_row.owner_run_id;
    ephemeral := v_row.ephemeral;
    expires_at := v_row.expires_at;
    updated_at := v_row.updated_at;
    execute format($fmt$
      insert into absurd.%I (item_type, task_id, run_id, step_name, last_seen_at, created_at, updated_at)
      values ('checkpoint_read', $1, $2, $3, $4, $5, $5)
      on conflict (task_id, run_id, step_name)
        where item_type = 'checkpoint_read'
      do update set
        last_seen_at = excluded.last_seen_at,
        updated_at = excluded.updated_at
    $fmt$, v_stable)
    using p_task_id, p_run_id, v_row.step_name, v_now, v_now;
    return next;
  end loop;
end;
$$
language plpgsql;

create function absurd.record_checkpoint_prefetch (p_task_id uuid, p_run_id uuid, p_step_names text[])
  returns void
  as $$
declare
  v_queue_name text;
  v_stable text;
  v_step text;
  v_now timestamptz := clock_timestamp();
begin
  if p_step_names is null then
    return;
  end if;
  select
    queue_name into v_queue_name
  from
    absurd.run_catalog
  where
    task_id = p_task_id
  order by
    attempt desc
  limit 1;
  if v_queue_name is null then
    return;
  end if;
  v_stable := absurd.format_table_name (v_queue_name, 's');
  foreach v_step in array p_step_names loop
    if v_step is null then
      continue;
    end if;
    execute format($fmt$
      insert into absurd.%I (item_type, task_id, run_id, step_name, last_seen_at, created_at, updated_at)
      values ('checkpoint_read', $1, $2, $3, $4, $5, $5)
      on conflict (task_id, run_id, step_name)
        where item_type = 'checkpoint_read'
      do update set
        last_seen_at = excluded.last_seen_at,
        updated_at = excluded.updated_at
    $fmt$, v_stable)
    using p_task_id, p_run_id, v_step, v_now, v_now;
  end loop;
end;
$$
language plpgsql;
