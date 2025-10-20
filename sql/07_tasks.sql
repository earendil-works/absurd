------------------------------------------------------------
-- Durable task tables
------------------------------------------------------------
create table absurd.tasks (
  task_id uuid primary key default absurd.portable_uuidv7 (),
  queue_name text not null,
  task_name text not null,
  params jsonb not null,
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  final_status text check (final_status in ('pending', 'completed', 'failed', 'abandoned')),
  constraint tasks_queue_fk foreign key (queue_name) references absurd.meta (queue_name)
);

create index on absurd.tasks (queue_name, created_at desc);

create table absurd.task_runs (
  run_id uuid primary key default absurd.portable_uuidv7 (),
  task_id uuid not null references absurd.tasks (task_id) on delete cascade,
  attempt integer not null,
  status text not null check (status in ('pending', 'running', 'sleeping', 'completed', 'failed', 'abandoned')),
  max_attempts integer,
  retry_strategy jsonb,
  next_wake_at timestamptz,
  wake_event text,
  last_claimed_at timestamptz,
  claimed_by text,
  lease_expires_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index on absurd.task_runs (task_id, attempt);

create index on absurd.task_runs (next_wake_at)
where
  status in ('pending', 'sleeping');

create table absurd.task_checkpoints (
  task_id uuid not null references absurd.tasks (task_id) on delete cascade,
  step_name text not null,
  owner_run_id uuid not null references absurd.task_runs (run_id) on delete cascade,
  status text not null default 'complete' check (status in ('pending', 'complete')),
  state jsonb,
  ephemeral boolean not null default false,
  expires_at timestamptz,
  updated_at timestamptz not null default now(),
  primary key (task_id, step_name)
);

create table absurd.task_checkpoint_reads (
  task_id uuid not null references absurd.tasks (task_id) on delete cascade,
  run_id uuid not null references absurd.task_runs (run_id) on delete cascade,
  step_name text not null,
  last_seen_at timestamptz not null default now(),
  primary key (task_id, run_id, step_name)
);

create table absurd.task_waits (
  task_id uuid not null references absurd.tasks (task_id) on delete cascade,
  run_id uuid not null references absurd.task_runs (run_id) on delete cascade,
  wait_type text not null check (wait_type in ('sleep', 'event')),
  wake_at timestamptz,
  wake_event text,
  wake_event_key text generated always as (coalesce(wake_event, '_')) stored,
  step_name text,
  payload jsonb,
  created_at timestamptz not null default now(),
  primary key (task_id, run_id, wait_type, wake_event_key)
);

create index on absurd.task_waits (wake_event)
where
  wait_type = 'event';

create table absurd.event_cache (
  queue_name text not null references absurd.meta (queue_name) on delete cascade,
  event_name text,
  event_key text generated always as (coalesce(event_name, '_')) stored,
  payload jsonb,
  emitted_at timestamptz not null default now(),
  primary key (queue_name, event_key)
);

create table absurd.task_archives (
  task_id uuid primary key references absurd.tasks (task_id),
  run_id uuid references absurd.task_runs (run_id),
  archived_at timestamptz not null default now(),
  final_state jsonb not null
);

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
  v_task_id uuid;
  v_run_id uuid;
  v_attempt integer := 1;
  v_headers jsonb;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_message jsonb;
  v_options jsonb := coalesce(p_options, '{}'::jsonb);
  v_qtable text := absurd.format_table_name (p_queue_name, 'q');
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
  insert into absurd.tasks (queue_name, task_name, params, final_status)
    values (p_queue_name, p_task_name, p_params, 'pending')
  returning
    tasks.task_id into v_task_id;
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
  insert into absurd.task_runs (run_id, task_id, attempt, status, max_attempts, retry_strategy, created_at, updated_at)
    values (v_run_id, v_task_id, v_attempt, 'pending', v_max_attempts, v_retry_strategy, v_now, v_now);
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
  v_run absurd.task_runs%rowtype;
  v_task absurd.tasks%rowtype;
  v_claimed_at timestamptz;
  v_lease_expires timestamptz;
  v_wait_event text;
  v_wait_payload jsonb;
begin
  for v_message in
  select
    *
  from
    absurd.read (p_queue_name, p_claim_timeout, p_qty)
    loop
      v_claimed_at := clock_timestamp();
      v_lease_expires := v_claimed_at + make_interval(secs => p_claim_timeout);
      update
        absurd.task_runs
      set
        status = 'running',
        claimed_by = p_worker_id,
        last_claimed_at = v_claimed_at,
        lease_expires_at = v_lease_expires,
        next_wake_at = null,
        wake_event = null,
        updated_at = v_claimed_at
      where
        task_runs.run_id = v_message.msg_id
      returning
        * into v_run;
      if not found then
        perform
          absurd.delete (p_queue_name, v_message.msg_id);
        continue;
      end if;
      select
        * into v_task
      from
        absurd.tasks
      where
        tasks.task_id = v_run.task_id;
      select
        w.wake_event,
        w.payload into v_wait_event,
        v_wait_payload
      from
        absurd.task_waits w
      where
        w.run_id = v_run.run_id
      order by
        w.created_at desc
      limit 1;
      delete from absurd.task_waits
      where task_waits.run_id = v_run.run_id;
      run_id := v_run.run_id;
      task_id := v_run.task_id;
      attempt := v_run.attempt;
      task_name := v_task.task_name;
      params := v_task.params;
      retry_strategy := v_run.retry_strategy;
      max_attempts := v_run.max_attempts;
      headers := v_message.headers;
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
begin
  select
    r.task_id into v_task_id
  from
    absurd.task_runs r
    join absurd.tasks t on t.task_id = r.task_id
  where
    r.run_id = p_run_id
    and t.queue_name = p_queue_name;
  if not found then
    raise exception 'run % not found for queue %', p_run_id, p_queue_name;
  end if;
  perform
    absurd.delete (p_queue_name, p_run_id);
  update
    absurd.task_runs
  set
    status = 'completed',
    updated_at = v_now,
    lease_expires_at = null,
    claimed_by = null,
    next_wake_at = null,
    wake_event = null
  where
    run_id = p_run_id;
  delete from absurd.task_waits
  where run_id = p_run_id;
  update
    absurd.tasks
  set
    final_status = 'completed',
    completed_at = v_now
  where
    task_id = v_task_id;
  if p_archive then
    insert into absurd.task_archives (task_id, run_id, archived_at, final_state)
      values (v_task_id, p_run_id, v_now, coalesce(p_final_state, '{}'::jsonb))
    on conflict (task_id)
      do update set
        run_id = excluded.run_id,
        archived_at = excluded.archived_at,
        final_state = excluded.final_state;
  end if;
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
  v_new_status text;
  v_headers jsonb;
begin
  select
    r.task_id,
    r.attempt,
    r.max_attempts,
    r.retry_strategy into v_task_id,
    v_attempt,
    v_max_attempts,
    v_retry_strategy
  from
    absurd.task_runs r
    join absurd.tasks t on t.task_id = r.task_id
  where
    r.run_id = p_run_id
    and t.queue_name = p_queue_name;
  if not found then
    raise exception 'run % not found for queue %', p_run_id, p_queue_name;
  end if;
  update
    absurd.task_runs
  set
    status = 'failed',
    updated_at = v_now,
    lease_expires_at = null,
    claimed_by = null,
    next_wake_at = null,
    wake_event = null
  where
    run_id = p_run_id;
  delete from absurd.task_waits
  where run_id = p_run_id;
  execute format($fmt$select headers from absurd.%I where msg_id = $1$fmt$, v_qtable) into v_headers
  using p_run_id;
  perform
    absurd.delete (p_queue_name, p_run_id);
  v_should_retry := v_max_attempts is null
    or v_attempt < v_max_attempts;
  if v_should_retry then
    v_next_attempt := v_attempt + 1;
    v_effective_retry_at := coalesce(p_retry_at, v_now);
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
    insert into absurd.task_runs (run_id, task_id, attempt, status, max_attempts, retry_strategy, next_wake_at, created_at, updated_at)
      values (v_new_run_id, v_task_id, v_next_attempt, v_new_status, v_max_attempts, v_retry_strategy, case when v_effective_retry_at > v_now then
          v_effective_retry_at
        else
          null
        end, v_now, v_now);
    if v_effective_retry_at > v_now then
      perform
        absurd.schedule_run (v_new_run_id, v_effective_retry_at, true);
    end if;
    update
      absurd.tasks
    set
      final_status = 'pending',
      completed_at = null
    where
      task_id = v_task_id;
  else
    update
      absurd.tasks
    set
      final_status = 'failed',
      completed_at = v_now
    where
      task_id = v_task_id;
  end if;
end;
$$
language plpgsql;

create function absurd.schedule_run (p_run_id uuid, p_wake_at timestamptz, p_suspend boolean default true)
  returns void
  as $$
declare
  v_task_id uuid;
  v_queue_name text;
  v_now timestamptz := clock_timestamp();
  v_status text;
begin
  select
    r.task_id,
    t.queue_name,
    r.status into v_task_id,
    v_queue_name,
    v_status
  from
    absurd.task_runs r
    join absurd.tasks t on t.task_id = r.task_id
  where
    r.run_id = p_run_id;
  if not found then
    raise exception 'run % not found', p_run_id;
  end if;
  if p_suspend then
    insert into absurd.task_waits (task_id, run_id, wait_type, wake_at, wake_event, payload, step_name)
      values (v_task_id, p_run_id, 'sleep', p_wake_at, null, null, null)
    on conflict (task_id, run_id, wait_type, wake_event_key)
      do update set
        wake_at = excluded.wake_at,
        payload = excluded.payload,
        step_name = excluded.step_name,
        created_at = excluded.created_at;
  else
    delete from absurd.task_waits
    where run_id = p_run_id
      and wait_type = 'sleep';
  end if;
  delete from absurd.task_waits
  where run_id = p_run_id
    and wait_type = 'event';
  update
    absurd.task_runs
  set
    status = case when p_suspend then
      'sleeping'
    else
      v_status
    end,
    next_wake_at = p_wake_at,
    wake_event = null,
    updated_at = v_now,
    lease_expires_at = case when p_suspend then
      null
    else
      lease_expires_at
    end,
    claimed_by = case when p_suspend then
      null
    else
      claimed_by
    end
  where
    run_id = p_run_id;
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
  v_task_id uuid;
  v_queue_name text;
  v_now timestamptz := clock_timestamp();
  v_event_payload jsonb;
begin
  select
    r.task_id,
    t.queue_name into v_task_id,
    v_queue_name
  from
    absurd.task_runs r
    join absurd.tasks t on t.task_id = r.task_id
  where
    r.run_id = p_run_id;
  if not found then
    raise exception 'run % not found', p_run_id;
  end if;
  select
    ec.payload into v_event_payload
  from
    absurd.event_cache ec
  where
    ec.queue_name = v_queue_name
    and ec.event_key = coalesce(p_event_name, '_');
  if found then
    should_suspend := false;
    payload := v_event_payload;
    return next;
    return;
  end if;
  delete from absurd.task_waits
  where run_id = p_run_id
    and wait_type = 'sleep';
  insert into absurd.task_waits (task_id, run_id, wait_type, wake_at, wake_event, payload, step_name)
    values (v_task_id, p_run_id, 'event', null, p_event_name, p_payload, p_step_name)
  on conflict (task_id, run_id, wait_type, wake_event_key)
    do update set
      payload = excluded.payload,
      wake_event = excluded.wake_event,
      step_name = excluded.step_name,
      created_at = excluded.created_at;
  update
    absurd.task_runs
  set
    status = 'sleeping',
    wake_event = p_event_name,
    next_wake_at = null,
    lease_expires_at = null,
    claimed_by = null,
    updated_at = v_now
  where
    run_id = p_run_id;
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
  v_event_key text := coalesce(p_event_name, '_');
begin
  insert into absurd.event_cache (queue_name, event_name, payload, emitted_at)
    values (p_queue_name, p_event_name, p_payload, v_now)
  on conflict (queue_name, event_key)
    do update set
      payload = excluded.payload,
      emitted_at = excluded.emitted_at;
  for v_wait in
  select
    w.task_id,
    w.run_id,
    w.step_name
  from
    absurd.task_waits w
    join absurd.tasks t on t.task_id = w.task_id
  where
    t.queue_name = p_queue_name
    and w.wait_type = 'event'
    and w.wake_event_key = v_event_key loop
      update
        absurd.task_waits
      set
        payload = p_payload
      where
        task_id = v_wait.task_id
        and run_id = v_wait.run_id
        and wait_type = 'event'
        and wake_event_key = v_event_key;
      if v_wait.step_name is not null then
        perform
          absurd.set_task_checkpoint_state (v_wait.task_id, v_wait.step_name, p_payload, v_wait.run_id, true, null);
      end if;
      update
        absurd.task_runs
      set
        status = 'pending',
        wake_event = null,
        next_wake_at = null,
        updated_at = v_now,
        lease_expires_at = null,
        claimed_by = null
      where
        run_id = v_wait.run_id;
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
  v_expires_at timestamptz;
  v_now timestamptz := clock_timestamp();
begin
  if p_ttl_seconds is not null then
    v_expires_at := v_now + make_interval(secs => p_ttl_seconds);
  else
    v_expires_at := null;
  end if;
  insert into absurd.task_checkpoints (task_id, step_name, owner_run_id, status, state, ephemeral, expires_at, updated_at)
    values (p_task_id, p_step_name, p_owner_run, 'complete', p_state, p_ephemeral, v_expires_at, v_now)
  on conflict (task_id, step_name)
    do update set
      owner_run_id = excluded.owner_run_id,
      status = excluded.status,
      state = excluded.state,
      ephemeral = excluded.ephemeral,
      expires_at = excluded.expires_at,
      updated_at = excluded.updated_at;
  delete from absurd.task_checkpoint_reads
  where task_id = p_task_id
    and step_name = p_step_name;
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
begin
  return query
  select
    task_checkpoints.step_name,
    task_checkpoints.state,
    task_checkpoints.status,
    task_checkpoints.owner_run_id,
    task_checkpoints.ephemeral,
    task_checkpoints.expires_at,
    task_checkpoints.updated_at
  from
    absurd.task_checkpoints
  where
    task_checkpoints.task_id = p_task_id
    and task_checkpoints.step_name = p_step_name
    and (p_include_pending
      or task_checkpoints.status = 'complete')
    and (task_checkpoints.expires_at is null
      or task_checkpoints.expires_at > clock_timestamp());
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
  v_row record;
  v_now timestamptz := clock_timestamp();
begin
  for v_row in
  select
    cp.step_name,
    cp.state,
    cp.status,
    cp.owner_run_id,
    cp.ephemeral,
    cp.expires_at,
    cp.updated_at
  from
    absurd.task_checkpoints cp
  where
    cp.task_id = p_task_id
    and cp.status = 'complete'
    and (cp.expires_at is null
      or cp.expires_at > v_now)
      loop
        checkpoint_name := v_row.step_name;
        state := v_row.state;
        status := v_row.status;
        owner_run_id := v_row.owner_run_id;
        ephemeral := v_row.ephemeral;
        expires_at := v_row.expires_at;
        updated_at := v_row.updated_at;
        insert into absurd.task_checkpoint_reads (task_id, run_id, step_name, last_seen_at)
          values (p_task_id, p_run_id, v_row.step_name, v_now)
        on conflict (task_id, run_id, step_name)
          do update set
            last_seen_at = v_now;
        return next;
      end loop;
end;
$$
language plpgsql;

create function absurd.record_checkpoint_prefetch (p_task_id uuid, p_run_id uuid, p_step_names text[])
  returns void
  as $$
declare
  v_step text;
  v_now timestamptz := clock_timestamp();
begin
  if p_step_names is null then
    return;
  end if;
  foreach v_step in array p_step_names loop
    if v_step is null then
      continue;
    end if;
    insert into absurd.task_checkpoint_reads (task_id, run_id, step_name, last_seen_at)
      values (p_task_id, p_run_id, v_step, v_now)
    on conflict (task_id, run_id, step_name)
      do update set
        last_seen_at = v_now;
  end loop;
end;
$$
language plpgsql;
