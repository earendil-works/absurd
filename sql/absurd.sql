create extension if not exists "uuid-ossp";

create schema if not exists absurd;

create table if not exists absurd.queues (
  queue_name text primary key,
  identifier text not null unique,
  created_at timestamptz not null default now()
);

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
  ts_ms := floor(extract(epoch from clock_timestamp()) * 1000)::bigint;
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

create function absurd.compute_queue_identifier (p_queue_name text)
  returns text
  language plpgsql
as $$
declare
  v_base text;
begin
  if p_queue_name is null or length(trim(p_queue_name)) = 0 then
    raise exception 'Queue name must be provided';
  end if;
  v_base := lower(regexp_replace(p_queue_name, '[^a-z0-9]+', '_', 'g'));
  v_base := trim(both '_' from v_base);
  if v_base is null or v_base = '' then
    v_base := 'queue';
  end if;
  return v_base;
end;
$$;

create function absurd.queue_table_name (p_identifier text, p_prefix text)
  returns text
  language sql
as $$
  select p_prefix || '_' || p_identifier;
$$;

create function absurd.get_queue_identifier (p_queue_name text)
  returns text
  language plpgsql
as $$
declare
  v_identifier text;
begin
  select identifier into v_identifier
  from absurd.queues
  where queue_name = p_queue_name;

  if v_identifier is null then
    raise exception 'Queue "%" does not exist', p_queue_name;
  end if;
  return v_identifier;
end;
$$;

create function absurd.ensure_queue_tables (p_identifier text)
  returns void
  language plpgsql
as $$
declare
  v_task_table text := absurd.queue_table_name(p_identifier, 't');
  v_run_table text := absurd.queue_table_name(p_identifier, 'r');
  v_checkpoint_table text := absurd.queue_table_name(p_identifier, 'c');
  v_event_table text := absurd.queue_table_name(p_identifier, 'e');
  v_waiter_table text := absurd.queue_table_name(p_identifier, 'w');
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
        enqueue_at timestamptz not null default now(),
        first_started_at timestamptz,
        state text not null check (state in (%L, %L, %L, %L, %L, %L)),
        attempts integer not null default 0,
        last_attempt_run uuid,
        completed_payload jsonb,
        cancelled_at timestamptz
     )',
    v_task_table,
    'pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled'
  );

  execute format(
    'create table if not exists absurd.%I (
        run_id uuid primary key,
        task_id uuid not null references absurd.%I(task_id) on delete cascade,
        attempt integer not null,
        state text not null check (state in (%L, %L, %L, %L, %L, %L)),
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
        created_at timestamptz not null default now()
     )',
    v_run_table,
    v_task_table,
    'pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled'
  );

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid not null references absurd.%I(task_id) on delete cascade,
        checkpoint_name text not null,
        state jsonb,
        status text not null default %L,
        owner_run_id uuid,
        updated_at timestamptz not null default now(),
        primary key (task_id, checkpoint_name)
     )',
    v_checkpoint_table,
    v_task_table,
    'committed'
  );

  execute format(
    'create table if not exists absurd.%I (
        event_name text primary key,
        payload jsonb,
        emitted_at timestamptz not null default now()
     )',
    v_event_table
  );

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid not null references absurd.%I(task_id) on delete cascade,
        run_id uuid not null references absurd.%I(run_id) on delete cascade,
        step_name text not null,
        event_name text not null,
        created_at timestamptz not null default now(),
        primary key (run_id, step_name)
     )',
    v_waiter_table,
    v_task_table,
    v_run_table
  );

  execute format(
    'create index if not exists %I on absurd.%I (state, available_at)',
    v_run_table || '_state_available_at_idx',
    v_run_table
  );

  execute format(
    'create index if not exists %I on absurd.%I (task_id)',
    v_run_table || '_task_id_idx',
    v_run_table
  );

  execute format(
    'create index if not exists %I on absurd.%I (event_name)',
    v_waiter_table || '_event_name_idx',
    v_waiter_table
  );
end;
$$;

create function absurd.create_queue (p_queue_name text)
  returns void
  language plpgsql
as $$
declare
  v_identifier text;
begin
  if p_queue_name is null or length(trim(p_queue_name)) = 0 then
    raise exception 'Queue name must be provided';
  end if;

  v_identifier := absurd.compute_queue_identifier(p_queue_name);

  begin
    insert into absurd.queues (queue_name, identifier)
    values (p_queue_name, v_identifier);
  exception when unique_violation then
    if not exists (select 1 from absurd.queues where queue_name = p_queue_name) then
      raise exception 'Queue identifier "%" already in use', v_identifier;
    end if;
  end;

  perform absurd.ensure_queue_tables(v_identifier);
end;
$$;

create function absurd.drop_queue (p_queue_name text)
  returns void
  language plpgsql
as $$
declare
  v_identifier text;
  v_task_table text;
  v_run_table text;
  v_checkpoint_table text;
  v_event_table text;
  v_waiter_table text;
begin
  select identifier into v_identifier
  from absurd.queues
  where queue_name = p_queue_name;

  if v_identifier is null then
    return;
  end if;

  v_task_table := absurd.queue_table_name(v_identifier, 't');
  v_run_table := absurd.queue_table_name(v_identifier, 'r');
  v_checkpoint_table := absurd.queue_table_name(v_identifier, 'c');
  v_event_table := absurd.queue_table_name(v_identifier, 'e');
  v_waiter_table := absurd.queue_table_name(v_identifier, 'w');

  execute format('drop table if exists absurd.%I cascade', v_waiter_table);
  execute format('drop table if exists absurd.%I cascade', v_event_table);
  execute format('drop table if exists absurd.%I cascade', v_checkpoint_table);
  execute format('drop table if exists absurd.%I cascade', v_run_table);
  execute format('drop table if exists absurd.%I cascade', v_task_table);

  delete from absurd.queues where queue_name = p_queue_name;
end;
$$;

create function absurd.list_queues ()
  returns table (queue_name text)
  language sql
as $$
  select queue_name from absurd.queues order by queue_name;
$$;

-- Remaining task lifecycle, checkpoint, and event functions are implemented below.

create function absurd.spawn_task (
  p_queue_name text,
  p_task_name text,
  p_params jsonb,
  p_options jsonb default '{}'::jsonb
)
  returns table (
    task_id uuid,
    run_id uuid,
    attempt integer
  )
  language plpgsql
as $$
declare
  v_identifier text;
  v_task_table text;
  v_run_table text;
  v_task_id uuid := absurd.portable_uuidv7();
  v_run_id uuid := absurd.portable_uuidv7();
  v_attempt integer := 1;
  v_headers jsonb;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_cancellation jsonb;
  v_now timestamptz := clock_timestamp();
  v_params jsonb := coalesce(p_params, 'null'::jsonb);
begin
  if p_task_name is null or length(trim(p_task_name)) = 0 then
    raise exception 'task_name must be provided';
  end if;

  v_identifier := absurd.get_queue_identifier(p_queue_name);
  v_task_table := absurd.queue_table_name(v_identifier, 't');
  v_run_table := absurd.queue_table_name(v_identifier, 'r');

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
  end if;

  execute format(
    'insert into absurd.%I (task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, enqueue_at, first_started_at, state, attempts, last_attempt_run, completed_payload, cancelled_at)
     values ($1, $2, $3, $4, $5, $6, $7, $8, null, %L, $9, $10, null, null)',
    v_task_table,
    'pending'
  )
  using v_task_id, p_task_name, v_params, v_headers, v_retry_strategy, v_max_attempts, v_cancellation, v_now, v_attempt, v_run_id;

  execute format(
    'insert into absurd.%I (run_id, task_id, attempt, state, available_at, wake_event, event_payload, result, failure_reason)
     values ($1, $2, $3, %L, $4, null, null, null, null)',
    v_run_table,
    'pending'
  )
  using v_run_id, v_task_id, v_attempt, v_now;

  return query select v_task_id, v_run_id, v_attempt;
end;
$$;

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
  v_identifier text;
  v_task_table text;
  v_run_table text;
  v_now timestamptz := clock_timestamp();
  v_claim_timeout integer := greatest(coalesce(p_claim_timeout, 30), 0);
  v_worker_id text := coalesce(nullif(p_worker_id, ''), 'worker');
  v_qty integer := greatest(coalesce(p_qty, 1), 1);
  v_claim_until timestamptz := null;
  v_sql text;
begin
  v_identifier := absurd.get_queue_identifier(p_queue_name);
  v_task_table := absurd.queue_table_name(v_identifier, 't');
  v_run_table := absurd.queue_table_name(v_identifier, 'r');

  if v_claim_timeout > 0 then
    v_claim_until := v_now + make_interval(secs => v_claim_timeout);
  end if;

  -- Apply cancellation rules before claiming.
  execute format(
    'with limits as (
        select task_id,
               (cancellation->>''max_delay_ms'')::bigint as max_delay_ms,
               (cancellation->>''max_duration_ms'')::bigint as max_duration_ms,
               enqueue_at,
               first_started_at,
               state
          from absurd.%I
         where state in (%L, %L, %L)
     ),
     to_cancel as (
        select task_id
          from limits
         where
           (
             max_delay_ms is not null
             and first_started_at is null
             and extract(epoch from ($1 - enqueue_at)) * 1000 >= max_delay_ms
           )
           or
           (
             max_duration_ms is not null
             and first_started_at is not null
             and extract(epoch from ($1 - first_started_at)) * 1000 >= max_duration_ms
           )
     )
     update absurd.%I t
        set state = %L,
            cancelled_at = coalesce(t.cancelled_at, $1)
      where t.task_id in (select task_id from to_cancel)',
    v_task_table,
    'pending', 'sleeping', 'running',
    v_task_table,
    'cancelled'
  ) using v_now;

  execute format(
    'update absurd.%I r
        set state = %L,
            claimed_by = null,
            claim_expires_at = null,
            available_at = greatest(available_at, $1),
            started_at = null
      where state = %L
        and claim_expires_at is not null
        and claim_expires_at <= $1',
    v_run_table,
    'pending',
    'running'
  ) using v_now;

  execute format(
    'update absurd.%I r
        set state = %L,
            claimed_by = null,
            claim_expires_at = null,
            available_at = $1,
            wake_event = null
      where task_id in (select task_id from absurd.%I where state = %L)
        and r.state <> %L',
    v_run_table,
    'cancelled',
    v_task_table,
    'cancelled',
    'cancelled'
  ) using v_now;

  v_sql := format(
    'with candidate as (
        select r.run_id
          from absurd.%I r
          join absurd.%I t on t.task_id = r.task_id
         where r.state in (%L, %L)
           and t.state in (%L, %L, %L)
           and r.available_at <= $1
         order by r.available_at, r.run_id
         limit $2
         for update skip locked
     ),
     updated as (
        update absurd.%I r
           set state = %L,
               claimed_by = $3,
               claim_expires_at = $4,
               started_at = $1,
               available_at = $1
         where run_id in (select run_id from candidate)
         returning r.run_id, r.task_id, r.attempt
     ),
     task_upd as (
        update absurd.%I t
           set state = %L,
               attempts = greatest(t.attempts, u.attempt),
               first_started_at = coalesce(t.first_started_at, $1),
               last_attempt_run = u.run_id
          from updated u
         where t.task_id = u.task_id
         returning t.task_id
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
     join absurd.%I r on r.run_id = u.run_id
     join absurd.%I t on t.task_id = u.task_id
     order by r.available_at, u.run_id',
    v_run_table,
    v_task_table,
    'pending', 'sleeping',
    'pending', 'sleeping', 'running',
    v_run_table, 'running',
    v_task_table, 'running',
    v_run_table,
    v_task_table
  );

  return query execute v_sql using v_now, v_qty, v_worker_id, v_claim_until;
end;
$$;

create function absurd.complete_run (
  p_queue_name text,
  p_run_id uuid,
  p_state jsonb default null
)
  returns void
  language plpgsql
as $$
declare
  v_identifier text;
  v_task_table text;
  v_run_table text;
  v_waiter_table text;
  v_task_id uuid;
  v_now timestamptz := clock_timestamp();
begin
  v_identifier := absurd.get_queue_identifier(p_queue_name);
  v_task_table := absurd.queue_table_name(v_identifier, 't');
  v_run_table := absurd.queue_table_name(v_identifier, 'r');
  v_waiter_table := absurd.queue_table_name(v_identifier, 'w');

  execute format(
    'select task_id
       from absurd.%I
      where run_id = $1
      for update',
    v_run_table
  )
  into v_task_id
  using p_run_id;

  if v_task_id is null then
    raise exception 'Run "%" not found in queue "%"', p_run_id, p_queue_name;
  end if;

  execute format(
    'update absurd.%I
        set state = %L,
            claimed_by = null,
            claim_expires_at = null,
            completed_at = $2,
            result = $3
      where run_id = $1',
    v_run_table,
    'completed'
  ) using p_run_id, v_now, p_state;

  execute format(
    'update absurd.%I
        set state = %L,
            completed_payload = $2,
            last_attempt_run = $3
      where task_id = $1',
    v_task_table,
    'completed'
  ) using v_task_id, p_state, p_run_id;

  execute format(
    'delete from absurd.%I where run_id = $1',
    v_waiter_table
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
  v_identifier text;
  v_task_table text;
  v_run_table text;
  v_task_id uuid;
begin
  v_identifier := absurd.get_queue_identifier(p_queue_name);
  v_task_table := absurd.queue_table_name(v_identifier, 't');
  v_run_table := absurd.queue_table_name(v_identifier, 'r');

  execute format(
    'select task_id
       from absurd.%I
      where run_id = $1
        and state = %L
      for update',
    v_run_table,
    'running'
  )
  into v_task_id
  using p_run_id;

  if v_task_id is null then
    raise exception 'Run "%" is not currently running in queue "%"', p_run_id, p_queue_name;
  end if;

  execute format(
    'update absurd.%I
        set state = %L,
            claimed_by = null,
            claim_expires_at = null,
            available_at = $2,
            wake_event = null
      where run_id = $1',
    v_run_table,
    'sleeping'
  ) using p_run_id, p_wake_at;

  execute format(
    'update absurd.%I
        set state = %L
      where task_id = $1',
    v_task_table,
    'sleeping'
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
  v_identifier text;
  v_task_table text;
  v_run_table text;
  v_waiter_table text;
  v_task_id uuid;
  v_attempt integer;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_now timestamptz := clock_timestamp();
  v_next_attempt integer;
  v_delay_seconds double precision := 0;
  v_next_available timestamptz;
  v_retry_kind text;
  v_base double precision;
  v_factor double precision;
  v_max_seconds double precision;
  v_first_started timestamptz;
  v_cancellation jsonb;
  v_max_duration_ms bigint;
  v_task_state text;
  v_task_cancel boolean := false;
  v_new_run_id uuid;
  v_task_state_after text;
  v_recorded_attempt integer;
  v_last_attempt_run uuid := p_run_id;
  v_cancelled_at timestamptz := null;
begin
  v_identifier := absurd.get_queue_identifier(p_queue_name);
  v_task_table := absurd.queue_table_name(v_identifier, 't');
  v_run_table := absurd.queue_table_name(v_identifier, 'r');
  v_waiter_table := absurd.queue_table_name(v_identifier, 'w');

  execute format(
    'select r.task_id, r.attempt
       from absurd.%I r
      where r.run_id = $1
        and r.state in (%L, %L)
      for update',
    v_run_table,
    'running',
    'sleeping'
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
    v_task_table
  )
  into v_retry_strategy, v_max_attempts, v_first_started, v_cancellation, v_task_state
  using v_task_id;

  execute format(
    'update absurd.%I
        set state = %L,
            claimed_by = null,
            claim_expires_at = null,
            wake_event = null,
            failed_at = $2,
            failure_reason = $3
      where run_id = $1',
    v_run_table,
    'failed'
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
      v_max_duration_ms := (v_cancellation->>'max_duration_ms')::bigint;
      if v_max_duration_ms is not null and v_first_started is not null then
        if extract(epoch from (v_next_available - v_first_started)) * 1000 >= v_max_duration_ms then
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
        v_run_table,
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
    v_task_table,
    v_task_state_after
  ) using v_task_id, v_task_state_after, v_recorded_attempt, v_last_attempt_run, v_cancelled_at;

  execute format(
    'delete from absurd.%I where run_id = $1',
    v_waiter_table
  ) using p_run_id;
end;
$$;

create function absurd.set_task_checkpoint_state (
  p_queue_name text,
  p_task_id uuid,
  p_step_name text,
  p_state jsonb,
  p_owner_run uuid
)
  returns void
  language plpgsql
as $$
declare
  v_identifier text;
  v_checkpoint_table text;
  v_run_table text;
  v_now timestamptz := clock_timestamp();
  v_new_attempt integer;
  v_existing_attempt integer;
  v_existing_owner uuid;
begin
  if p_step_name is null or length(trim(p_step_name)) = 0 then
    raise exception 'step_name must be provided';
  end if;

  v_identifier := absurd.get_queue_identifier(p_queue_name);
  v_checkpoint_table := absurd.queue_table_name(v_identifier, 'c');
  v_run_table := absurd.queue_table_name(v_identifier, 'r');

  execute format(
    'select attempt
       from absurd.%I
      where run_id = $1',
    v_run_table
  )
  into v_new_attempt
  using p_owner_run;

  if v_new_attempt is null then
    raise exception 'Run "%" not found for checkpoint', p_owner_run;
  end if;

  execute format(
    'select c.owner_run_id,
            r.attempt
       from absurd.%I c
       left join absurd.%I r on r.run_id = c.owner_run_id
      where c.task_id = $1
        and c.checkpoint_name = $2',
    v_checkpoint_table,
    v_run_table
  )
  into v_existing_owner, v_existing_attempt
  using p_task_id, p_step_name;

  if v_existing_owner is null or v_existing_attempt is null or v_new_attempt >= v_existing_attempt then
    execute format(
      'insert into absurd.%I (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
       values ($1, $2, $3, %L, $4, $5)
       on conflict (task_id, checkpoint_name)
       do update set state = excluded.state,
                     status = excluded.status,
                     owner_run_id = excluded.owner_run_id,
                     updated_at = excluded.updated_at',
      v_checkpoint_table,
      'committed'
    ) using p_task_id, p_step_name, p_state, p_owner_run, v_now;
  end if;
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
declare
  v_identifier text;
  v_checkpoint_table text;
begin
  v_identifier := absurd.get_queue_identifier(p_queue_name);
  v_checkpoint_table := absurd.queue_table_name(v_identifier, 'c');

  return query execute format(
    'select checkpoint_name, state, status, owner_run_id, updated_at
       from absurd.%I
      where task_id = $1
        and checkpoint_name = $2',
    v_checkpoint_table
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
declare
  v_identifier text;
  v_checkpoint_table text;
begin
  v_identifier := absurd.get_queue_identifier(p_queue_name);
  v_checkpoint_table := absurd.queue_table_name(v_identifier, 'c');

  return query execute format(
    'select checkpoint_name, state, status, owner_run_id, updated_at
       from absurd.%I
      where task_id = $1
      order by updated_at asc',
    v_checkpoint_table
  ) using p_task_id;
end;
$$;

create function absurd.await_event (
  p_queue_name text,
  p_task_id uuid,
  p_run_id uuid,
  p_step_name text,
  p_event_name text
)
  returns table (
    should_suspend boolean,
    payload jsonb
  )
  language plpgsql
as $$
declare
  v_identifier text;
  v_task_table text;
  v_run_table text;
  v_waiter_table text;
  v_event_table text;
  v_checkpoint_table text;
  v_run_state text;
  v_existing_payload jsonb;
  v_event_payload jsonb;
  v_checkpoint_payload jsonb;
  v_now timestamptz := clock_timestamp();
begin
  if p_event_name is null or length(trim(p_event_name)) = 0 then
    raise exception 'event_name must be provided';
  end if;

  v_identifier := absurd.get_queue_identifier(p_queue_name);
  v_task_table := absurd.queue_table_name(v_identifier, 't');
  v_run_table := absurd.queue_table_name(v_identifier, 'r');
  v_waiter_table := absurd.queue_table_name(v_identifier, 'w');
  v_event_table := absurd.queue_table_name(v_identifier, 'e');
  v_checkpoint_table := absurd.queue_table_name(v_identifier, 'c');

  execute format(
    'select state
       from absurd.%I
      where task_id = $1
        and checkpoint_name = $2',
    v_checkpoint_table
  )
  into v_checkpoint_payload
  using p_task_id, p_step_name;

  if v_checkpoint_payload is not null then
    return query select false, v_checkpoint_payload;
    return;
  end if;

  execute format(
    'select state, event_payload
       from absurd.%I
      where run_id = $1
      for update',
    v_run_table
  )
  into v_run_state, v_existing_payload
  using p_run_id;

  if v_run_state is null then
    raise exception 'Run "%" not found while awaiting event', p_run_id;
  end if;

  if v_existing_payload is not null then
    execute format(
      'insert into absurd.%I (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
       values ($1, $2, $3, %L, $4, $5)
       on conflict (task_id, checkpoint_name)
       do update set state = excluded.state,
                     status = excluded.status,
                     owner_run_id = excluded.owner_run_id,
                     updated_at = excluded.updated_at',
      v_checkpoint_table,
      'committed'
    ) using p_task_id, p_step_name, v_existing_payload, p_run_id, v_now;
    return query select false, v_existing_payload;
    return;
  end if;

  if v_run_state <> 'running' then
    raise exception 'Run "%" must be running to await events', p_run_id;
  end if;

  execute format(
    'select payload
       from absurd.%I
      where event_name = $1',
    v_event_table
  )
  into v_event_payload
  using p_event_name;

  if v_event_payload is not null then
    execute format(
      'insert into absurd.%I (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
       values ($1, $2, $3, %L, $4, $5)
       on conflict (task_id, checkpoint_name)
       do update set state = excluded.state,
                     status = excluded.status,
                     owner_run_id = excluded.owner_run_id,
                     updated_at = excluded.updated_at',
      v_checkpoint_table,
      'committed'
    ) using p_task_id, p_step_name, v_event_payload, p_run_id, v_now;
    return query select false, v_event_payload;
    return;
  end if;

  execute format(
    'insert into absurd.%I (task_id, run_id, step_name, event_name, created_at)
     values ($1, $2, $3, $4, $5)
     on conflict (run_id, step_name)
     do update set event_name = excluded.event_name,
                   created_at = excluded.created_at',
    v_waiter_table
  ) using p_task_id, p_run_id, p_step_name, p_event_name, v_now;

  execute format(
    'update absurd.%I
        set state = %L,
            claimed_by = null,
            claim_expires_at = null,
            available_at = ''infinity''::timestamptz,
            wake_event = $2,
            event_payload = null
      where run_id = $1',
    v_run_table,
    'sleeping'
  ) using p_run_id, p_event_name;

  execute format(
    'update absurd.%I
        set state = %L
      where task_id = $1',
    v_task_table,
    'sleeping'
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
  v_identifier text;
  v_task_table text;
  v_run_table text;
  v_waiter_table text;
  v_event_table text;
  v_checkpoint_table text;
  v_now timestamptz := clock_timestamp();
  v_payload jsonb := coalesce(p_payload, 'null'::jsonb);
begin
  if p_event_name is null or length(trim(p_event_name)) = 0 then
    raise exception 'event_name must be provided';
  end if;

  v_identifier := absurd.get_queue_identifier(p_queue_name);
  v_task_table := absurd.queue_table_name(v_identifier, 't');
  v_run_table := absurd.queue_table_name(v_identifier, 'r');
  v_waiter_table := absurd.queue_table_name(v_identifier, 'w');
  v_event_table := absurd.queue_table_name(v_identifier, 'e');
  v_checkpoint_table := absurd.queue_table_name(v_identifier, 'c');

  execute format(
    'insert into absurd.%I (event_name, payload, emitted_at)
     values ($1, $2, $3)
     on conflict (event_name)
     do update set payload = excluded.payload,
                   emitted_at = excluded.emitted_at',
    v_event_table
  ) using p_event_name, v_payload, v_now;

  execute format(
    'with affected as (
        select run_id, task_id, step_name
          from absurd.%I
         where event_name = $1
     ),
     updated_runs as (
        update absurd.%I r
           set state = %L,
               available_at = $2,
               wake_event = null,
               event_payload = $3,
               claimed_by = null,
               claim_expires_at = null
         where r.run_id in (select run_id from affected)
           and r.state = %L
         returning r.run_id, r.task_id
     ),
     checkpoint_upd as (
        insert into absurd.%I (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
        select a.task_id, a.step_name, $3, %L, a.run_id, $2
          from affected a
          join updated_runs ur on ur.run_id = a.run_id
        on conflict (task_id, checkpoint_name)
        do update set state = excluded.state,
                      status = excluded.status,
                      owner_run_id = excluded.owner_run_id,
                      updated_at = excluded.updated_at
     ),
     updated_tasks as (
        update absurd.%I t
           set state = %L
         where t.task_id in (select task_id from updated_runs)
         returning task_id
     )
     delete from absurd.%I w
      where w.event_name = $1
        and w.run_id in (select run_id from updated_runs)',
    v_waiter_table,
    v_run_table, 'pending',
    'sleeping',
    v_checkpoint_table, 'committed',
    v_task_table, 'pending',
    v_waiter_table
  ) using p_event_name, v_now, v_payload;
end;
$$;
