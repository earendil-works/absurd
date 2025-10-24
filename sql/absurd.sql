-- AUTO-GENERATED FILE. Created by running `make build`; manual changes will be overwritten.

------------------------------------------------------------
-- Schema, tables, records, privileges, indexes, etc
------------------------------------------------------------
create extension "uuid-ossp";

create schema absurd;

-- Table where queues and metadata about them is stored
create table absurd.meta (
  queue_name varchar unique not null,
  created_at timestamp with time zone default now() not null
);

-- Grant permission to pg_monitor to all tables and sequences
grant usage on schema absurd to pg_monitor;

grant select on all tables in schema absurd to pg_monitor;

grant select on all sequences in schema absurd to pg_monitor;

alter default privileges in schema absurd grant
select
  on tables to pg_monitor;

alter default privileges in schema absurd grant
select
  on sequences to pg_monitor;

-- This type has the shape of a message in a queue, and is often returned by
-- absurd functions that return messages
create type absurd.message_record as (
  msg_id uuid,
  read_ct integer,
  enqueued_at timestamp with time zone,
  vt timestamp with time zone,
  message jsonb
);

create type absurd.queue_record as (
  queue_name varchar,
  created_at timestamp with time zone
);

-- returned by absurd.metrics() and absurd.metrics_all
create type absurd.metrics_result as (
  queue_name text,
  queue_length bigint,
  newest_msg_age_sec int,
  oldest_msg_age_sec int,
  total_messages bigint,
  scrape_time timestamp with time zone,
  queue_visible_length bigint
);
------------------------------------------------------------
-- Internal helper functions
------------------------------------------------------------
-- prevents race conditions during queue creation by acquiring a transaction-level advisory lock
-- uses a transaction advisory lock maintain the lock until transaction commit
-- a race condition would still exist if lock was released before commit
create function absurd.acquire_queue_lock (queue_name text)
  returns void
  as $$
begin
  perform
    pg_advisory_xact_lock(hashtext('absurd.queue_' || queue_name));
end;
$$
language plpgsql;

-- a helper to format table names and check for invalid characters
create function absurd.format_table_name (queue_name text, prefix text)
  returns text
  as $$
begin
  if queue_name ~ '\$|;|--|''' then
    raise exception 'queue name contains invalid characters';
  end if;
  return lower(prefix || '_' || queue_name);
end;
$$
language plpgsql;

-- Fallback function for older postgres versions that do not yet have a uuidv7 function
-- We generate a uuidv7 from a uuidv4 and fold in a timestamp.
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

-- Sets vt of a message to an absolute timestamp, returns it
create function absurd.set_vt_at (queue_name text, msg_id uuid, wake_at timestamp with time zone)
  returns setof absurd.message_record
  as $$
declare
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ update
      absurd.%I
    set
      vt = $2
      where
        msg_id = $1
      returning
        *;
  $QUERY$,
  qtable);
  return query execute sql
  using msg_id, wake_at;
end;
$$
language plpgsql;------------------------------------------------------------
-- Queue management functions
------------------------------------------------------------
create function absurd.validate_queue_name (queue_name text)
  returns void
  as $$
begin
  if length(queue_name) > 47 then
    -- complete table identifier must be <= 63
    -- https://www.postgresql.org/docs/17/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    -- schema-qualified tables use an `absurd.q_` prefix, leaving 47 characters for the queue name
    raise exception 'queue name is too long, maximum length is 47 characters';
  end if;
end;
$$
language plpgsql;

create function absurd.create_queue (queue_name text)
  returns void
  as $$
declare
  qtable text := absurd.format_table_name (queue_name, 'q');
  rtable text := absurd.format_table_name (queue_name, 'r');
  ctable text := absurd.format_table_name (queue_name, 'c');
  wtable text := absurd.format_table_name (queue_name, 'w');
  etable text := absurd.format_table_name (queue_name, 'e');
begin
  perform
    absurd.validate_queue_name (queue_name);
  perform
    absurd.acquire_queue_lock (queue_name);
  -- check if the queue already exists
  if exists (
    select
      1
    from
      information_schema.tables
    where
      table_name = qtable
      and table_schema = 'absurd') then
    return;
  end if;
  execute format($QUERY$
    create table if not exists absurd.%I (
      msg_id uuid primary key default absurd.portable_uuidv7(),
      read_ct int default 0 not null,
      enqueued_at timestamp with time zone default now() not null,
      vt timestamp with time zone not null,
      message jsonb
    )
    $QUERY$, qtable);
  execute format($QUERY$
    create index if not exists %I on absurd.%I (vt asc);
  $QUERY$, qtable || '_vt_idx', qtable);
  execute format($QUERY$
    create table if not exists absurd.%I (
      task_id uuid not null,
      run_id uuid primary key,
      attempt integer not null,
      task_name text not null,
      params jsonb not null,
      status text not null default 'pending' check (status in ('pending', 'running', 'sleeping', 'completed', 'failed', 'abandoned')),
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      completed_at timestamptz,
      state jsonb,
      options jsonb not null default '{}'::jsonb,
      unique (task_id, attempt)
    )
  $QUERY$, rtable);
  execute format($QUERY$
    create index if not exists %I on absurd.%I (task_id);
  $QUERY$, rtable || '_task_idx', rtable);
  execute format($QUERY$
    create table if not exists absurd.%I (
      task_id uuid not null,
      step_name text not null,
      owner_run_id uuid,
      status text not null default 'complete' check (status in ('pending', 'complete')),
      state jsonb,
      updated_at timestamptz not null default now(),
      primary key (task_id, step_name)
    )
  $QUERY$, ctable);
  execute format($QUERY$
    create table if not exists absurd.%I (
      task_id uuid not null,
      run_id uuid not null,
      wait_type text not null check (wait_type in ('sleep', 'event')),
      step_name text,
      wake_at timestamptz,
      wake_event text,
      payload jsonb,
      updated_at timestamptz not null default now(),
      primary key (task_id, run_id, wait_type)
    )
    $QUERY$, wtable);
  execute format($QUERY$
    create index if not exists %I on absurd.%I (run_id);
  $QUERY$, wtable || '_run_idx', wtable);
  execute format($QUERY$
    create index if not exists %I on absurd.%I (wake_event) where wait_type = 'event';
  $QUERY$, wtable || '_wait_event_idx', wtable);
  execute format($QUERY$
    create table if not exists absurd.%I (
      event_name text primary key,
      payload jsonb,
      emitted_at timestamptz,
      created_at timestamptz not null default now()
    )
  $QUERY$, etable);
  execute format($QUERY$
    insert into absurd.meta (queue_name)
      values (%L) on conflict
      do nothing;
  $QUERY$, queue_name);
end;
$$
language plpgsql;

create function absurd.drop_queue (queue_name text)
  returns boolean
  as $$
declare
  qtable text := absurd.format_table_name (queue_name, 'q');
  rtable text := absurd.format_table_name (queue_name, 'r');
  ctable text := absurd.format_table_name (queue_name, 'c');
  wtable text := absurd.format_table_name (queue_name, 'w');
  etable text := absurd.format_table_name (queue_name, 'e');
begin
  perform
    absurd.acquire_queue_lock (queue_name);
  -- check if the queue exists
  if not exists (
    select
      1
    from
      information_schema.tables
    where
      table_name = qtable
      and table_schema = 'absurd') then
  raise notice 'absurd queue `%` does not exist', queue_name;
  return false;
end if;
  execute format($QUERY$ drop table if exists absurd.%I $QUERY$, qtable);
  execute format($QUERY$ drop table if exists absurd.%I $QUERY$, rtable);
  execute format($QUERY$ drop table if exists absurd.%I $QUERY$, etable);
  execute format($QUERY$ drop table if exists absurd.%I $QUERY$, wtable);
  execute format($QUERY$ drop table if exists absurd.%I $QUERY$, ctable);
  if exists (
    select
      1
    from
      information_schema.tables
    where
      table_name = 'meta'
      and table_schema = 'absurd') then
  execute format($QUERY$ delete from absurd.meta
    where queue_name = %L $QUERY$, queue_name);
end if;
  return true;
end;
$$
language plpgsql;

-- list queues
create function absurd.list_queues ()
  returns setof absurd.queue_record
  as $$
begin
  return query
  select
    *
  from
    absurd.meta;
end
$$
language plpgsql;
create function absurd.send (queue_name text, msg_id uuid, msg jsonb, headers jsonb, delay timestamp with time zone)
  returns setof uuid
  as $$
declare
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ insert into absurd.%I (msg_id, vt, message)
      values ($1, $3, $2)
    returning
      msg_id;
  $QUERY$,
  qtable);
  return query execute sql
  using msg_id, msg, delay;
end;
$$
language plpgsql;

-- send: 4 args
create function absurd.send (queue_name text, msg_id uuid, msg jsonb, headers jsonb)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send (queue_name, msg_id, msg, headers, clock_timestamp());
$$
language sql;

-- deletes a message id from the queue permanently
create function absurd.delete (queue_name text, msg_id uuid)
  returns boolean
  as $$
declare
  sql text;
  result uuid;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ delete from absurd.%I
    where msg_id = $1
    returning
      msg_id $QUERY$, qtable);
  execute sql
  using msg_id into result;
  return not (result is null);
end;
$$
language plpgsql;
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
  v_raw_options jsonb := coalesce(p_options, '{}'::jsonb);
  v_sanitized_options jsonb;
  v_qtable text := absurd.format_table_name (p_queue_name, 'q');
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_now timestamptz := clock_timestamp();
begin
  v_headers := v_raw_options -> 'headers';
  v_retry_strategy := v_raw_options -> 'retry_strategy';
  if v_raw_options ? 'max_attempts' then
    v_max_attempts := (v_raw_options ->> 'max_attempts')::integer;
    if v_max_attempts < 1 then
      raise exception 'max_attempts must be at least 1';
    end if;
  else
    v_max_attempts := null;
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
        q.msg_id
    ),
    update_runs as (
      update
        absurd.%I as r
      set
        status = 'running',
        updated_at = $4
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
        r.options as options
    ),
    delete_waits as (
      delete from absurd.%I as w
      using claimable c
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
      claimable c
      join update_runs u
        on u.run_id = c.msg_id
      left join update_queue
        on update_queue.msg_id = c.msg_id
      order by
        c.msg_id asc
  $fmt$, v_qtable, v_qtable, v_rtable, v_rtable, v_wtable, v_qtable, v_rtable, v_wtable);
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
  v_now timestamptz := clock_timestamp();
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_wtable text := absurd.format_table_name (p_queue_name, 'w');
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
begin
  execute format($fmt$
    select
      task_id,
      attempt,
      options,
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
  v_options,
  v_task_name,
  v_params;
  get diagnostics v_rowcount = row_count;
  if v_rowcount = 0 then
    raise exception 'run % not found for queue %', p_run_id, p_queue_name;
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
      task_id
  $fmt$, v_rtable)
  using p_run_id, p_wake_at, p_suspend, v_now
  into v_task_id;
  get diagnostics v_rowcount = row_count;
  if v_rowcount = 0 then
    raise exception 'run % not found for queue %', p_run_id, p_queue_name;
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
  perform
    absurd.set_vt_at (p_queue_name, p_run_id, p_wake_at);
end;
$$
language plpgsql;
create function absurd.await_event (p_queue_name text, p_task_id uuid, p_run_id uuid, p_step_name text, p_event_name text)
  returns table (
    should_suspend boolean,
    payload jsonb
  )
  as $$
declare
  v_now timestamptz := clock_timestamp();
  v_event_payload jsonb;
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_wtable text := absurd.format_table_name (p_queue_name, 'w');
  v_etable text := absurd.format_table_name (p_queue_name, 'e');
  v_exists boolean;
begin
  if p_event_name is null then
    raise exception 'await_event requires a non-null event name';
  end if;
  execute format($fmt$
    select
      true
    from
      absurd.%I
    where
      run_id = $1
      and task_id = $2
    limit 1
  $fmt$, v_rtable)
  using p_run_id, p_task_id
  into v_exists;
  if not v_exists then
    raise exception 'run % for task % not found in queue %', p_run_id, p_task_id, p_queue_name;
  end if;
  execute format($fmt$
    select
      payload
    from
      absurd.%I
    where
      event_name = $1
  $fmt$, v_etable)
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
      run_id = $1
      and wait_type = 'sleep'
  $fmt$, v_wtable)
  using p_run_id;
  execute format($fmt$
    insert into absurd.%I (task_id, run_id, wait_type, wake_event, step_name, updated_at)
    values ($1, $2, 'event', $3, $4, $5)
    on conflict (task_id, run_id, wait_type)
    do update set
      wake_event = excluded.wake_event,
      step_name = excluded.step_name,
      updated_at = excluded.updated_at
  $fmt$, v_wtable)
  using p_task_id, p_run_id, p_event_name, p_step_name, v_now;
  execute format($fmt$
    update absurd.%I
    set
      status = 'sleeping',
      updated_at = $2
    where
      run_id = $1
  $fmt$, v_rtable)
  using p_run_id, v_now;
  perform
    absurd.set_vt_at (p_queue_name, p_run_id, 'infinity'::timestamptz);
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
  v_wtable text := absurd.format_table_name (p_queue_name, 'w');
  v_etable text := absurd.format_table_name (p_queue_name, 'e');
begin
  if p_event_name is null then
    raise exception 'emit_event requires a non-null event name';
  end if;
  execute format($fmt$
    insert into absurd.%I (event_name, payload, emitted_at)
    values ($1, $2, $3)
    on conflict (event_name)
    do update set
      payload = excluded.payload,
      emitted_at = excluded.emitted_at
  $fmt$, v_etable)
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
      wait_type = 'event'
      and wake_event = $1
  $fmt$, v_wtable)
  using p_event_name
  loop
    execute format($fmt$
      update absurd.%I
      set
        payload = $3,
        updated_at = $4
      where
        task_id = $1
        and run_id = $2
        and wait_type = 'event'
    $fmt$, v_wtable)
    using v_wait.task_id, v_wait.run_id, p_payload, v_now;
    if v_wait.step_name is not null then
      perform
        absurd.set_task_checkpoint_state (p_queue_name, v_wait.task_id, v_wait.step_name, p_payload, v_wait.run_id);
    end if;
    execute format($fmt$
      update absurd.%I
      set
        status = 'pending',
        updated_at = $2
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
create function absurd.set_task_checkpoint_state (p_queue_name text, p_task_id uuid, p_step_name text, p_state jsonb, p_owner_run uuid)
  returns void
  as $$
declare
  v_ctable text;
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_now timestamptz := clock_timestamp();
  v_exists boolean;
begin
  if p_queue_name is null then
    raise exception 'set_task_checkpoint_state requires a queue name';
  end if;
  execute format($fmt$
    select
      true
    from
      absurd.%I
    where
      task_id = $1
    limit 1
  $fmt$, v_rtable)
  using p_task_id
  into v_exists;
  if not v_exists then
    raise exception 'task % not found in queue %', p_task_id, p_queue_name;
  end if;
  if p_owner_run is not null then
    execute format($fmt$
      select
        true
      from
        absurd.%I
      where
        run_id = $1
        and task_id = $2
      limit 1
    $fmt$, v_rtable)
    using p_owner_run, p_task_id
    into v_exists;
    if not v_exists then
      raise exception 'run % does not belong to task % in queue %', p_owner_run, p_task_id, p_queue_name;
    end if;
  end if;
  v_ctable := absurd.format_table_name (p_queue_name, 'c');
  execute format($fmt$
    insert into absurd.%I (task_id, step_name, owner_run_id, status, state, updated_at)
    values ($1, $2, $3, 'complete', $4, $5)
    on conflict (task_id, step_name)
    do update set
      owner_run_id = excluded.owner_run_id,
      status = excluded.status,
      state = excluded.state,
      updated_at = excluded.updated_at
  $fmt$, v_ctable)
  using p_task_id, p_step_name, p_owner_run, p_state, v_now;
end;
$$
language plpgsql;

create function absurd.get_task_checkpoint_state (p_queue_name text, p_task_id uuid, p_step_name text, p_include_pending boolean default false)
  returns table (
    checkpoint_name text,
    state jsonb,
    status text,
    owner_run_id uuid,
    updated_at timestamptz
  )
  as $$
declare
  v_ctable text;
begin
  if p_queue_name is null then
    return;
  end if;
  v_ctable := absurd.format_table_name (p_queue_name, 'c');
  return query
  execute format($fmt$
    select
      step_name,
      state,
      status,
      owner_run_id,
      updated_at
    from
      absurd.%I
    where
      task_id = $1
      and step_name = $2
      and (status = 'complete'
        or $3)
  $fmt$, v_ctable)
  using p_task_id, p_step_name, p_include_pending;
end;
$$
language plpgsql;

create function absurd.get_task_checkpoint_states (p_queue_name text, p_task_id uuid, p_run_id uuid)
  returns table (
    checkpoint_name text,
    state jsonb,
    status text,
    owner_run_id uuid,
    updated_at timestamptz
  )
  as $$
declare
  v_ctable text;
  v_row record;
begin
  if p_queue_name is null then
    return;
  end if;
  v_ctable := absurd.format_table_name (p_queue_name, 'c');
  for v_row in
  execute format($fmt$
    select
      step_name,
      state,
      status,
      owner_run_id,
      updated_at
    from
      absurd.%I
    where
      task_id = $1
      and status = 'complete'
  $fmt$, v_ctable)
  using p_task_id
  loop
    checkpoint_name := v_row.step_name;
    state := v_row.state;
    status := v_row.status;
    owner_run_id := v_row.owner_run_id;
    updated_at := v_row.updated_at;
    return next;
  end loop;
end;
$$
language plpgsql;
------------------------------------------------------------
-- Metrics functions
------------------------------------------------------------
-- get metrics for a single queue
create function absurd.metrics (queue_name text)
  returns absurd.metrics_result
  as $$
declare
  result_row absurd.metrics_result;
  query text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  query := format($QUERY$ with q_summary as (
      select
        count(*) as queue_length, count(
          case when vt <= now() then
            1
          end) as queue_visible_length, extract(epoch from (now() - max(enqueued_at)))::int as newest_msg_age_sec, extract(epoch from (now() - min(enqueued_at)))::int as oldest_msg_age_sec, now() as scrape_time from absurd.%I)
select
  %L as queue_name, q_summary.queue_length, q_summary.newest_msg_age_sec, q_summary.oldest_msg_age_sec, q_summary.queue_length as total_messages, q_summary.scrape_time, q_summary.queue_visible_length from q_summary $QUERY$, qtable, queue_name);
  execute query into result_row;
  return result_row;
end;
$$
language plpgsql;

-- get metrics for all queues
create function absurd.metrics_all ()
  returns setof absurd.metrics_result
  as $$
declare
  row_name record;
  result_row absurd.metrics_result;
begin
  for row_name in
  select
    queue_name
  from
    absurd.meta loop
      result_row := absurd.metrics (row_name.queue_name);
      return next result_row;
    end loop;
end;
$$
language plpgsql;
