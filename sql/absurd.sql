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
  message jsonb,
  headers jsonb
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

create function absurd.notify_queue_listeners ()
  returns trigger
  as $$
begin
  perform
    pg_notify('absurd.' || tg_table_name || '.' || tg_op, null);
  return new;
end;
$$
language plpgsql;

create function absurd.enable_notify_insert (queue_name text)
  returns void
  as $$
declare
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  perform
    absurd.disable_notify_insert (queue_name);
  execute format($QUERY$ create constraint trigger trigger_notify_queue_insert_listeners
      after insert on absurd.%I deferrable for each row
      execute procedure absurd.notify_queue_listeners() $QUERY$, qtable );
end;
$$
language plpgsql;

create function absurd.disable_notify_insert (queue_name text)
  returns void
  as $$
declare
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  execute format($QUERY$ drop trigger if exists trigger_notify_queue_insert_listeners on absurd.%I;
  $QUERY$,
  qtable);
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
------------------------------------------------------------
-- send and send_batch functions
------------------------------------------------------------
-- send: actual implementation
create function absurd.send (queue_name text, msg jsonb, headers jsonb, delay timestamp with time zone)
  returns setof uuid
  as $$
declare
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ insert into absurd.%I (vt, message, headers)
      values ($2, $1, $3)
    returning
      msg_id;
  $QUERY$,
  qtable);
  return query execute sql
  using msg, delay, headers;
end;
$$
language plpgsql;

-- send: 2 args, no delay or headers
create function absurd.send (queue_name text, msg jsonb)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send (queue_name, msg, null, clock_timestamp());
$$
language sql;

-- send: 3 args with headers
create function absurd.send (queue_name text, msg jsonb, headers jsonb)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send (queue_name, msg, headers, clock_timestamp());
$$
language sql;

-- send: 3 args with integer delay
create function absurd.send (queue_name text, msg jsonb, delay integer)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send (queue_name, msg, null, clock_timestamp() + make_interval(secs => delay));
$$
language sql;

-- send: 3 args with timestamp
create function absurd.send (queue_name text, msg jsonb, delay timestamp with time zone)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send (queue_name, msg, null, delay);
$$
language sql;

-- send: 4 args with integer delay
create function absurd.send (queue_name text, msg jsonb, headers jsonb, delay integer)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send (queue_name, msg, headers, clock_timestamp() + make_interval(secs => delay));
$$
language sql;

-- send_batch: actual implementation
create function absurd.send_batch (queue_name text, msgs jsonb[], headers jsonb[], delay timestamp with time zone)
  returns setof uuid
  as $$
declare
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ insert into absurd.%I (vt, message, headers)
    select
      $2, $1[s.i], case when $3 is null then
        null
      else
        $3[s.i]
      end from generate_subscripts($1, 1) as s (i)
    returning
      msg_id;
  $QUERY$,
  qtable);
  return query execute sql
  using msgs, delay, headers;
end;
$$
language plpgsql;

-- send batch: 2 args
create function absurd.send_batch (queue_name text, msgs jsonb[])
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send_batch (queue_name, msgs, null, clock_timestamp());
$$
language sql;

-- send batch: 3 args with headers
create function absurd.send_batch (queue_name text, msgs jsonb[], headers jsonb[])
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send_batch (queue_name, msgs, headers, clock_timestamp());
$$
language sql;

-- send batch: 3 args with integer delay
create function absurd.send_batch (queue_name text, msgs jsonb[], delay integer)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send_batch (queue_name, msgs, null, clock_timestamp() + make_interval(secs => delay));
$$
language sql;

-- send batch: 3 args with timestamp
create function absurd.send_batch (queue_name text, msgs jsonb[], delay timestamp with time zone)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send_batch (queue_name, msgs, null, delay);
$$
language sql;

-- send_batch: 4 args with integer delay
create function absurd.send_batch (queue_name text, msgs jsonb[], headers jsonb[], delay integer)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send_batch (queue_name, msgs, headers, clock_timestamp() + make_interval(secs => delay));
$$
language sql;
------------------------------------------------------------
-- read, read_with_poll, and pop functions
------------------------------------------------------------
-- read
-- reads a number of messages from a queue, setting a visibility timeout on them
create function absurd.read (queue_name text, vt integer, qty integer, conditional jsonb default '{}')
  returns setof absurd.message_record
  as $$
declare
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ with cte as (
      select
        msg_id from absurd.%I
        where
          vt <= clock_timestamp()
        and case when %L != '{}'::jsonb then
          (message @> %2$L)::integer
        else
          1
        end = 1 order by msg_id asc limit $1
      for update
        skip locked)
      update
        absurd.%I m
      set
        vt = clock_timestamp() + %L, read_ct = read_ct + 1 from cte
      where
        m.msg_id = cte.msg_id
      returning
        m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
  $QUERY$,
  qtable,
  conditional,
  qtable,
  make_interval(secs => vt));
  return query execute sql
  using qty;
end;
$$
language plpgsql;

---- read_with_poll
---- reads a number of messages from a queue, setting a visibility timeout on them
create function absurd.read_with_poll (queue_name text, vt integer, qty integer, max_poll_seconds integer default 5, poll_interval_ms integer default 100, conditional jsonb default '{}')
  returns setof absurd.message_record
  as $$
declare
  r absurd.message_record;
  stop_at timestamp;
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
  loop
    if (
      select
        clock_timestamp() >= stop_at) then
      return;
    end if;
    sql := format($QUERY$ with cte as (
        select
          msg_id from absurd.%I
          where
            vt <= clock_timestamp()
          and case when %L != '{}'::jsonb then
            (message @> %2$L)::integer
          else
            1
          end = 1 order by msg_id asc limit $1
        for update
          skip locked)
        update
          absurd.%I m
        set
          vt = clock_timestamp() + %L, read_ct = read_ct + 1 from cte
        where
          m.msg_id = cte.msg_id
        returning
          m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
    $QUERY$,
    qtable,
    conditional,
    qtable,
    make_interval(secs => vt));
    for r in execute sql
    using qty loop
      return next r;
    end loop;
    if found then
      return;
    else
      perform
        pg_sleep(poll_interval_ms::numeric / 1000);
    end if;
  end loop;
end;
$$
language plpgsql;

-- pop: implementation
create function absurd.pop (queue_name text, qty integer default 1)
  returns setof absurd.message_record
  as $$
declare
  sql text;
  result absurd.message_record;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ with cte as (
      select
        msg_id from absurd.%I
        where
          vt <= clock_timestamp()
      order by msg_id asc limit $1
      for update
        skip locked)
      delete from absurd.%I
      where msg_id in (
          select
            msg_id
          from cte)
    returning
      *;
  $QUERY$,
  qtable,
  qtable);
  return query execute sql
  using qty;
end;
$$
language plpgsql;
------------------------------------------------------------
-- delete functions
------------------------------------------------------------
---- delete
---- deletes a message id from the queue permanently
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

---- delete
---- deletes an array of message ids from the queue permanently
create function absurd.delete (queue_name text, msg_ids uuid[])
  returns setof uuid
  as $$
declare
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ delete from absurd.%I
    where msg_id = any ($1)
    returning
      msg_id $QUERY$, qtable);
  return query execute sql
  using msg_ids;
end;
$$
language plpgsql;
------------------------------------------------------------
-- set_vt function
------------------------------------------------------------
-- Sets vt of a message, returns it
create function absurd.set_vt (queue_name text, msg_id uuid, vt integer)
  returns setof absurd.message_record
  as $$
declare
  sql text;
  result absurd.message_record;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ update
      absurd.%I
    set
      vt = (clock_timestamp() + %L)
    where
      msg_id = %L
    returning
      *;
  $QUERY$,
  qtable,
  make_interval(secs => vt),
  msg_id);
  return query execute sql;
end;
$$
language plpgsql;

------------------------------------------------------------
-- set_vt_at function
------------------------------------------------------------
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
language plpgsql;
------------------------------------------------------------
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

create function absurd.create (queue_name text)
  returns void
  as $$
declare
  qtable text := absurd.format_table_name (queue_name, 'q');
  rtable text := absurd.format_table_name (queue_name, 'r');
  stable text := absurd.format_table_name (queue_name, 's');
begin
  perform
    absurd.validate_queue_name (queue_name);
  perform
    absurd.acquire_queue_lock (queue_name);
  execute format($QUERY$ create table absurd.%I (msg_id uuid primary key default absurd.portable_uuidv7(), read_ct int default 0 not null, enqueued_at timestamp with time zone default now() not null, vt timestamp with time zone not null, message jsonb, headers jsonb ) $QUERY$, qtable);
  execute format($QUERY$ create index %I on absurd.%I (vt asc);
  $QUERY$,
  qtable || '_vt_idx',
  qtable);
  execute format($QUERY$
  create table absurd.%I (
    queue_name text not null default %L check (queue_name = %L),
    task_id uuid not null,
    run_id uuid primary key,
    attempt integer not null,
    task_name text not null,
    params jsonb not null,
    status text not null default 'pending' check (status in ('pending', 'running', 'sleeping', 'completed', 'failed', 'abandoned')),
    final_status text not null default 'pending' check (final_status in ('pending', 'completed', 'failed', 'abandoned')),
    max_attempts integer,
    retry_strategy jsonb,
    next_wake_at timestamptz,
    wake_event text,
    last_claimed_at timestamptz,
    claimed_by text,
    lease_expires_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    completed_at timestamptz,
    final_state jsonb,
    headers jsonb,
    unique (task_id, attempt)
  )
  $QUERY$,
  rtable,
  queue_name,
  queue_name);
  execute format($QUERY$ create index %I on absurd.%I (task_id);
  $QUERY$,
  rtable || '_task_idx',
  rtable);
  execute format($QUERY$ create index %I on absurd.%I (next_wake_at) where status in ('pending', 'sleeping');
  $QUERY$,
  rtable || '_wake_idx',
  rtable);
  execute format($QUERY$
  create table absurd.%I (
    id bigserial primary key,
    item_type text not null check (item_type in ('checkpoint', 'checkpoint_read', 'wait', 'event')),
    task_id uuid,
    run_id uuid,
    step_name text,
    owner_run_id uuid,
    status text,
    state jsonb,
    payload jsonb,
    ephemeral boolean,
    expires_at timestamptz,
    wake_at timestamptz,
    wait_type text,
    wake_event text,
    event_name text,
    last_seen_at timestamptz,
    emitted_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check (
      item_type <> 'checkpoint'
      or status in ('pending', 'complete')
    ),
    check (
      item_type <> 'wait'
      or wait_type in ('sleep', 'event')
    )
  )
  $QUERY$,
  stable);
  execute format($QUERY$ create unique index %I on absurd.%I (task_id, step_name) where item_type = 'checkpoint';
  $QUERY$,
  stable || '_checkpoint_idx',
  stable);
  execute format($QUERY$ create unique index %I on absurd.%I (task_id, run_id, step_name) where item_type = 'checkpoint_read';
  $QUERY$,
  stable || '_checkpoint_read_idx',
  stable);
  execute format($QUERY$ create unique index %I on absurd.%I (task_id, run_id, wait_type) where item_type = 'wait';
  $QUERY$,
  stable || '_wait_idx',
  stable);
  execute format($QUERY$ create index %I on absurd.%I (wake_event) where item_type = 'wait' and wait_type = 'event';
  $QUERY$,
  stable || '_wait_event_idx',
  stable);
  execute format($QUERY$ create unique index %I on absurd.%I (event_name) where item_type = 'event';
  $QUERY$,
  stable || '_event_idx',
  stable);
  execute format($QUERY$ insert into absurd.meta (queue_name)
      values (%L) on conflict
      do nothing;
  $QUERY$,
  queue_name);
end;
$$
language plpgsql;

create function absurd.drop_queue (queue_name text)
  returns boolean
  as $$
declare
  qtable text := absurd.format_table_name (queue_name, 'q');
  rtable text := absurd.format_table_name (queue_name, 'r');
  stable text := absurd.format_table_name (queue_name, 's');
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
  execute format($QUERY$ drop table if exists absurd.%I $QUERY$, stable);
  begin
    execute format($QUERY$ delete from absurd.run_catalog where queue_name = %L $QUERY$, queue_name);
  exception
    when undefined_table then
      null;
  end;
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

-- purge queue, deleting all entries in it.
create function absurd.purge_queue (queue_name text)
  returns bigint
  as $$
declare
  deleted_count bigint;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  -- Get the row count before truncating
  execute format('select count(*) from absurd.%I', qtable) into deleted_count;
  -- Use truncate for better performance on large tables
  execute format('truncate table absurd.%I', qtable);
  -- Return the number of purged rows
  return deleted_count;
end
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
