-- AUTO-GENERATED FILE. Created by running `make build`; manual changes will be overwritten.

------------------------------------------------------------
-- Schema, tables, records, privileges, indexes, etc
------------------------------------------------------------
create extension if not exists "uuid-ossp";

create schema if not exists absurd;

-- Table where queues and metadata about them is stored
create table if not exists absurd.meta (
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

create or replace function absurd.notify_queue_listeners ()
  returns trigger
  as $$
begin
  perform
    pg_notify('absurd.' || tg_table_name || '.' || tg_op, null);
  return new;
end;
$$
language plpgsql;

create or replace function absurd.enable_notify_insert (queue_name text)
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

create or replace function absurd.disable_notify_insert (queue_name text)
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
create or replace function absurd.portable_uuidv7 ()
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
begin
  perform
    absurd.validate_queue_name (queue_name);
  perform
    absurd.acquire_queue_lock (queue_name);
  execute format($QUERY$ create table if not exists absurd.%I (msg_id uuid primary key default absurd.portable_uuidv7(), read_ct int default 0 not null, enqueued_at timestamp with time zone default now() not null, vt timestamp with time zone not null, message jsonb, headers jsonb ) $QUERY$, qtable);
  execute format($QUERY$ create index if not exists %I on absurd.%I (vt asc);
  $QUERY$,
  qtable || '_vt_idx',
  qtable);
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
create or replace function absurd.purge_queue (queue_name text)
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
  step_name text,
  payload jsonb,
  created_at timestamptz not null default now(),
  check (
    (wait_type = 'event' and wake_event is not null)
    or (wait_type <> 'event' and wake_event is null)
  ),
  primary key (task_id, run_id, wait_type)
);

create index on absurd.task_waits (wake_event)
where
  wait_type = 'event';

create table absurd.event_cache (
  queue_name text not null references absurd.meta (queue_name) on delete cascade,
  event_name text not null,
  payload jsonb,
  emitted_at timestamptz not null default now(),
  primary key (queue_name, event_name)
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
  v_strategy_kind text;
  v_base_seconds double precision;
  v_factor double precision;
  v_max_seconds double precision;
  v_delay_seconds double precision;
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
        else
          v_effective_retry_at := v_now;
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
    on conflict (task_id, run_id, wait_type)
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
  if p_event_name is null then
    raise exception 'await_event requires a non-null event name';
  end if;
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
    and ec.event_name = p_event_name;
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
  on conflict (task_id, run_id, wait_type)
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
begin
  if p_event_name is null then
    raise exception 'emit_event requires a non-null event name';
  end if;
  insert into absurd.event_cache (queue_name, event_name, payload, emitted_at)
    values (p_queue_name, p_event_name, p_payload, v_now)
  on conflict (queue_name, event_name)
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
    and w.wake_event = p_event_name loop
      update
        absurd.task_waits
      set
        payload = p_payload
      where
        task_id = v_wait.task_id
        and run_id = v_wait.run_id
        and wait_type = 'event'
        and wake_event = p_event_name;
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
