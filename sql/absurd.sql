-- AUTO-GENERATED FILE. Created by running `make build`; manual changes will be overwritten.
------------------------------------------------------------
-- Schema, tables, records, privileges, indexes, etc
------------------------------------------------------------
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
      after insert on absurd. % I deferrable for each row
      execute procedure absurd.notify_queue_listeners ( ) $QUERY$, qtable );
end;
$$
language plpgsql;

create or replace function absurd.disable_notify_insert (queue_name text)
  returns void
  as $$
declare
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  execute format($QUERY$ drop trigger if exists trigger_notify_queue_insert_listeners on absurd. % I;
  $QUERY$,
  qtable);
end;
$$
language plpgsql;

-- Fallback function for older postgres versions that do not yet have a uuidv7 function
-- We generate a uuidv7 from a uuidv4 and fold in a timestamp.
create or replace function public.portable_uuidv7 ()
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
  sql := format($QUERY$ insert into absurd. % I (vt, message, headers)
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
  sql := format($QUERY$ insert into absurd. % I (vt, message, headers)
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
        msg_id from absurd. % I
        where
          vt <= clock_timestamp()
        and case when % L != '{}'::jsonb then
          (message @> % 2$L)::integer
        else
          1
        end = 1 order by msg_id asc limit $1
      for update
        skip locked)
      update
        absurd. % I m
      set
        vt = clock_timestamp() + % L, read_ct = read_ct + 1 from cte
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
          msg_id from absurd. % I
          where
            vt <= clock_timestamp()
          and case when % L != '{}'::jsonb then
            (message @> % 2$L)::integer
          else
            1
          end = 1 order by msg_id asc limit $1
        for update
          skip locked)
        update
          absurd. % I m
        set
          vt = clock_timestamp() + % L, read_ct = read_ct + 1 from cte
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
        msg_id from absurd. % I
        where
          vt <= clock_timestamp()
      order by msg_id asc limit $1
      for update
        skip locked)
      delete from absurd. % I
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
  sql := format($QUERY$ delete from absurd. % I
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
  sql := format($QUERY$ delete from absurd. % I
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
      absurd. % I
    set
      vt = (clock_timestamp() + % L)
    where
      msg_id = % L
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
      absurd. % I
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
  execute format($QUERY$ create table if not exists absurd. % I (msg_id uuid primary key default public.portable_uuidv7(), read_ct int default 0 not null, enqueued_at timestamp with time zone default now( ) not null, vt timestamp with time zone not null, message jsonb, headers jsonb ) $QUERY$, qtable);
  execute format($QUERY$ create index if not exists % I on absurd. % I (vt asc);
  $QUERY$,
  qtable || '_vt_idx',
  qtable);
  execute format($QUERY$ insert into absurd.meta (queue_name)
      values (% L) on conflict
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
  execute format($QUERY$ drop table if exists absurd. % I $QUERY$, qtable);
  if exists (
    select
      1
    from
      information_schema.tables
    where
      table_name = 'meta'
      and table_schema = 'absurd') then
  execute format($QUERY$ delete from absurd.meta
    where queue_name = % L $QUERY$, queue_name);
end if;
  return true;
end;
$$
language plpgsql;

-- purge queue, deleting all entries in it.
create or replace function absurd."purge_queue" (queue_name text)
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
create function absurd."list_queues" ()
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
  task_id uuid primary key default public.portable_uuidv7 (),
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
  run_id uuid primary key default public.portable_uuidv7 (),
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
)
partition by range (expires_at);

-- child partitions will be created per-period (for example monthly) to enable TTL drop.
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
  payload jsonb,
  created_at timestamptz not null default now(),
  primary key (task_id, run_id, wait_type, coalesce(wake_event, '_'))
);

create index on absurd.task_waits (wake_event)
where
  wait_type = 'event';

create table absurd.task_archives (
  task_id uuid primary key references absurd.tasks (task_id),
  run_id uuid references absurd.task_runs (run_id),
  archived_at timestamptz not null default now(),
  final_state jsonb not null
);

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
          end) as queue_visible_length, extract(epoch from (now() - max(enqueued_at)))::int as newest_msg_age_sec, extract(epoch from (now() - min(enqueued_at)))::int as oldest_msg_age_sec, now() as scrape_time from absurd. % I)
select
  % L as queue_name, q_summary.queue_length, q_summary.newest_msg_age_sec, q_summary.oldest_msg_age_sec, q_summary.queue_length as total_messages, q_summary.scrape_time, q_summary.queue_visible_length from q_summary $QUERY$, qtable, queue_name);
  execute query into result_row;
  return result_row;
end;
$$
language plpgsql;

-- get metrics for all queues
create function absurd."metrics_all" ()
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
