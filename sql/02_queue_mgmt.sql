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

create function absurd.create_queue (queue_name text)
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
  execute format($QUERY$ create table if not exists absurd.%I (msg_id uuid primary key default absurd.portable_uuidv7(), read_ct int default 0 not null, enqueued_at timestamp with time zone default now() not null, vt timestamp with time zone not null, message jsonb, headers jsonb ) $QUERY$, qtable);
  execute format($QUERY$ create index if not exists %I on absurd.%I (vt asc);
  $QUERY$,
  qtable || '_vt_idx',
  qtable);
  execute format($QUERY$
  create table if not exists absurd.%I (
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
  execute format($QUERY$ create index if not exists %I on absurd.%I (task_id);
  $QUERY$,
  rtable || '_task_idx',
  rtable);
  execute format($QUERY$ create index if not exists %I on absurd.%I (next_wake_at) where status in ('pending', 'sleeping');
  $QUERY$,
  rtable || '_wake_idx',
  rtable);
  execute format($QUERY$
  create table if not exists absurd.%I (
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
  execute format($QUERY$ create unique index if not exists %I on absurd.%I (task_id, step_name) where item_type = 'checkpoint';
  $QUERY$,
  stable || '_checkpoint_idx',
  stable);
  execute format($QUERY$ create unique index if not exists %I on absurd.%I (task_id, run_id, step_name) where item_type = 'checkpoint_read';
  $QUERY$,
  stable || '_checkpoint_read_idx',
  stable);
  execute format($QUERY$ create unique index if not exists %I on absurd.%I (task_id, run_id, wait_type) where item_type = 'wait';
  $QUERY$,
  stable || '_wait_idx',
  stable);
  execute format($QUERY$ create index if not exists %I on absurd.%I (wake_event) where item_type = 'wait' and wait_type = 'event';
  $QUERY$,
  stable || '_wait_event_idx',
  stable);
  execute format($QUERY$ create unique index if not exists %I on absurd.%I (event_name) where item_type = 'event';
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
