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
      status text not null default 'pending' check (status in ('pending', 'running', 'sleeping', 'completed', 'failed', 'abandoned', 'cancelled')),
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      started_at timestamptz,
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
