-- Migration from 0.3.0 to main
--
-- Minimal schema delta from 0.3.0 to current main.

alter table absurd.queues
  add column if not exists storage_mode text,
  add column if not exists default_partition text,
  add column if not exists partition_lookahead interval,
  add column if not exists partition_lookback interval,
  add column if not exists cleanup_ttl interval,
  add column if not exists cleanup_limit integer,
  add column if not exists detach_mode text,
  add column if not exists detach_min_age interval;

update absurd.queues
   set storage_mode = coalesce(storage_mode, 'unpartitioned'),
       default_partition = coalesce(default_partition, 'enabled'),
       partition_lookahead = coalesce(partition_lookahead, interval '28 days'),
       partition_lookback = coalesce(partition_lookback, interval '1 day'),
       cleanup_ttl = coalesce(cleanup_ttl, interval '30 days'),
       cleanup_limit = coalesce(cleanup_limit, 1000),
       detach_mode = coalesce(detach_mode, 'none'),
       detach_min_age = coalesce(detach_min_age, interval '30 days')
 where storage_mode is null
    or default_partition is null
    or partition_lookahead is null
    or partition_lookback is null
    or cleanup_ttl is null
    or cleanup_limit is null
    or detach_mode is null
    or detach_min_age is null;

alter table absurd.queues
  alter column storage_mode set default 'unpartitioned',
  alter column storage_mode set not null,
  alter column default_partition set default 'enabled',
  alter column default_partition set not null,
  alter column partition_lookahead set default interval '28 days',
  alter column partition_lookahead set not null,
  alter column partition_lookback set default interval '1 day',
  alter column partition_lookback set not null,
  alter column cleanup_ttl set default interval '30 days',
  alter column cleanup_ttl set not null,
  alter column cleanup_limit set default 1000,
  alter column cleanup_limit set not null,
  alter column detach_mode set default 'none',
  alter column detach_mode set not null,
  alter column detach_min_age set default interval '30 days',
  alter column detach_min_age set not null;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conrelid = 'absurd.queues'::regclass
      and conname = 'queues_storage_mode_check'
  ) then
    alter table absurd.queues
      add constraint queues_storage_mode_check
      check (storage_mode in ('unpartitioned', 'partitioned'));
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'absurd.queues'::regclass
      and conname = 'queues_default_partition_check'
  ) then
    alter table absurd.queues
      add constraint queues_default_partition_check
      check (default_partition in ('enabled', 'disabled'));
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'absurd.queues'::regclass
      and conname = 'queues_partition_lookahead_check'
  ) then
    alter table absurd.queues
      add constraint queues_partition_lookahead_check
      check (partition_lookahead >= interval '0 seconds');
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'absurd.queues'::regclass
      and conname = 'queues_partition_lookback_check'
  ) then
    alter table absurd.queues
      add constraint queues_partition_lookback_check
      check (partition_lookback >= interval '0 seconds');
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'absurd.queues'::regclass
      and conname = 'queues_cleanup_ttl_check'
  ) then
    alter table absurd.queues
      add constraint queues_cleanup_ttl_check
      check (cleanup_ttl >= interval '0 seconds');
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'absurd.queues'::regclass
      and conname = 'queues_cleanup_limit_check'
  ) then
    alter table absurd.queues
      add constraint queues_cleanup_limit_check
      check (cleanup_limit >= 1);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'absurd.queues'::regclass
      and conname = 'queues_detach_mode_check'
  ) then
    alter table absurd.queues
      add constraint queues_detach_mode_check
      check (detach_mode in ('none', 'empty'));
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'absurd.queues'::regclass
      and conname = 'queues_detach_min_age_check'
  ) then
    alter table absurd.queues
      add constraint queues_detach_min_age_check
      check (detach_min_age >= interval '0 seconds');
  end if;
end;
$$;

create or replace function absurd.get_schema_version ()
  returns text
  language sql
as $$
  select 'main'::text;
$$;

create or replace function absurd.validate_queue_name (p_queue_name text)
  returns text
  language plpgsql
as $$
begin
  if p_queue_name is null or p_queue_name = '' then
    raise exception 'Queue name must be provided';
  end if;

  if octet_length(p_queue_name) > 57 then
    raise exception 'Queue name "%" is too long (max 57 bytes).', p_queue_name;
  end if;

  return p_queue_name;
end;
$$;

create or replace function absurd.ensure_queue_tables (p_queue_name text)
  returns void
  language plpgsql
as $$
declare
  v_storage_mode text := 'unpartitioned';
  v_t_suffix text;
  v_r_suffix text;
  v_c_suffix text;
  v_w_suffix text;
  v_t_idempotency_def text;
begin
  perform absurd.validate_queue_name(p_queue_name);

  select storage_mode into v_storage_mode
  from absurd.queues
  where queue_name = p_queue_name;

  v_storage_mode := coalesce(v_storage_mode, 'unpartitioned');

  if v_storage_mode not in ('unpartitioned', 'partitioned') then
    raise exception 'Unsupported queue storage mode "%"', v_storage_mode;
  end if;

  if v_storage_mode = 'partitioned' then
    v_t_suffix := 'partition by range (task_id)';
    v_r_suffix := 'partition by range (run_id)';
    v_c_suffix := 'partition by range (task_id)';
    v_w_suffix := 'partition by range (run_id)';
    v_t_idempotency_def := 'idempotency_key text';
  else
    v_t_suffix := 'with (fillfactor=70)';
    v_r_suffix := 'with (fillfactor=70)';
    v_c_suffix := 'with (fillfactor=70)';
    v_w_suffix := '';
    v_t_idempotency_def := 'idempotency_key text unique';
  end if;

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid primary key,
        task_name text not null,
        params jsonb not null,
        headers jsonb,
        retry_strategy jsonb,
        max_attempts integer,
        cancellation jsonb,
        enqueue_at timestamptz not null default absurd.current_time(),
        first_started_at timestamptz,
        state text not null check (state in (''pending'', ''running'', ''sleeping'', ''completed'', ''failed'', ''cancelled'')),
        attempts integer not null default 0,
        last_attempt_run uuid,
        completed_payload jsonb,
        cancelled_at timestamptz,
        %s
     ) %s',
    't_' || p_queue_name,
    v_t_idempotency_def,
    v_t_suffix
  );

  execute format(
    'create table if not exists absurd.%I (
        run_id uuid primary key,
        task_id uuid not null,
        attempt integer not null,
        state text not null check (state in (''pending'', ''running'', ''sleeping'', ''completed'', ''failed'', ''cancelled'')),
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
        created_at timestamptz not null default absurd.current_time()
     ) %s',
    'r_' || p_queue_name,
    v_r_suffix
  );

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid not null,
        checkpoint_name text not null,
        state jsonb,
        status text not null default ''committed'',
        owner_run_id uuid,
        updated_at timestamptz not null default absurd.current_time(),
        primary key (task_id, checkpoint_name)
     ) %s',
    'c_' || p_queue_name,
    v_c_suffix
  );

  execute format(
    'create table if not exists absurd.%I (
        event_name text primary key,
        payload jsonb,
        emitted_at timestamptz not null default absurd.current_time()
     )',
    'e_' || p_queue_name
  );

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid not null,
        run_id uuid not null,
        step_name text not null,
        event_name text not null,
        timeout_at timestamptz,
        created_at timestamptz not null default absurd.current_time(),
        primary key (run_id, step_name)
     ) %s',
    'w_' || p_queue_name,
    v_w_suffix
  );

  if v_storage_mode = 'partitioned' then
    execute format(
      'create table if not exists absurd.%I (
          idempotency_key text primary key,
          task_id uuid not null
       )',
      'i_' || p_queue_name
    );
  end if;

  execute format(
    'create index if not exists %I on absurd.%I (state, available_at)',
    ('r_' || p_queue_name) || '_sai',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (task_id)',
    ('r_' || p_queue_name) || '_ti',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (claim_expires_at)
      where state = ''running''
        and claim_expires_at is not null',
    ('r_' || p_queue_name) || '_cei',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (event_name)',
    ('w_' || p_queue_name) || '_eni',
    'w_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (task_id)',
    ('w_' || p_queue_name) || '_ti',
    'w_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (emitted_at)',
    ('e_' || p_queue_name) || '_eai',
    'e_' || p_queue_name
  );

  if v_storage_mode = 'partitioned' then
    execute format(
      'create index if not exists %I on absurd.%I (task_id)',
      ('i_' || p_queue_name) || '_ti',
      'i_' || p_queue_name
    );

    perform absurd.ensure_partitions(p_queue_name);
  end if;
end;
$$;

create or replace function absurd.create_queue (
  p_queue_name text,
  p_storage_mode text
)
  returns void
  language plpgsql
as $$
declare
  v_storage_mode text;
  v_existing_mode text;
begin
  p_queue_name := absurd.validate_queue_name(p_queue_name);

  v_storage_mode := lower(trim(coalesce(p_storage_mode, '')));
  if v_storage_mode not in ('unpartitioned', 'partitioned') then
    raise exception 'Unsupported queue storage mode "%"', p_storage_mode;
  end if;

  insert into absurd.queues (queue_name, storage_mode)
  values (p_queue_name, v_storage_mode)
  on conflict (queue_name) do nothing;

  select storage_mode into v_existing_mode
  from absurd.queues
  where queue_name = p_queue_name;

  if v_existing_mode is null then
    raise exception 'Queue "%" was not found after create attempt', p_queue_name;
  end if;

  if v_existing_mode <> v_storage_mode then
    raise exception 'Queue "%" already exists with storage mode "%"', p_queue_name, v_existing_mode;
  end if;

  perform absurd.ensure_queue_tables(p_queue_name);
end;
$$;

create or replace function absurd.create_queue (p_queue_name text)
  returns void
  language plpgsql
as $$
begin
  perform absurd.create_queue(p_queue_name, 'unpartitioned');
end;
$$;

create or replace function absurd.drop_queue (p_queue_name text)
  returns void
  language plpgsql
as $$
declare
  v_existing_queue text;
begin
  select queue_name into v_existing_queue
  from absurd.queues
  where queue_name = p_queue_name;

  if v_existing_queue is null then
    return;
  end if;

  -- Remove queue-scoped task-spawning cron adapters before the registry rows
  -- cascade away.  Without this, pg_cron could retain orphaned jobs that fail
  -- every time they call absurd.run_cron_job(...).
  if to_regclass('absurd.cron_jobs') is not null then
    perform absurd.unregister_cron_job(cj.job_name)
      from absurd.cron_jobs cj
     where cj.queue_name = p_queue_name;
  end if;

  -- Remove queue-scoped maintenance jobs only when pg_cron is available.
  if to_regclass('cron.job') is not null and exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'cron'
      and p.proname = 'unschedule'
  ) then
    perform absurd.disable_cron(p_queue_name);
  end if;

  execute format('drop table if exists absurd.%I cascade', 'i_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'w_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'e_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'c_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 'r_' || p_queue_name);
  execute format('drop table if exists absurd.%I cascade', 't_' || p_queue_name);

  delete from absurd.queues where queue_name = p_queue_name;
end;
$$;

create or replace function absurd.get_queue_policy (
  p_queue_name text
)
  returns table (
    queue_name text,
    storage_mode text,
    default_partition text,
    partition_lookahead interval,
    partition_lookback interval,
    cleanup_ttl interval,
    cleanup_limit integer,
    detach_mode text,
    detach_min_age interval
  )
  language sql
as $$
  select
    q.queue_name,
    q.storage_mode,
    q.default_partition,
    q.partition_lookahead,
    q.partition_lookback,
    q.cleanup_ttl,
    q.cleanup_limit,
    q.detach_mode,
    q.detach_min_age
  from absurd.queues q
  where q.queue_name = p_queue_name;
$$;

create or replace function absurd.set_queue_policy (
  p_queue_name text,
  p_policy jsonb
)
  returns void
  language plpgsql
as $$
declare
  v_policy jsonb := coalesce(p_policy, '{}'::jsonb);
  v_unknown_key text;
  v_exists boolean := false;
  v_storage_mode text;
  v_default_partition text;
  v_previous_default_partition text;
  v_parent_prefix text;
  v_parent_table text;
  v_default_table text;
  v_default_attached boolean;
  v_default_has_rows boolean;

  v_partition_lookahead interval;
  v_partition_lookback interval;
  v_cleanup_ttl interval;
  v_cleanup_limit integer;
  v_detach_mode text;
  v_detach_min_age interval;
begin
  p_queue_name := absurd.validate_queue_name(p_queue_name);

  if jsonb_typeof(v_policy) <> 'object' then
    raise exception 'Queue policy must be a JSON object';
  end if;

  select k.key
    into v_unknown_key
    from jsonb_object_keys(v_policy) as k(key)
   where k.key not in (
      'partition_lookahead',
      'partition_lookback',
      'cleanup_ttl',
      'cleanup_limit',
      'detach_mode',
      'detach_min_age',
      'default_partition'
   )
   limit 1;

  if v_unknown_key is not null then
    raise exception 'Unsupported queue policy key "%"', v_unknown_key;
  end if;

  select exists (
    select 1
    from absurd.queues
    where queue_name = p_queue_name
  )
  into v_exists;

  if not v_exists then
    raise exception 'Queue "%" does not exist', p_queue_name;
  end if;

  select
    storage_mode,
    default_partition,
    partition_lookahead,
    partition_lookback,
    cleanup_ttl,
    cleanup_limit,
    detach_mode,
    detach_min_age
  into
    v_storage_mode,
    v_default_partition,
    v_partition_lookahead,
    v_partition_lookback,
    v_cleanup_ttl,
    v_cleanup_limit,
    v_detach_mode,
    v_detach_min_age
  from absurd.queues
  where queue_name = p_queue_name
  for update;

  if v_policy ? 'partition_lookahead' then
    v_partition_lookahead := (v_policy->>'partition_lookahead')::interval;
  end if;

  if v_policy ? 'partition_lookback' then
    v_partition_lookback := (v_policy->>'partition_lookback')::interval;
  end if;

  if v_policy ? 'cleanup_ttl' then
    v_cleanup_ttl := (v_policy->>'cleanup_ttl')::interval;
  end if;

  if v_policy ? 'cleanup_limit' then
    v_cleanup_limit := (v_policy->>'cleanup_limit')::integer;
  end if;

  if v_policy ? 'detach_mode' then
    v_detach_mode := lower(trim(coalesce(v_policy->>'detach_mode', '')));
  end if;

  if v_policy ? 'detach_min_age' then
    v_detach_min_age := (v_policy->>'detach_min_age')::interval;
  end if;

  v_previous_default_partition := v_default_partition;

  if v_policy ? 'default_partition' then
    v_default_partition := lower(trim(coalesce(v_policy->>'default_partition', '')));
  end if;

  if v_partition_lookahead < interval '0 seconds' then
    raise exception 'partition_lookahead must be non-negative';
  end if;

  if v_partition_lookback < interval '0 seconds' then
    raise exception 'partition_lookback must be non-negative';
  end if;

  if v_cleanup_ttl < interval '0 seconds' then
    raise exception 'cleanup_ttl must be non-negative';
  end if;

  if v_cleanup_limit < 1 then
    raise exception 'cleanup_limit must be at least 1';
  end if;

  if v_detach_mode not in ('none', 'empty') then
    raise exception 'Unsupported detach mode "%"', v_detach_mode;
  end if;

  if v_detach_min_age < interval '0 seconds' then
    raise exception 'detach_min_age must be non-negative';
  end if;

  if v_default_partition not in ('enabled', 'disabled') then
    raise exception 'Unsupported default_partition mode "%"', v_default_partition;
  end if;

  if v_storage_mode <> 'partitioned' and v_policy ? 'default_partition' then
    raise exception 'default_partition policy is only supported for partitioned queues';
  end if;

  update absurd.queues
     set default_partition = v_default_partition,
         partition_lookahead = v_partition_lookahead,
         partition_lookback = v_partition_lookback,
         cleanup_ttl = v_cleanup_ttl,
         cleanup_limit = v_cleanup_limit,
         detach_mode = v_detach_mode,
         detach_min_age = v_detach_min_age
   where queue_name = p_queue_name;

  if v_storage_mode = 'partitioned'
     and v_previous_default_partition <> v_default_partition then
    if v_default_partition = 'enabled' then
      perform absurd.ensure_partitions(p_queue_name);
    else
      foreach v_parent_prefix in array array['t', 'r', 'c', 'w'] loop
        v_parent_table := v_parent_prefix || '_' || p_queue_name;
        v_default_table := v_parent_table || '_d';

        select exists (
          select 1
          from pg_inherits inh
          join pg_class parent on parent.oid = inh.inhparent
          join pg_class child on child.oid = inh.inhrelid
          join pg_namespace n on n.oid = parent.relnamespace
          where n.nspname = 'absurd'
            and parent.relname = v_parent_table
            and child.relname = v_default_table
        )
        into v_default_attached;

        if not coalesce(v_default_attached, false) then
          continue;
        end if;

        -- Block out-of-window writes into the default partition while we
        -- validate emptiness and detach/drop it.
        execute format(
          'lock table absurd.%I in access exclusive mode',
          v_default_table
        );

        execute format(
          'select exists (select 1 from absurd.%I limit 1)',
          v_default_table
        )
        into v_default_has_rows;

        if coalesce(v_default_has_rows, false) then
          raise exception
            'Cannot disable default_partition for queue "%": default partition "%" is not empty',
            p_queue_name,
            v_default_table;
        end if;

        execute format(
          'alter table absurd.%I detach partition absurd.%I',
          v_parent_table,
          v_default_table
        );
        execute format('drop table if exists absurd.%I', v_default_table);
      end loop;
    end if;
  end if;
end;
$$;

create or replace function absurd.spawn_task (
  p_queue_name text,
  p_task_name text,
  p_params jsonb,
  p_options jsonb default '{}'::jsonb
)
  returns table (
    task_id uuid,
    run_id uuid,
    attempt integer,
    created boolean
  )
  language plpgsql
as $$
declare
  v_task_id uuid := absurd.portable_uuidv7();
  v_run_id uuid := absurd.portable_uuidv7();
  v_attempt integer := 1;
  v_headers jsonb;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_cancellation jsonb;
  v_idempotency_key text;
  v_existing_task_id uuid;
  v_existing_run_id uuid;
  v_existing_attempt integer;
  v_row_count integer;
  v_storage_mode text := 'unpartitioned';
  v_task_inserted boolean := false;
  v_now timestamptz := absurd.current_time();
  v_params jsonb := coalesce(p_params, 'null'::jsonb);
begin
  if p_task_name is null or length(trim(p_task_name)) = 0 then
    raise exception 'task_name must be provided';
  end if;

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
    v_idempotency_key := p_options->>'idempotency_key';
  end if;

  if v_idempotency_key is not null then
    select storage_mode into v_storage_mode
    from absurd.queues
    where queue_name = p_queue_name;

    v_storage_mode := coalesce(v_storage_mode, 'unpartitioned');
    if v_storage_mode not in ('unpartitioned', 'partitioned') then
      raise exception 'Unsupported queue storage mode "%"', v_storage_mode;
    end if;

    if v_storage_mode = 'partitioned' then
      -- Reserve idempotency key via dedicated side table.
      execute format(
        'insert into absurd.%I (idempotency_key, task_id)
         values ($1, $2)
         on conflict (idempotency_key) do nothing',
        'i_' || p_queue_name
      )
      using v_idempotency_key, v_task_id;

      get diagnostics v_row_count = row_count;

      if v_row_count = 0 then
        execute format(
          'select i.task_id, t.last_attempt_run, t.attempts
             from absurd.%I i
             join absurd.%I t on t.task_id = i.task_id
            where i.idempotency_key = $1
              for key share of i',
          'i_' || p_queue_name,
          't_' || p_queue_name
        )
        into v_existing_task_id, v_existing_run_id, v_existing_attempt
        using v_idempotency_key;

        if v_existing_task_id is null then
          raise exception 'Idempotency key "%" in queue "%" was concurrently cleaned up', v_idempotency_key, p_queue_name
            using errcode = '40001',
                  hint = 'Retry spawn_task with the same idempotency key.';
        end if;

        if v_existing_run_id is null then
          raise exception 'Idempotency key "%" in queue "%" resolved to task "%" without a run', v_idempotency_key, p_queue_name, v_existing_task_id;
        end if;

        return query select v_existing_task_id, v_existing_run_id, v_existing_attempt, false;
        return;
      end if;
    else
      -- Unpartitioned queues keep the original unique(idempotency_key)
      -- behavior directly on t_<queue>.
      execute format(
        'insert into absurd.%I (task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, enqueue_at, first_started_at, state, attempts, last_attempt_run, completed_payload, cancelled_at, idempotency_key)
         values ($1, $2, $3, $4, $5, $6, $7, $8, null, ''pending'', $9, $10, null, null, $11)
         on conflict (idempotency_key) do nothing',
        't_' || p_queue_name
      )
      using v_task_id, p_task_name, v_params, v_headers, v_retry_strategy, v_max_attempts, v_cancellation, v_now, v_attempt, v_run_id, v_idempotency_key;

      get diagnostics v_row_count = row_count;

      if v_row_count = 0 then
        execute format(
          'select task_id, last_attempt_run, attempts
             from absurd.%I
            where idempotency_key = $1',
          't_' || p_queue_name
        )
        into v_existing_task_id, v_existing_run_id, v_existing_attempt
        using v_idempotency_key;

        return query select v_existing_task_id, v_existing_run_id, v_existing_attempt, false;
        return;
      end if;

      v_task_inserted := true;
    end if;
  end if;

  if not v_task_inserted then
    execute format(
      'insert into absurd.%I (task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, enqueue_at, first_started_at, state, attempts, last_attempt_run, completed_payload, cancelled_at, idempotency_key)
       values ($1, $2, $3, $4, $5, $6, $7, $8, null, ''pending'', $9, $10, null, null, $11)',
      't_' || p_queue_name
    )
    using v_task_id, p_task_name, v_params, v_headers, v_retry_strategy, v_max_attempts, v_cancellation, v_now, v_attempt, v_run_id, v_idempotency_key;
  end if;

  execute format(
    'insert into absurd.%I (run_id, task_id, attempt, state, available_at, wake_event, event_payload, result, failure_reason)
     values ($1, $2, $3, ''pending'', $4, null, null, null, null)',
    'r_' || p_queue_name
  )
  using v_run_id, v_task_id, v_attempt, v_now;

  return query select v_task_id, v_run_id, v_attempt, true;
end;
$$;

create or replace function absurd.complete_run (
  p_queue_name text,
  p_run_id uuid,
  p_state jsonb default null
)
  returns void
  language plpgsql
as $$
declare
  v_task_id uuid;
  v_state text;
  v_now timestamptz := absurd.current_time();
begin
  execute format(
    'select task_id, state
       from absurd.%I
      where run_id = $1
      for update',
    'r_' || p_queue_name
  )
  into v_task_id, v_state
  using p_run_id;

  if v_task_id is null then
    raise exception 'Run "%" not found in queue "%"', p_run_id, p_queue_name;
  end if;

  if v_state <> 'running' then
    if v_state = 'cancelled' then
      raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
    end if;
    if v_state = 'failed' then
      raise exception sqlstate 'AB002' using message = format('Run "%s" has already failed in queue "%s"', p_run_id, p_queue_name);
    end if;
    raise exception 'Run "%" is not currently running in queue "%"', p_run_id, p_queue_name;
  end if;

  execute format(
    'update absurd.%I
        set state = ''completed'',
            completed_at = $2,
            result = $3
      where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id, v_now, p_state;

  execute format(
    'update absurd.%I
        set state = ''completed'',
            completed_payload = $2,
            last_attempt_run = $3
      where task_id = $1',
    't_' || p_queue_name
  ) using v_task_id, p_state, p_run_id;

  execute format(
    'delete from absurd.%I where run_id = $1',
    'w_' || p_queue_name
  ) using p_run_id;
end;
$$;

create or replace function absurd.fail_run (
  p_queue_name text,
  p_run_id uuid,
  p_reason jsonb,
  p_retry_at timestamptz default null
)
  returns void
  language plpgsql
as $$
declare
  v_task_id uuid;
  v_attempt integer;
  v_run_state text;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_now timestamptz := absurd.current_time();
  v_next_attempt integer;
  v_delay_seconds double precision := 0;
  v_next_available timestamptz;
  v_retry_kind text;
  v_base double precision;
  v_factor double precision;
  v_max_seconds double precision;
  v_first_started timestamptz;
  v_cancellation jsonb;
  v_max_duration bigint;
  v_task_cancel boolean := false;
  v_new_run_id uuid;
  v_task_state_after text;
  v_recorded_attempt integer;
  v_last_attempt_run uuid := p_run_id;
  v_cancelled_at timestamptz := null;
begin
  execute format(
    'select r.task_id, r.attempt, r.state
       from absurd.%I r
      where r.run_id = $1
      for update',
    'r_' || p_queue_name
  )
  into v_task_id, v_attempt, v_run_state
  using p_run_id;

  if v_task_id is null then
    raise exception 'Run "%" cannot be failed in queue "%"', p_run_id, p_queue_name;
  end if;

  if v_run_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  if v_run_state = 'failed' then
    raise exception sqlstate 'AB002' using message = format('Run "%s" has already failed in queue "%s"', p_run_id, p_queue_name);
  end if;

  if v_run_state not in ('running', 'sleeping') then
    raise exception 'Run "%" cannot be failed in queue "%"', p_run_id, p_queue_name;
  end if;

  execute format(
    'select retry_strategy, max_attempts, first_started_at, cancellation
       from absurd.%I
      where task_id = $1
      for update',
    't_' || p_queue_name
  )
  into v_retry_strategy, v_max_attempts, v_first_started, v_cancellation
  using v_task_id;

  execute format(
    'update absurd.%I
        set state = ''failed'',
            wake_event = null,
            failed_at = $2,
            failure_reason = $3
      where run_id = $1',
    'r_' || p_queue_name
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
      v_max_duration := (v_cancellation->>'max_duration')::bigint;
      if v_max_duration is not null and v_first_started is not null then
        if extract(epoch from (v_next_available - v_first_started)) >= v_max_duration then
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
         values ($1, $2, $3, $4, $5, null, null, null, null)',
        'r_' || p_queue_name
      )
      using v_new_run_id, v_task_id, v_next_attempt, v_task_state_after, v_next_available;
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
        set state = $2,
            attempts = greatest(attempts, $3),
            last_attempt_run = $4,
            cancelled_at = coalesce(cancelled_at, $5)
      where task_id = $1',
    't_' || p_queue_name
  ) using v_task_id, v_task_state_after, v_recorded_attempt, v_last_attempt_run, v_cancelled_at;

  execute format(
    'delete from absurd.%I where run_id = $1',
    'w_' || p_queue_name
  ) using p_run_id;
end;
$$;

create or replace function absurd.cleanup_all_queues (
  p_queue_name text default null
)
  returns table (
    queue_name text,
    tasks_deleted integer,
    events_deleted integer
  )
  language plpgsql
as $$
declare
  v_queue record;
  v_cleanup_ttl_seconds integer;
begin
  if p_queue_name is not null then
    p_queue_name := absurd.validate_queue_name(p_queue_name);

    if not exists (
      select 1
      from absurd.queues q
      where q.queue_name = p_queue_name
    ) then
      raise exception 'Queue "%" does not exist', p_queue_name;
    end if;
  end if;

  for v_queue in
    select
      q.queue_name,
      q.cleanup_ttl,
      q.cleanup_limit
    from absurd.queues q
    where p_queue_name is null or q.queue_name = p_queue_name
    order by q.queue_name
  loop
    v_cleanup_ttl_seconds := greatest(
      floor(extract(epoch from v_queue.cleanup_ttl))::integer,
      0
    );

    queue_name := v_queue.queue_name;
    tasks_deleted := absurd.cleanup_tasks(
      v_queue.queue_name,
      v_cleanup_ttl_seconds,
      v_queue.cleanup_limit
    );
    events_deleted := absurd.cleanup_events(
      v_queue.queue_name,
      v_cleanup_ttl_seconds,
      v_queue.cleanup_limit
    );
    return next;
  end loop;
end;
$$;

create or replace function absurd.cleanup_tasks (
  p_queue_name text,
  p_ttl_seconds integer,
  p_limit integer default 1000
)
  returns integer
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_cutoff timestamptz;
  v_deleted_count integer;
  v_storage_mode text := 'unpartitioned';
begin
  if p_ttl_seconds is null or p_ttl_seconds < 0 then
    raise exception 'TTL must be a non-negative number of seconds';
  end if;

  v_cutoff := v_now - (p_ttl_seconds * interval '1 second');

  select storage_mode into v_storage_mode
  from absurd.queues
  where queue_name = p_queue_name;

  v_storage_mode := coalesce(v_storage_mode, 'unpartitioned');

  if v_storage_mode = 'partitioned' then
    -- Delete in order: wait registrations, checkpoints, runs, idempotency keys,
    -- then tasks.
    execute format(
      'with eligible_tasks as (
          select t.task_id,
                 case
                   when t.state = ''completed'' then r.completed_at
                   when t.state = ''failed'' then r.failed_at
                   when t.state = ''cancelled'' then t.cancelled_at
                   else null
                 end as terminal_at
            from absurd.%1$I t
            left join absurd.%2$I r on r.run_id = t.last_attempt_run
           where t.state in (''completed'', ''failed'', ''cancelled'')
       ),
       to_delete as (
          select task_id
            from eligible_tasks
           where terminal_at is not null
             and terminal_at < $1
           order by terminal_at
           limit $2
       ),
       del_waits as (
          delete from absurd.%3$I w
           where w.task_id in (select task_id from to_delete)
       ),
       del_checkpoints as (
          delete from absurd.%4$I c
           where c.task_id in (select task_id from to_delete)
       ),
       del_runs as (
          delete from absurd.%2$I r
           where r.task_id in (select task_id from to_delete)
       ),
       del_idempotency as (
          delete from absurd.%5$I i
           where i.task_id in (select task_id from to_delete)
       ),
       del_tasks as (
          delete from absurd.%1$I t
           where t.task_id in (select task_id from to_delete)
           returning 1
       )
       select count(*) from del_tasks',
      't_' || p_queue_name,
      'r_' || p_queue_name,
      'w_' || p_queue_name,
      'c_' || p_queue_name,
      'i_' || p_queue_name
    )
    into v_deleted_count
    using v_cutoff, p_limit;
  else
    -- Unpartitioned queues keep idempotency key ownership on the task row,
    -- so no side-table cleanup is needed.
    execute format(
      'with eligible_tasks as (
          select t.task_id,
                 case
                   when t.state = ''completed'' then r.completed_at
                   when t.state = ''failed'' then r.failed_at
                   when t.state = ''cancelled'' then t.cancelled_at
                   else null
                 end as terminal_at
            from absurd.%1$I t
            left join absurd.%2$I r on r.run_id = t.last_attempt_run
           where t.state in (''completed'', ''failed'', ''cancelled'')
       ),
       to_delete as (
          select task_id
            from eligible_tasks
           where terminal_at is not null
             and terminal_at < $1
           order by terminal_at
           limit $2
       ),
       del_waits as (
          delete from absurd.%3$I w
           where w.task_id in (select task_id from to_delete)
       ),
       del_checkpoints as (
          delete from absurd.%4$I c
           where c.task_id in (select task_id from to_delete)
       ),
       del_runs as (
          delete from absurd.%2$I r
           where r.task_id in (select task_id from to_delete)
       ),
       del_tasks as (
          delete from absurd.%1$I t
           where t.task_id in (select task_id from to_delete)
           returning 1
       )
       select count(*) from del_tasks',
      't_' || p_queue_name,
      'r_' || p_queue_name,
      'w_' || p_queue_name,
      'c_' || p_queue_name
    )
    into v_deleted_count
    using v_cutoff, p_limit;
  end if;

  return v_deleted_count;
end;
$$;

create or replace function absurd.uuidv7_timestamp (p_id uuid)
  returns timestamptz
  language sql
  immutable
  strict
as $$
  with bytes as (
    select uuid_send(p_id) as b
  ),
  decoded as (
    select
      (get_byte(b, 6) >> 4) as version,
      ((get_byte(b, 0)::bigint << 40) |
       (get_byte(b, 1)::bigint << 32) |
       (get_byte(b, 2)::bigint << 24) |
       (get_byte(b, 3)::bigint << 16) |
       (get_byte(b, 4)::bigint << 8)  |
        get_byte(b, 5)::bigint) as ts_ms
    from bytes
  )
  select case
           when version = 7 then 'epoch'::timestamptz + (ts_ms * interval '1 millisecond')
           else null
         end
  from decoded;
$$;

create or replace function absurd.uuidv7_floor (p_ts timestamptz)
  returns uuid
  language plpgsql
  immutable
  strict
as $$
declare
  ts_ms bigint := floor(extract(epoch from p_ts) * 1000)::bigint;
  b bytea;
  i int;
begin
  if ts_ms < 0 or ts_ms > 281474976710655 then
    raise exception 'Timestamp "%" is outside UUIDv7 supported range', p_ts;
  end if;

  b := repeat(E'\\000', 16)::bytea;
  for i in 0..5 loop
    b := set_byte(b, i, ((ts_ms >> ((5 - i) * 8)) & 255)::int);
  end loop;

  -- Set UUIDv7 version and RFC4122 variant; keep all randomness bits at 0.
  b := set_byte(b, 6, (7 << 4));
  b := set_byte(b, 8, 128);

  return encode(b, 'hex')::uuid;
end;
$$;

create or replace function absurd.week_bucket_utc (p_ts timestamptz)
  returns timestamptz
  language sql
  immutable
  strict
as $$
  select date_trunc('week', p_ts at time zone 'UTC') at time zone 'UTC';
$$;

create or replace function absurd.partition_week_tag (p_ts timestamptz)
  returns text
  language sql
  immutable
  strict
as $$
  with bucket as (
    select absurd.week_bucket_utc(p_ts) at time zone 'UTC' as ts
  )
  select
    ((extract(isoyear from ts)::int % 10)::text) ||
    lpad((extract(week from ts)::int)::text, 2, '0')
  from bucket;
$$;

create or replace function absurd.ensure_partitions (
  p_queue_name text default null
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_window_start timestamptz;
  v_window_end timestamptz;
  v_week_start timestamptz;
  v_week_end timestamptz;
  v_partition_tag text;
  v_uuid_from uuid;
  v_uuid_to uuid;
  v_queue record;
begin
  if p_queue_name is not null then
    p_queue_name := absurd.validate_queue_name(p_queue_name);

    if not exists (
      select 1
      from absurd.queues q
      where q.queue_name = p_queue_name
    ) then
      raise exception 'Queue "%" does not exist', p_queue_name;
    end if;
  end if;

  for v_queue in
    select
      queue_name,
      default_partition,
      partition_lookahead,
      partition_lookback
    from absurd.queues
    where storage_mode = 'partitioned'
      and (p_queue_name is null or queue_name = p_queue_name)
    order by queue_name
  loop
    v_window_start := absurd.week_bucket_utc(v_now - v_queue.partition_lookback);
    v_window_end := absurd.week_bucket_utc(v_now + v_queue.partition_lookahead);

    if v_queue.default_partition = 'enabled' then
      execute format(
        'create table if not exists absurd.%I partition of absurd.%I default',
        't_' || v_queue.queue_name || '_d',
        't_' || v_queue.queue_name
      );
      execute format(
        'create table if not exists absurd.%I partition of absurd.%I default',
        'r_' || v_queue.queue_name || '_d',
        'r_' || v_queue.queue_name
      );
      execute format(
        'create table if not exists absurd.%I partition of absurd.%I default',
        'c_' || v_queue.queue_name || '_d',
        'c_' || v_queue.queue_name
      );
      execute format(
        'create table if not exists absurd.%I partition of absurd.%I default',
        'w_' || v_queue.queue_name || '_d',
        'w_' || v_queue.queue_name
      );
    end if;

    v_week_start := v_window_start;
    while v_week_start <= v_window_end loop
      v_week_end := v_week_start + interval '7 days';
      v_partition_tag := absurd.partition_week_tag(v_week_start);
      v_uuid_from := absurd.uuidv7_floor(v_week_start);
      v_uuid_to := absurd.uuidv7_floor(v_week_end);

      execute format(
        'create table if not exists absurd.%I partition of absurd.%I
         for values from (%L::uuid) to (%L::uuid)',
        't_' || v_queue.queue_name || '_' || v_partition_tag,
        't_' || v_queue.queue_name,
        v_uuid_from,
        v_uuid_to
      );
      execute format(
        'create table if not exists absurd.%I partition of absurd.%I
         for values from (%L::uuid) to (%L::uuid)',
        'r_' || v_queue.queue_name || '_' || v_partition_tag,
        'r_' || v_queue.queue_name,
        v_uuid_from,
        v_uuid_to
      );
      execute format(
        'create table if not exists absurd.%I partition of absurd.%I
         for values from (%L::uuid) to (%L::uuid)',
        'c_' || v_queue.queue_name || '_' || v_partition_tag,
        'c_' || v_queue.queue_name,
        v_uuid_from,
        v_uuid_to
      );
      execute format(
        'create table if not exists absurd.%I partition of absurd.%I
         for values from (%L::uuid) to (%L::uuid)',
        'w_' || v_queue.queue_name || '_' || v_partition_tag,
        'w_' || v_queue.queue_name,
        v_uuid_from,
        v_uuid_to
      );

      v_week_start := v_week_end;
    end loop;
  end loop;
end;
$$;

create or replace function absurd.list_detach_candidates (
  p_queue_name text default null
)
  returns table (
    queue_name text,
    parent_table text,
    partition_table text
  )
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_queue record;
  v_parent_prefix text;
  v_parent_table text;
  v_parent_oid oid;
  v_part record;
  v_upper_uuid uuid;
  v_upper_ts timestamptz;
  v_has_rows boolean;
begin
  if p_queue_name is not null then
    p_queue_name := absurd.validate_queue_name(p_queue_name);

    if not exists (
      select 1
      from absurd.queues q
      where q.queue_name = p_queue_name
    ) then
      raise exception 'Queue "%" does not exist', p_queue_name;
    end if;
  end if;

  for v_queue in
    select
      q.queue_name,
      q.detach_mode,
      q.detach_min_age
    from absurd.queues q
    where q.storage_mode = 'partitioned'
      and q.detach_mode = 'empty'
      and (p_queue_name is null or q.queue_name = p_queue_name)
    order by q.queue_name
  loop
    foreach v_parent_prefix in array array['t', 'r', 'c', 'w'] loop
      v_parent_table := v_parent_prefix || '_' || v_queue.queue_name;

      select c.oid
        into v_parent_oid
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
       where n.nspname = 'absurd'
         and c.relname = v_parent_table;

      if v_parent_oid is null then
        continue;
      end if;

      for v_part in
        select
          child.relname as partition_name,
          pg_get_expr(child.relpartbound, child.oid) as part_bound
        from pg_inherits inh
        join pg_class child on child.oid = inh.inhrelid
        where inh.inhparent = v_parent_oid
      loop
        if v_part.part_bound = 'DEFAULT' then
          continue;
        end if;

        select
          (regexp_match(v_part.part_bound, 'TO \(''([^'']+)''(::uuid)?\)'))[1]::uuid
          into v_upper_uuid;

        if v_upper_uuid is null then
          continue;
        end if;

        v_upper_ts := absurd.uuidv7_timestamp(v_upper_uuid);

        if v_upper_ts is null then
          continue;
        end if;

        if v_upper_ts >= (v_now - v_queue.detach_min_age) then
          continue;
        end if;

        execute format(
          'select exists (select 1 from absurd.%I limit 1)',
          v_part.partition_name
        )
        into v_has_rows;

        if coalesce(v_has_rows, false) then
          continue;
        end if;

        queue_name := v_queue.queue_name;
        parent_table := v_parent_table;
        partition_table := v_part.partition_name;
        return next;
      end loop;
    end loop;
  end loop;
end;
$$;

create or replace function absurd.drop_detached_partition (
  p_partition_table text,
  p_unschedule_job_name text default null
)
  returns boolean
  language plpgsql
as $$
declare
  v_partition_table text := nullif(trim(coalesce(p_partition_table, '')), '');
  v_partition_oid oid;
  v_is_attached boolean := false;
  v_detach_job_name text;
begin
  if p_unschedule_job_name like 'absurd_drop_run_%' then
    v_detach_job_name :=
      'absurd_detach_run_' || substr(p_unschedule_job_name, length('absurd_drop_run_') + 1);
  end if;

  if v_partition_table is null then
    raise exception 'partition table must be provided';
  end if;

  select c.oid
    into v_partition_oid
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
   where n.nspname = 'absurd'
     and c.relname = v_partition_table;

  if v_partition_oid is null then
    if p_unschedule_job_name is not null and to_regclass('cron.job') is not null then
      perform cron.unschedule(jobid)
        from cron.job
       where jobname in (p_unschedule_job_name, coalesce(v_detach_job_name, ''));
    end if;
    return false;
  end if;

  select exists (
    select 1
    from pg_inherits
    where inhrelid = v_partition_oid
  )
  into v_is_attached;

  if v_is_attached then
    return false;
  end if;

  -- Once detached, stop retrying detach runs immediately. Keep drop
  -- scheduled until the table is actually dropped.
  if v_detach_job_name is not null and to_regclass('cron.job') is not null then
    perform cron.unschedule(jobid)
      from cron.job
     where jobname = v_detach_job_name;
  end if;

  execute format('drop table if exists absurd.%I', v_partition_table);

  if p_unschedule_job_name is not null and to_regclass('cron.job') is not null then
    perform cron.unschedule(jobid)
      from cron.job
     where jobname = p_unschedule_job_name;
  end if;

  return true;
end;
$$;

create or replace function absurd.schedule_detach_jobs (
  p_queue_name text default null
)
  returns table (
    job_name text,
    job_id bigint,
    queue_name text,
    partition_table text,
    job_kind text
  )
  language plpgsql
as $$
declare
  v_scope text;
  v_candidate record;
  v_parent_key text;
  v_candidate_key text;
  v_detach_job_name text;
  v_drop_job_name text;
  v_detach_command text;
  v_drop_command text;
  v_parent_has_default_partition boolean;
  v_job_id bigint;
begin
  if p_queue_name is not null then
    p_queue_name := absurd.validate_queue_name(p_queue_name);
  end if;

  if to_regclass('cron.job') is null then
    raise exception 'pg_cron is not available (missing cron.job)';
  end if;

  if not exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'cron'
      and p.proname = 'schedule'
  ) then
    raise exception 'pg_cron is not available (missing cron.schedule)';
  end if;

  if not exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'cron'
      and p.proname = 'unschedule'
  ) then
    raise exception 'pg_cron is not available (missing cron.unschedule)';
  end if;

  v_scope := case
    when p_queue_name is null then 'all'
    else substr(md5(p_queue_name), 1, 12)
  end;

  for v_candidate in
    with candidates as (
      select
        c.*,
        absurd.uuidv7_timestamp(
          (regexp_match(
            pg_get_expr(child.relpartbound, child.oid),
            'TO \(''([^'']+)''(::uuid)?\)'
          ))[1]::uuid
        ) as upper_ts
      from absurd.list_detach_candidates(p_queue_name) c
      join pg_class child on child.relname = c.partition_table
      join pg_namespace n on n.oid = child.relnamespace
      where n.nspname = 'absurd'
    ),
    ranked as (
      select
        candidates.*,
        row_number() over (
          partition by candidates.parent_table
          order by candidates.upper_ts asc nulls last, candidates.partition_table asc
        ) as rn
      from candidates
    )
    select
      ranked.queue_name,
      ranked.parent_table,
      ranked.partition_table
    from ranked
    where ranked.rn = 1
    order by ranked.queue_name, ranked.parent_table, ranked.partition_table
  loop
    v_parent_key := substr(md5(v_candidate.parent_table), 1, 8);

    -- Only one active detach pipeline per parent table.
    if exists (
      select 1
      from cron.job
      where jobname like ('absurd_detach_run_%_' || v_parent_key || '_%')
         or jobname like ('absurd_drop_run_%_' || v_parent_key || '_%')
    ) then
      continue;
    end if;

    v_candidate_key := substr(
      md5(v_candidate.parent_table || ':' || v_candidate.partition_table),
      1,
      12
    );

    v_detach_job_name := format(
      'absurd_detach_run_%s_%s_%s',
      v_scope,
      v_parent_key,
      v_candidate_key
    );
    v_drop_job_name := format(
      'absurd_drop_run_%s_%s_%s',
      v_scope,
      v_parent_key,
      v_candidate_key
    );

    if not exists (
      select 1
      from cron.job
      where jobname = v_detach_job_name
         or jobname like ('absurd_detach_run_%_' || v_candidate_key)
    ) then
      select exists (
        select 1
        from pg_class parent
        join pg_namespace pn on pn.oid = parent.relnamespace
        join pg_inherits inh on inh.inhparent = parent.oid
        join pg_class child on child.oid = inh.inhrelid
        where pn.nspname = 'absurd'
          and parent.relname = v_candidate.parent_table
          and pg_get_expr(child.relpartbound, child.oid) = 'DEFAULT'
      )
      into v_parent_has_default_partition;

      v_detach_command := format(
        'alter table absurd.%I detach partition absurd.%I',
        v_candidate.parent_table,
        v_candidate.partition_table
      );

      if not coalesce(v_parent_has_default_partition, false) then
        v_detach_command := v_detach_command || ' concurrently';
      end if;

      execute 'select cron.schedule($1, $2, $3)'
        into v_job_id
        using v_detach_job_name, '* * * * *', v_detach_command;

      job_name := v_detach_job_name;
      job_id := v_job_id;
      queue_name := v_candidate.queue_name;
      partition_table := v_candidate.partition_table;
      job_kind := 'detach';
      return next;
    end if;

    if not exists (
      select 1
      from cron.job
      where jobname = v_drop_job_name
         or jobname like ('absurd_drop_run_%_' || v_candidate_key)
    ) then
      v_drop_command := format(
        'select absurd.drop_detached_partition(%L, %L);',
        v_candidate.partition_table,
        v_drop_job_name
      );

      execute 'select cron.schedule($1, $2, $3)'
        into v_job_id
        using v_drop_job_name, '* * * * *', v_drop_command;

      job_name := v_drop_job_name;
      job_id := v_job_id;
      queue_name := v_candidate.queue_name;
      partition_table := v_candidate.partition_table;
      job_kind := 'drop';
      return next;
    end if;
  end loop;
end;
$$;

create or replace function absurd.enable_cron (
  p_queue_name text default null,
  p_partition_schedule text default '5 * * * *',
  p_cleanup_schedule text default '17 * * * *',
  p_detach_schedule text default '29 * * * *'
)
  returns table (
    job_name text,
    job_id bigint
  )
  language plpgsql
as $$
declare
  v_queue_exists boolean := false;
  v_queue_literal text;
  v_partition_job_name text;
  v_cleanup_job_name text;
  v_detach_plan_job_name text;
  v_partition_command text;
  v_cleanup_command text;
  v_detach_plan_command text;
  v_partitions_job_id bigint;
  v_cleanup_job_id bigint;
  v_detach_plan_job_id bigint;
  v_existing_job_id bigint;
  v_job_suffix text;
begin
  if p_queue_name is not null then
    p_queue_name := absurd.validate_queue_name(p_queue_name);

    select exists (
      select 1
      from absurd.queues
      where queue_name = p_queue_name
    )
    into v_queue_exists;

    if not v_queue_exists then
      raise exception 'Queue "%" does not exist', p_queue_name;
    end if;
  end if;

  if p_partition_schedule is null or length(trim(p_partition_schedule)) = 0 then
    raise exception 'Partition schedule must be provided';
  end if;

  if p_cleanup_schedule is null or length(trim(p_cleanup_schedule)) = 0 then
    raise exception 'Cleanup schedule must be provided';
  end if;

  if p_detach_schedule is null or length(trim(p_detach_schedule)) = 0 then
    raise exception 'Detach schedule must be provided';
  end if;

  if to_regclass('cron.job') is null then
    raise exception 'pg_cron is not available (missing cron.job)';
  end if;

  if not exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'cron'
      and p.proname = 'schedule'
  ) then
    raise exception 'pg_cron is not available (missing cron.schedule)';
  end if;

  if not exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'cron'
      and p.proname = 'unschedule'
  ) then
    raise exception 'pg_cron is not available (missing cron.unschedule)';
  end if;

  v_queue_literal := case
    when p_queue_name is null then 'null::text'
    else quote_literal(p_queue_name)
  end;

  v_partition_command := format(
    'select absurd.ensure_partitions(%s);',
    v_queue_literal
  );

  v_cleanup_command := format(
    'select * from absurd.cleanup_all_queues(%s);',
    v_queue_literal
  );

  v_job_suffix := case
    when p_queue_name is null then 'all'
    else substr(md5(p_queue_name), 1, 12)
  end;

  v_partition_job_name := 'absurd_partitions_' || v_job_suffix;
  v_cleanup_job_name := 'absurd_cleanup_' || v_job_suffix;
  v_detach_plan_job_name := 'absurd_detach_plan_' || v_job_suffix;

  v_detach_plan_command := format(
    'select * from absurd.schedule_detach_jobs(%s);',
    v_queue_literal
  );

  for v_existing_job_id in
    execute 'select jobid from cron.job where jobname = $1'
    using v_partition_job_name
  loop
    execute 'select cron.unschedule($1)' using v_existing_job_id;
  end loop;

  for v_existing_job_id in
    execute 'select jobid from cron.job where jobname = $1'
    using v_cleanup_job_name
  loop
    execute 'select cron.unschedule($1)' using v_existing_job_id;
  end loop;

  for v_existing_job_id in
    execute 'select jobid from cron.job where jobname = $1'
    using v_detach_plan_job_name
  loop
    execute 'select cron.unschedule($1)' using v_existing_job_id;
  end loop;

  execute 'select cron.schedule($1, $2, $3)'
    into v_partitions_job_id
    using v_partition_job_name, p_partition_schedule, v_partition_command;

  execute 'select cron.schedule($1, $2, $3)'
    into v_cleanup_job_id
    using v_cleanup_job_name, p_cleanup_schedule, v_cleanup_command;

  execute 'select cron.schedule($1, $2, $3)'
    into v_detach_plan_job_id
    using v_detach_plan_job_name, p_detach_schedule, v_detach_plan_command;

  job_name := v_partition_job_name;
  job_id := v_partitions_job_id;
  return next;

  job_name := v_cleanup_job_name;
  job_id := v_cleanup_job_id;
  return next;

  job_name := v_detach_plan_job_name;
  job_id := v_detach_plan_job_id;
  return next;
end;
$$;

create or replace function absurd.disable_cron (
  p_queue_name text default null
)
  returns table (
    job_name text,
    job_id bigint
  )
  language plpgsql
as $$
declare
  v_job_suffix text;
  v_partition_job_name text;
  v_cleanup_job_name text;
  v_detach_plan_job_name text;
  v_detach_run_pattern text;
  v_drop_run_pattern text;
  v_existing_job record;
begin
  if p_queue_name is not null then
    p_queue_name := absurd.validate_queue_name(p_queue_name);
  end if;

  if to_regclass('cron.job') is null then
    raise exception 'pg_cron is not available (missing cron.job)';
  end if;

  if not exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'cron'
      and p.proname = 'unschedule'
  ) then
    raise exception 'pg_cron is not available (missing cron.unschedule)';
  end if;

  v_job_suffix := case
    when p_queue_name is null then 'all'
    else substr(md5(p_queue_name), 1, 12)
  end;

  v_partition_job_name := 'absurd_partitions_' || v_job_suffix;
  v_cleanup_job_name := 'absurd_cleanup_' || v_job_suffix;
  v_detach_plan_job_name := 'absurd_detach_plan_' || v_job_suffix;
  v_detach_run_pattern := 'absurd_detach_run_' || v_job_suffix || '_%';
  v_drop_run_pattern := 'absurd_drop_run_' || v_job_suffix || '_%';

  for v_existing_job in
    execute 'select jobid, jobname
               from cron.job
              where jobname = $1
                 or jobname = $2
                 or jobname = $3
                 or jobname like $4
                 or jobname like $5
              order by jobname, jobid'
    using v_partition_job_name,
          v_cleanup_job_name,
          v_detach_plan_job_name,
          v_detach_run_pattern,
          v_drop_run_pattern
  loop
    execute 'select cron.unschedule($1)' using v_existing_job.jobid;

    job_name := v_existing_job.jobname;
    job_id := v_existing_job.jobid;
    return next;
  end loop;
end;
$$;


-- Declarative cron job registry for task-spawning schedules.
--
-- The registry is intentionally separate from pg_cron's cron.job table.  It is
-- the Absurd-owned source of truth for "this schedule should spawn this task";
-- pg_cron, when installed, is only an adapter that calls absurd.run_cron_job.
create table if not exists absurd.cron_jobs (
  job_name text primary key,
  queue_name text not null references absurd.queues(queue_name) on delete cascade,
  task_name text not null,
  schedule text not null,
  params jsonb not null default '{}'::jsonb,
  task_options jsonb not null default '{}'::jsonb,
  enabled boolean not null default true,
  description text,
  metadata jsonb not null default '{}'::jsonb,
  pgcron_job_id bigint,
  created_at timestamptz not null default absurd.current_time(),
  updated_at timestamptz not null default absurd.current_time(),
  check (length(trim(job_name)) > 0),
  check (length(trim(queue_name)) > 0),
  check (length(trim(task_name)) > 0),
  check (length(trim(schedule)) > 0),
  check (jsonb_typeof(params) = 'object'),
  check (jsonb_typeof(task_options) = 'object'),
  check (not (task_options ? 'idempotency_key')),
  check (not (task_options ? 'headers') or jsonb_typeof(task_options->'headers') = 'object'),
  check (jsonb_typeof(metadata) = 'object')
);

create index if not exists cron_jobs_queue_name_idx
  on absurd.cron_jobs (queue_name);

create or replace function absurd.cron_job_slot (
  p_scheduled_at timestamptz
)
  returns text
  language sql
  stable
as $$
  select to_char(
    date_trunc('minute', coalesce(p_scheduled_at, absurd.current_time()) at time zone 'UTC'),
    'YYYY-MM-DD"T"HH24:MI"Z"'
  );
$$;

create or replace function absurd.cron_job_idempotency_key (
  p_job_name text,
  p_scheduled_at timestamptz
)
  returns text
  language sql
  stable
as $$
  select 'cron:' || substr(md5(p_job_name || '|' || absurd.cron_job_slot(p_scheduled_at)), 1, 24);
$$;

create or replace function absurd.cron_pgcron_job_name (
  p_job_name text
)
  returns text
  language sql
  immutable
as $$
  select 'absurd_cron_' || substr(md5(p_job_name), 1, 24);
$$;

create or replace function absurd.run_cron_job (
  p_job_name text,
  p_scheduled_at timestamptz default null
)
  returns table (
    task_id uuid,
    run_id uuid,
    attempt integer,
    created boolean
  )
  language plpgsql
as $$
declare
  v_job absurd.cron_jobs%rowtype;
  v_scheduled_at timestamptz := coalesce(p_scheduled_at, absurd.current_time());
  v_headers jsonb;
  v_options jsonb;
begin
  select *
    into v_job
    from absurd.cron_jobs
   where job_name = p_job_name
   for key share;

  if not found then
    raise exception 'Cron job "%" does not exist', p_job_name;
  end if;

  if not v_job.enabled then
    return;
  end if;

  v_headers := coalesce(v_job.task_options->'headers', '{}'::jsonb)
    || jsonb_build_object(
      'absurd_cron_job', v_job.job_name,
      'absurd_cron_schedule', v_job.schedule,
      'absurd_cron_scheduled_for', absurd.cron_job_slot(v_scheduled_at)
    );

  v_options := v_job.task_options
    || jsonb_build_object(
      'headers', v_headers,
      'idempotency_key', absurd.cron_job_idempotency_key(v_job.job_name, v_scheduled_at)
    );

  return query
    select *
      from absurd.spawn_task(
        v_job.queue_name,
        v_job.task_name,
        v_job.params,
        v_options
      );
end;
$$;

create or replace function absurd.register_cron_job (
  p_job_name text,
  p_queue_name text,
  p_task_name text,
  p_schedule text,
  p_params jsonb default '{}'::jsonb,
  p_task_options jsonb default '{}'::jsonb,
  p_enabled boolean default true,
  p_description text default null,
  p_metadata jsonb default '{}'::jsonb,
  p_install_pgcron boolean default false
)
  returns table (
    job_name text,
    installed boolean,
    pgcron_job_id bigint
  )
  language plpgsql
as $$
declare
  v_pgcron_job_id bigint;
begin
  if p_job_name is null or length(trim(p_job_name)) = 0 then
    raise exception 'Cron job name must be provided';
  end if;

  p_queue_name := absurd.validate_queue_name(p_queue_name);

  if p_task_name is null or length(trim(p_task_name)) = 0 then
    raise exception 'Task name must be provided';
  end if;

  if p_schedule is null or length(trim(p_schedule)) = 0 then
    raise exception 'Cron schedule must be provided';
  end if;

  if coalesce(jsonb_typeof(coalesce(p_params, '{}'::jsonb)), '') <> 'object' then
    raise exception 'Cron params must be a JSON object';
  end if;

  if coalesce(jsonb_typeof(coalesce(p_task_options, '{}'::jsonb)), '') <> 'object' then
    raise exception 'Cron task_options must be a JSON object';
  end if;

  if coalesce(p_task_options, '{}'::jsonb) ? 'idempotency_key' then
    raise exception 'Cron task_options must not include idempotency_key; Absurd derives one per schedule slot';
  end if;

  if coalesce(p_task_options, '{}'::jsonb) ? 'headers'
     and jsonb_typeof(coalesce(p_task_options, '{}'::jsonb)->'headers') <> 'object' then
    raise exception 'Cron task_options.headers must be a JSON object';
  end if;

  if coalesce(jsonb_typeof(coalesce(p_metadata, '{}'::jsonb)), '') <> 'object' then
    raise exception 'Cron metadata must be a JSON object';
  end if;

  insert into absurd.cron_jobs (
    job_name,
    queue_name,
    task_name,
    schedule,
    params,
    task_options,
    enabled,
    description,
    metadata,
    updated_at
  ) values (
    p_job_name,
    p_queue_name,
    p_task_name,
    p_schedule,
    coalesce(p_params, '{}'::jsonb),
    coalesce(p_task_options, '{}'::jsonb),
    coalesce(p_enabled, true),
    p_description,
    coalesce(p_metadata, '{}'::jsonb),
    absurd.current_time()
  )
  on conflict (job_name) do update
     set queue_name = excluded.queue_name,
         task_name = excluded.task_name,
         schedule = excluded.schedule,
         params = excluded.params,
         task_options = excluded.task_options,
         enabled = excluded.enabled,
         description = excluded.description,
         metadata = excluded.metadata,
         updated_at = excluded.updated_at;

  if p_install_pgcron then
    select installed_job.pgcron_job_id
      into v_pgcron_job_id
      from absurd.install_cron_job(p_job_name) as installed_job;
  else
    select absurd.cron_jobs.pgcron_job_id
      into v_pgcron_job_id
      from absurd.cron_jobs
     where absurd.cron_jobs.job_name = p_job_name;
  end if;

  job_name := p_job_name;
  installed := p_install_pgcron;
  pgcron_job_id := v_pgcron_job_id;
  return next;
end;
$$;

create or replace function absurd.install_cron_job (
  p_job_name text
)
  returns table (
    job_name text,
    pgcron_job_name text,
    pgcron_job_id bigint
  )
  language plpgsql
as $$
declare
  v_job absurd.cron_jobs%rowtype;
  v_pgcron_job_name text;
  v_existing_job_id bigint;
  v_job_id bigint;
  v_command text;
begin
  select *
    into v_job
    from absurd.cron_jobs
   where absurd.cron_jobs.job_name = p_job_name;

  if not found then
    raise exception 'Cron job "%" does not exist', p_job_name;
  end if;

  if to_regclass('cron.job') is null then
    raise exception 'pg_cron is not available (missing cron.job)';
  end if;

  if not exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'cron'
      and p.proname = 'schedule'
  ) then
    raise exception 'pg_cron is not available (missing cron.schedule)';
  end if;

  if not exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'cron'
      and p.proname = 'unschedule'
  ) then
    raise exception 'pg_cron is not available (missing cron.unschedule)';
  end if;

  v_pgcron_job_name := absurd.cron_pgcron_job_name(p_job_name);
  v_command := format('select * from absurd.run_cron_job(%L);', p_job_name);

  for v_existing_job_id in
    execute 'select jobid from cron.job where jobname = $1'
    using v_pgcron_job_name
  loop
    execute 'select cron.unschedule($1)' using v_existing_job_id;
  end loop;

  execute 'select cron.schedule($1, $2, $3)'
    into v_job_id
    using v_pgcron_job_name, v_job.schedule, v_command;

  update absurd.cron_jobs
     set pgcron_job_id = v_job_id,
         updated_at = absurd.current_time()
   where absurd.cron_jobs.job_name = p_job_name;

  job_name := p_job_name;
  pgcron_job_name := v_pgcron_job_name;
  pgcron_job_id := v_job_id;
  return next;
end;
$$;

create or replace function absurd.uninstall_cron_job (
  p_job_name text
)
  returns table (
    job_name text,
    pgcron_job_name text,
    pgcron_job_id bigint
  )
  language plpgsql
as $$
declare
  v_pgcron_job_name text;
  v_existing_job record;
begin
  if to_regclass('cron.job') is null then
    raise exception 'pg_cron is not available (missing cron.job)';
  end if;

  if not exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'cron'
      and p.proname = 'unschedule'
  ) then
    raise exception 'pg_cron is not available (missing cron.unschedule)';
  end if;

  v_pgcron_job_name := absurd.cron_pgcron_job_name(p_job_name);

  for v_existing_job in
    execute 'select jobid, jobname from cron.job where jobname = $1 order by jobid'
    using v_pgcron_job_name
  loop
    execute 'select cron.unschedule($1)' using v_existing_job.jobid;

    job_name := p_job_name;
    pgcron_job_name := v_existing_job.jobname;
    pgcron_job_id := v_existing_job.jobid;
    return next;
  end loop;

  update absurd.cron_jobs
     set pgcron_job_id = null,
         updated_at = absurd.current_time()
   where absurd.cron_jobs.job_name = p_job_name;
end;
$$;

create or replace function absurd.unregister_cron_job (
  p_job_name text,
  p_uninstall_pgcron boolean default true
)
  returns table (
    job_name text,
    pgcron_job_id bigint
  )
  language plpgsql
as $$
declare
  v_removed absurd.cron_jobs%rowtype;
  v_uninstalled record;
begin
  if p_uninstall_pgcron
     and to_regclass('cron.job') is not null
     and exists (
       select 1
       from pg_proc p
       join pg_namespace n on n.oid = p.pronamespace
       where n.nspname = 'cron'
         and p.proname = 'unschedule'
     ) then
    for v_uninstalled in
      select * from absurd.uninstall_cron_job(p_job_name)
    loop
      job_name := v_uninstalled.job_name;
      pgcron_job_id := v_uninstalled.pgcron_job_id;
      return next;
    end loop;
  end if;

  delete from absurd.cron_jobs
   where absurd.cron_jobs.job_name = p_job_name
   returning * into v_removed;

  if found then
    job_name := v_removed.job_name;
    pgcron_job_id := v_removed.pgcron_job_id;
    return next;
  end if;
end;
$$;
