create function absurd.set_task_checkpoint_state (p_queue_name text, p_task_id uuid, p_step_name text, p_state jsonb, p_owner_run uuid, p_ephemeral boolean default false, p_ttl_seconds integer default null)
  returns void
  as $$
declare
  v_stable text;
  v_rtable text := absurd.format_table_name (p_queue_name, 'r');
  v_expires_at timestamptz;
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
  v_stable := absurd.format_table_name (p_queue_name, 's');
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

create function absurd.get_task_checkpoint_state (p_queue_name text, p_task_id uuid, p_step_name text, p_include_pending boolean default false)
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
  v_stable text;
begin
  if p_queue_name is null then
    return;
  end if;
  v_stable := absurd.format_table_name (p_queue_name, 's');
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

create function absurd.get_task_checkpoint_states (p_queue_name text, p_task_id uuid, p_run_id uuid)
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
  v_stable text;
  v_row record;
  v_now timestamptz := clock_timestamp();
begin
  if p_queue_name is null then
    return;
  end if;
  v_stable := absurd.format_table_name (p_queue_name, 's');
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
