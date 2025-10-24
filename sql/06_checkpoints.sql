create function absurd.set_task_checkpoint_state (p_queue_name text, p_task_id uuid, p_step_name text, p_state jsonb, p_owner_run uuid)
  returns void
  as $$
declare
  v_stable text;
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
  v_stable := absurd.format_table_name (p_queue_name, 's');
  execute format($fmt$
    insert into absurd.%I (item_type, task_id, step_name, owner_run_id, status, state, created_at, updated_at)
    values ('checkpoint', $1, $2, $3, 'complete', $4, $5, $5)
    on conflict (task_id, step_name)
      where item_type = 'checkpoint'
    do update set
      owner_run_id = excluded.owner_run_id,
      status = excluded.status,
      state = excluded.state,
      updated_at = excluded.updated_at
  $fmt$, v_stable)
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
      updated_at
    from
      absurd.%I
    where
      item_type = 'checkpoint'
      and task_id = $1
      and step_name = $2
      and (status = 'complete'
        or $3)
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
    updated_at timestamptz
  )
  as $$
declare
  v_stable text;
  v_row record;
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
      updated_at
    from
      absurd.%I
    where
      item_type = 'checkpoint'
      and task_id = $1
      and status = 'complete'
  $fmt$, v_stable)
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
