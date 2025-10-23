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
