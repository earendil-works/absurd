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
