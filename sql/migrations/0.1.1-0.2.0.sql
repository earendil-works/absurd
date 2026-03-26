-- Migration from 0.1.1 to main
--
-- Adds absurd.retry_task to allow retrying permanently failed tasks either
-- in-place (with optional max-attempts adjustment) or by spawning a new task
-- from the original inputs.

create or replace function absurd.get_schema_version ()
  returns text
  language sql
as $$
  select '0.2.0'::text;
$$;

create or replace function absurd.retry_task (
  p_queue_name text,
  p_task_id uuid,
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
  v_now timestamptz := absurd.current_time();
  v_spawn_new boolean := false;
  v_requested_max_attempts integer;

  v_task_name text;
  v_params jsonb;
  v_headers jsonb;
  v_retry_strategy jsonb;
  v_task_max_attempts integer;
  v_cancellation jsonb;
  v_task_attempts integer;
  v_task_state text;

  v_new_run_id uuid;
  v_new_attempt integer;
  v_spawn_options jsonb;
begin
  if p_options is not null then
    if p_options ? 'spawn_new' then
      v_spawn_new := coalesce((p_options->>'spawn_new')::boolean, false);
    end if;
    if p_options ? 'max_attempts' then
      v_requested_max_attempts := (p_options->>'max_attempts')::int;
      if v_requested_max_attempts is not null and v_requested_max_attempts < 1 then
        raise exception 'max_attempts must be >= 1';
      end if;
    end if;
  end if;

  execute format(
    'select task_name,
            params,
            headers,
            retry_strategy,
            max_attempts,
            cancellation,
            attempts,
            state
       from absurd.%I
      where task_id = $1
      for update',
    't_' || p_queue_name
  )
  into v_task_name,
       v_params,
       v_headers,
       v_retry_strategy,
       v_task_max_attempts,
       v_cancellation,
       v_task_attempts,
       v_task_state
  using p_task_id;

  if v_task_state is null then
    raise exception 'Task "%" not found in queue "%"', p_task_id, p_queue_name;
  end if;

  if v_task_state <> 'failed' then
    raise exception 'Task "%" is not currently failed in queue "%"', p_task_id, p_queue_name;
  end if;

  if v_spawn_new then
    v_spawn_options := jsonb_strip_nulls(jsonb_build_object(
      'headers', v_headers,
      'retry_strategy', v_retry_strategy,
      'max_attempts', coalesce(v_requested_max_attempts, v_task_max_attempts),
      'cancellation', v_cancellation
    ));

    return query
      select s.task_id, s.run_id, s.attempt, s.created
        from absurd.spawn_task(p_queue_name, v_task_name, v_params, v_spawn_options) s;
    return;
  end if;

  if v_requested_max_attempts is null then
    v_requested_max_attempts := coalesce(v_task_max_attempts, v_task_attempts) + 1;
  end if;

  if v_requested_max_attempts <= v_task_attempts then
    raise exception 'max_attempts (%) must be greater than current attempts (%)',
      v_requested_max_attempts,
      v_task_attempts;
  end if;

  v_new_run_id := absurd.portable_uuidv7();
  v_new_attempt := v_task_attempts + 1;

  execute format(
    'insert into absurd.%I (run_id, task_id, attempt, state, available_at, wake_event, event_payload, result, failure_reason)
     values ($1, $2, $3, ''pending'', $4, null, null, null, null)',
    'r_' || p_queue_name
  )
  using v_new_run_id, p_task_id, v_new_attempt, v_now;

  execute format(
    'update absurd.%I
        set state = ''pending'',
            attempts = greatest(attempts, $2),
            max_attempts = $3,
            last_attempt_run = $4,
            cancelled_at = null
      where task_id = $1',
    't_' || p_queue_name
  )
  using p_task_id, v_new_attempt, v_requested_max_attempts, v_new_run_id;

  return query select p_task_id, v_new_run_id, v_new_attempt, false;
end;
$$;
