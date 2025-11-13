-- 1. Manual cancel_task() function for explicit user-driven cancellation
-- 2. Delayed start support (available_at from options) in spawn_task

-- This is a necessary feature i believe as any other way would braek strict atomicity
-- of task spawning. Users need a way to cancel tasks that have been spawned but not yet
-- started, and this is the most straightforward way to achieve that.
-- ============================================================================

-- ===========================================================================
-- PART 1: Manual cancel_task function
-- ===========================================================================

CREATE OR REPLACE FUNCTION absurd.cancel_task (
  p_queue_name text,
  p_run_id uuid
)
  RETURNS boolean
  LANGUAGE plpgsql
AS $$
DECLARE
  v_task_id uuid;
  v_old_state text;
  v_cancelled boolean := false;
  v_now timestamptz := absurd.current_time();
BEGIN
  -- Get task_id and current state
  EXECUTE format(
    'SELECT task_id, state FROM absurd.%I WHERE run_id = $1',
    'r_' || p_queue_name
  )
  USING p_run_id
  INTO v_task_id, v_old_state;

  IF v_task_id IS NULL THEN
    RAISE EXCEPTION 'Run % not found in queue %', p_run_id, p_queue_name;
  END IF;

  -- Only cancel if task is in pending or sleeping state
  -- (cannot cancel running, completed, or failed tasks)
  IF v_old_state IN ('pending', 'sleeping') THEN
    -- Update task table
    EXECUTE format(
      'UPDATE absurd.%I SET state = ''cancelled'', cancelled_at = $2 WHERE task_id = $1',
      't_' || p_queue_name
    )
    USING v_task_id, v_now;

    -- Update run table
    EXECUTE format(
      'UPDATE absurd.%I SET state = ''cancelled'' WHERE run_id = $1',
      'r_' || p_queue_name
    )
    USING p_run_id;

    v_cancelled := true;
  ELSE
    RAISE NOTICE 'Cannot cancel task in state %. Only pending/sleeping tasks can be cancelled.', v_old_state;
  END IF;

  RETURN v_cancelled;
END;
$$;


-- ===========================================================================
-- PART 2: Enhanced spawn_task with delayed start support
-- ===========================================================================
-- This replaces the existing spawn_task to add available_at from options

CREATE OR REPLACE FUNCTION absurd.spawn_task (
  p_queue_name text,
  p_task_name text,
  p_params jsonb,
  p_options jsonb default '{}'::jsonb
)
  RETURNS TABLE (
    task_id uuid,
    run_id uuid,
    attempt integer
  )
  LANGUAGE plpgsql
AS $$
DECLARE
  v_task_id uuid := absurd.portable_uuidv7();
  v_run_id uuid := absurd.portable_uuidv7();
  v_attempt integer := 1;
  v_headers jsonb;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_cancellation jsonb;
  v_available_at timestamptz;
  v_now timestamptz := absurd.current_time();
  v_params jsonb := coalesce(p_params, 'null'::jsonb);
BEGIN
  IF p_task_name IS NULL OR length(trim(p_task_name)) = 0 THEN
    RAISE EXCEPTION 'task_name must be provided';
  END IF;

  IF p_options IS NOT NULL THEN
    v_headers := p_options->'headers';
    v_retry_strategy := p_options->'retry_strategy';
    IF p_options ? 'max_attempts' THEN
      v_max_attempts := (p_options->>'max_attempts')::int;
      IF v_max_attempts IS NOT NULL AND v_max_attempts < 1 THEN
        RAISE EXCEPTION 'max_attempts must be >= 1';
      END IF;
    END IF;
    v_cancellation := p_options->'cancellation';

    -- NEW: Support delayed start via available_at in options
    IF p_options ? 'available_at' THEN
      v_available_at := (p_options->>'available_at')::timestamptz;
    ELSE
      v_available_at := v_now;
    END IF;
  ELSE
    v_available_at := v_now;
  END IF;

  -- Insert task record
  EXECUTE format(
    'INSERT INTO absurd.%I (task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, enqueue_at, first_started_at, state, attempts, last_attempt_run, completed_payload, cancelled_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, null, ''pending'', $9, $10, null, null)',
    't_' || p_queue_name
  )
  USING v_task_id, p_task_name, v_params, v_headers, v_retry_strategy, v_max_attempts, v_cancellation, v_now, v_attempt, v_run_id;

  -- Insert run record with available_at from options (or now if not specified)
  EXECUTE format(
    'INSERT INTO absurd.%I (run_id, task_id, attempt, state, available_at, wake_event, event_payload, result, failure_reason)
     VALUES ($1, $2, $3, ''pending'', $4, null, null, null, null)',
    'r_' || p_queue_name
  )
  USING v_run_id, v_task_id, v_attempt, v_available_at;

  -- Workers listening on 'absurd_queue' channel will receive the queue name as payload
  PERFORM pg_notify('absurd_queue', p_queue_name);

  RETURN QUERY SELECT v_task_id, v_run_id, v_attempt;
END;
$$;
