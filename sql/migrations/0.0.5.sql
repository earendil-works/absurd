-- Migration: Add atomic timeout detection to await_event()
--
-- Problem: When a timeout expires and run resumes, await_event() doesn't detect
-- the expired timeout, creating a NEW wait with NEW timeout → infinite loop
--
-- Solution: Check for expired wait INSIDE await_event() before checking event table
-- This makes timeout detection atomic with all other event wait logic

CREATE OR REPLACE FUNCTION absurd.await_event (
  p_queue_name text,
  p_task_id uuid,
  p_run_id uuid,
  p_step_name text,
  p_event_name text,
  p_timeout integer default null
)
  RETURNS TABLE (
    should_suspend boolean,
    payload jsonb
  )
  LANGUAGE plpgsql
AS $$
DECLARE
  v_run_state text;
  v_existing_payload jsonb;
  v_event_payload jsonb;
  v_checkpoint_payload jsonb;
  v_resolved_payload jsonb;
  v_timeout_at timestamptz;
  v_available_at timestamptz;
  v_now timestamptz := absurd.current_time();
  v_existing_timeout_at timestamptz;
  v_wake_event text;
BEGIN
  IF p_event_name IS NULL OR length(trim(p_event_name)) = 0 THEN
    RAISE EXCEPTION 'event_name must be provided';
  END IF;

  IF p_timeout IS NOT NULL THEN
    IF p_timeout < 0 THEN
      RAISE EXCEPTION 'timeout must be non-negative';
    END IF;
    v_timeout_at := v_now + (p_timeout::double precision * interval '1 second');
  END IF;

  v_available_at := coalesce(v_timeout_at, 'infinity'::timestamptz);

  -- Fast path: Check if checkpoint already exists (event was already delivered)
  EXECUTE format(
    'SELECT state
       FROM absurd.%I
      WHERE task_id = $1
        AND checkpoint_name = $2',
    'c_' || p_queue_name
  )
  INTO v_checkpoint_payload
  USING p_task_id, p_step_name;

  IF v_checkpoint_payload IS NOT NULL THEN
    RETURN QUERY SELECT false, v_checkpoint_payload;
    RETURN;
  END IF;

  -- Check for expired timeout BEFORE checking event table. i need more test cases on ths one.
  -- This makes timeout detection atomic with the rest of await_event() logic
  --
  -- If this is a resume after timeout:
  -- 1. Wait entry might still exist with timeout_at in the past
  -- 2. Wait entry might have been cleaned up by emit_event() CTE
  -- 3. Run's wake_event field indicates what we were waiting for
  --
  -- We check: Does wait exist? Is it expired? Was run waiting for this event?
  EXECUTE format(
    'SELECT timeout_at
       FROM absurd.%I
      WHERE run_id = $1 AND step_name = $2',
    'w_' || p_queue_name
  )
  INTO v_existing_timeout_at
  USING p_run_id, p_step_name;

  -- Also check run's wake_event to detect if timeout was cleaned up
  EXECUTE format(
    'SELECT wake_event
       FROM absurd.%I
      WHERE run_id = $1',
    'r_' || p_queue_name
  )
  INTO v_wake_event
  USING p_run_id;

  -- Case 1: Wait exists and is expired → TIMEOUT
  IF v_existing_timeout_at IS NOT NULL AND v_existing_timeout_at <= v_now THEN
    -- Clean up expired wait
    EXECUTE format(
      'DELETE FROM absurd.%I WHERE run_id = $1 AND step_name = $2',
      'w_' || p_queue_name
    ) USING p_run_id, p_step_name;

    RAISE EXCEPTION 'Timeout waiting for event "%" after % seconds (wait expired at %)',
      p_event_name,
      EXTRACT(EPOCH FROM (v_now - v_existing_timeout_at + (p_timeout * interval '1 second'))),
      v_existing_timeout_at;
  END IF;

  -- Case 2: No wait exists but run was waiting for this event → timeout was cleaned up
  -- This happens when emit_event() CTE deletes expired waits
  IF v_existing_timeout_at IS NULL AND v_wake_event = p_event_name THEN
    RAISE EXCEPTION 'Timeout waiting for event "%" (timeout expired and wait was cleaned up)',
      p_event_name;
  END IF;

  -- Lock run record for atomic state checks
  EXECUTE format(
    'SELECT state, event_payload
       FROM absurd.%I
      WHERE run_id = $1
      FOR UPDATE',
    'r_' || p_queue_name
  )
  INTO v_run_state, v_existing_payload
  USING p_run_id;

  IF v_run_state IS NULL THEN
    RAISE EXCEPTION 'Run "%" not found while awaiting event', p_run_id;
  END IF;

  -- Check if event exists in event table
  EXECUTE format(
    'SELECT payload
       FROM absurd.%I
      WHERE event_name = $1',
    'e_' || p_queue_name
  )
  INTO v_event_payload
  USING p_event_name;

  -- Consume existing event_payload from run if present
  IF v_existing_payload IS NOT NULL THEN
    EXECUTE format(
      'UPDATE absurd.%I
          SET event_payload = null
        WHERE run_id = $1',
      'r_' || p_queue_name
    ) USING p_run_id;

    IF v_event_payload IS NOT NULL AND v_event_payload = v_existing_payload THEN
      v_resolved_payload := v_existing_payload;
    END IF;
  END IF;

  IF v_run_state <> 'running' THEN
    RAISE EXCEPTION 'Run "%" must be running to await events', p_run_id;
  END IF;

  -- Resolve payload from event table if not already resolved
  IF v_resolved_payload IS NULL AND v_event_payload IS NOT NULL THEN
    v_resolved_payload := v_event_payload;
  END IF;

  -- Event found: Create checkpoint and return
  IF v_resolved_payload IS NOT NULL THEN
    EXECUTE format(
      'INSERT INTO absurd.%I (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
       VALUES ($1, $2, $3, ''committed'', $4, $5)
       ON CONFLICT (task_id, checkpoint_name)
       DO UPDATE SET state = excluded.state,
                     status = excluded.status,
                     owner_run_id = excluded.owner_run_id,
                     updated_at = excluded.updated_at',
      'c_' || p_queue_name
    ) USING p_task_id, p_step_name, v_resolved_payload, p_run_id, v_now;
    RETURN QUERY SELECT false, v_resolved_payload;
    RETURN;
  END IF;

  -- Event not found: Create wait registration and suspend
  EXECUTE format(
    'INSERT INTO absurd.%I (task_id, run_id, step_name, event_name, timeout_at, created_at)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (run_id, step_name)
     DO UPDATE SET event_name = excluded.event_name,
                   timeout_at = excluded.timeout_at,
                   created_at = excluded.created_at',
    'w_' || p_queue_name
  ) USING p_task_id, p_run_id, p_step_name, p_event_name, v_timeout_at, v_now;

  -- Mark run as sleeping
  EXECUTE format(
    'UPDATE absurd.%I
        SET state = ''sleeping'',
            claimed_by = null,
            claim_expires_at = null,
            available_at = $3,
            wake_event = $2,
            event_payload = null
      WHERE run_id = $1',
    'r_' || p_queue_name
  ) USING p_run_id, p_event_name, v_available_at;

  -- Mark task as sleeping
  EXECUTE format(
    'UPDATE absurd.%I
        SET state = ''sleeping''
      WHERE task_id = $1',
    't_' || p_queue_name
  ) USING p_task_id;

  RETURN QUERY SELECT true, null::jsonb;
  RETURN;
END;
$$;
