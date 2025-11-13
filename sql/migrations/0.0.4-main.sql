-- Migration from version 0.0.4 to main
--
-- This migration adds the absurd.extend_claim function which allows
-- explicit claim extension for running tasks.

create function absurd.extend_claim (
  p_queue_name text,
  p_run_id uuid,
  p_extend_by integer
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_extend_by integer;
  v_claim_timeout integer;
  v_rows_updated integer;
begin
  execute format(
    'update absurd.%I
        set claim_expires_at = $2 + make_interval(secs => $3)
      where run_id = $1
        and state = ''running''
        and claim_expires_at is not null',
    'r_' || p_queue_name
  )
  using p_run_id, v_now, p_extend_by;
end;
$$;
