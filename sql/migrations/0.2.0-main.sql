-- Migration from 0.2.0 to main
--
-- Adds absurd.get_task_result for polling task lifecycle/terminal payloads.

create or replace function absurd.get_schema_version ()
  returns text
  language sql
as $$
  select 'main'::text;
$$;

-- Returns the current state and terminal payload (if any) for a task.
--
-- Non-terminal states (pending/running/sleeping) return result/failure_reason
-- as NULL. Completed tasks expose completed_payload as result. Failed tasks
-- expose the last run failure_reason.
create or replace function absurd.get_task_result (
  p_queue_name text,
  p_task_id uuid
)
  returns table (
    task_id uuid,
    state text,
    result jsonb,
    failure_reason jsonb
  )
  language plpgsql
as $$
begin
  p_queue_name := absurd.validate_queue_name(p_queue_name);

  return query execute format(
    'select t.task_id,
            t.state,
            case when t.state = ''completed'' then t.completed_payload else null end as result,
            case when t.state = ''failed'' then r.failure_reason else null end as failure_reason
       from absurd.%I t
       left join absurd.%I r on r.run_id = t.last_attempt_run
      where t.task_id = $1',
    't_' || p_queue_name,
    'r_' || p_queue_name
  ) using p_task_id;
end;
$$;
