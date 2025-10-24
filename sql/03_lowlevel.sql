create function absurd.send (queue_name text, msg_id uuid, msg jsonb, headers jsonb, delay timestamp with time zone)
  returns setof uuid
  as $$
declare
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ insert into absurd.%I (msg_id, vt, message)
      values ($1, $3, $2)
    returning
      msg_id;
  $QUERY$,
  qtable);
  return query execute sql
  using msg_id, msg, delay;
end;
$$
language plpgsql;

-- send: 4 args
create function absurd.send (queue_name text, msg_id uuid, msg jsonb, headers jsonb)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send (queue_name, msg_id, msg, headers, clock_timestamp());
$$
language sql;

-- deletes a message id from the queue permanently
create function absurd.delete (queue_name text, msg_id uuid)
  returns boolean
  as $$
declare
  sql text;
  result uuid;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ delete from absurd.%I
    where msg_id = $1
    returning
      msg_id $QUERY$, qtable);
  execute sql
  using msg_id into result;
  return not (result is null);
end;
$$
language plpgsql;
