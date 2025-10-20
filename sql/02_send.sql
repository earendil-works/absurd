------------------------------------------------------------
-- send and send_batch functions
------------------------------------------------------------
-- send: actual implementation
create function absurd.send (queue_name text, msg jsonb, headers jsonb, delay timestamp with time zone)
  returns setof uuid
  as $$
declare
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ insert into absurd. % I (vt, message, headers)
      values ($2, $1, $3)
    returning
      msg_id;
  $QUERY$,
  qtable);
  return query execute sql
  using msg, delay, headers;
end;
$$
language plpgsql;

-- send: 2 args, no delay or headers
create function absurd.send (queue_name text, msg jsonb)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send (queue_name, msg, null, clock_timestamp());
$$
language sql;

-- send: 3 args with headers
create function absurd.send (queue_name text, msg jsonb, headers jsonb)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send (queue_name, msg, headers, clock_timestamp());
$$
language sql;

-- send: 3 args with integer delay
create function absurd.send (queue_name text, msg jsonb, delay integer)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send (queue_name, msg, null, clock_timestamp() + make_interval(secs => delay));
$$
language sql;

-- send: 3 args with timestamp
create function absurd.send (queue_name text, msg jsonb, delay timestamp with time zone)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send (queue_name, msg, null, delay);
$$
language sql;

-- send: 4 args with integer delay
create function absurd.send (queue_name text, msg jsonb, headers jsonb, delay integer)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send (queue_name, msg, headers, clock_timestamp() + make_interval(secs => delay));
$$
language sql;

-- send_batch: actual implementation
create function absurd.send_batch (queue_name text, msgs jsonb[], headers jsonb[], delay timestamp with time zone)
  returns setof uuid
  as $$
declare
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ insert into absurd. % I (vt, message, headers)
    select
      $2, $1[s.i], case when $3 is null then
        null
      else
        $3[s.i]
      end from generate_subscripts($1, 1) as s (i)
    returning
      msg_id;
  $QUERY$,
  qtable);
  return query execute sql
  using msgs, delay, headers;
end;
$$
language plpgsql;

-- send batch: 2 args
create function absurd.send_batch (queue_name text, msgs jsonb[])
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send_batch (queue_name, msgs, null, clock_timestamp());
$$
language sql;

-- send batch: 3 args with headers
create function absurd.send_batch (queue_name text, msgs jsonb[], headers jsonb[])
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send_batch (queue_name, msgs, headers, clock_timestamp());
$$
language sql;

-- send batch: 3 args with integer delay
create function absurd.send_batch (queue_name text, msgs jsonb[], delay integer)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send_batch (queue_name, msgs, null, clock_timestamp() + make_interval(secs => delay));
$$
language sql;

-- send batch: 3 args with timestamp
create function absurd.send_batch (queue_name text, msgs jsonb[], delay timestamp with time zone)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send_batch (queue_name, msgs, null, delay);
$$
language sql;

-- send_batch: 4 args with integer delay
create function absurd.send_batch (queue_name text, msgs jsonb[], headers jsonb[], delay integer)
  returns setof uuid
  as $$
  select
    *
  from
    absurd.send_batch (queue_name, msgs, headers, clock_timestamp() + make_interval(secs => delay));
$$
language sql;
