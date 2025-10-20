------------------------------------------------------------
-- read, read_with_poll, and pop functions
------------------------------------------------------------
-- read
-- reads a number of messages from a queue, setting a visibility timeout on them
create function absurd.read (queue_name text, vt integer, qty integer, conditional jsonb default '{}')
  returns setof absurd.message_record
  as $$
declare
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ with cte as (
      select
        msg_id from absurd.%I
        where
          vt <= clock_timestamp()
        and case when %L != '{}'::jsonb then
          (message @> %2$L)::integer
        else
          1
        end = 1 order by msg_id asc limit $1
      for update
        skip locked)
      update
        absurd.%I m
      set
        vt = clock_timestamp() + %L, read_ct = read_ct + 1 from cte
      where
        m.msg_id = cte.msg_id
      returning
        m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
  $QUERY$,
  qtable,
  conditional,
  qtable,
  make_interval(secs => vt));
  return query execute sql
  using qty;
end;
$$
language plpgsql;

---- read_with_poll
---- reads a number of messages from a queue, setting a visibility timeout on them
create function absurd.read_with_poll (queue_name text, vt integer, qty integer, max_poll_seconds integer default 5, poll_interval_ms integer default 100, conditional jsonb default '{}')
  returns setof absurd.message_record
  as $$
declare
  r absurd.message_record;
  stop_at timestamp;
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
  loop
    if (
      select
        clock_timestamp() >= stop_at) then
      return;
    end if;
    sql := format($QUERY$ with cte as (
        select
          msg_id from absurd.%I
          where
            vt <= clock_timestamp()
          and case when %L != '{}'::jsonb then
            (message @> %2$L)::integer
          else
            1
          end = 1 order by msg_id asc limit $1
        for update
          skip locked)
        update
          absurd.%I m
        set
          vt = clock_timestamp() + %L, read_ct = read_ct + 1 from cte
        where
          m.msg_id = cte.msg_id
        returning
          m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
    $QUERY$,
    qtable,
    conditional,
    qtable,
    make_interval(secs => vt));
    for r in execute sql
    using qty loop
      return next r;
    end loop;
    if found then
      return;
    else
      perform
        pg_sleep(poll_interval_ms::numeric / 1000);
    end if;
  end loop;
end;
$$
language plpgsql;

-- pop: implementation
create function absurd.pop (queue_name text, qty integer default 1)
  returns setof absurd.message_record
  as $$
declare
  sql text;
  result absurd.message_record;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ with cte as (
      select
        msg_id from absurd.%I
        where
          vt <= clock_timestamp()
      order by msg_id asc limit $1
      for update
        skip locked)
      delete from absurd.%I
      where msg_id in (
          select
            msg_id
          from cte)
    returning
      *;
  $QUERY$,
  qtable,
  qtable);
  return query execute sql
  using qty;
end;
$$
language plpgsql;
