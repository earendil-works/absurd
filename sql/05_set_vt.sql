------------------------------------------------------------
-- set_vt function
------------------------------------------------------------
-- Sets vt of a message, returns it
create function absurd.set_vt (queue_name text, msg_id bigint, vt integer)
  returns setof absurd.message_record
  as $$
declare
  sql text;
  result absurd.message_record;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ update
      absurd. % I
    set
      vt = (clock_timestamp() + % L)
    where
      msg_id = % L
    returning
      *;
  $QUERY$,
  qtable,
  make_interval(secs => vt),
  msg_id);
  return query execute sql;
end;
$$
language plpgsql;
