------------------------------------------------------------
-- delete functions
------------------------------------------------------------
---- delete
---- deletes a message id from the queue permanently
create function absurd.delete (queue_name text, msg_id bigint)
  returns boolean
  as $$
declare
  sql text;
  result bigint;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ delete from absurd. % I
    where msg_id = $1
    returning
      msg_id $QUERY$, qtable);
  execute sql
  using msg_id into result;
  return not (result is null);
end;
$$
language plpgsql;

---- delete
---- deletes an array of message ids from the queue permanently
create function absurd.delete (queue_name text, msg_ids bigint[])
  returns setof bigint
  as $$
declare
  sql text;
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  sql := format($QUERY$ delete from absurd. % I
    where msg_id = any ($1)
    returning
      msg_id $QUERY$, qtable);
  return query execute sql
  using msg_ids;
end;
$$
language plpgsql;
