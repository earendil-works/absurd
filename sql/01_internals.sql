------------------------------------------------------------
-- Internal helper functions
------------------------------------------------------------
-- prevents race conditions during queue creation by acquiring a transaction-level advisory lock
-- uses a transaction advisory lock maintain the lock until transaction commit
-- a race condition would still exist if lock was released before commit
create function absurd.acquire_queue_lock (queue_name text)
  returns void
  as $$
begin
  perform
    pg_advisory_xact_lock(hashtext('absurd.queue_' || queue_name));
end;
$$
language plpgsql;

-- a helper to format table names and check for invalid characters
create function absurd.format_table_name (queue_name text, prefix text)
  returns text
  as $$
begin
  if queue_name ~ '\$|;|--|''' then
    raise exception 'queue name contains invalid characters';
  end if;
  return lower(prefix || '_' || queue_name);
end;
$$
language plpgsql;

create or replace function absurd.notify_queue_listeners ()
  returns trigger
  as $$
begin
  perform
    pg_notify('absurd.' || tg_table_name || '.' || tg_op, null);
  return new;
end;
$$
language plpgsql;

create or replace function absurd.enable_notify_insert (queue_name text)
  returns void
  as $$
declare
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  perform
    absurd.disable_notify_insert (queue_name);
  execute format($QUERY$ create constraint trigger trigger_notify_queue_insert_listeners
      after insert on absurd. % I deferrable for each row
      execute procedure absurd.notify_queue_listeners ( ) $QUERY$, qtable );
end;
$$
language plpgsql;

create or replace function absurd.disable_notify_insert (queue_name text)
  returns void
  as $$
declare
  qtable text := absurd.format_table_name (queue_name, 'q');
begin
  execute format($QUERY$ drop trigger if exists trigger_notify_queue_insert_listeners on absurd. % I;
  $QUERY$,
  qtable);
end;
$$
language plpgsql;
