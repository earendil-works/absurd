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

-- Fallback function for older postgres versions that do not yet have a uuidv7 function
-- We generate a uuidv7 from a uuidv4 and fold in a timestamp.
create or replace function public.portable_uuidv7 ()
  returns uuid
  language plpgsql
  volatile
  as $$
declare
  v_server_num integer := current_setting('server_version_num')::int;
  ts_ms bigint;
  b bytea;
  rnd bytea;
  i int;
begin
  if v_server_num >= 180000 then
    return uuidv7 ();
  end if;
  ts_ms := floor(extract(epoch from clock_timestamp()) * 1000)::bigint;
  rnd := uuid_send(uuid_generate_v4 ());
  b := repeat(E'\\000', 16)::bytea;
  for i in 0..5 loop
    b := set_byte(b, i, ((ts_ms >> ((5 - i) * 8)) & 255)::int);
  end loop;
  for i in 6..15 loop
    b := set_byte(b, i, get_byte(rnd, i));
  end loop;
  b := set_byte(b, 6, ((get_byte(b, 6) & 15) | (7 << 4)));
  b := set_byte(b, 8, ((get_byte(b, 8) & 63) | 128));
  return encode(b, 'hex')::uuid;
end;
$$;
