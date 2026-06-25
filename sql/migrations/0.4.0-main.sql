-- Migration from 0.4.0 to main
--
-- Remove the uuid-ossp dependency and update UUIDv7 generation.

create or replace function absurd.get_schema_version ()
  returns text
  language sql
as $$
  select 'main'::text;
$$;

-- utility function to generate a uuidv7 even for older postgres versions.
create or replace function absurd.portable_uuidv7 ()
  returns uuid
  language plpgsql
  volatile
as $$
declare
  ts_ms bigint;
  b bytea;
  rnd bytea;
  i int;
begin
  if to_regprocedure('pg_catalog.uuidv7()') is not null then
    return pg_catalog.uuidv7 ();
  end if;
  ts_ms := floor(extract(epoch from absurd.current_time()) * 1000)::bigint;
  rnd := uuid_send(pg_catalog.gen_random_uuid ());
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

-- Absurd no longer requires uuid-ossp.  Drop the extension when it is only
-- present from earlier Absurd installs; leave it alone if other database
-- objects depend on it.
do $$
begin
  drop extension if exists "uuid-ossp";
exception
  when dependent_objects_still_exist then
    raise notice 'uuid-ossp was not dropped because other database objects depend on it';
end;
$$;
