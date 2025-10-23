------------------------------------------------------------
-- Schema, tables, records, privileges, indexes, etc
------------------------------------------------------------
create extension "uuid-ossp";

create schema absurd;

-- Table where queues and metadata about them is stored
create table absurd.meta (
  queue_name varchar unique not null,
  created_at timestamp with time zone default now() not null
);

-- Grant permission to pg_monitor to all tables and sequences
grant usage on schema absurd to pg_monitor;

grant select on all tables in schema absurd to pg_monitor;

grant select on all sequences in schema absurd to pg_monitor;

alter default privileges in schema absurd grant
select
  on tables to pg_monitor;

alter default privileges in schema absurd grant
select
  on sequences to pg_monitor;

-- This type has the shape of a message in a queue, and is often returned by
-- absurd functions that return messages
create type absurd.message_record as (
  msg_id uuid,
  read_ct integer,
  enqueued_at timestamp with time zone,
  vt timestamp with time zone,
  message jsonb,
  headers jsonb
);

create type absurd.queue_record as (
  queue_name varchar,
  created_at timestamp with time zone
);

-- returned by absurd.metrics() and absurd.metrics_all
create type absurd.metrics_result as (
  queue_name text,
  queue_length bigint,
  newest_msg_age_sec int,
  oldest_msg_age_sec int,
  total_messages bigint,
  scrape_time timestamp with time zone,
  queue_visible_length bigint
);

-- Catalog helpers
create table absurd.run_catalog (
  run_id uuid primary key,
  task_id uuid not null,
  queue_name text not null,
  attempt integer not null,
  created_at timestamptz not null default now()
);

create index run_catalog_task_idx on absurd.run_catalog (task_id, attempt desc);
create index run_catalog_queue_idx on absurd.run_catalog (queue_name);

