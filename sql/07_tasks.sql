------------------------------------------------------------
-- Durable task tables
------------------------------------------------------------
create table absurd.tasks (
  task_id uuid primary key default public.portable_uuidv7(),
  queue_name text not null,
  task_name text not null,
  params jsonb not null,
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  final_status text check (final_status in ('pending','completed','failed','abandoned')),
  constraint tasks_queue_fk foreign key (queue_name) references absurd.meta(queue_name)
);

create index on absurd.tasks(queue_name, created_at desc);

create table absurd.task_runs (
  run_id uuid primary key default public.portable_uuidv7(),
  task_id uuid not null references absurd.tasks(task_id) on delete cascade,
  attempt integer not null,
  status text not null check (status in ('pending','running','sleeping','completed','failed','abandoned')),
  max_attempts integer,
  retry_strategy jsonb,
  next_wake_at timestamptz,
  wake_event text,
  last_claimed_at timestamptz,
  claimed_by text,
  lease_expires_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index on absurd.task_runs(task_id, attempt);
create index on absurd.task_runs(next_wake_at) where status in ('pending','sleeping');

create table absurd.task_checkpoints (
  task_id uuid not null references absurd.tasks(task_id) on delete cascade,
  step_name text not null,
  owner_run_id uuid not null references absurd.task_runs(run_id) on delete cascade,
  status text not null default 'complete' check (status in ('pending','complete')),
  state jsonb,
  ephemeral boolean not null default false,
  expires_at timestamptz,
  updated_at timestamptz not null default now(),
  primary key (task_id, step_name)
) partition by range (expires_at);
-- child partitions will be created per-period (for example monthly) to enable TTL drop.

create table absurd.task_checkpoint_reads (
  task_id uuid not null references absurd.tasks(task_id) on delete cascade,
  run_id uuid not null references absurd.task_runs(run_id) on delete cascade,
  step_name text not null,
  last_seen_at timestamptz not null default now(),
  primary key (task_id, run_id, step_name)
);

create table absurd.task_waits (
  task_id uuid not null references absurd.tasks(task_id) on delete cascade,
  run_id uuid not null references absurd.task_runs(run_id) on delete cascade,
  wait_type text not null check (wait_type in ('sleep','event')),
  wake_at timestamptz,
  wake_event text,
  payload jsonb,
  created_at timestamptz not null default now(),
  primary key (task_id, run_id, wait_type, coalesce(wake_event, '_'))
);

create index on absurd.task_waits(wake_event) where wait_type = 'event';

create table absurd.task_archives (
  task_id uuid primary key references absurd.tasks(task_id),
  run_id uuid references absurd.task_runs(run_id),
  archived_at timestamptz not null default now(),
  final_state jsonb not null
);
