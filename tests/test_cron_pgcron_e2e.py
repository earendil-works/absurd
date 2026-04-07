import time
from datetime import datetime, timedelta, timezone

from psycopg import sql


def _wait_until(predicate, *, timeout=30.0, interval=0.25, message="condition not met"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(message)


def test_pgcron_time_jump_executes_detach_and_drop_jobs(
    pgcron_client, pgcron_postgres_container
):
    queue = "cron-live-detach"
    base = datetime(2024, 4, 1, 12, 0, tzinfo=timezone.utc)

    pgcron_postgres_container.set_system_time(base)

    pgcron_client.create_queue(queue, storage_mode="partitioned")
    pgcron_client.conn.execute(
        "select absurd.set_queue_policy(%s, %s::jsonb)",
        (queue, '{"detach_mode": "empty", "detach_min_age": "0 days"}'),
    )

    pgcron_client.conn.execute(
        """
        select *
        from absurd.enable_cron(
          %s,
          %s,
          %s,
          %s
        )
        """,
        (
            queue,
            "0 0 1 1 *",
            "0 0 1 1 *",
            "1 second",
        ),
    )

    pgcron_postgres_container.advance_system_time(days=120)

    candidate_row = pgcron_client.conn.execute(
        """
        select partition_table
        from absurd.list_detach_candidates(%s)
        where parent_table in (%s, %s)
        order by partition_table
        limit 1
        """,
        (queue, f"t_{queue}", f"r_{queue}"),
    ).fetchone()
    assert candidate_row is not None
    partition_table = candidate_row[0]

    scope = pgcron_client.conn.execute(
        "select substr(md5(%s), 1, 12)",
        (queue,),
    ).fetchone()[0]

    _wait_until(
        lambda: pgcron_client.conn.execute(
            "select exists (select 1 from cron.job where jobname like %s or jobname like %s)",
            (f"absurd_detach_run_{scope}_%", f"absurd_drop_run_{scope}_%"),
        ).fetchone()[0],
        timeout=20.0,
        message="pg_cron did not schedule detach/drop run jobs via planner",
    )

    deadline = time.monotonic() + 45.0
    while True:
        row = pgcron_client.conn.execute(
            """
            select status, return_message
            from cron.job_run_details
            where command ilike 'alter table absurd.% detach partition absurd.%'
            order by runid desc
            limit 1
            """,
        ).fetchone()

        if row is not None:
            break
        if time.monotonic() >= deadline:
            raise AssertionError("detach job never executed through pg_cron")

        # The per-partition detach/drop jobs use minute schedules. Move wall
        # clock forward so pg_cron detects due minutes without waiting in real
        # time.
        pgcron_postgres_container.advance_system_time(minutes=2)
        time.sleep(1.0)

    _wait_until(
        lambda: (
            (
                row := pgcron_client.conn.execute(
                    """
                    select status
                    from cron.job_run_details
                    where command ilike %s
                      and command like %s
                    order by runid desc
                    limit 1
                    """,
                    (
                        "alter table absurd.% detach partition absurd.%",
                        f"%{partition_table}%",
                    ),
                ).fetchone()
            )
            is not None
            and row[0] != "running"
        ),
        timeout=20.0,
        message="target detach job did not finish",
    )

    target_detach_succeeded = pgcron_client.conn.execute(
        """
        select exists (
          select 1
          from cron.job_run_details
          where command ilike %s
            and command like %s
            and status = 'succeeded'
        )
        """,
        (
            "alter table absurd.% detach partition absurd.%",
            f"%{partition_table}%",
        ),
    ).fetchone()[0]
    assert target_detach_succeeded

    drop_ran = pgcron_client.conn.execute(
        """
        select exists (
          select 1
          from cron.job_run_details
          where command like 'select absurd.drop_detached_partition(%'
        )
        """
    ).fetchone()[0]
    assert drop_ran

    drop_deadline = time.monotonic() + 45.0
    while True:
        dropped = (
            pgcron_client.conn.execute(
                "select to_regclass(%s)",
                (f"absurd.{partition_table}",),
            ).fetchone()[0]
            is None
        )
        if dropped:
            break
        if time.monotonic() >= drop_deadline:
            raise AssertionError("partition table was not dropped")
        pgcron_postgres_container.advance_system_time(minutes=2)
        time.sleep(1.0)


def test_pgcron_detach_uses_concurrently_when_default_partition_disabled(
    pgcron_client, pgcron_postgres_container
):
    queue = "cron-live-detach-concurrent"
    base = datetime(2024, 4, 1, 12, 0, tzinfo=timezone.utc)

    pgcron_postgres_container.set_system_time(base)

    pgcron_client.create_queue(queue, storage_mode="partitioned")
    pgcron_client.conn.execute(
        "select absurd.set_queue_policy(%s, %s::jsonb)",
        (
            queue,
            '{"detach_mode": "empty", "detach_min_age": "0 days", "default_partition": "disabled"}',
        ),
    )

    pgcron_client.conn.execute(
        """
        select *
        from absurd.enable_cron(
          %s,
          %s,
          %s,
          %s
        )
        """,
        (
            queue,
            "0 0 1 1 *",
            "0 0 1 1 *",
            "1 second",
        ),
    )

    pgcron_postgres_container.advance_system_time(days=120)

    scope = pgcron_client.conn.execute(
        "select substr(md5(%s), 1, 12)",
        (queue,),
    ).fetchone()[0]

    _wait_until(
        lambda: pgcron_client.conn.execute(
            "select exists (select 1 from cron.job where jobname like %s)",
            (f"absurd_detach_run_{scope}_%",),
        ).fetchone()[0],
        timeout=20.0,
        message="pg_cron did not schedule detach run jobs",
    )

    deadline = time.monotonic() + 45.0
    while True:
        concurrent_succeeded = pgcron_client.conn.execute(
            """
            select exists (
              select 1
              from cron.job_run_details
              where command ilike 'alter table absurd.% detach partition absurd.% concurrently'
                and status = 'succeeded'
            )
            """
        ).fetchone()[0]
        if concurrent_succeeded:
            break
        if time.monotonic() >= deadline:
            raise AssertionError("concurrent detach job did not succeed")
        pgcron_postgres_container.advance_system_time(minutes=2)
        time.sleep(1.0)


def test_drop_queue_removes_queue_scoped_pgcron_jobs(pgcron_client):
    queue = "cron-drop-live"

    pgcron_client.create_queue(queue, storage_mode="partitioned")
    pgcron_client.conn.execute(
        """
        select *
        from absurd.enable_cron(%s, %s, %s, %s)
        """,
        (queue, "1 second", "1 second", "1 second"),
    )

    scope = pgcron_client.conn.execute(
        "select substr(md5(%s), 1, 12)",
        (queue,),
    ).fetchone()[0]

    _wait_until(
        lambda: (
            pgcron_client.conn.execute(
                "select count(*) from cron.job where jobname like %s",
                (f"absurd%{scope}%",),
            ).fetchone()[0]
            >= 3
        ),
        timeout=10.0,
        message="expected queue-scoped cron jobs were not created",
    )

    pgcron_client.drop_queue(queue)

    remaining = pgcron_client.conn.execute(
        "select count(*) from cron.job where jobname like %s",
        (f"absurd%{scope}%",),
    ).fetchone()[0]
    assert remaining == 0


def test_pgcron_cleanup_all_queues_cleans_old_rows_only(
    pgcron_client,
    pgcron_postgres_container,
):
    queue = "cron-cleanup-live"
    base = datetime(2024, 5, 1, 12, 0, tzinfo=timezone.utc)

    pgcron_postgres_container.set_system_time(base)

    pgcron_client.create_queue(queue, storage_mode="partitioned")
    pgcron_client.conn.execute(
        "select absurd.set_queue_policy(%s, %s::jsonb)",
        (queue, '{"cleanup_ttl": "3600 seconds", "cleanup_limit": 100}'),
    )

    old_spawn = pgcron_client.spawn_task(queue, "cleanup-old", {"kind": "old"})
    old_claim = pgcron_client.claim_tasks(queue)[0]
    pgcron_client.complete_run(queue, old_claim["run_id"], {"ok": True})
    pgcron_client.emit_event(queue, "cleanup-old-event", {"kind": "old"})

    pgcron_postgres_container.advance_system_time(hours=3)

    live_spawn = pgcron_client.spawn_task(queue, "cleanup-live", {"kind": "live"})
    pgcron_client.emit_event(queue, "cleanup-live-event", {"kind": "live"})

    assert pgcron_client.count_tasks(queue) == 2
    assert pgcron_client.count_events(queue) == 2

    pgcron_client.conn.execute(
        """
        select *
        from absurd.enable_cron(%s, %s, %s, %s)
        """,
        (queue, "0 0 1 1 *", "1 second", "0 0 1 1 *"),
    )

    _wait_until(
        lambda: (
            pgcron_client.count_tasks(queue) == 1
            and pgcron_client.count_events(queue) == 1
        ),
        timeout=20.0,
        message="cleanup cron job did not remove expected old rows",
    )

    assert pgcron_client.get_task(queue, old_spawn.task_id) is None
    assert pgcron_client.get_task(queue, live_spawn.task_id) is not None

    cleanup_job_ran = pgcron_client.conn.execute(
        """
        select exists (
          select 1
          from cron.job_run_details
          where command like 'select * from absurd.cleanup_all_queues(%'
            and status = 'succeeded'
        )
        """
    ).fetchone()[0]
    assert cleanup_job_ran


def test_partitioned_idempotency_key_survives_week_boundary_with_real_time(
    pgcron_client,
    pgcron_postgres_container,
):
    queue = "cron-idempotency-live"
    base = datetime(2024, 5, 1, 9, 0, tzinfo=timezone.utc)

    pgcron_postgres_container.set_system_time(base)
    pgcron_client.create_queue(queue, storage_mode="partitioned")

    first = pgcron_client.spawn_task(
        queue,
        "idem",
        {"n": 1},
        {"idempotency_key": "cross-week-idem"},
    )

    pgcron_postgres_container.advance_system_time(days=8)

    second = pgcron_client.spawn_task(
        queue,
        "idem",
        {"n": 2},
        {"idempotency_key": "cross-week-idem"},
    )

    assert second.task_id == first.task_id
    assert second.run_id == first.run_id
    assert pgcron_client.count_tasks(queue) == 1

    row = pgcron_client.conn.execute(
        sql.SQL("select task_id from absurd.{tbl} where idempotency_key = %s").format(
            tbl=sql.Identifier(f"i_{queue}")
        ),
        ("cross-week-idem",),
    ).fetchone()
    assert row is not None
    assert str(row[0]) == str(first.task_id)


def test_partitioned_events_resume_across_week_boundary_with_real_time(
    pgcron_client,
    pgcron_postgres_container,
):
    queue = "cron-events-live"
    base = datetime(2024, 5, 1, 9, 0, tzinfo=timezone.utc)
    event_name = "cross-week-event"
    payload = {"ok": True}

    pgcron_postgres_container.set_system_time(base)
    pgcron_client.create_queue(queue, storage_mode="partitioned")

    spawned = pgcron_client.spawn_task(queue, "waiter", {"step": 1})
    claim = pgcron_client.claim_tasks(queue)[0]
    assert claim["run_id"] == spawned.run_id

    first_wait = pgcron_client.await_event(
        queue,
        spawned.task_id,
        spawned.run_id,
        "wait",
        event_name,
        None,
    )
    assert first_wait["should_suspend"] is True

    pgcron_postgres_container.advance_system_time(days=8)
    pgcron_client.emit_event(queue, event_name, payload)

    reclaimed = pgcron_client.claim_tasks(queue)[0]
    assert reclaimed["run_id"] == spawned.run_id

    resumed = pgcron_client.await_event(
        queue,
        spawned.task_id,
        spawned.run_id,
        "wait",
        event_name,
        None,
    )
    assert resumed["should_suspend"] is False
    assert resumed["payload"] == payload


def test_ensure_partitions_and_default_partition_fallback_with_real_time(
    pgcron_client,
    pgcron_postgres_container,
):
    queue = "cron-default-live"
    base = datetime(2024, 5, 1, 9, 0, tzinfo=timezone.utc)

    pgcron_postgres_container.set_system_time(base)
    pgcron_client.create_queue(queue, storage_mode="partitioned")

    # C1: ensure_partitions can precreate future week partitions.
    pgcron_client.conn.execute(
        "select absurd.set_queue_policy(%s, %s::jsonb)",
        (queue, '{"partition_lookahead": "70 days"}'),
    )
    pgcron_client.conn.execute("select absurd.ensure_partitions(%s)", (queue,))

    future_tag = pgcron_client.conn.execute(
        "select absurd.partition_week_tag(%s)",
        (base + timedelta(days=63),),
    ).fetchone()[0]

    for prefix in ["t", "r", "c", "w"]:
        rel = pgcron_client.conn.execute(
            "select to_regclass(%s)",
            (f"absurd.{prefix}_{queue}_{future_tag}",),
        ).fetchone()[0]
        assert rel is not None

    # C2: out-of-window rows should route into default partitions.
    pgcron_client.conn.execute(
        "select absurd.set_queue_policy(%s, %s::jsonb)",
        (queue, '{"partition_lookahead": "7 days", "partition_lookback": "1 day"}'),
    )
    pgcron_client.conn.execute("select absurd.ensure_partitions(%s)", (queue,))

    pgcron_postgres_container.advance_system_time(days=240)

    spawned = pgcron_client.spawn_task(queue, "default-route", {"kind": "late"})

    task_partition = pgcron_client.conn.execute(
        sql.SQL(
            "select tableoid::regclass::text from absurd.{tbl} where task_id = %s"
        ).format(tbl=sql.Identifier(f"t_{queue}")),
        (spawned.task_id,),
    ).fetchone()[0]
    run_partition = pgcron_client.conn.execute(
        sql.SQL(
            "select tableoid::regclass::text from absurd.{tbl} where run_id = %s"
        ).format(tbl=sql.Identifier(f"r_{queue}")),
        (spawned.run_id,),
    ).fetchone()[0]

    assert task_partition.replace('"', "") == f"absurd.t_{queue}_d"
    assert run_partition.replace('"', "") == f"absurd.r_{queue}_d"


def test_tasks_in_default_partition_are_claimable_and_completable(
    pgcron_client,
    pgcron_postgres_container,
):
    queue = "cron-default-taskflow"
    base = datetime(2024, 5, 1, 9, 0, tzinfo=timezone.utc)

    pgcron_postgres_container.set_system_time(base)
    pgcron_client.create_queue(queue, storage_mode="partitioned")

    # Keep partition window narrow so a far-future task lands in default.
    pgcron_client.conn.execute(
        "select absurd.set_queue_policy(%s, %s::jsonb)",
        (queue, '{"partition_lookahead": "7 days", "partition_lookback": "1 day"}'),
    )
    pgcron_client.conn.execute("select absurd.ensure_partitions(%s)", (queue,))

    pgcron_postgres_container.advance_system_time(days=240)

    spawned = pgcron_client.spawn_task(queue, "default-task", {"kind": "late"})

    task_partition = pgcron_client.conn.execute(
        sql.SQL(
            "select tableoid::regclass::text from absurd.{tbl} where task_id = %s"
        ).format(tbl=sql.Identifier(f"t_{queue}")),
        (spawned.task_id,),
    ).fetchone()[0]
    run_partition = pgcron_client.conn.execute(
        sql.SQL(
            "select tableoid::regclass::text from absurd.{tbl} where run_id = %s"
        ).format(tbl=sql.Identifier(f"r_{queue}")),
        (spawned.run_id,),
    ).fetchone()[0]

    assert task_partition.replace('"', "") == f"absurd.t_{queue}_d"
    assert run_partition.replace('"', "") == f"absurd.r_{queue}_d"

    claim = pgcron_client.claim_tasks(queue, worker="default-worker")
    assert claim
    assert claim[0]["run_id"] == spawned.run_id

    pgcron_client.complete_run(queue, spawned.run_id, {"ok": True})

    task_row = pgcron_client.get_task(queue, spawned.task_id)
    run_row = pgcron_client.get_run(queue, spawned.run_id)
    assert task_row is not None and task_row["state"] == "completed"
    assert run_row is not None and run_row["state"] == "completed"
