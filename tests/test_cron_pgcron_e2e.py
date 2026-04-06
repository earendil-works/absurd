import time


def _wait_until(predicate, *, timeout=30.0, interval=0.25, message="condition not met"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(message)


def test_pgcron_time_jump_executes_detach_and_drop_jobs(pgcron_client, pgcron_postgres_container):
    queue = "cron-live-detach"

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
            where command ilike 'alter table absurd.% detach partition absurd.% concurrently%'
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

    detach_row = pgcron_client.conn.execute(
        """
        select status, return_message
        from cron.job_run_details
        where command ilike 'alter table absurd.% detach partition absurd.% concurrently%'
        order by runid desc
        limit 1
        """,
    ).fetchone()
    assert detach_row is not None
    assert detach_row[0] == "failed"
    assert "cannot be executed from a function" in (detach_row[1] or "")

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

    # Since DETACH ... CONCURRENTLY fails under pg_cron execution context,
    # the source partition is still present.
    assert (
        pgcron_client.conn.execute(
            "select to_regclass(%s)",
            (f"absurd.{partition_table}",),
        ).fetchone()[0]
        is not None
    )


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
        lambda: pgcron_client.conn.execute(
            "select count(*) from cron.job where jobname like %s",
            (f"absurd%{scope}%",),
        ).fetchone()[0]
        >= 3,
        timeout=10.0,
        message="expected queue-scoped cron jobs were not created",
    )

    pgcron_client.drop_queue(queue)

    remaining = pgcron_client.conn.execute(
        "select count(*) from cron.job where jobname like %s",
        (f"absurd%{scope}%",),
    ).fetchone()[0]
    assert remaining == 0
