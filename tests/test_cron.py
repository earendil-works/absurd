from datetime import datetime, timedelta, timezone

import pytest


def _install_mock_cron(conn):
    conn.execute("create schema if not exists cron")
    conn.execute("drop table if exists cron.job")
    conn.execute(
        """
        create table cron.job (
          jobid bigint generated always as identity primary key,
          jobname text not null,
          schedule text not null,
          command text not null
        )
        """
    )

    conn.execute(
        """
        create or replace function cron.schedule(
          p_jobname text,
          p_schedule text,
          p_command text
        )
          returns bigint
          language plpgsql
        as $$
        declare
          v_jobid bigint;
        begin
          insert into cron.job (jobname, schedule, command)
          values (p_jobname, p_schedule, p_command)
          returning jobid into v_jobid;
          return v_jobid;
        end;
        $$;
        """
    )

    conn.execute(
        """
        create or replace function cron.unschedule(
          p_jobid bigint
        )
          returns boolean
          language plpgsql
        as $$
        begin
          delete from cron.job where jobid = p_jobid;
          return found;
        end;
        $$;
        """
    )


def test_cleanup_all_queues_runs_batch_for_all_queues(client):
    q1 = "cleanup-all-1"
    q2 = "cleanup-all-2"
    client.create_queue(q1)
    client.create_queue(q2)

    base = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    for queue in [q1, q2]:
        spawned = client.spawn_task(queue, "cleanup-task", {"q": queue})
        claim = client.claim_tasks(queue)[0]
        client.complete_run(queue, claim["run_id"], {"ok": True})
        client.emit_event(queue, f"event:{spawned.task_id}", {"queue": queue})
        client.conn.execute(
            "select absurd.set_queue_policy(%s, %s::jsonb)",
            (queue, '{"cleanup_ttl": "3600 seconds", "cleanup_limit": 10}'),
        )

    client.set_fake_now(base + timedelta(days=2))

    rows = client.conn.execute(
        """
        select queue_name, tasks_deleted, events_deleted
        from absurd.cleanup_all_queues()
        order by queue_name
        """,
    ).fetchall()

    assert rows == [(q1, 1, 1), (q2, 1, 1)]


def test_enable_cron_requires_cron_schema(client):
    if client.conn.execute("select to_regclass('cron.job')").fetchone()[0] is not None:
        pytest.skip("cron.job already exists in this environment")

    with pytest.raises(Exception):
        client.conn.execute("select * from absurd.enable_cron()")


def test_enable_cron_schedules_partition_cleanup_and_detach_planner_jobs(client):
    queue = "cron-partitioned"
    client.create_queue(queue, storage_mode="partitioned")
    _install_mock_cron(client.conn)

    rows = client.conn.execute(
        """
        select job_name, job_id
        from absurd.enable_cron(
          %s,
          %s,
          %s
        )
        order by job_name
        """,
        (
            queue,
            "*/15 * * * *",
            "7 * * * *",
        ),
    ).fetchall()

    assert len(rows) == 3
    assert rows[0][0].startswith("absurd_cleanup_")
    assert rows[1][0].startswith("absurd_detach_plan_")
    assert rows[2][0].startswith("absurd_partitions_")

    jobs = client.conn.execute(
        "select jobname, schedule, command from cron.job order by jobname"
    ).fetchall()
    assert len(jobs) == 3

    cleanup_job = jobs[0]
    detach_plan_job = jobs[1]
    partition_job = jobs[2]
    assert cleanup_job[1] == "7 * * * *"
    assert "absurd.cleanup_all_queues('cron-partitioned')" in cleanup_job[2]
    assert detach_plan_job[1] == "29 * * * *"
    assert "absurd.schedule_detach_jobs('cron-partitioned')" in detach_plan_job[2]
    assert partition_job[1] == "*/15 * * * *"
    assert "absurd.ensure_partitions('cron-partitioned')" in partition_job[2]

    # Re-enable with different schedules; old jobs should be replaced, not duplicated.
    client.conn.execute(
        """
        select *
        from absurd.enable_cron(
          %s,
          %s,
          %s
        )
        """,
        (
            queue,
            "0 * * * *",
            "0 3 * * *",
        ),
    )

    job_count = client.conn.execute("select count(*) from cron.job").fetchone()
    assert job_count is not None
    assert job_count[0] == 3


def test_schedule_detach_jobs_schedules_detach_and_drop_jobs(client):
    queue = "cron-detach-jobs"
    base = datetime(2024, 4, 1, 12, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)
    client.create_queue(queue, storage_mode="partitioned")
    client.conn.execute(
        "select absurd.set_queue_policy(%s, %s::jsonb)",
        (queue, '{"detach_mode": "empty", "detach_min_age": "0 days"}'),
    )

    _install_mock_cron(client.conn)

    client.set_fake_now(base + timedelta(days=90))
    scope = client.conn.execute(
        "select substr(md5(%s), 1, 12)",
        (queue,),
    ).fetchone()[0]

    scheduled = client.conn.execute(
        """
        select job_name, job_kind, partition_table
        from absurd.schedule_detach_jobs(%s)
        order by job_name
        """,
        (queue,),
    ).fetchall()

    assert scheduled
    kinds = {row[1] for row in scheduled}
    assert kinds == {"detach", "drop"}

    jobs = client.conn.execute(
        """
        select jobname, schedule, command
        from cron.job
        where jobname like %s
           or jobname like %s
        order by jobname
        """,
        (f"absurd_detach_run_{scope}_%", f"absurd_drop_run_{scope}_%"),
    ).fetchall()

    assert jobs
    for _, schedule, _ in jobs:
        assert schedule == "* * * * *"

    detach_jobs = [
        job for job in jobs if job[0].startswith(f"absurd_detach_run_{scope}_")
    ]
    drop_jobs = [job for job in jobs if job[0].startswith(f"absurd_drop_run_{scope}_")]
    assert detach_jobs
    assert drop_jobs
    assert "detach partition" in detach_jobs[0][2].lower()
    assert "concurrently" not in detach_jobs[0][2].lower()
    assert "cron.unschedule" not in detach_jobs[0][2].lower()
    assert "absurd.drop_detached_partition" in drop_jobs[0][2]

    # Idempotent: second pass should create no new jobs.
    scheduled_again = client.conn.execute(
        "select * from absurd.schedule_detach_jobs(%s)",
        (queue,),
    ).fetchall()
    assert scheduled_again == []


def test_disable_cron_unschedules_queue_jobs(client):
    queue = "cron-disable-queue"
    base = datetime(2024, 4, 1, 12, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)
    client.create_queue(queue, storage_mode="partitioned")
    _install_mock_cron(client.conn)

    client.conn.execute(
        "select absurd.set_queue_policy(%s, %s::jsonb)",
        (queue, '{"detach_mode": "empty", "detach_min_age": "0 days"}'),
    )

    client.conn.execute(
        """
        select *
        from absurd.enable_cron(%s, %s, %s)
        """,
        (queue, "*/10 * * * *", "7 * * * *"),
    )

    # Seed per-partition detach/drop jobs for this queue scope.
    client.set_fake_now(base + timedelta(days=90))
    client.conn.execute(
        "select * from absurd.schedule_detach_jobs(%s)",
        (queue,),
    )

    removed = client.conn.execute(
        """
        select job_name, job_id
        from absurd.disable_cron(%s)
        order by job_name
        """,
        (queue,),
    ).fetchall()

    assert len(removed) >= 3
    removed_names = [row[0] for row in removed]
    assert any(name.startswith("absurd_cleanup_") for name in removed_names)
    assert any(name.startswith("absurd_detach_plan_") for name in removed_names)
    assert any(name.startswith("absurd_partitions_") for name in removed_names)
    assert any(name.startswith("absurd_detach_run_") for name in removed_names)
    assert any(name.startswith("absurd_drop_run_") for name in removed_names)

    # Idempotent: disabling again removes nothing.
    removed_again = client.conn.execute(
        "select * from absurd.disable_cron(%s)",
        (queue,),
    ).fetchall()
    assert removed_again == []


def test_disable_cron_without_queue_only_removes_global_jobs(client):
    queue = "cron-disable-global"
    client.create_queue(queue, storage_mode="partitioned")
    _install_mock_cron(client.conn)

    # Queue-specific jobs.
    client.conn.execute(
        """
        select *
        from absurd.enable_cron(%s, %s, %s)
        """,
        (queue, "*/20 * * * *", "11 * * * *"),
    )

    # Global jobs.
    client.conn.execute("select * from absurd.enable_cron()")

    removed = client.conn.execute(
        """
        select job_name
        from absurd.disable_cron()
        order by job_name
        """
    ).fetchall()

    assert [row[0] for row in removed] == [
        "absurd_cleanup_all",
        "absurd_detach_plan_all",
        "absurd_partitions_all",
    ]

    remaining = client.conn.execute(
        "select jobname from cron.job order by jobname"
    ).fetchall()
    assert len(remaining) == 3
    assert remaining[0][0].startswith("absurd_cleanup_")
    assert remaining[1][0].startswith("absurd_detach_plan_")
    assert remaining[2][0].startswith("absurd_partitions_")


def test_drop_queue_unschedules_queue_scoped_cron_jobs(client):
    queue = "cron-drop-queue"
    client.create_queue(queue, storage_mode="partitioned")
    _install_mock_cron(client.conn)

    client.conn.execute(
        """
        select *
        from absurd.enable_cron(%s, %s, %s)
        """,
        (queue, "*/20 * * * *", "11 * * * *"),
    )

    scope = client.conn.execute(
        "select substr(md5(%s), 1, 12)",
        (queue,),
    ).fetchone()[0]

    # Seed queue-scoped detach/drop run jobs too.
    client.conn.execute(
        """
        insert into cron.job (jobname, schedule, command)
        values
          (%s, '* * * * *', 'select 1'),
          (%s, '* * * * *', 'select 1')
        """,
        (
            f"absurd_detach_run_{scope}_demo",
            f"absurd_drop_run_{scope}_demo",
        ),
    )

    client.drop_queue(queue)

    remaining = client.conn.execute(
        """
        select jobname
        from cron.job
        where jobname like %s
        order by jobname
        """,
        (f"absurd%{scope}%",),
    ).fetchall()
    assert remaining == []
