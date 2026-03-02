from datetime import datetime, timedelta, timezone
from psycopg.sql import SQL


def test_parse_cron_field_wildcard(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('*', 0, 59)"
    ).fetchone()[0]
    assert result == list(range(0, 60))


def test_parse_cron_field_exact(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('5', 0, 59)"
    ).fetchone()[0]
    assert result == [5]


def test_parse_cron_field_step(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('*/15', 0, 59)"
    ).fetchone()[0]
    assert result == [0, 15, 30, 45]


def test_parse_cron_field_range(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('9-12', 0, 23)"
    ).fetchone()[0]
    assert result == [9, 10, 11, 12]


def test_parse_cron_field_range_step(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('9-17/2', 0, 23)"
    ).fetchone()[0]
    assert result == [9, 11, 13, 15, 17]


def test_parse_cron_field_list(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('1,3,5', 0, 6)"
    ).fetchone()[0]
    assert result == [1, 3, 5]


def test_parse_cron_field_list_with_range(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('1,3-5,7', 0, 10)"
    ).fetchone()[0]
    assert result == [1, 3, 4, 5, 7]


def test_parse_cron_field_day_names(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('MON-FRI', 0, 6)"
    ).fetchone()[0]
    assert result == [1, 2, 3, 4, 5]


def test_parse_cron_field_month_names(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('JAN,JUN,DEC', 1, 12)"
    ).fetchone()[0]
    assert result == [1, 6, 12]


def test_next_cron_time_every_minute(client):
    """'* * * * *' from 09:30:45 -> 09:31:00."""
    result = client.conn.execute(
        "select absurd.next_cron_time('* * * * *', '2024-06-15 09:30:45+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 15, 9, 31, tzinfo=timezone.utc)


def test_next_cron_time_specific_time(client):
    """'30 9 * * *' from 09:00 -> same day 09:30; from 09:31 -> next day 09:30."""
    result = client.conn.execute(
        "select absurd.next_cron_time('30 9 * * *', '2024-06-15 09:00:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 15, 9, 30, tzinfo=timezone.utc)

    result = client.conn.execute(
        "select absurd.next_cron_time('30 9 * * *', '2024-06-15 09:31:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 16, 9, 30, tzinfo=timezone.utc)


def test_next_cron_time_step(client):
    """'*/15 * * * *' from 09:07 -> 09:15."""
    result = client.conn.execute(
        "select absurd.next_cron_time('*/15 * * * *', '2024-06-15 09:07:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 15, 9, 15, tzinfo=timezone.utc)


def test_next_cron_time_day_of_week(client):
    """'0 9 * * 1' (Monday 9am). 2024-06-15 is Saturday -> Monday 2024-06-17."""
    result = client.conn.execute(
        "select absurd.next_cron_time('0 9 * * 1', '2024-06-15 10:00:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 17, 9, 0, tzinfo=timezone.utc)


def test_next_cron_time_shorthand_daily(client):
    """'@daily' from 00:01 -> next day 00:00."""
    result = client.conn.execute(
        "select absurd.next_cron_time('@daily', '2024-06-15 00:01:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 16, 0, 0, tzinfo=timezone.utc)


def test_next_cron_time_shorthand_hourly(client):
    """'@hourly' from 09:15 -> 10:00."""
    result = client.conn.execute(
        "select absurd.next_cron_time('@hourly', '2024-06-15 09:15:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 15, 10, 0, tzinfo=timezone.utc)


def test_next_cron_time_every_interval(client):
    """'@every 300' from 09:10:00 -> 09:15:00."""
    result = client.conn.execute(
        "select absurd.next_cron_time('@every 300', '2024-06-15 09:10:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 6, 15, 9, 15, tzinfo=timezone.utc)


def test_next_cron_time_month_boundary(client):
    """'0 0 1 * *' (first of month) from Jan 2 -> Feb 1."""
    result = client.conn.execute(
        "select absurd.next_cron_time('0 0 1 * *', '2024-01-02 00:00:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2024, 2, 1, 0, 0, tzinfo=timezone.utc)


def test_next_cron_time_year_boundary(client):
    """'0 0 1 1 *' (Jan 1 midnight) from Dec 2024 -> Jan 1 2025."""
    result = client.conn.execute(
        "select absurd.next_cron_time('0 0 1 1 *', '2024-12-15 00:00:00+00'::timestamptz)"
    ).fetchone()[0]
    assert result == datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_schedule_table_created_with_queue(client):
    """create_queue should also create the s_{queue} table."""
    queue = "sched-tbl"
    client.create_queue(queue)
    result = client.conn.execute(
        "select count(*) from information_schema.tables "
        "where table_schema = 'absurd' and table_name = %s",
        (f"s_{queue}",)
    ).fetchone()[0]
    assert result == 1


def test_schedule_table_dropped_with_queue(client):
    """drop_queue should also drop the s_{queue} table."""
    queue = "sched-drop"
    client.create_queue(queue)
    client.drop_queue(queue)
    result = client.conn.execute(
        "select count(*) from information_schema.tables "
        "where table_schema = 'absurd' and table_name = %s",
        (f"s_{queue}",)
    ).fetchone()[0]
    assert result == 0


def test_create_and_get_schedule(client):
    queue = "sched-crud"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    result = client.create_schedule(queue, "my-job", "process-data", "*/5 * * * *")
    assert result["schedule_name"] == "my-job"
    assert result["next_run_at"] is not None

    sched = client.get_schedule(queue, "my-job")
    assert sched["task_name"] == "process-data"
    assert sched["schedule_expr"] == "*/5 * * * *"
    assert sched["enabled"] is True
    assert sched["catchup_policy"] == "skip"
    assert sched["next_run_at"] == datetime(2024, 7, 1, 10, 5, tzinfo=timezone.utc)


def test_create_schedule_with_options(client):
    queue = "sched-opts"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(
        queue, "daily-report", "gen-report", "@daily",
        options={
            "params": {"format": "pdf"},
            "headers": {"trace-id": "abc"},
            "max_attempts": 3,
            "catchup_policy": "all",
        },
    )
    sched = client.get_schedule(queue, "daily-report")
    assert sched["params"] == {"format": "pdf"}
    assert sched["headers"] == {"trace-id": "abc"}
    assert sched["max_attempts"] == 3
    assert sched["catchup_policy"] == "all"


def test_list_schedules(client):
    queue = "sched-list"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "job-a", "task-a", "@hourly")
    client.create_schedule(queue, "job-b", "task-b", "@daily")

    schedules = client.list_schedules(queue)
    names = [s["schedule_name"] for s in schedules]
    assert "job-a" in names
    assert "job-b" in names


def test_delete_schedule(client):
    queue = "sched-del"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "temp-job", "task-x", "@hourly")
    assert client.get_schedule(queue, "temp-job") is not None

    client.delete_schedule(queue, "temp-job")
    assert client.get_schedule(queue, "temp-job") is None


def test_update_schedule_expression(client):
    queue = "sched-upd"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "upd-job", "task-y", "*/5 * * * *")
    sched_before = client.get_schedule(queue, "upd-job")
    assert sched_before["next_run_at"] == datetime(2024, 7, 1, 10, 5, tzinfo=timezone.utc)

    client.update_schedule(queue, "upd-job", {"schedule_expr": "@hourly"})
    sched_after = client.get_schedule(queue, "upd-job")
    assert sched_after["schedule_expr"] == "@hourly"
    assert sched_after["next_run_at"] == datetime(2024, 7, 1, 11, 0, tzinfo=timezone.utc)


def test_update_schedule_disable(client):
    queue = "sched-dis"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "dis-job", "task-z", "@hourly")
    client.update_schedule(queue, "dis-job", {"enabled": False})

    sched = client.get_schedule(queue, "dis-job")
    assert sched["enabled"] is False


def test_tick_spawns_task_when_due(client):
    """tick_schedules spawns a task when next_run_at has passed."""
    queue = "tick-basic"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "tick-job", "my-task", "*/5 * * * *",
                           params={"key": "value"})

    # Advance past next_run_at (10:05)
    client.set_fake_now(base + timedelta(minutes=6))

    # tick_schedules should spawn a task
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))

    # Verify a task was spawned
    tasks = client.conn.execute(
        SQL("select task_name, params, idempotency_key from absurd.{table}").format(
            table=client.get_table("t", queue)
        )
    ).fetchall()
    assert len(tasks) == 1
    assert tasks[0][0] == "my-task"
    assert tasks[0][1] == {"key": "value"}
    assert "sched:tick-job:" in tasks[0][2]

    # Verify next_run_at advanced
    sched = client.get_schedule(queue, "tick-job")
    assert sched["next_run_at"] == datetime(2024, 7, 1, 10, 10, tzinfo=timezone.utc)
    assert sched["last_triggered_at"] == datetime(2024, 7, 1, 10, 5, tzinfo=timezone.utc)


def test_tick_skips_disabled_schedule(client):
    """tick_schedules ignores disabled schedules."""
    queue = "tick-disabled"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "off-job", "task-off", "*/5 * * * *")
    client.update_schedule(queue, "off-job", {"enabled": False})

    client.set_fake_now(base + timedelta(minutes=10))
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))

    assert client.count_tasks(queue) == 0


def test_tick_skip_catchup_policy(client):
    """With catchup_policy='skip', only one task is spawned even if many runs missed."""
    queue = "tick-skip"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "skip-job", "task-skip", "*/5 * * * *")

    # Skip ahead 30 minutes - 6 runs missed
    client.set_fake_now(base + timedelta(minutes=30))
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))

    assert client.count_tasks(queue) == 1

    sched = client.get_schedule(queue, "skip-job")
    # next_run_at should be in the future (next */5 after 10:30)
    assert sched["next_run_at"] == datetime(2024, 7, 1, 10, 35, tzinfo=timezone.utc)


def test_tick_all_catchup_policy_drip(client):
    """With catchup_policy='all', spawns up to 5 per tick."""
    queue = "tick-all"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "all-job", "task-all", "*/5 * * * *",
                           options={"catchup_policy": "all"})

    # Skip ahead 60 minutes - 12 runs missed
    client.set_fake_now(base + timedelta(minutes=60))

    # First tick: spawns 5
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))
    assert client.count_tasks(queue) == 5

    # Second tick: spawns 5 more
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))
    assert client.count_tasks(queue) == 10

    # Third tick: spawns remaining 2
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))
    assert client.count_tasks(queue) == 12

    # Schedule should now be current
    sched = client.get_schedule(queue, "all-job")
    assert sched["next_run_at"] == datetime(2024, 7, 1, 11, 5, tzinfo=timezone.utc)


def test_tick_idempotent(client):
    """Calling tick_schedules twice at the same time doesn't double-spawn."""
    queue = "tick-idempotent"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "idem-job", "task-idem", "*/5 * * * *")

    client.set_fake_now(base + timedelta(minutes=6))
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))

    assert client.count_tasks(queue) == 1


def test_tick_every_interval(client):
    """@every N schedule works with tick."""
    queue = "tick-every"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "every-job", "task-every", "@every 300")

    # next_run_at should be base + 300s = 10:05
    sched = client.get_schedule(queue, "every-job")
    assert sched["next_run_at"] == datetime(2024, 7, 1, 10, 5, tzinfo=timezone.utc)

    client.set_fake_now(base + timedelta(minutes=6))
    client.conn.execute("select absurd.tick_schedules(%s)", (queue,))

    assert client.count_tasks(queue) == 1
    sched = client.get_schedule(queue, "every-job")
    assert sched["next_run_at"] == datetime(2024, 7, 1, 10, 10, tzinfo=timezone.utc)


def test_claim_task_ticks_schedules(client):
    """claim_task automatically calls tick_schedules, spawning and returning scheduled tasks."""
    queue = "claim-tick"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.create_schedule(queue, "auto-job", "auto-task", "*/5 * * * *",
                           params={"source": "schedule"})

    # Advance past first run time
    client.set_fake_now(base + timedelta(minutes=6))

    # claim_task should tick schedules internally, spawning the task,
    # then claim and return it
    claimed = client.claim_tasks(queue, worker="sched-worker")
    assert len(claimed) == 1
    assert claimed[0]["task_name"] == "auto-task"
    assert claimed[0]["params"] == {"source": "schedule"}


def test_claim_task_ticks_then_claims_mixed(client):
    """claim_task handles both scheduled and manually spawned tasks."""
    queue = "claim-mixed"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    # Manual task
    client.spawn_task(queue, "manual-task", {"source": "manual"})

    # Schedule
    client.create_schedule(queue, "sched-mix", "sched-task", "*/5 * * * *",
                           params={"source": "schedule"})

    # Advance past schedule
    client.set_fake_now(base + timedelta(minutes=6))

    claimed = client.claim_tasks(queue, worker="w", qty=5)
    task_names = [c["task_name"] for c in claimed]
    assert "manual-task" in task_names
    assert "sched-task" in task_names


def test_full_schedule_lifecycle(client):
    """Full lifecycle: create schedule, tick via claim, complete task, tick again."""
    queue = "lifecycle-sched"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    # Create schedule
    client.create_schedule(queue, "heartbeat", "ping", "@every 300",
                           params={"target": "db"})

    sched = client.get_schedule(queue, "heartbeat")
    assert sched["next_run_at"] == base + timedelta(seconds=300)

    # Advance past first run
    client.set_fake_now(base + timedelta(minutes=6))

    # claim_task ticks and returns the scheduled task
    claimed = client.claim_tasks(queue)
    assert len(claimed) == 1
    assert claimed[0]["task_name"] == "ping"
    assert claimed[0]["params"] == {"target": "db"}

    # Complete the task
    client.complete_run(queue, claimed[0]["run_id"], {"status": "pong"})

    # Verify schedule advanced
    sched = client.get_schedule(queue, "heartbeat")
    assert sched["next_run_at"] == base + timedelta(seconds=600)

    # Advance past second run
    client.set_fake_now(base + timedelta(minutes=11))

    # Second tick via claim
    claimed2 = client.claim_tasks(queue)
    assert len(claimed2) == 1
    assert claimed2[0]["task_name"] == "ping"

    # Complete second task
    client.complete_run(queue, claimed2[0]["run_id"])

    # Delete schedule
    client.delete_schedule(queue, "heartbeat")
    assert client.get_schedule(queue, "heartbeat") is None

    # No more tasks spawned
    client.set_fake_now(base + timedelta(minutes=20))
    claimed3 = client.claim_tasks(queue)
    assert len(claimed3) == 0
