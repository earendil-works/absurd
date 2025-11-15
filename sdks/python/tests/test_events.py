"""Tests for event system"""

from datetime import datetime, timezone, timedelta
from psycopg import sql

from absurd_sdk import Absurd, TimeoutError


def _set_fake_now(conn, fake_time):
    """Set the fake timestamp for testing"""
    if fake_time is None:
        conn.execute("set absurd.fake_now = default")
    else:
        # SET doesn't support parameterized queries, need to format as string
        from psycopg import sql
        conn.execute(
            sql.SQL("set absurd.fake_now = '{}'").format(sql.SQL(fake_time.isoformat()))
        )


def _get_task(conn, queue, task_id):
    """Get task details"""
    query = sql.SQL(
        "select state, completed_payload from absurd.{table} where task_id = %s"
    ).format(table=sql.Identifier(f"t_{queue}"))
    result = conn.execute(query, (task_id,)).fetchone()
    if not result:
        return None
    return {"state": result[0], "completed_payload": result[1]}


def _get_run(conn, queue, run_id):
    """Get run details"""
    query = sql.SQL(
        "select state, wake_event, nullif(available_at, 'infinity'::timestamptz) as available_at from absurd.{table} where run_id = %s"
    ).format(table=sql.Identifier(f"r_{queue}"))
    result = conn.execute(query, (run_id,)).fetchone()
    if not result:
        return None
    return {"state": result[0], "wake_event": result[1], "available_at": result[2]}


def test_await_and_emit_event_flow(conn, queue_name):
    """Test the await and emit event flow"""
    queue = queue_name("event_flow")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    event_name = f"test_event_{queue}"

    @client.register_task("waiter")
    def waiter_task(params, ctx):
        payload = ctx.await_event(event_name, timeout=60)
        return {"received": payload}

    spawned = client.spawn("waiter", {"step": 1})

    # Start processing, task should suspend waiting for event
    client.work_batch("worker1", 60, 1)

    sleeping_run = _get_run(conn, queue, spawned["run_id"])
    assert sleeping_run["state"] == "sleeping"
    assert sleeping_run["wake_event"] == event_name

    # Emit event
    payload = {"value": 42}
    client.emit_event(event_name, payload)

    # Task should now be pending
    pending_run = _get_run(conn, queue, spawned["run_id"])
    assert pending_run["state"] == "pending"

    # Resume and complete
    client.work_batch("worker1", 60, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {"received": payload}


def test_event_emitted_before_await_is_cached(conn, queue_name):
    """Test that events emitted before await are cached and retrieved"""
    queue = queue_name("cached_event")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    event_name = f"cached_event_{queue}"
    payload = {"data": "pre-emitted"}

    # Emit event before task even exists
    client.emit_event(event_name, payload)

    @client.register_task("late-waiter")
    def late_waiter_task(params, ctx):
        received = ctx.await_event(event_name)
        return {"received": received}

    spawned = client.spawn("late-waiter", None)

    # Should complete immediately with cached event
    client.work_batch("worker1", 60, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {"received": payload}


def test_await_event_with_timeout_expires_and_wakes_task(conn, queue_name):
    """Test that awaitEvent with timeout expires and wakes the task"""
    queue = queue_name("timeout_event")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    event_name = f"timeout_event_{queue}"
    base_time = datetime(2024, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
    timeout_seconds = 600

    _set_fake_now(conn, base_time)

    @client.register_task("timeout-waiter")
    def timeout_waiter_task(params, ctx):
        try:
            payload = ctx.await_event(event_name, timeout=timeout_seconds)
            return {"timedOut": False, "result": payload}
        except TimeoutError:
            return {"timedOut": True, "result": None}

    spawned = client.spawn("timeout-waiter", None)
    client.work_batch("worker1", 120, 1)

    # Check wait registration exists
    wait_count_query = sql.SQL("SELECT COUNT(*) FROM absurd.{table}").format(
        table=sql.Identifier(f"w_{queue}")
    )
    wait_count = conn.execute(wait_count_query).fetchone()[0]
    assert wait_count == 1

    sleeping_run = _get_run(conn, queue, spawned["run_id"])
    assert sleeping_run["state"] == "sleeping"
    assert sleeping_run["wake_event"] == event_name

    expected_wake = base_time + timedelta(seconds=timeout_seconds)
    assert sleeping_run["available_at"] == expected_wake

    _set_fake_now(conn, expected_wake + timedelta(seconds=1))
    client.work_batch("worker1", 120, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {"timedOut": True, "result": None}

    wait_count_after = conn.execute(wait_count_query).fetchone()[0]
    assert wait_count_after == 0


def test_multiple_tasks_can_await_same_event(conn, queue_name):
    """Test that multiple tasks can await the same event"""
    queue = queue_name("broadcast_event")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    event_name = f"broadcast_event_{queue}"

    @client.register_task("multi-waiter")
    def multi_waiter_task(params, ctx):
        payload = ctx.await_event(event_name)
        return {"taskNum": params["taskNum"], "received": payload}

    tasks = [
        client.spawn("multi-waiter", {"taskNum": 1}),
        client.spawn("multi-waiter", {"taskNum": 2}),
        client.spawn("multi-waiter", {"taskNum": 3}),
    ]

    # All tasks suspend waiting for event
    client.work_batch("worker1", 60, 10)

    for spawned in tasks:
        task = _get_task(conn, queue, spawned["task_id"])
        assert task["state"] == "sleeping"

    # Emit event once
    payload = {"data": "broadcast"}
    client.emit_event(event_name, payload)

    # All tasks should resume and complete
    client.work_batch("worker1", 60, 10)

    for i, spawned in enumerate(tasks):
        task = _get_task(conn, queue, spawned["task_id"])
        assert task["state"] == "completed"
        assert task["completed_payload"] == {"taskNum": i + 1, "received": payload}


def test_await_event_timeout_does_not_recreate_wait_on_resume(conn, queue_name):
    """Test that awaitEvent timeout does not recreate wait registration on resume"""
    queue = queue_name("timeout_no_loop")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    event_name = f"timeout_no_loop_{queue}"
    base_time = datetime(2024, 5, 2, 11, 0, 0, tzinfo=timezone.utc)
    _set_fake_now(conn, base_time)

    @client.register_task("timeout-no-loop")
    def timeout_no_loop_task(params, ctx):
        try:
            ctx.await_event(event_name, step_name="wait", timeout=10)
            return {"stage": "unexpected"}
        except TimeoutError:
            payload = ctx.await_event(event_name, step_name="wait", timeout=10)
            return {"stage": "resumed", "payload": payload}

    spawned = client.spawn("timeout-no-loop", None)
    client.work_batch("worker-timeout", 60, 1)

    wait_count_query = sql.SQL("SELECT COUNT(*) FROM absurd.{table}").format(
        table=sql.Identifier(f"w_{queue}")
    )
    wait_count = conn.execute(wait_count_query).fetchone()[0]
    assert wait_count == 1

    _set_fake_now(conn, base_time + timedelta(seconds=15))
    client.work_batch("worker-timeout", 60, 1)

    wait_count_after = conn.execute(wait_count_query).fetchone()[0]
    assert wait_count_after == 0

    run = _get_run(conn, queue, spawned["run_id"])
    assert run["state"] == "completed"

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {"stage": "resumed", "payload": None}
