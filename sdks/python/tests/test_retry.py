"""Tests for retry strategies and cancellation"""

import pytest
from datetime import datetime, timezone, timedelta
from psycopg import sql

from absurd_sdk import Absurd


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
        "select state, attempts, completed_payload, cancelled_at from absurd.{table} where task_id = %s"
    ).format(table=sql.Identifier(f"t_{queue}"))
    result = conn.execute(query, (task_id,)).fetchone()
    if not result:
        return None
    return {
        "state": result[0],
        "attempts": result[1],
        "completed_payload": result[2],
        "cancelled_at": result[3],
    }


def _get_run(conn, queue, run_id):
    """Get run details"""
    query = sql.SQL(
        "select state, failure_reason from absurd.{table} where run_id = %s"
    ).format(table=sql.Identifier(f"r_{queue}"))
    result = conn.execute(query, (run_id,)).fetchone()
    if not result:
        return None
    return {"state": result[0], "failure_reason": result[1]}


def test_fail_run_without_strategy_requeues_immediately(conn, queue_name):
    """Test that failed runs without strategy retry immediately"""
    queue = queue_name("retry")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    attempts = []

    @client.register_task("no-strategy", default_max_attempts=3)
    def no_strategy_task(params, ctx):
        attempts.append(len(attempts) + 1)
        if len(attempts) < 2:
            raise Exception("boom")
        return {"attempts": len(attempts)}

    spawned = client.spawn("no-strategy", {"payload": 1})

    # First attempt fails
    client.work_batch("worker1", 60, 1)
    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "pending"
    assert task["attempts"] == 2

    # Second attempt succeeds
    client.work_batch("worker1", 60, 1)
    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["attempts"] == 2
    assert task["completed_payload"] == {"attempts": 2}


def test_exponential_backoff_retry_strategy(conn, queue_name):
    """Test exponential backoff retry strategy"""
    queue = queue_name("exp_retry")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    base_time = datetime(2024, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
    _set_fake_now(conn, base_time)

    attempts = []

    @client.register_task("exp-backoff")
    def exp_backoff_task(params, ctx):
        attempts.append(len(attempts) + 1)
        if len(attempts) < 3:
            raise Exception(f"fail-{len(attempts)}")
        return {"success": True}

    spawned = client.spawn(
        "exp-backoff",
        None,
        max_attempts=3,
        retry_strategy={"kind": "exponential", "base_seconds": 40, "factor": 2},
    )

    # First attempt - fails, schedules retry with 40s backoff
    client.work_batch("worker1", 60, 1)
    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "sleeping"
    assert task["attempts"] == 2

    # Advance time past first backoff (40 seconds)
    _set_fake_now(conn, base_time + timedelta(seconds=40))

    # Second attempt - fails again with 80s backoff (40 * 2)
    client.work_batch("worker1", 60, 1)
    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "sleeping"
    assert task["attempts"] == 3

    # Advance time past second backoff (80 seconds from second failure)
    _set_fake_now(conn, base_time + timedelta(seconds=40 + 80))

    # Third attempt - succeeds
    client.work_batch("worker1", 60, 1)
    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["attempts"] == 3
    assert task["completed_payload"] == {"success": True}


def test_fixed_backoff_retry_strategy(conn, queue_name):
    """Test fixed backoff retry strategy"""
    queue = queue_name("fixed_retry")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    base_time = datetime(2024, 5, 1, 11, 0, 0, tzinfo=timezone.utc)
    _set_fake_now(conn, base_time)

    attempts = []

    @client.register_task("fixed-backoff")
    def fixed_backoff_task(params, ctx):
        attempts.append(len(attempts) + 1)
        if len(attempts) < 2:
            raise Exception("first-fail")
        return {"attempts": len(attempts)}

    spawned = client.spawn(
        "fixed-backoff",
        None,
        max_attempts=2,
        retry_strategy={"kind": "fixed", "base_seconds": 10},
    )

    # First attempt fails
    client.work_batch("worker1", 60, 1)
    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "sleeping"

    # Advance time past fixed backoff (10 seconds)
    _set_fake_now(conn, base_time + timedelta(seconds=10))

    # Second attempt succeeds
    client.work_batch("worker1", 60, 1)
    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["attempts"] == 2


def test_task_fails_permanently_after_max_attempts(conn, queue_name):
    """Test that task fails after exhausting max attempts"""
    queue = queue_name("max_fail")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("always-fail", default_max_attempts=2)
    def always_fail_task(params, ctx):
        raise Exception("always fails")

    spawned = client.spawn("always-fail", None)

    # Attempt 1
    client.work_batch("worker1", 60, 1)
    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "pending"

    # Attempt 2 (final)
    client.work_batch("worker1", 60, 1)
    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "failed"
    assert task["attempts"] == 2


def test_cancellation_by_max_duration(conn, queue_name):
    """Test task cancellation by max duration"""
    queue = queue_name("cancel_duration")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    base_time = datetime(2024, 5, 1, 9, 0, 0, tzinfo=timezone.utc)
    _set_fake_now(conn, base_time)

    @client.register_task("duration-cancel")
    def duration_cancel_task(params, ctx):
        raise Exception("always fails")

    spawned = client.spawn(
        "duration-cancel",
        None,
        max_attempts=4,
        retry_strategy={"kind": "fixed", "base_seconds": 30},
        cancellation={"max_duration": 90},
    )

    client.work_batch("worker1", 60, 1)

    _set_fake_now(conn, base_time + timedelta(seconds=91))
    client.work_batch("worker1", 60, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "cancelled"
    assert task["cancelled_at"] is not None


def test_cancellation_by_max_delay(conn, queue_name):
    """Test task cancellation by max delay"""
    queue = queue_name("cancel_delay")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    base_time = datetime(2024, 5, 1, 8, 0, 0, tzinfo=timezone.utc)
    _set_fake_now(conn, base_time)

    @client.register_task("delay-cancel")
    def delay_cancel_task(params, ctx):
        return {"done": True}

    spawned = client.spawn("delay-cancel", None, cancellation={"max_delay": 60})

    _set_fake_now(conn, base_time + timedelta(seconds=61))
    client.work_batch("worker1", 60, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "cancelled"
    assert task["cancelled_at"] is not None


def test_manual_cancel_pending_task(conn, queue_name):
    """Test manually cancelling a pending task"""
    queue = queue_name("manual_cancel_pending")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("pending-cancel")
    def pending_cancel_task(params, ctx):
        return {"ok": True}

    spawned = client.spawn("pending-cancel", {"data": 1})
    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "pending"

    client.cancel_task(spawned["task_id"])

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "cancelled"
    assert task["cancelled_at"] is not None

    claims = client.claim_tasks(worker_id="worker-1", claim_timeout=60)
    assert len(claims) == 0


def test_manual_cancel_running_task(conn, queue_name):
    """Test manually cancelling a running task"""
    queue = queue_name("manual_cancel_running")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("running-cancel")
    def running_cancel_task(params, ctx):
        return {"ok": True}

    spawned = client.spawn("running-cancel", {"data": 1})
    claims = client.claim_tasks(worker_id="worker-1", claim_timeout=60)
    assert len(claims) == 1
    assert claims[0]["task_id"] == spawned["task_id"]

    client.cancel_task(spawned["task_id"])

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "cancelled"
    assert task["cancelled_at"] is not None


def test_cancel_blocks_checkpoint_writes(conn, queue_name):
    """Test that cancel blocks checkpoint writes"""
    queue = queue_name("checkpoint_cancel")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("checkpoint-cancel")
    def checkpoint_cancel_task(params, ctx):
        return {"ok": True}

    spawned = client.spawn("checkpoint-cancel", {"data": 1})
    claims = client.claim_tasks(worker_id="worker-1", claim_timeout=60)

    client.cancel_task(spawned["task_id"])

    # Try to write checkpoint - should get AB001 error (or DatabaseError with the right message)
    with pytest.raises(Exception) as exc_info:
        conn.execute(
            "SELECT absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s)",
            (
                queue,
                spawned["task_id"],
                "step-1",
                '{"result": "value"}',
                claims[0]["run_id"],
                60,
            ),
        )
    # The error should indicate the task was cancelled
    assert "cancelled" in str(exc_info.value).lower() or (
        hasattr(exc_info.value, "pgcode") and exc_info.value.pgcode == "AB001"
    )


def test_cancel_is_idempotent(conn, queue_name):
    """Test that cancelling is idempotent"""
    queue = queue_name("idempotent_cancel")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("idempotent-cancel")
    def idempotent_cancel_task(params, ctx):
        return {"ok": True}

    spawned = client.spawn("idempotent-cancel", {"data": 1})
    client.cancel_task(spawned["task_id"])
    first = _get_task(conn, queue, spawned["task_id"])
    assert first["cancelled_at"] is not None

    client.cancel_task(spawned["task_id"])
    second = _get_task(conn, queue, spawned["task_id"])
    assert second["cancelled_at"] == first["cancelled_at"]


def test_cancelling_completed_task_is_noop(conn, queue_name):
    """Test that cancelling a completed task is a no-op"""
    queue = queue_name("complete_cancel")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("complete-cancel")
    def complete_cancel_task(params, ctx):
        return {"status": "done"}

    spawned = client.spawn("complete-cancel", {"data": 1})
    client.work_batch("worker-1", 60, 1)

    client.cancel_task(spawned["task_id"])

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["cancelled_at"] is None


def test_cancelling_failed_task_is_noop(conn, queue_name):
    """Test that cancelling a failed task is a no-op"""
    queue = queue_name("failed_cancel")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("failed-cancel", default_max_attempts=1)
    def failed_cancel_task(params, ctx):
        raise Exception("boom")

    spawned = client.spawn("failed-cancel", {"data": 1})
    client.work_batch("worker-1", 60, 1)

    client.cancel_task(spawned["task_id"])

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "failed"
    assert task["cancelled_at"] is None


def test_cancel_non_existent_task_errors(conn, queue_name):
    """Test that cancelling a non-existent task raises an error"""
    queue = queue_name("cancel_missing")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    with pytest.raises(Exception, match="not found"):
        client.cancel_task("019a32d3-8425-7ae2-a5af-2f17a6707666")
