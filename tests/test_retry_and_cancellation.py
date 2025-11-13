from datetime import datetime, timedelta, timezone

import pytest


def test_retry_flow_and_cancellation(client):
    queue = "retry"
    client.create_queue(queue)

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    client.set_fake_now(start)

    spawn = client.spawn_task(
        queue,
        "flaky",
        {"payload": 1},
        {
            "retry_strategy": {"kind": "fixed", "base_seconds": 30},
            "max_attempts": 4,
            "cancellation": {"max_duration": 90},
        },
    )

    first_claim = client.claim_tasks(queue, worker="worker-1")[0]
    assert first_claim["attempt"] == 1

    client.fail_run(queue, first_claim["run_id"], {"message": "boom"})

    runs_after_first_fail = client.get_runs(queue, spawn.task_id)
    assert len(runs_after_first_fail) == 2
    first_run, second_run = runs_after_first_fail
    assert first_run["state"] == "failed"
    assert second_run["state"] == "sleeping"
    assert second_run["attempt"] == 2
    assert second_run["available_at"] == start + timedelta(seconds=30)

    client.set_fake_now(start + timedelta(seconds=30))
    second_claim = client.claim_tasks(queue, worker="worker-1")[0]
    assert second_claim["attempt"] == 2

    client.set_fake_now(start + timedelta(seconds=91))
    client.fail_run(queue, second_claim["run_id"], {"message": "still boom"})

    task_row = client.get_task(queue, spawn.task_id)
    assert task_row is not None
    assert task_row["state"] == "cancelled"
    assert task_row["cancelled_at"] is not None

    runs_final = client.get_runs(queue, spawn.task_id)
    # No new attempt should have been scheduled after cancellation.
    assert len(runs_final) == 2


def test_manual_cancel_pending_task(client):
    """Test explicitly cancelling a task that hasn't been claimed yet."""
    queue = "manual_cancel"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "pending-task", {"data": "test"})

    # Task should be in pending state
    task = client.get_task(queue, spawn.task_id)
    assert task["state"] == "pending"

    # Cancel the task
    client.cancel_task(queue, spawn.task_id)

    # Task should now be cancelled
    task = client.get_task(queue, spawn.task_id)
    assert task["state"] == "cancelled"
    assert task["cancelled_at"] is not None

    # Task should not be claimable
    claims = client.claim_tasks(queue, worker="worker-1")
    assert len(claims) == 0


def test_manual_cancel_running_task(client):
    """Test explicitly cancelling a task that is currently running."""
    queue = "cancel_running"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "running-task", {"data": "test"})

    # Claim the task
    claim = client.claim_tasks(queue, worker="worker-1")[0]
    assert claim["task_id"] == spawn.task_id

    # Task should be running
    task = client.get_task(queue, spawn.task_id)
    assert task["state"] == "running"

    # Cancel the task
    client.cancel_task(queue, spawn.task_id)

    # Task should now be cancelled
    task = client.get_task(queue, spawn.task_id)
    assert task["state"] == "cancelled"
    assert task["cancelled_at"] is not None


def test_cancel_prevents_checkpoint_writes(client):
    """Test that checkpoint writes fail after task is cancelled."""
    queue = "cancel_checkpoint"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "checkpoint-task", {"data": "test"})
    claim = client.claim_tasks(queue, worker="worker-1")[0]

    # Cancel the task
    client.cancel_task(queue, spawn.task_id)

    # Try to write a checkpoint - should fail with AB001 error
    with pytest.raises(Exception) as exc_info:
        client.set_task_checkpoint_state(
            queue, spawn.task_id, "step1", {"result": "value"}, claim["run_id"]
        )

    assert "AB001" in str(exc_info.value) or "cancelled" in str(exc_info.value).lower()


def test_cancel_prevents_await_event(client):
    """Test that await_event fails after task is cancelled."""
    queue = "cancel_event"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "event-task", {"data": "test"})
    claim = client.claim_tasks(queue, worker="worker-1")[0]

    # Cancel the task
    client.cancel_task(queue, spawn.task_id)

    # Try to await event - should fail with AB001 error
    with pytest.raises(Exception) as exc_info:
        client.await_event(
            queue, spawn.task_id, claim["run_id"], "wait-step", "some-event"
        )

    assert "AB001" in str(exc_info.value) or "cancelled" in str(exc_info.value).lower()


def test_cancel_prevents_extend_claim(client):
    """Test that extending claim fails after task is cancelled."""
    queue = "cancel_extend"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "extend-task", {"data": "test"})
    claim = client.claim_tasks(queue, worker="worker-1")[0]

    # Cancel the task
    client.cancel_task(queue, spawn.task_id)

    # Try to extend claim - should fail with AB001 error
    with pytest.raises(Exception) as exc_info:
        client.extend_claim(queue, claim["run_id"], 30)

    assert "AB001" in str(exc_info.value) or "cancelled" in str(exc_info.value).lower()


def test_cancel_idempotent(client):
    """Test that cancelling an already cancelled task is idempotent."""
    queue = "cancel_idempotent"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "idempotent-task", {"data": "test"})

    # Cancel the task twice
    client.cancel_task(queue, spawn.task_id)
    first_cancelled_at = client.get_task(queue, spawn.task_id)["cancelled_at"]

    client.cancel_task(queue, spawn.task_id)
    second_cancelled_at = client.get_task(queue, spawn.task_id)["cancelled_at"]

    # cancelled_at should remain the same
    assert first_cancelled_at == second_cancelled_at


def test_cancel_completed_task_noop(client):
    """Test that cancelling a completed task is a no-op."""
    queue = "cancel_completed"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "completed-task", {"data": "test"})
    claim = client.claim_tasks(queue, worker="worker-1")[0]

    # Complete the task
    client.complete_run(queue, claim["run_id"], {"result": "done"})

    task = client.get_task(queue, spawn.task_id)
    assert task["state"] == "completed"

    # Try to cancel - should be a no-op
    client.cancel_task(queue, spawn.task_id)

    task = client.get_task(queue, spawn.task_id)
    assert task["state"] == "completed"
    assert task["cancelled_at"] is None


def test_cancel_failed_task_noop(client):
    """Test that cancelling a failed task is a no-op."""
    queue = "cancel_failed"
    client.create_queue(queue)

    spawn = client.spawn_task(
        queue, "failed-task", {"data": "test"}, {"max_attempts": 1}
    )
    claim = client.claim_tasks(queue, worker="worker-1")[0]

    # Fail the task
    client.fail_run(queue, claim["run_id"], {"error": "boom"})

    task = client.get_task(queue, spawn.task_id)
    assert task["state"] == "failed"

    # Try to cancel - should be a no-op
    client.cancel_task(queue, spawn.task_id)

    task = client.get_task(queue, spawn.task_id)
    assert task["state"] == "failed"
    assert task["cancelled_at"] is None


def test_cancel_sleeping_task(client):
    """Test cancelling a task that is sleeping (awaiting event)."""
    queue = "cancel_sleeping"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "sleeping-task", {"data": "test"})
    claim = client.claim_tasks(queue, worker="worker-1")[0]

    # Make task sleep by awaiting an event
    result = client.await_event(
        queue,
        spawn.task_id,
        claim["run_id"],
        "wait-step",
        "some-event",
        timeout_seconds=300,
    )
    assert result["should_suspend"] is True

    # Task should be sleeping
    task = client.get_task(queue, spawn.task_id)
    assert task["state"] == "sleeping"

    # Cancel the task
    client.cancel_task(queue, spawn.task_id)

    # Task should be cancelled
    task = client.get_task(queue, spawn.task_id)
    assert task["state"] == "cancelled"
    assert task["cancelled_at"] is not None

    # Run should be cancelled as well
    run = client.get_run(queue, claim["run_id"])
    assert run["state"] == "cancelled"


def test_cancel_nonexistent_task(client):
    """Test that cancelling a non-existent task raises an error."""
    queue = "cancel_nonexistent"
    client.create_queue(queue)

    fake_task_id = "019a32d3-8425-7ae2-a5af-2f17a6707666"

    with pytest.raises(Exception) as exc_info:
        client.cancel_task(queue, fake_task_id)

    assert "not found" in str(exc_info.value).lower()
