import uuid
from datetime import datetime, timedelta, timezone


def test_get_task_result_returns_none_for_unknown_task(client):
    queue = "task-result-missing"
    client.create_queue(queue)

    assert client.get_task_result(queue, uuid.uuid4()) is None


def test_get_task_result_tracks_lifecycle_states(client):
    queue = "task-result-lifecycle"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 12, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    spawn = client.spawn_task(queue, "task", {"value": 1})

    pending = client.get_task_result(queue, spawn.task_id)
    assert pending is not None
    assert pending["state"] == "pending"
    assert pending["result"] is None
    assert pending["failure_reason"] is None

    claim = client.claim_tasks(queue, worker="worker-a", claim_timeout=60)[0]
    running = client.get_task_result(queue, spawn.task_id)
    assert running is not None
    assert running["state"] == "running"
    assert running["result"] is None
    assert running["failure_reason"] is None

    wake_at = base + timedelta(minutes=5)
    client.schedule_run(queue, claim["run_id"], wake_at)

    sleeping = client.get_task_result(queue, spawn.task_id)
    assert sleeping is not None
    assert sleeping["state"] == "sleeping"
    assert sleeping["result"] is None
    assert sleeping["failure_reason"] is None

    client.set_fake_now(wake_at)
    resumed = client.claim_tasks(queue, worker="worker-b", claim_timeout=60)[0]
    client.complete_run(queue, resumed["run_id"], {"ok": True})

    completed = client.get_task_result(queue, spawn.task_id)
    assert completed is not None
    assert completed["state"] == "completed"
    assert completed["result"] == {"ok": True}
    assert completed["failure_reason"] is None


def test_get_task_result_returns_failure_reason_for_failed_task(client):
    queue = "task-result-failed"
    client.create_queue(queue)

    spawn = client.spawn_task(
        queue,
        "always-fail",
        {"value": 1},
        {"max_attempts": 1},
    )
    claim = client.claim_tasks(queue, worker="worker", claim_timeout=60)[0]

    reason = {"name": "Error", "message": "boom"}
    client.fail_run(queue, claim["run_id"], reason)

    failed = client.get_task_result(queue, spawn.task_id)
    assert failed is not None
    assert failed["state"] == "failed"
    assert failed["result"] is None
    assert failed["failure_reason"] == reason


def test_get_task_result_for_cancelled_task_has_no_payload(client):
    queue = "task-result-cancelled"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "cancel-me", {"value": 1})
    client.cancel_task(queue, spawn.task_id)

    cancelled = client.get_task_result(queue, spawn.task_id)
    assert cancelled is not None
    assert cancelled["state"] == "cancelled"
    assert cancelled["result"] is None
    assert cancelled["failure_reason"] is None
