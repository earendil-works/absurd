from datetime import datetime, timedelta, timezone

import pytest


def test_extend_claim_updates_running_lease(client):
    queue = "extend_claim_success"
    client.create_queue(queue)

    base = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    spawn = client.spawn_task(queue, "task", {"value": 1})
    claim = client.claim_tasks(queue, worker="worker-1", claim_timeout=60)[0]
    run_id = claim["run_id"]

    initial = client.get_run(queue, run_id)
    assert initial is not None
    assert initial["claim_expires_at"] == base + timedelta(seconds=60)

    client.set_fake_now(base + timedelta(seconds=10))
    client.extend_claim(queue, run_id, 30)

    updated = client.get_run(queue, run_id)
    assert updated is not None
    assert updated["claim_expires_at"] == base + timedelta(seconds=40)
    assert updated["task_id"] == spawn.task_id


def test_extend_claim_rejects_invalid_extend_by(client):
    queue = "extend_claim_invalid"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "task", {"value": 1})
    claim = client.claim_tasks(queue, worker="worker-1", claim_timeout=60)[0]

    for idx, invalid in enumerate([0, -1]):
        savepoint = f"extend_claim_invalid_{idx}"
        client.conn.execute(f"savepoint {savepoint}")
        with pytest.raises(Exception) as exc_info:
            client.extend_claim(queue, claim["run_id"], invalid)
        assert "extend_by must be > 0" in str(exc_info.value)
        client.conn.execute(f"rollback to savepoint {savepoint}")
        client.conn.execute(f"release savepoint {savepoint}")

    savepoint = "extend_claim_invalid_null"
    client.conn.execute(f"savepoint {savepoint}")
    with pytest.raises(Exception) as exc_info:
        client.conn.execute(
            "select absurd.extend_claim(%s, %s, %s)",
            (queue, claim["run_id"], None),
        )
    assert "extend_by must be > 0" in str(exc_info.value)
    client.conn.execute(f"rollback to savepoint {savepoint}")
    client.conn.execute(f"release savepoint {savepoint}")

    assert client.get_task(queue, spawn.task_id) is not None


def test_extend_claim_missing_run_errors(client):
    queue = "extend_claim_missing"
    client.create_queue(queue)

    missing_run = "019a32d3-8425-7ae2-a5af-2f17a6707666"

    with pytest.raises(Exception) as exc_info:
        client.extend_claim(queue, missing_run, 30)

    assert "not found" in str(exc_info.value).lower()


def test_extend_claim_non_running_run_errors(client):
    queue = "extend_claim_non_running"
    client.create_queue(queue)

    base = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    spawn = client.spawn_task(queue, "task", {"value": 1})
    claim = client.claim_tasks(queue, worker="worker-1", claim_timeout=60)[0]
    run_id = claim["run_id"]

    wake_at = base + timedelta(minutes=5)
    client.schedule_run(queue, run_id, wake_at)

    savepoint = "extend_claim_non_running"
    client.conn.execute(f"savepoint {savepoint}")
    with pytest.raises(Exception) as exc_info:
        client.extend_claim(queue, run_id, 30)

    assert "not currently running" in str(exc_info.value).lower()
    client.conn.execute(f"rollback to savepoint {savepoint}")
    client.conn.execute(f"release savepoint {savepoint}")
    assert client.get_task(queue, spawn.task_id)["state"] == "sleeping"
