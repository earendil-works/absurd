import json
from datetime import datetime, timezone

import pytest
from psycopg import sql

from absurd_sdk import Absurd, SuspendTask, _create_task_context


def _fetch_checkpoint(conn, queue, task_id, checkpoint_name):
    query = sql.SQL(
        "select state from absurd.{table} where task_id = %s and checkpoint_name = %s"
    ).format(table=sql.Identifier(f"c_{queue}"))
    result = conn.execute(query, (task_id, checkpoint_name)).fetchone()
    return result[0] if result else None


def _fetch_run(conn, queue, run_id):
    query = sql.SQL(
        "select state, nullif(available_at, 'infinity'::timestamptz) as available_at, wake_event "
        "from absurd.{table} where run_id = %s"
    ).format(table=sql.Identifier(f"r_{queue}"))
    return conn.execute(query, (run_id,)).fetchone()


def test_step_persists_and_reuses_checkpoints(conn, queue_name):
    queue = queue_name("steps")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    client.spawn("profile-step", {"user_id": 7}, queue=queue)
    task = client.claim_tasks(worker_id="worker")[0]

    ctx = _create_task_context(task["task_id"], conn, queue, task, claim_timeout=120)

    calls = []

    def compute():
        calls.append("called")
        return {"user_id": task["params"]["user_id"]}

    first = ctx.step("load-profile", compute)
    assert first == {"user_id": 7}
    assert calls == ["called"]
    assert _fetch_checkpoint(conn, queue, task["task_id"], "load-profile") == {
        "user_id": 7
    }

    # A brand new context observes the persisted checkpoint instead of recomputing.
    resumed = _create_task_context(
        task["task_id"], conn, queue, task, claim_timeout=120
    )
    assert resumed.step("load-profile", compute) == {"user_id": 7}
    assert calls == ["called"]

    conn.execute(
        "SELECT absurd.complete_run(%s, %s, %s)",
        (queue, task["run_id"], json.dumps({"status": "done"})),
    )


def test_sleep_for_schedules_future_run(conn, queue_name):
    queue = queue_name("sleep")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    client.spawn("snoozer", {}, queue=queue)
    task = client.claim_tasks(worker_id="worker")[0]
    ctx = _create_task_context(task["task_id"], conn, queue, task, claim_timeout=90)

    before = datetime.now(timezone.utc)
    with pytest.raises(SuspendTask):
        ctx.sleep_for("retry", duration=2)

    checkpoint = _fetch_checkpoint(conn, queue, task["task_id"], "retry")
    assert checkpoint is not None
    scheduled_wake = datetime.fromisoformat(checkpoint)
    assert scheduled_wake > before

    run_state, available_at, wake_event = _fetch_run(conn, queue, task["run_id"])
    assert run_state == "sleeping"
    assert wake_event is None
    assert available_at is not None
    assert available_at >= scheduled_wake


def test_await_event_flow(conn, queue_name):
    queue = queue_name("events")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    client.spawn("waiter", {}, queue=queue)
    task = client.claim_tasks(worker_id="worker")[0]
    ctx = _create_task_context(task["task_id"], conn, queue, task, claim_timeout=120)

    event_name = "user.ready"

    with pytest.raises(SuspendTask):
        ctx.await_event(event_name, step_name="wait")

    run_state, available_at, wake_event = _fetch_run(conn, queue, task["run_id"])
    assert run_state == "sleeping"
    assert wake_event == event_name
    assert available_at is None

    payload = {"status": "ok"}
    client.emit_event(event_name, payload, queue_name=queue)

    replay = client.claim_tasks(worker_id="worker")[0]
    resumed_ctx = _create_task_context(
        replay["task_id"], conn, queue, replay, claim_timeout=120
    )

    delivered = resumed_ctx.await_event(event_name, step_name="wait")
    assert delivered == payload
    assert _fetch_checkpoint(conn, queue, replay["task_id"], "wait") == payload

    conn.execute(
        "SELECT absurd.complete_run(%s, %s, %s)",
        (queue, replay["run_id"], json.dumps({"status": "done"})),
    )
