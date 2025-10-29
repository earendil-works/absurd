from datetime import datetime, timezone


def test_await_and_emit_event_flow(client):
    queue = "events"
    client.create_queue(queue)

    now = datetime(2024, 2, 1, 12, 0, tzinfo=timezone.utc)
    client.set_fake_now(now)

    spawn = client.spawn_task(queue, "waiter", {"step": 1})

    claimed = client.claim_tasks(queue)[0]
    assert claimed["run_id"] == spawn.run_id

    event_name = "test-event"
    response = client.await_event(
        queue,
        spawn.task_id,
        claimed["run_id"],
        step_name="wait",
        event_name=event_name,
        timeout_seconds=60,
    )
    assert response["should_suspend"] is True
    assert response["payload"] is None

    sleeping_run = client.get_run(queue, claimed["run_id"])
    assert sleeping_run is not None
    assert sleeping_run["state"] == "sleeping"
    assert sleeping_run["wake_event"] == event_name

    payload = {"value": 42}
    client.emit_event(queue, event_name, payload)

    pending_run = client.get_run(queue, claimed["run_id"])
    assert pending_run is not None
    assert pending_run["state"] == "pending"

    replay_claim = client.claim_tasks(queue)[0]
    assert replay_claim["run_id"] == spawn.run_id

    resume = client.await_event(
        queue,
        spawn.task_id,
        replay_claim["run_id"],
        step_name="wait",
        event_name=event_name,
        timeout_seconds=60,
    )
    assert resume["should_suspend"] is False
    assert resume["payload"] == payload

    checkpoint = client.get_checkpoint(queue, spawn.task_id, "wait")
    assert checkpoint is not None
    assert checkpoint["state"] == payload
