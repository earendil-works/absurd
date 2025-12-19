from datetime import datetime, timedelta, timezone

from psycopg import sql


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


def test_await_event_timeout_does_not_recreate_wait(client):
    """
    When a timeout expires and the run resumes, await_event() should detect
    the expired timeout and return immediately without creating a new wait
    registration (which would cause an infinite loop).
    """
    queue = "timeout-no-loop"
    client.create_queue(queue)

    base = datetime(2024, 5, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    # Spawn and claim
    spawn = client.spawn_task(queue, "waiter", {"step": 1})
    claim = client.claim_tasks(queue)[0]
    run_id = claim["run_id"]

    event = "foo"
    # First await registers a wait with a 10s timeout and suspends
    resp1 = client.await_event(queue, spawn.task_id, run_id, "wait", event, 10)
    assert resp1["should_suspend"] is True
    assert resp1["payload"] is None

    # Wait table has one row
    wtbl = client.get_table("w", queue)
    wait_count1 = client.conn.execute(
        sql.SQL("select count(*) from absurd.{t}").format(t=wtbl)
    ).fetchone()[0]
    assert wait_count1 == 1

    # Advance time past timeout and resume
    client.set_fake_now(base + timedelta(seconds=15))
    resumed = client.claim_tasks(queue)[0]
    assert resumed["run_id"] == run_id

    # After resume, the expired wait should be cleaned up
    wait_count_after_resume = client.conn.execute(
        sql.SQL("select count(*) from absurd.{t}").format(t=wtbl)
    ).fetchone()[0]
    assert wait_count_after_resume == 0

    # Calling await_event again MUST NOT create a new wait; it should
    # return immediately as a timeout (no suspend).
    resp2 = client.await_event(queue, spawn.task_id, run_id, "wait", event, 10)
    assert resp2["should_suspend"] is False
    assert resp2["payload"] is None

    # Verify the run remains running; no re-sleep occurred
    run = client.get_run(queue, run_id)
    assert run is not None
    assert run["state"] == "running"

    # And the wait table remains empty (no new registration)
    wait_count_final = client.conn.execute(
        sql.SQL("select count(*) from absurd.{t}").format(t=wtbl)
    ).fetchone()[0]
    assert wait_count_final == 0


def test_event_emitted_before_await(client):
    """
    Verify that if an event is emitted BEFORE await_event is called,
    the task immediately receives the event without suspending.
    This tests one side of the race condition fix.
    """
    queue = "emit-first"
    client.create_queue(queue)

    now = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(now)

    # Emit event BEFORE spawning/awaiting
    event_name = "early-bird"
    payload = {"data": "already-here"}
    client.emit_event(queue, event_name, payload)

    # Now spawn a task that will await this event
    spawn = client.spawn_task(queue, "waiter", {"step": 1})
    claim = client.claim_tasks(queue)[0]

    # await_event should find the event immediately and NOT suspend
    response = client.await_event(
        queue,
        spawn.task_id,
        claim["run_id"],
        step_name="wait",
        event_name=event_name,
        timeout_seconds=None,
    )

    # Should NOT suspend - event already exists
    assert response["should_suspend"] is False
    assert response["payload"] == payload

    # Run should still be running (not sleeping)
    run = client.get_run(queue, claim["run_id"])
    assert run is not None
    assert run["state"] == "running"

    # Checkpoint should exist with the payload
    checkpoint = client.get_checkpoint(queue, spawn.task_id, "wait")
    assert checkpoint is not None
    assert checkpoint["state"] == payload
