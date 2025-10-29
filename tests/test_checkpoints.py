from datetime import datetime, timedelta, timezone

from psycopg.types.json import Jsonb


def test_set_checkpoint_extends_claim_lease(client):
    queue = "checkpoint-lease"
    client.create_queue(queue)

    base = datetime(2024, 5, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    # Spawn and claim a task with a 60-second timeout
    spawn = client.spawn_task(queue, "multi-step", {"value": 1})
    claim = client.claim_tasks(queue, worker="worker-1", claim_timeout=60)[0]
    run_id = claim["run_id"]
    task_id = claim["task_id"]

    # Verify initial claim expiry
    run_before = client.get_run(queue, run_id)
    assert run_before is not None
    assert run_before["claimed_by"] == "worker-1"
    initial_expires_at = run_before["claim_expires_at"]
    expected_initial_expires = base + timedelta(seconds=60)
    assert initial_expires_at == expected_initial_expires

    # Advance time by 30 seconds (halfway through the lease)
    client.set_fake_now(base + timedelta(seconds=30))

    # Set a checkpoint with extend_claim_by=60 (simulating SDK step execution)
    client.conn.execute(
        "select absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s)",
        (queue, task_id, "step-1", Jsonb({"result": "done"}), run_id, 60),
    )

    # Verify the checkpoint was persisted
    checkpoint = client.get_checkpoint(queue, task_id, "step-1")
    assert checkpoint is not None
    assert checkpoint["state"] == {"result": "done"}
    assert checkpoint["owner_run_id"] == run_id

    # Test get_task_checkpoint_state function directly
    result = client.conn.execute(
        """
        select checkpoint_name, state, status, owner_run_id, updated_at
        from absurd.get_task_checkpoint_state(%s, %s, %s)
        """,
        (queue, task_id, "step-1"),
    )
    checkpoint_row = result.fetchone()
    assert checkpoint_row is not None
    assert checkpoint_row[0] == "step-1"  # checkpoint_name
    assert checkpoint_row[1] == {"result": "done"}  # state
    assert checkpoint_row[2] == "committed"  # status
    assert checkpoint_row[3] == run_id  # owner_run_id

    # Verify the lease was extended
    run_after = client.get_run(queue, run_id)
    assert run_after is not None
    new_expires_at = run_after["claim_expires_at"]
    expected_new_expires = base + timedelta(seconds=30) + timedelta(seconds=60)
    assert new_expires_at == expected_new_expires
    assert new_expires_at > initial_expires_at

    # Set another checkpoint 20 seconds later to verify continuous extension
    client.set_fake_now(base + timedelta(seconds=50))
    client.conn.execute(
        "select absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s)",
        (queue, task_id, "step-2", Jsonb({"result": "also-done"}), run_id, 60),
    )

    run_final = client.get_run(queue, run_id)
    assert run_final is not None
    final_expires_at = run_final["claim_expires_at"]
    expected_final_expires = base + timedelta(seconds=50) + timedelta(seconds=60)
    assert final_expires_at == expected_final_expires
    assert final_expires_at > new_expires_at


def test_checkpoint_preloading_survives_retry(client):
    queue = "checkpoint-retry"
    client.create_queue(queue)

    base = datetime(2024, 5, 2, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    # Spawn a task with retry configuration
    spawn = client.spawn_task(
        queue,
        "flaky-multi-step",
        {"value": 1},
        {
            "retry_strategy": {"kind": "fixed", "base_seconds": 10},
            "max_attempts": 3,
        },
    )
    task_id = spawn.task_id

    # Claim and execute first attempt
    claim1 = client.claim_tasks(queue, worker="worker-1")[0]
    run_id_1 = claim1["run_id"]
    assert claim1["attempt"] == 1

    # Simulate step execution by persisting checkpoints
    client.conn.execute(
        "select absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s)",
        (queue, task_id, "step-1", Jsonb({"data": "first"}), run_id_1, 60),
    )
    client.conn.execute(
        "select absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s)",
        (queue, task_id, "step-2", Jsonb({"data": "second"}), run_id_1, 60),
    )
    client.conn.execute(
        "select absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s)",
        (queue, task_id, "step-3", Jsonb({"data": "third"}), run_id_1, 60),
    )

    # Verify checkpoints were created
    checkpoint_1 = client.get_checkpoint(queue, task_id, "step-1")
    checkpoint_2 = client.get_checkpoint(queue, task_id, "step-2")
    checkpoint_3 = client.get_checkpoint(queue, task_id, "step-3")
    assert checkpoint_1 is not None
    assert checkpoint_2 is not None
    assert checkpoint_3 is not None
    assert checkpoint_1["owner_run_id"] == run_id_1
    assert checkpoint_2["owner_run_id"] == run_id_1
    assert checkpoint_3["owner_run_id"] == run_id_1

    # Fail the first attempt
    client.fail_run(queue, run_id_1, {"message": "transient failure"})

    # Advance time and claim the retry
    client.set_fake_now(base + timedelta(seconds=10))
    claim2 = client.claim_tasks(queue, worker="worker-2")[0]
    run_id_2 = claim2["run_id"]
    assert claim2["attempt"] == 2
    assert run_id_2 != run_id_1

    # Simulate TaskContext.create by calling get_task_checkpoint_states
    # This is what the SDK does to preload all checkpoints
    result = client.conn.execute(
        """
        select checkpoint_name, state, status, owner_run_id, updated_at
        from absurd.get_task_checkpoint_states(%s, %s, %s)
        """,
        (queue, task_id, run_id_2),
    )
    checkpoints = result.fetchall()

    # Verify all three checkpoints from attempt 1 are visible to attempt 2
    assert len(checkpoints) == 3
    checkpoint_names = {row[0] for row in checkpoints}
    assert checkpoint_names == {"step-1", "step-2", "step-3"}

    # Verify checkpoint data is intact
    checkpoint_map = {row[0]: row[1] for row in checkpoints}
    assert checkpoint_map["step-1"] == {"data": "first"}
    assert checkpoint_map["step-2"] == {"data": "second"}
    assert checkpoint_map["step-3"] == {"data": "third"}

    # Verify the checkpoints still reference the original run
    for row in checkpoints:
        assert row[3] == run_id_1  # owner_run_id should still be from attempt 1

    # Now simulate the retry completing step-4 and failing again
    client.conn.execute(
        "select absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s)",
        (queue, task_id, "step-4", Jsonb({"data": "fourth"}), run_id_2, 60),
    )

    # Fail the second attempt
    client.fail_run(queue, run_id_2, {"message": "another failure"})

    # Claim the third attempt
    client.set_fake_now(base + timedelta(seconds=20))
    claim3 = client.claim_tasks(queue, worker="worker-3")[0]
    run_id_3 = claim3["run_id"]
    assert claim3["attempt"] == 3

    # Verify attempt 3 sees checkpoints from both attempt 1 and attempt 2
    result = client.conn.execute(
        """
        select checkpoint_name, state, status, owner_run_id, updated_at
        from absurd.get_task_checkpoint_states(%s, %s, %s)
        """,
        (queue, task_id, run_id_3),
    )
    checkpoints_attempt_3 = result.fetchall()

    assert len(checkpoints_attempt_3) == 4
    checkpoint_names_3 = {row[0] for row in checkpoints_attempt_3}
    assert checkpoint_names_3 == {"step-1", "step-2", "step-3", "step-4"}


def test_sleep_until_checkpoint_prevents_infinite_reschedule(client):
    queue = "sleep-checkpoint"
    client.create_queue(queue)

    base = datetime(2024, 5, 3, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    # Spawn and claim a task
    spawn = client.spawn_task(queue, "sleepy-task", {"value": 1})
    claim = client.claim_tasks(queue, worker="worker-1", claim_timeout=120)[0]
    run_id = claim["run_id"]
    task_id = claim["task_id"]

    # Simulate sleepUntil behavior: persist ISO timestamp checkpoint
    wake_time = base + timedelta(minutes=5)
    wake_time_iso = wake_time.isoformat()
    step_name = "sleep-step"

    # First execution: check if checkpoint exists (it doesn't)
    checkpoint_before = client.get_checkpoint(queue, task_id, step_name)
    assert checkpoint_before is None

    # Store the wake time as a checkpoint (simulating SDK's sleepUntil)
    client.conn.execute(
        "select absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s)",
        (queue, task_id, step_name, Jsonb(wake_time_iso), run_id, 120),
    )

    # Verify the checkpoint was stored
    checkpoint_after = client.get_checkpoint(queue, task_id, step_name)
    assert checkpoint_after is not None
    assert checkpoint_after["state"] == wake_time_iso
    assert checkpoint_after["owner_run_id"] == run_id

    # Schedule the run to wake at the specified time
    client.schedule_run(queue, run_id, wake_time)

    # Verify the run is sleeping
    run_sleeping = client.get_run(queue, run_id)
    assert run_sleeping is not None
    assert run_sleeping["state"] == "sleeping"
    assert run_sleeping["available_at"] == wake_time

    # Advance time to the wake time
    client.set_fake_now(wake_time)

    # Task wakes and gets claimed again
    claim2 = client.claim_tasks(queue, worker="worker-1", claim_timeout=120)[0]
    run_id_2 = claim2["run_id"]
    assert run_id_2 == run_id  # Same run, now awake
    assert claim2["attempt"] == 1

    # Simulate the SDK checking the checkpoint on resume
    # The SDK reads the checkpoint and sees the wake time has passed
    checkpoint_on_resume = client.get_checkpoint(queue, task_id, step_name)
    assert checkpoint_on_resume is not None
    assert checkpoint_on_resume["state"] == wake_time_iso

    # Parse the stored wake time
    stored_wake_time = datetime.fromisoformat(checkpoint_on_resume["state"])

    # Verify current time is past the wake time (so we don't re-schedule)
    current_time = wake_time  # This is what client.set_fake_now set
    assert current_time >= stored_wake_time

    # The SDK would now continue execution instead of calling schedule_run again
    # Verify no new schedule_run call would change the state
    run_final = client.get_run(queue, run_id)
    assert run_final is not None
    assert run_final["state"] == "running"

    # Complete the task to verify normal flow
    client.complete_run(queue, run_id, {"result": "success"})

    task_final = client.get_task(queue, task_id)
    assert task_final is not None
    assert task_final["state"] == "completed"


def test_sleep_until_checkpoint_with_multiple_sleep_steps(client):
    queue = "multi-sleep"
    client.create_queue(queue)

    base = datetime(2024, 5, 4, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    spawn = client.spawn_task(queue, "multi-sleep-task", {"value": 1})
    claim = client.claim_tasks(queue, worker="worker-1", claim_timeout=120)[0]
    run_id = claim["run_id"]
    task_id = claim["task_id"]

    # First sleep step
    first_wake = base + timedelta(minutes=5)
    first_wake_iso = first_wake.isoformat()
    client.conn.execute(
        "select absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s)",
        (queue, task_id, "sleep-1", Jsonb(first_wake_iso), run_id, 120),
    )
    client.schedule_run(queue, run_id, first_wake)

    # Advance to first wake time
    client.set_fake_now(first_wake)
    claim2 = client.claim_tasks(queue, worker="worker-1", claim_timeout=120)[0]
    assert claim2["run_id"] == run_id

    # Verify first checkpoint exists
    checkpoint1 = client.get_checkpoint(queue, task_id, "sleep-1")
    assert checkpoint1 is not None
    assert checkpoint1["state"] == first_wake_iso

    # Second sleep step (using a different step name to avoid collision)
    second_wake = first_wake + timedelta(minutes=10)
    second_wake_iso = second_wake.isoformat()
    client.conn.execute(
        "select absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s)",
        (queue, task_id, "sleep-2", Jsonb(second_wake_iso), run_id, 120),
    )
    client.schedule_run(queue, run_id, second_wake)

    # Advance to second wake time
    client.set_fake_now(second_wake)
    claim3 = client.claim_tasks(queue, worker="worker-1", claim_timeout=120)[0]
    assert claim3["run_id"] == run_id

    # Verify both checkpoints exist with correct values
    checkpoint1_final = client.get_checkpoint(queue, task_id, "sleep-1")
    checkpoint2_final = client.get_checkpoint(queue, task_id, "sleep-2")
    assert checkpoint1_final is not None
    assert checkpoint2_final is not None
    assert checkpoint1_final["state"] == first_wake_iso
    assert checkpoint2_final["state"] == second_wake_iso

    # Complete the task
    client.complete_run(queue, run_id, {"result": "success"})
    task = client.get_task(queue, task_id)
    assert task["state"] == "completed"
