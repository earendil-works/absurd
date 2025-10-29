from datetime import datetime, timedelta, timezone


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
