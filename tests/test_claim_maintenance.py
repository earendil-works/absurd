from datetime import datetime, timedelta, timezone


def test_claim_time_cancellation_preserves_terminal_run_history(client):
    queue = "claim_cancel_integrity"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    spawn = client.spawn_task(
        queue,
        "task",
        {"value": 1},
        {
            "retry_strategy": {"kind": "fixed", "base_seconds": 0},
            "max_attempts": 3,
            "cancellation": {"max_duration": 1},
        },
    )

    claim1 = client.claim_tasks(queue, worker="worker-1", claim_timeout=60)[0]
    run_id_1 = claim1["run_id"]

    client.fail_run(queue, run_id_1, {"message": "retry"})

    runs_after_fail = client.get_runs(queue, spawn.task_id)
    assert len(runs_after_fail) == 2
    run_id_2 = runs_after_fail[1]["run_id"]
    assert runs_after_fail[0]["state"] == "failed"
    assert runs_after_fail[1]["state"] == "pending"

    client.set_fake_now(base + timedelta(seconds=2))
    _ = client.claim_tasks(queue, worker="worker-2", qty=1)

    final_runs = client.get_runs(queue, spawn.task_id)
    assert len(final_runs) == 2

    run1 = next(run for run in final_runs if run["run_id"] == run_id_1)
    run2 = next(run for run in final_runs if run["run_id"] == run_id_2)

    assert run1["state"] == "failed"
    assert run2["state"] == "cancelled"

    task = client.get_task(queue, spawn.task_id)
    assert task is not None
    assert task["state"] == "cancelled"


def test_claim_expired_sweep_is_bounded_by_qty(client):
    queue = "claim_expired_bounded"
    client.create_queue(queue)

    base = datetime(2024, 7, 2, 9, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    for idx in range(5):
        client.spawn_task(queue, f"task-{idx}", {"idx": idx})

    claimed = client.claim_tasks(queue, worker="worker-a", claim_timeout=1, qty=5)
    assert len(claimed) == 5
    original_run_ids = {row["run_id"] for row in claimed}

    client.set_fake_now(base + timedelta(seconds=10))

    _ = client.claim_tasks(queue, worker="sweeper", claim_timeout=30, qty=1)

    states_after_first = [client.get_run(queue, run_id)["state"] for run_id in original_run_ids]
    assert states_after_first.count("failed") == 1

    _ = client.claim_tasks(queue, worker="sweeper", claim_timeout=30, qty=2)

    states_after_second = [client.get_run(queue, run_id)["state"] for run_id in original_run_ids]
    assert states_after_second.count("failed") == 3

