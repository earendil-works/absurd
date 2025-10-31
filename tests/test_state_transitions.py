from datetime import datetime, timedelta, timezone

from psycopg.sql import SQL


def test_schedule_run_moves_task_between_running_and_sleeping(client):
    queue = "schedule-state"
    client.create_queue(queue)

    base = datetime(2024, 4, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    spawn = client.spawn_task(queue, "schedule-task", {"step": "start"})
    claim = client.claim_tasks(queue, worker="worker-1", claim_timeout=120)[0]
    run_id = claim["run_id"]

    wake_at = base + timedelta(minutes=5)
    client.schedule_run(queue, run_id, wake_at)

    run_after_schedule = client.get_run(queue, run_id)
    assert run_after_schedule is not None
    assert run_after_schedule["state"] == "sleeping"
    assert run_after_schedule["available_at"] == wake_at
    assert run_after_schedule["wake_event"] is None

    task_after_schedule = client.get_task(queue, spawn.task_id)
    assert task_after_schedule is not None
    assert task_after_schedule["state"] == "sleeping"

    client.set_fake_now(wake_at)
    resumed = client.claim_tasks(queue, worker="worker-1", claim_timeout=120)[0]
    assert resumed["run_id"] == run_id
    assert resumed["attempt"] == 1

    run_after_resume = client.get_run(queue, run_id)
    assert run_after_resume is not None
    assert run_after_resume["state"] == "running"
    assert run_after_resume["started_at"] == wake_at


def test_claim_timeout_releases_run_to_new_worker(client):
    queue = "lease-expiry"
    client.create_queue(queue)

    base = datetime(2024, 4, 2, 9, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    spawn = client.spawn_task(queue, "lease", {"step": "attempt"})
    first_claim = client.claim_tasks(queue, worker="worker-a", claim_timeout=30)[0]
    run_id = first_claim["run_id"]

    running = client.get_run(queue, run_id)
    assert running is not None
    assert running["state"] == "running"
    assert running["claimed_by"] == "worker-a"
    expected_expiry = base + timedelta(seconds=30)
    assert running["claim_expires_at"] == expected_expiry

    client.set_fake_now(base + timedelta(minutes=5))

    still_running = client.get_run(queue, run_id)
    assert still_running is not None
    assert still_running["state"] == "running"
    assert still_running["claimed_by"] == "worker-a"

    second_claim = client.claim_tasks(queue, worker="worker-b", claim_timeout=45)[0]
    new_run_id = second_claim["run_id"]
    assert new_run_id != run_id
    assert second_claim["attempt"] == 2

    run_after_reclaim = client.get_run(queue, new_run_id)
    assert run_after_reclaim is not None
    assert run_after_reclaim["state"] == "running"
    assert run_after_reclaim["claimed_by"] == "worker-b"
    assert run_after_reclaim["claim_expires_at"] == base + timedelta(
        minutes=5, seconds=45
    )
    assert run_after_reclaim["started_at"] == base + timedelta(minutes=5)

    expired_run = client.get_run(queue, run_id)
    assert expired_run is not None
    assert expired_run["state"] == "failed"
    assert expired_run["claimed_by"] == "worker-a"
    assert expired_run["claim_expires_at"] == expected_expiry
    assert expired_run["failure_reason"] is not None
    assert expired_run["failure_reason"]["name"] == "$ClaimTimeout"
    assert expired_run["failure_reason"]["workerId"] == "worker-a"
    assert expired_run["failure_reason"]["attempt"] == 1
    assert "claimExpiredAt" in expired_run["failure_reason"]


def test_event_timeout_wakes_sleeping_run(client):
    queue = "timeout-wake"
    client.create_queue(queue)

    base = datetime(2024, 4, 3, 8, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    spawn = client.spawn_task(queue, "waiter", {"step": 1})
    claim = client.claim_tasks(queue, worker="worker-timeout")[0]
    run_id = claim["run_id"]

    event_name = "timeout-event"
    timeout_seconds = 600
    response = client.await_event(
        queue,
        spawn.task_id,
        run_id,
        step_name="wait",
        event_name=event_name,
        timeout_seconds=timeout_seconds,
    )
    assert response["should_suspend"] is True
    assert response["payload"] is None

    wait_table = client.get_table("w", queue)
    wait_count = client.conn.execute(
        SQL("select count(*) from absurd.{table}").format(table=wait_table)
    ).fetchone()[0]
    assert wait_count == 1

    sleeping_run = client.get_run(queue, run_id)
    assert sleeping_run is not None
    assert sleeping_run["state"] == "sleeping"
    assert sleeping_run["wake_event"] == event_name
    expected_wake = base + timedelta(seconds=timeout_seconds)
    assert sleeping_run["available_at"] == expected_wake

    client.set_fake_now(expected_wake + timedelta(seconds=1))
    resumed = client.claim_tasks(queue, worker="worker-timeout")[0]
    assert resumed["run_id"] == run_id
    assert resumed["attempt"] == 1
    assert resumed["wake_event"] == event_name

    run_after_resume = client.get_run(queue, run_id)
    assert run_after_resume is not None
    assert run_after_resume["state"] == "running"
    assert run_after_resume["wake_event"] == event_name
    assert run_after_resume["event_payload"] is None

    wait_count_after = client.conn.execute(
        SQL("select count(*) from absurd.{table}").format(table=wait_table)
    ).fetchone()[0]
    assert wait_count_after == 0


def test_fail_run_without_strategy_requeues_immediately(client):
    queue = "fail-none"
    client.create_queue(queue)

    base = datetime(2024, 4, 4, 7, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    spawn = client.spawn_task(queue, "no-strategy", {"payload": 1})
    claim = client.claim_tasks(queue, worker="worker")[0]

    client.fail_run(queue, claim["run_id"], {"message": "boom"})

    runs = client.get_runs(queue, spawn.task_id)
    assert len(runs) == 2
    first_run, second_run = runs
    assert first_run["state"] == "failed"
    assert first_run["claimed_by"] == "worker"
    assert first_run["claim_expires_at"] == base + timedelta(seconds=60)
    assert second_run["state"] == "pending"
    assert second_run["attempt"] == 2
    assert second_run["available_at"] == base

    task = client.get_task(queue, spawn.task_id)
    assert task is not None
    assert task["state"] == "pending"
    assert task["attempts"] == 2


def test_fail_run_respects_explicit_retry_at(client):
    queue = "fail-retry-at"
    client.create_queue(queue)

    base = datetime(2024, 4, 5, 6, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    spawn = client.spawn_task(queue, "retry-at", {"payload": 1})
    claim = client.claim_tasks(queue, worker="worker")[0]
    future = base + timedelta(minutes=15)

    client.fail_run(queue, claim["run_id"], {"message": "later"}, retry_at=future)

    runs = client.get_runs(queue, spawn.task_id)
    assert len(runs) == 2
    first_run, second_run = runs
    assert first_run["state"] == "failed"
    assert first_run["claimed_by"] == "worker"
    assert first_run["claim_expires_at"] == base + timedelta(seconds=60)
    assert second_run["state"] == "sleeping"
    assert second_run["available_at"] == future

    task = client.get_task(queue, spawn.task_id)
    assert task is not None
    assert task["state"] == "sleeping"


def test_fail_run_exponential_strategy_and_max_attempts(client):
    queue = "fail-exp"
    client.create_queue(queue)

    base = datetime(2024, 4, 6, 5, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    spawn = client.spawn_task(
        queue,
        "flaky-exp",
        {"payload": 1},
        {
            "retry_strategy": {
                "kind": "exponential",
                "base_seconds": 40,
                "factor": 3,
                "max_seconds": 90,
            },
            "max_attempts": 3,
        },
    )

    claim1 = client.claim_tasks(queue, worker="worker")[0]
    assert claim1["attempt"] == 1

    client.fail_run(queue, claim1["run_id"], {"message": "fail-1"})

    runs_after_first_fail = client.get_runs(queue, spawn.task_id)
    assert len(runs_after_first_fail) == 2
    _, second_run = runs_after_first_fail
    assert second_run["state"] == "sleeping"
    first_expected_available = base + timedelta(seconds=40)
    assert second_run["available_at"] == first_expected_available

    client.set_fake_now(first_expected_available)
    claim2 = client.claim_tasks(queue, worker="worker")[0]
    assert claim2["attempt"] == 2

    client.set_fake_now(first_expected_available + timedelta(seconds=10))
    client.fail_run(queue, claim2["run_id"], {"message": "fail-2"})

    runs_after_second_fail = client.get_runs(queue, spawn.task_id)
    assert len(runs_after_second_fail) == 3
    third_run = runs_after_second_fail[2]
    assert third_run["state"] == "sleeping"
    second_fail_time = first_expected_available + timedelta(seconds=10)
    expected_third_available = second_fail_time + timedelta(seconds=90)
    assert third_run["available_at"] == expected_third_available

    client.set_fake_now(expected_third_available + timedelta(seconds=5))
    client.fail_run(queue, third_run["run_id"], {"message": "final-fail"})

    final_runs = client.get_runs(queue, spawn.task_id)
    assert len(final_runs) == 3
    assert final_runs[0]["claimed_by"] == "worker"
    assert final_runs[0]["claim_expires_at"] == base + timedelta(seconds=60)
    assert final_runs[1]["claimed_by"] == "worker"
    assert final_runs[1]["claim_expires_at"] == first_expected_available + timedelta(
        seconds=60
    )
    assert final_runs[-1]["state"] == "failed"

    task = client.get_task(queue, spawn.task_id)
    assert task is not None
    assert task["state"] == "failed"
    assert task["attempts"] == 3


def test_spawn_claim_complete_flow(client):
    queue = "lifecycle"
    client.create_queue(queue)

    base = datetime(2024, 4, 7, 9, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    spawn = client.spawn_task(queue, "echo", {"message": "hi"})

    claimed = client.claim_tasks(queue, worker="tester", claim_timeout=120)
    assert len(claimed) == 1
    run = claimed[0]
    assert run["task_id"] == spawn.task_id
    assert run["attempt"] == 1

    client.complete_run(queue, run["run_id"], {"status": "ok"})

    task_row = client.get_task(queue, spawn.task_id)
    assert task_row is not None
    assert task_row["state"] == "completed"
    assert task_row["completed_payload"] == {"status": "ok"}
    assert task_row["last_attempt_run"] == run["run_id"]

    run_row = client.get_run(queue, run["run_id"])
    assert run_row is not None
    assert run_row["state"] == "completed"
    assert run_row["result"] == {"status": "ok"}
    assert run_row["claimed_by"] == "tester"
    assert run_row["claim_expires_at"] == base + timedelta(seconds=120)
