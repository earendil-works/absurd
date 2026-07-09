from datetime import datetime, timedelta, timezone


def test_oversized_retry_delay_does_not_wedge_queue(client):
    """An oversized retry delay must not overflow the interval type in
    fail_run. Before the clamp, fail_run raised 'interval out of range',
    leaving the run stuck 'running' so claim_task's expired-lease sweep
    raised for every worker on the queue."""
    queue = "overflow"
    client.create_queue(queue)

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    client.set_fake_now(start)

    poison = client.spawn_task(
        queue,
        "poison",
        {},
        {
            "retry_strategy": {"kind": "fixed", "base_seconds": 1e18},
            "max_attempts": 3,
        },
    )

    claim = client.claim_tasks(queue, worker="worker-a")[0]
    assert claim["task_id"] == poison.task_id

    # Must not raise "interval out of range".
    client.fail_run(queue, claim["run_id"], {"message": "boom"})

    # Retry was scheduled at the clamped ceiling (100 years), not overflowed.
    runs = client.get_runs(queue, poison.task_id)
    assert len(runs) == 2
    retry = runs[1]
    assert retry["state"] == "sleeping"
    assert retry["available_at"] == start + timedelta(seconds=3153600000)

    # The queue is still usable: a task spawned afterwards is claimable
    # (before the fix, the stuck poison run made claim_task raise for
    # every worker).
    healthy = client.spawn_task(queue, "healthy", {})
    next_claim = client.claim_tasks(queue, worker="worker-b")
    assert [c["task_id"] for c in next_claim] == [healthy.task_id]


def test_clamp_delay_seconds_bounds(client):
    result = client.conn.execute(
        "select absurd.clamp_delay_seconds(1e18) as hi, "
        "absurd.clamp_delay_seconds(-5) as lo, "
        "absurd.clamp_delay_seconds(30) as ok"
    ).fetchone()
    assert result == (3153600000.0, 0.0, 30.0)
