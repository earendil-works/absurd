from datetime import datetime, timezone

import pytest


def test_retry_task_extends_attempts_on_failed_task(client):
    queue = "retry-task-extend"
    client.create_queue(queue)

    now = datetime(2024, 5, 1, 12, 0, tzinfo=timezone.utc)
    client.set_fake_now(now)

    spawn = client.spawn_task(
        queue,
        "retry-me",
        {"payload": 1},
        {"max_attempts": 1},
    )
    claim = client.claim_tasks(queue, worker="worker")[0]
    client.fail_run(queue, claim["run_id"], {"message": "boom"})

    failed_task = client.get_task(queue, spawn.task_id)
    assert failed_task["state"] == "failed"
    assert failed_task["attempts"] == 1

    retry = client.retry_task(queue, spawn.task_id, {"max_attempts": 3})

    assert retry["task_id"] == spawn.task_id
    assert retry["created"] is False
    assert retry["attempt"] == 2

    task_after = client.get_task(queue, spawn.task_id)
    assert task_after["state"] == "pending"
    assert task_after["attempts"] == 2

    runs = client.get_runs(queue, spawn.task_id)
    assert len(runs) == 2
    assert runs[0]["state"] == "failed"
    assert runs[1]["run_id"] == retry["run_id"]
    assert runs[1]["attempt"] == 2
    assert runs[1]["state"] == "pending"


def test_retry_task_spawn_new_task_with_same_inputs(client):
    queue = "retry-task-spawn-new"
    client.create_queue(queue)

    spawn = client.spawn_task(
        queue,
        "retry-me",
        {"payload": {"a": 1}},
        {
            "max_attempts": 1,
            "retry_strategy": {"kind": "fixed", "base_seconds": 10},
            "headers": {"x-trace-id": "abc"},
        },
    )

    claim = client.claim_tasks(queue, worker="worker")[0]
    client.set_task_checkpoint_state(
        queue,
        spawn.task_id,
        "step-1",
        {"ok": True},
        claim["run_id"],
    )
    client.fail_run(queue, claim["run_id"], {"message": "final"})

    retry = client.retry_task(queue, spawn.task_id, {"spawn_new": True})

    assert retry["created"] is True
    assert retry["attempt"] == 1
    assert retry["task_id"] != spawn.task_id

    old_task = client.get_task(queue, spawn.task_id)
    new_task = client.get_task(queue, retry["task_id"])
    assert old_task["state"] == "failed"
    assert new_task["state"] == "pending"

    old_checkpoint = client.get_checkpoint(queue, spawn.task_id, "step-1")
    new_checkpoint = client.get_checkpoint(queue, retry["task_id"], "step-1")
    assert old_checkpoint is not None
    assert new_checkpoint is None


@pytest.mark.parametrize(
    "options, error",
    [
        ({"max_attempts": 1}, "must be greater than current attempts"),
    ],
)
def test_retry_task_requires_increased_attempts(client, options, error):
    queue = "retry-task-validation"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "retry-me", {"payload": 1}, {"max_attempts": 1})
    claim = client.claim_tasks(queue, worker="worker")[0]
    client.fail_run(queue, claim["run_id"], {"message": "boom"})

    with pytest.raises(Exception, match=error):
        client.retry_task(queue, spawn.task_id, options)


def test_retry_task_defaults_to_one_more_attempt_when_not_provided(client):
    queue = "retry-task-default-attempts"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "retry-me", {"payload": 1}, {"max_attempts": 1})
    claim = client.claim_tasks(queue, worker="worker")[0]
    client.fail_run(queue, claim["run_id"], {"message": "boom"})

    retry = client.retry_task(queue, spawn.task_id)

    assert retry["task_id"] == spawn.task_id
    assert retry["created"] is False
    assert retry["attempt"] == 2

    task_after = client.get_task(queue, spawn.task_id)
    assert task_after is not None
    assert task_after["state"] == "pending"
    assert task_after["attempts"] == 2
