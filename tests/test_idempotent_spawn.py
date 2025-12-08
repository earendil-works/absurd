from datetime import datetime, timezone


def test_spawn_with_idempotency_key_creates_task(client):
    queue = "idempotent-spawn"
    client.create_queue(queue)

    base = datetime(2024, 5, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    result = client.spawn_task(
        queue,
        "my-task",
        {"value": 42},
        {"idempotency_key": "unique-key-1"},
    )

    assert result.task_id is not None
    assert result.run_id is not None
    assert result.attempt == 1

    task = client.get_task(queue, result.task_id)
    assert task is not None
    assert task["task_name"] == "my-task"
    assert task["state"] == "pending"


def test_spawn_with_same_idempotency_key_returns_existing_task(client):
    queue = "idempotent-dup"
    client.create_queue(queue)

    base = datetime(2024, 5, 2, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    first = client.spawn_task(
        queue,
        "my-task",
        {"value": 1},
        {"idempotency_key": "dup-key"},
    )

    assert first.task_id is not None
    assert first.run_id is not None
    assert first.attempt == 1

    second = client.spawn_task(
        queue,
        "my-task",
        {"value": 2},
        {"idempotency_key": "dup-key"},
    )

    assert second.task_id == first.task_id
    assert second.run_id == first.run_id
    assert second.attempt == first.attempt

    assert client.count_tasks(queue) == 1


def test_spawn_without_idempotency_key_always_creates_new_task(client):
    queue = "no-idempotency"
    client.create_queue(queue)

    base = datetime(2024, 5, 3, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    first = client.spawn_task(queue, "my-task", {"value": 1})
    second = client.spawn_task(queue, "my-task", {"value": 2})

    assert first.task_id != second.task_id
    assert first.run_id is not None
    assert second.run_id is not None
    assert client.count_tasks(queue) == 2


def test_different_idempotency_keys_create_separate_tasks(client):
    queue = "diff-keys"
    client.create_queue(queue)

    base = datetime(2024, 5, 4, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    first = client.spawn_task(
        queue,
        "my-task",
        {"value": 1},
        {"idempotency_key": "key-a"},
    )
    second = client.spawn_task(
        queue,
        "my-task",
        {"value": 2},
        {"idempotency_key": "key-b"},
    )

    assert first.task_id != second.task_id
    assert first.run_id is not None
    assert second.run_id is not None
    assert client.count_tasks(queue) == 2


def test_idempotency_key_persists_after_task_completes(client):
    queue = "complete-idem"
    client.create_queue(queue)

    base = datetime(2024, 5, 5, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    first = client.spawn_task(
        queue,
        "my-task",
        {"value": 1},
        {"idempotency_key": "complete-key"},
    )

    claim = client.claim_tasks(queue, worker="worker")[0]
    client.complete_run(queue, claim["run_id"], {"result": "done"})

    task = client.get_task(queue, first.task_id)
    assert task["state"] == "completed"

    second = client.spawn_task(
        queue,
        "my-task",
        {"value": 2},
        {"idempotency_key": "complete-key"},
    )

    assert second.task_id == first.task_id
    assert second.run_id == first.run_id
    assert second.attempt == first.attempt


def test_idempotency_key_works_across_different_task_names(client):
    queue = "cross-name"
    client.create_queue(queue)

    base = datetime(2024, 5, 6, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    first = client.spawn_task(
        queue,
        "task-a",
        {"value": 1},
        {"idempotency_key": "shared-key"},
    )

    second = client.spawn_task(
        queue,
        "task-b",
        {"value": 2},
        {"idempotency_key": "shared-key"},
    )

    assert second.task_id == first.task_id
    assert second.run_id == first.run_id
    assert second.attempt == first.attempt

    task = client.get_task(queue, first.task_id)
    assert task["task_name"] == "task-a"
