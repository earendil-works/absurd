from datetime import datetime, timedelta, timezone

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

MUTATIONS = (
    "await_event",
    "complete_run",
    "extend_claim",
    "fail_run",
    "schedule_run",
    "set_task_checkpoint_state",
)


def call_worker_mutation(client, mutation, queue, task_id, run_id, worker):
    if mutation == "await_event":
        client.conn.execute(
            "select * from absurd.await_event(%s, %s, %s, %s, %s, %s, %s)",
            (queue, task_id, run_id, "wait", "event", 60, worker),
        ).fetchone()
    elif mutation == "complete_run":
        client.conn.execute(
            "select absurd.complete_run(%s, %s, %s, %s)",
            (queue, run_id, Jsonb({"ok": True}), worker),
        )
    elif mutation == "extend_claim":
        client.conn.execute(
            "select absurd.extend_claim(%s, %s, %s, %s)",
            (queue, run_id, 60, worker),
        )
    elif mutation == "fail_run":
        client.conn.execute(
            "select absurd.fail_run(%s, %s, %s, %s, %s)",
            (queue, run_id, Jsonb({"name": "Failure"}), None, worker),
        )
    elif mutation == "schedule_run":
        client.conn.execute(
            "select absurd.schedule_run(%s, %s, %s, %s)",
            (queue, run_id, datetime(2026, 1, 1, tzinfo=timezone.utc), worker),
        )
    elif mutation == "set_task_checkpoint_state":
        client.conn.execute(
            "select absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s, %s)",
            (queue, task_id, "step", Jsonb({"ok": True}), run_id, None, worker),
        )
    else:
        raise AssertionError(f"unknown mutation: {mutation}")


@pytest.mark.parametrize("mutation", MUTATIONS)
def test_worker_mutations_accept_the_claim_owner(client, mutation):
    queue = f"worker_owner_{mutation}"
    client.create_queue(queue)
    spawn = client.spawn_task(queue, "task")
    claim = client.claim_tasks(queue, worker="worker-1")[0]

    call_worker_mutation(
        client,
        mutation,
        queue,
        spawn.task_id,
        claim["run_id"],
        "worker-1",
    )

    run = client.get_run(queue, claim["run_id"])
    if mutation in ("await_event", "schedule_run"):
        assert run["state"] == "sleeping"
    elif mutation == "complete_run":
        assert run["state"] == "completed"
    elif mutation == "fail_run":
        assert run["state"] == "failed"
    else:
        assert run["state"] == "running"

    if mutation == "set_task_checkpoint_state":
        checkpoint = client.get_checkpoint(queue, spawn.task_id, "step")
        assert checkpoint["state"] == {"ok": True}


@pytest.mark.parametrize("mutation", MUTATIONS)
def test_worker_mutations_reject_another_worker(client, mutation):
    queue = f"worker_mismatch_{mutation}"
    client.create_queue(queue)
    spawn = client.spawn_task(queue, "task")
    claim = client.claim_tasks(queue, worker="worker-1")[0]

    with pytest.raises(Exception) as exc_info, client.conn.transaction():
        call_worker_mutation(
            client,
            mutation,
            queue,
            spawn.task_id,
            claim["run_id"],
            "worker-2",
        )

    assert getattr(exc_info.value, "sqlstate", None) == "AB004"
    assert client.get_run(queue, claim["run_id"])["state"] == "running"


def test_worker_claim_rejects_an_expired_lease(client):
    queue = "worker_expired"
    client.create_queue(queue)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    client.set_fake_now(now)
    spawn = client.spawn_task(queue, "task")
    claim = client.claim_tasks(queue, worker="worker-1", claim_timeout=60)[0]
    client.set_fake_now(now + timedelta(seconds=60))

    with pytest.raises(Exception) as exc_info, client.conn.transaction():
        client.conn.execute(
            "select absurd.assert_worker_claim(%s, %s, %s, %s)",
            (queue, spawn.task_id, claim["run_id"], "worker-1"),
        )

    assert getattr(exc_info.value, "sqlstate", None) == "AB004"


@pytest.mark.parametrize("worker", (None, ""))
def test_worker_claim_uses_the_same_default_worker_normalization(client, worker):
    queue = f"worker_default_{'null' if worker is None else 'empty'}"
    client.create_queue(queue)
    spawn = client.spawn_task(queue, "task")
    claim = client.claim_tasks(queue, worker=worker, claim_timeout=0)[0]

    client.conn.execute(
        "select absurd.assert_worker_claim(%s, %s, %s, %s)",
        (queue, spawn.task_id, claim["run_id"], worker),
    )


def test_worker_claim_rejects_a_missing_task(client):
    queue = "worker_missing_task"
    client.create_queue(queue)
    spawn = client.spawn_task(queue, "task")
    claim = client.claim_tasks(queue, worker="worker-1")[0]
    client.conn.execute(
        sql.SQL("delete from absurd.{} where task_id = %s").format(
            client.get_table("t", queue)
        ),
        (spawn.task_id,),
    )

    with pytest.raises(Exception) as exc_info, client.conn.transaction():
        client.conn.execute(
            "select absurd.assert_worker_claim(%s, %s, %s, %s)",
            (queue, spawn.task_id, claim["run_id"], "worker-1"),
        )

    assert getattr(exc_info.value, "sqlstate", None) == "AB004"


def test_worker_claim_rejects_a_run_from_another_task(client):
    queue = "worker_cross_task"
    client.create_queue(queue)
    first = client.spawn_task(queue, "first")
    second = client.spawn_task(queue, "second")
    claim = client.claim_tasks(queue, worker="worker-1", qty=2)[0]

    claimed_task_id = claim["task_id"]
    other_task_id = (
        second.task_id if claimed_task_id == first.task_id else first.task_id
    )

    with pytest.raises(Exception) as exc_info, client.conn.transaction():
        client.conn.execute(
            "select absurd.assert_worker_claim(%s, %s, %s, %s)",
            (queue, other_task_id, claim["run_id"], "worker-1"),
        )

    assert getattr(exc_info.value, "sqlstate", None) == "AB004"


def test_worker_claim_preserves_the_cancellation_error(client):
    queue = "worker_cancelled"
    client.create_queue(queue)
    spawn = client.spawn_task(queue, "task")
    claim = client.claim_tasks(queue, worker="worker-1")[0]
    client.cancel_task(queue, spawn.task_id)

    with pytest.raises(Exception) as exc_info, client.conn.transaction():
        client.conn.execute(
            "select absurd.assert_worker_claim(%s, %s, %s, %s)",
            (queue, spawn.task_id, claim["run_id"], "worker-1"),
        )

    assert getattr(exc_info.value, "sqlstate", None) == "AB001"
