import asyncio
from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from psycopg import sql

from absurd_sdk import (
    Absurd,
    AsyncAbsurd,
    LostClaimError,
    _complete_task_run,
    _complete_task_run_async,
    _create_async_task_context,
    _create_task_context,
    _fail_task_run,
    _fail_task_run_async,
)

WORKER_MUTATIONS = (
    "await_event",
    "complete_run",
    "extend_claim",
    "fail_run",
    "schedule_run",
    "set_task_checkpoint_state",
)


def _invoke_worker_mutation(conn, queue, task, ctx, mutation):
    if mutation == "await_event":
        ctx.await_event("worker-claim-event")
    elif mutation == "complete_run":
        _complete_task_run(conn, queue, task["run_id"], {"ok": True}, task["worker_id"])
    elif mutation == "extend_claim":
        ctx.heartbeat(60)
    elif mutation == "fail_run":
        _fail_task_run(
            conn,
            queue,
            task["run_id"],
            RuntimeError("failure"),
            worker_id=task["worker_id"],
        )
    elif mutation == "schedule_run":
        ctx._schedule_run_after(60)
    elif mutation == "set_task_checkpoint_state":
        ctx._persist_checkpoint("step", {"ok": True})
    else:
        raise AssertionError(f"Unknown mutation: {mutation}")


async def _invoke_worker_mutation_async(conn, queue, task, ctx, mutation):
    if mutation == "await_event":
        await ctx.await_event("worker-claim-event")
    elif mutation == "complete_run":
        await _complete_task_run_async(
            conn, queue, task["run_id"], {"ok": True}, task["worker_id"]
        )
    elif mutation == "extend_claim":
        await ctx.heartbeat(60)
    elif mutation == "fail_run":
        await _fail_task_run_async(
            conn,
            queue,
            task["run_id"],
            RuntimeError("failure"),
            worker_id=task["worker_id"],
        )
    elif mutation == "schedule_run":
        await ctx._schedule_run_after(60)
    elif mutation == "set_task_checkpoint_state":
        await ctx._persist_checkpoint("step", {"ok": True})
    else:
        raise AssertionError(f"Unknown mutation: {mutation}")


def test_claimed_task_and_context_keep_worker_id(conn, queue_name):
    queue = queue_name("worker_context")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()
    client.spawn("task", {}, queue=queue)

    task = client.claim_tasks(worker_id="worker-owner")[0]
    ctx = _create_task_context(task["task_id"], conn, queue, task, claim_timeout=60)

    assert task["worker_id"] == "worker-owner"
    assert ctx.worker_id == "worker-owner"


def test_claim_tasks_generates_a_unique_worker_id_by_default(conn, queue_name):
    queue = queue_name("worker_default")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()
    client.spawn("task", {}, queue=queue)
    client.spawn("task", {}, queue=queue)

    first = client.claim_tasks()[0]
    second = client.claim_tasks()[0]

    assert first["worker_id"]
    assert second["worker_id"]
    assert first["worker_id"] != second["worker_id"]


@pytest.mark.parametrize("mutation", WORKER_MUTATIONS)
def test_sync_worker_mutations_map_lost_claim(conn, queue_name, mutation):
    queue = queue_name(f"sync_worker_{mutation}")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()
    client.spawn("task", {}, queue=queue)
    task = client.claim_tasks(worker_id="worker-owner", claim_timeout=60)[0]
    task["worker_id"] = "worker-other"
    ctx = _create_task_context(task["task_id"], conn, queue, task, claim_timeout=60)

    with pytest.raises(LostClaimError) as exc_info:
        _invoke_worker_mutation(conn, queue, task, ctx, mutation)

    assert exc_info.value.__cause__ is not None
    assert getattr(exc_info.value.__cause__, "sqlstate", None) == "AB004"


@pytest.mark.parametrize("mutation", WORKER_MUTATIONS)
def test_async_worker_mutations_map_lost_claim(db_dsn, queue_name, mutation):
    queue = queue_name(f"async_worker_{mutation}")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run_case():
        client = AsyncAbsurd(db_dsn, queue_name=queue)
        try:
            await client.spawn("task", {}, queue=queue)
            task = (
                await client.claim_tasks(worker_id="worker-owner", claim_timeout=60)
            )[0]
            task["worker_id"] = "worker-other"
            assert client._conn is not None
            ctx = await _create_async_task_context(
                task["task_id"], client._conn, queue, task, claim_timeout=60
            )

            with pytest.raises(LostClaimError) as exc_info:
                await _invoke_worker_mutation_async(
                    client._conn, queue, task, ctx, mutation
                )

            assert exc_info.value.__cause__ is not None
            assert getattr(exc_info.value.__cause__, "sqlstate", None) == "AB004"
        finally:
            await client.close()

    asyncio.run(run_case())


def test_lost_claim_stops_stale_handler(conn, queue_name):
    queue = queue_name("stale_handler")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    conn.execute("SELECT set_config('absurd.fake_now', %s, false)", (now.isoformat(),))
    continued = []

    @client.register_task("stale-handler")
    def stale_handler(_params, ctx):
        conn.execute(
            "SELECT set_config('absurd.fake_now', %s, false)",
            ((now + timedelta(seconds=60)).isoformat(),),
        )
        ctx.heartbeat()
        continued.append(True)
        return {"ok": True}

    spawned = client.spawn("stale-handler", {})
    client.work_batch(worker_id="expiring-worker", claim_timeout=60)

    run_state = conn.execute(
        sql.SQL("SELECT state FROM absurd.{table} WHERE run_id = %s").format(
            table=sql.Identifier(f"r_{queue}")
        ),
        (spawned["run_id"],),
    ).fetchone()
    assert continued == []
    assert run_state == ("running",)
