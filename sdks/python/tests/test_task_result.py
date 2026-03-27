import asyncio
import json
import threading
import time
import uuid

import psycopg
import pytest

from absurd_sdk import (
    Absurd,
    AsyncAbsurd,
    TaskResultSnapshot,
    TimeoutError,
    _create_async_task_context,
    _create_task_context,
)


def test_fetch_task_result_returns_none_for_unknown_task(conn, queue_name):
    queue = queue_name("task_result_missing")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    assert client.fetch_task_result(str(uuid.uuid4())) is None


def test_fetch_task_result_tracks_lifecycle(conn, queue_name):
    queue = queue_name("task_result")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("result-target")
    def result_target(_params, _ctx):
        return {"ok": True}

    spawned = client.spawn("result-target", {})
    assert client.fetch_task_result(spawned["task_id"]) == TaskResultSnapshot(
        state="pending"
    )

    claim = client.claim_tasks(worker_id="worker")[0]
    assert client.fetch_task_result(spawned["task_id"]) == TaskResultSnapshot(
        state="running"
    )

    conn.execute(
        "SELECT absurd.complete_run(%s, %s, %s)",
        (queue, claim["run_id"], json.dumps({"ok": True})),
    )

    assert client.fetch_task_result(spawned["task_id"]) == TaskResultSnapshot(
        state="completed", result={"ok": True}
    )


def test_await_task_result_times_out(conn, queue_name):
    queue = queue_name("task_result_timeout")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    spawned = client.spawn("never-finishes", {}, queue=queue)

    with pytest.raises(TimeoutError):
        client.await_task_result(spawned["task_id"], timeout=0.05)


def test_task_context_await_task_result_waits_for_terminal_state(conn, db_dsn, queue_name):
    parent_queue = queue_name("task_result_ctx_parent")
    child_queue = queue_name("task_result_ctx_child")
    parent_client = Absurd(conn, queue_name=parent_queue)
    child_client = Absurd(conn, queue_name=child_queue)
    parent_client.create_queue()
    child_client.create_queue()

    parent = parent_client.spawn("parent", {}, queue=parent_queue)
    parent_claim = parent_client.claim_tasks(worker_id="parent-worker")[0]
    ctx = _create_task_context(
        parent_claim["task_id"], conn, parent_queue, parent_claim, claim_timeout=120
    )

    child = child_client.spawn("child", {}, queue=child_queue)

    def finish_child_task():
        with psycopg.connect(db_dsn, autocommit=True) as worker_conn:
            time.sleep(0.1)
            row = worker_conn.execute(
                """SELECT run_id
                   FROM absurd.claim_task(%s, %s, %s, %s)""",
                (child_queue, "child-worker", 120, 1),
            ).fetchone()
            assert row is not None
            worker_conn.execute(
                "SELECT absurd.complete_run(%s, %s, %s)",
                (child_queue, row[0], json.dumps({"child": "ok"})),
            )

    thread = threading.Thread(target=finish_child_task)
    thread.start()
    try:
        snapshot = ctx.await_task_result(
            child["task_id"], queue_name=child_queue, timeout=2
        )
        assert snapshot == TaskResultSnapshot(state="completed", result={"child": "ok"})
    finally:
        thread.join()


def test_task_context_await_task_result_rejects_same_queue(conn, queue_name):
    queue = queue_name("task_result_ctx_same_queue")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    parent = client.spawn("parent", {}, queue=queue)
    parent_claim = client.claim_tasks(worker_id="parent-worker")[0]
    ctx = _create_task_context(
        parent_claim["task_id"], conn, queue, parent_claim, claim_timeout=120
    )

    child = client.spawn("child", {}, queue=queue)
    with pytest.raises(ValueError, match="same queue"):
        ctx.await_task_result(child["task_id"], timeout=30)


def test_async_await_task_result_resolves(db_dsn, queue_name):
    queue = queue_name("task_result_async")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run():
        observer = AsyncAbsurd(db_dsn, queue_name=queue)
        worker = AsyncAbsurd(db_dsn, queue_name=queue)

        try:

            @worker.register_task("finishes")
            async def finishes(_params, _ctx):
                return {"done": True}

            spawned = await worker.spawn("finishes", {})
            assert await observer.fetch_task_result(
                spawned["task_id"]
            ) == TaskResultSnapshot(state="pending")

            async def process_once():
                await asyncio.sleep(0.1)
                await worker.work_batch(worker_id="async-worker")

            process_task = asyncio.create_task(process_once())
            snapshot = await observer.await_task_result(spawned["task_id"], timeout=2)
            await process_task
            return snapshot
        finally:
            await observer.close()
            await worker.close()

    snapshot = asyncio.run(run())
    assert snapshot == TaskResultSnapshot(state="completed", result={"done": True})


def test_async_task_context_await_task_result_rejects_same_queue(db_dsn, queue_name):
    queue = queue_name("task_result_ctx_same_queue_async")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run_case():
        client = AsyncAbsurd(db_dsn, queue_name=queue)
        try:
            parent = await client.spawn("parent", {}, queue=queue)
            parent_claim = (await client.claim_tasks(worker_id="parent-worker"))[0]
            assert parent_claim["task_id"] == parent["task_id"]
            assert client._conn is not None

            ctx = await _create_async_task_context(
                parent_claim["task_id"], client._conn, queue, parent_claim, claim_timeout=120
            )

            child = await client.spawn("child", {}, queue=queue)
            with pytest.raises(ValueError, match="same queue"):
                await ctx.await_task_result(child["task_id"], timeout=30)
        finally:
            await client.close()

    asyncio.run(run_case())
