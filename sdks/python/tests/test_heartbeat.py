import asyncio
import json

import psycopg
import pytest
from psycopg import sql

from absurd_sdk import (
    Absurd,
    AsyncAbsurd,
    FailedTask,
    _create_async_task_context,
    _create_task_context,
)


def _fetch_run(conn, queue, run_id):
    query = sql.SQL(
        "select state, claim_expires_at, failure_reason from absurd.{table} where run_id = %s"
    ).format(table=sql.Identifier(f"r_{queue}"))
    return conn.execute(query, (run_id,)).fetchone()


def _fetch_task(conn, queue, task_id):
    query = sql.SQL("select state from absurd.{table} where task_id = %s").format(
        table=sql.Identifier(f"t_{queue}")
    )
    return conn.execute(query, (task_id,)).fetchone()


def test_sync_heartbeat_zero_is_rejected_by_sql(conn, queue_name):
    queue = queue_name("heartbeat_invalid")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("bad-heartbeat", default_max_attempts=1)
    def bad_heartbeat(_params, ctx):
        ctx.heartbeat(0)
        return {"ok": True}

    spawned = client.spawn("bad-heartbeat", {})
    client.work_batch(worker_id="worker", claim_timeout=60)

    task = _fetch_task(conn, queue, spawned["task_id"])
    assert task is not None
    assert task[0] == "failed"

    run = _fetch_run(conn, queue, spawned["run_id"])
    assert run is not None
    assert run[0] == "failed"
    assert "extend_by must be > 0" in str(run[2])


def test_async_heartbeat_zero_is_rejected_by_sql(db_dsn, queue_name):
    queue = queue_name("heartbeat_invalid_async")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run_case():
        client = AsyncAbsurd(db_dsn, queue_name=queue)

        @client.register_task("bad-heartbeat-async", default_max_attempts=1)
        async def bad_heartbeat_async(_params, ctx):
            await ctx.heartbeat(0)
            return {"ok": True}

        spawned = await client.spawn("bad-heartbeat-async", {})
        await client.work_batch(worker_id="worker", claim_timeout=60)
        await client.close()
        return spawned

    spawned = asyncio.run(run_case())

    with psycopg.connect(db_dsn, autocommit=True) as check_conn:
        task = _fetch_task(check_conn, queue, spawned["task_id"])
        assert task is not None
        assert task[0] == "failed"

        run = _fetch_run(check_conn, queue, spawned["run_id"])
        assert run is not None
        assert run[0] == "failed"
        assert "extend_by must be > 0" in str(run[2])


def test_sync_heartbeat_on_failed_run_raises_failed_task(conn, queue_name):
    queue = queue_name("heartbeat_failed")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("heartbeat-task")
    def heartbeat_task(_params, _ctx):
        return {"ok": True}

    spawned = client.spawn("heartbeat-task", {"value": 1})
    task = client.claim_tasks(worker_id="worker", claim_timeout=60)[0]

    conn.execute(
        "SELECT absurd.fail_run(%s, %s, %s, %s)",
        (
            queue,
            task["run_id"],
            json.dumps({"name": "$ClaimTimeout", "message": "timeout"}),
            None,
        ),
    )

    ctx = _create_task_context(task["task_id"], conn, queue, task, claim_timeout=60)
    with pytest.raises(FailedTask):
        ctx.heartbeat(30)

    run = _fetch_run(conn, queue, spawned["run_id"])
    assert run is not None
    assert run[0] == "failed"


def test_sync_execute_task_swallows_failed_task_checkpoint(conn, queue_name):
    queue = queue_name("checkpoint_failed_wrapper")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("checkpoint-failed")
    def checkpoint_failed(_params, ctx):
        ctx.step("persist", lambda: {"value": 1})
        return {"ok": True}

    spawned = client.spawn("checkpoint-failed", {"value": 1})
    task = client.claim_tasks(worker_id="worker", claim_timeout=60)[0]

    conn.execute(
        "SELECT absurd.fail_run(%s, %s, %s, %s)",
        (
            queue,
            task["run_id"],
            json.dumps({"name": "$ClaimTimeout", "message": "timeout"}),
            None,
        ),
    )

    # Must not raise: FailedTask is a terminal control-flow signal.
    client._execute_task(task, 60)

    checkpoint = conn.execute(
        sql.SQL(
            "select 1 from absurd.{table} where task_id = %s and checkpoint_name = %s"
        ).format(table=sql.Identifier(f"c_{queue}")),
        (spawned["task_id"], "persist"),
    ).fetchone()
    assert checkpoint is None


def test_async_heartbeat_on_failed_run_raises_failed_task(db_dsn, queue_name):
    queue = queue_name("heartbeat_failed_async")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run_case():
        client = AsyncAbsurd(db_dsn, queue_name=queue)

        @client.register_task("heartbeat-task-async")
        async def heartbeat_task_async(_params, _ctx):
            return {"ok": True}

        spawned = await client.spawn("heartbeat-task-async", {"value": 1})
        tasks = await client.claim_tasks(worker_id="worker", claim_timeout=60)
        task = tasks[0]

        assert client._conn is not None
        await client._conn.execute(
            "SELECT absurd.fail_run(%s, %s, %s, %s)",
            (
                queue,
                task["run_id"],
                json.dumps({"name": "$ClaimTimeout", "message": "timeout"}),
                None,
            ),
        )

        ctx = await _create_async_task_context(
            task["task_id"], client._conn, queue, task, claim_timeout=60
        )
        with pytest.raises(FailedTask):
            await ctx.heartbeat(30)

        await client.close()
        return spawned

    spawned = asyncio.run(run_case())

    with psycopg.connect(db_dsn, autocommit=True) as check_conn:
        run = _fetch_run(check_conn, queue, spawned["run_id"])
        assert run is not None
        assert run[0] == "failed"


def test_sync_execute_task_swallows_cancelled_task_on_complete_run(conn, queue_name):
    queue = queue_name("complete_cancel_wrapper")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("complete-cancel")
    def complete_cancel(_params, _ctx):
        return {"ok": True}

    spawned = client.spawn("complete-cancel", {"value": 1})
    task = client.claim_tasks(worker_id="worker", claim_timeout=60)[0]

    client.cancel_task(spawned["task_id"])

    # Must not raise: cancellation after claim should be treated as terminal control flow.
    client._execute_task(task, 60)

    run = _fetch_run(conn, queue, spawned["run_id"])
    assert run is not None
    assert run[0] == "cancelled"

    task_row = _fetch_task(conn, queue, spawned["task_id"])
    assert task_row is not None
    assert task_row[0] == "cancelled"


def test_sync_execute_task_swallows_cancelled_task_on_fail_run(conn, queue_name):
    queue = queue_name("fail_cancel_wrapper")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("fail-cancel")
    def fail_cancel(_params, _ctx):
        raise RuntimeError("boom")

    spawned = client.spawn("fail-cancel", {"value": 1})
    task = client.claim_tasks(worker_id="worker", claim_timeout=60)[0]

    client.cancel_task(spawned["task_id"])

    # Must not raise: fail_run should map cancellation to CancelledTask and be swallowed.
    client._execute_task(task, 60)

    run = _fetch_run(conn, queue, spawned["run_id"])
    assert run is not None
    assert run[0] == "cancelled"

    task_row = _fetch_task(conn, queue, spawned["task_id"])
    assert task_row is not None
    assert task_row[0] == "cancelled"


def test_async_execute_task_swallows_cancelled_task_on_complete_run(db_dsn, queue_name):
    queue = queue_name("complete_cancel_wrapper_async")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run_case():
        client = AsyncAbsurd(db_dsn, queue_name=queue)

        @client.register_task("complete-cancel-async")
        async def complete_cancel_async(_params, _ctx):
            return {"ok": True}

        spawned = await client.spawn("complete-cancel-async", {"value": 1})
        tasks = await client.claim_tasks(worker_id="worker", claim_timeout=60)
        task = tasks[0]

        await client.cancel_task(spawned["task_id"])

        # Must not raise.
        await client._execute_task(task, 60)

        await client.close()
        return spawned

    spawned = asyncio.run(run_case())

    with psycopg.connect(db_dsn, autocommit=True) as check_conn:
        run = _fetch_run(check_conn, queue, spawned["run_id"])
        assert run is not None
        assert run[0] == "cancelled"

        task_row = _fetch_task(check_conn, queue, spawned["task_id"])
        assert task_row is not None
        assert task_row[0] == "cancelled"


def test_async_execute_task_swallows_cancelled_task_on_fail_run(db_dsn, queue_name):
    queue = queue_name("fail_cancel_wrapper_async")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run_case():
        client = AsyncAbsurd(db_dsn, queue_name=queue)

        @client.register_task("fail-cancel-async")
        async def fail_cancel_async(_params, _ctx):
            raise RuntimeError("boom")

        spawned = await client.spawn("fail-cancel-async", {"value": 1})
        tasks = await client.claim_tasks(worker_id="worker", claim_timeout=60)
        task = tasks[0]

        await client.cancel_task(spawned["task_id"])

        # Must not raise.
        await client._execute_task(task, 60)

        await client.close()
        return spawned

    spawned = asyncio.run(run_case())

    with psycopg.connect(db_dsn, autocommit=True) as check_conn:
        run = _fetch_run(check_conn, queue, spawned["run_id"])
        assert run is not None
        assert run[0] == "cancelled"

        task_row = _fetch_task(check_conn, queue, spawned["task_id"])
        assert task_row is not None
        assert task_row[0] == "cancelled"
