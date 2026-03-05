import asyncio

import psycopg
from psycopg import sql

from absurd_sdk import Absurd, AsyncAbsurd


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
