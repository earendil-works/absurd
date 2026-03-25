import asyncio

from psycopg import sql
import psycopg

from absurd_sdk import Absurd, AsyncAbsurd


def _get_task(conn, queue, task_id):
    query = sql.SQL(
        "select state, attempts from absurd.{table} where task_id = %s"
    ).format(table=sql.Identifier(f"t_{queue}"))
    return conn.execute(query, (task_id,)).fetchone()


def _count_checkpoints(conn, queue, task_id):
    query = sql.SQL("select count(*) from absurd.{table} where task_id = %s").format(
        table=sql.Identifier(f"c_{queue}")
    )
    return conn.execute(query, (task_id,)).fetchone()[0]


def test_retry_task_defaults_to_one_more_attempt(conn, queue_name):
    queue = queue_name("retry_task_default")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("always-fail", default_max_attempts=1)
    def always_fail(params, ctx):
        raise Exception("boom")

    spawned = client.spawn("always-fail", {"payload": 1})
    client.work_batch("worker", 60, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task[0] == "failed"
    assert task[1] == 1

    retry = client.retry_task(spawned["task_id"])

    assert retry["task_id"] == spawned["task_id"]
    assert retry["attempt"] == 2
    assert retry["created"] is False

    task = _get_task(conn, queue, spawned["task_id"])
    assert task[0] == "pending"
    assert task[1] == 2


def test_retry_task_spawn_new_creates_fresh_task(conn, queue_name):
    queue = queue_name("retry_task_spawn_new")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("checkpoint-then-fail", default_max_attempts=1)
    def checkpoint_then_fail(params, ctx):
        ctx.step("step-1", lambda: {"ok": True})
        raise Exception("boom")

    spawned = client.spawn("checkpoint-then-fail", {"payload": 1})
    client.work_batch("worker", 60, 1)

    assert _count_checkpoints(conn, queue, spawned["task_id"]) == 1

    retry = client.retry_task(spawned["task_id"], spawn_new=True)

    assert retry["created"] is True
    assert retry["attempt"] == 1
    assert retry["task_id"] != spawned["task_id"]

    old_task = _get_task(conn, queue, spawned["task_id"])
    new_task = _get_task(conn, queue, retry["task_id"])
    assert old_task[0] == "failed"
    assert new_task[0] == "pending"
    assert _count_checkpoints(conn, queue, retry["task_id"]) == 0


def test_async_retry_task(db_dsn, queue_name):
    queue = queue_name("async_retry_task")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run():
        client = AsyncAbsurd(db_dsn, queue_name=queue)

        @client.register_task("always-fail", default_max_attempts=1)
        async def always_fail(params, ctx):
            raise Exception("boom")

        spawned = await client.spawn("always-fail", {"payload": 1})
        await client.work_batch("worker", 60, 1)

        retried = await client.retry_task(spawned["task_id"])
        await client.close()
        return spawned, retried

    spawned, retried = asyncio.run(run())

    assert retried["task_id"] == spawned["task_id"]
    assert retried["attempt"] == 2
    assert retried["created"] is False
