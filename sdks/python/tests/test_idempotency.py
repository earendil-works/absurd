import asyncio

from psycopg import sql
import psycopg

from absurd_sdk import Absurd, AsyncAbsurd


def _count_tasks(conn, queue):
    query = sql.SQL("select count(*) from absurd.{table}").format(
        table=sql.Identifier(f"t_{queue}")
    )
    return conn.execute(query).fetchone()[0]


def _get_task(conn, queue, task_id):
    query = sql.SQL(
        "select task_id, task_name, state from absurd.{table} where task_id = %s"
    ).format(table=sql.Identifier(f"t_{queue}"))
    return conn.execute(query, (task_id,)).fetchone()


def test_spawn_with_idempotency_key_creates_task(conn, queue_name):
    queue = queue_name("idem_create")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("my-task")
    def my_task(params, ctx):
        return {"done": True}

    result = client.spawn("my-task", {"value": 42}, idempotency_key="unique-key-1")

    assert result["task_id"] is not None
    assert result["run_id"] is not None
    assert result["attempt"] == 1

    task = _get_task(conn, queue, result["task_id"])
    assert task is not None
    assert task[1] == "my-task"
    assert task[2] == "pending"


def test_spawn_with_same_idempotency_key_returns_existing_task(conn, queue_name):
    queue = queue_name("idem_dup")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("dup-task")
    def dup_task(params, ctx):
        return {"done": True}

    first = client.spawn("dup-task", {"value": 1}, idempotency_key="dup-key")
    second = client.spawn("dup-task", {"value": 2}, idempotency_key="dup-key")

    assert first["task_id"] == second["task_id"]
    assert first["run_id"] == second["run_id"]
    assert first["attempt"] == second["attempt"]
    assert _count_tasks(conn, queue) == 1


def test_spawn_without_idempotency_key_always_creates_new_task(conn, queue_name):
    queue = queue_name("no_idem")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("no-idem-task")
    def no_idem_task(params, ctx):
        return {"done": True}

    first = client.spawn("no-idem-task", {"value": 1})
    second = client.spawn("no-idem-task", {"value": 2})

    assert first["task_id"] != second["task_id"]
    assert _count_tasks(conn, queue) == 2


def test_different_idempotency_keys_create_separate_tasks(conn, queue_name):
    queue = queue_name("diff_keys")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("diff-keys-task")
    def diff_keys_task(params, ctx):
        return {"done": True}

    first = client.spawn("diff-keys-task", {"value": 1}, idempotency_key="key-a")
    second = client.spawn("diff-keys-task", {"value": 2}, idempotency_key="key-b")

    assert first["task_id"] != second["task_id"]
    assert _count_tasks(conn, queue) == 2


def test_idempotency_key_persists_after_task_completes(conn, queue_name):
    queue = queue_name("idem_complete")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("complete-task")
    def complete_task(params, ctx):
        return {"result": "done"}

    first = client.spawn("complete-task", {"value": 1}, idempotency_key="complete-key")
    client.work_batch(worker_id="test-worker")

    task = _get_task(conn, queue, first["task_id"])
    assert task[2] == "completed"

    second = client.spawn("complete-task", {"value": 2}, idempotency_key="complete-key")

    assert second["task_id"] == first["task_id"]
    assert second["run_id"] == first["run_id"]


def test_idempotent_spawn_executes_only_once(conn, queue_name):
    queue = queue_name("fire_forget")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    exec_count = []

    @client.register_task("fire-forget-task")
    def fire_forget_task(params, ctx):
        exec_count.append(1)
        return {"done": True}

    client.spawn("fire-forget-task", {}, idempotency_key="daily-report:2025-01-15")
    client.spawn("fire-forget-task", {}, idempotency_key="daily-report:2025-01-15")
    client.spawn("fire-forget-task", {}, idempotency_key="daily-report:2025-01-15")

    client.work_batch(worker_id="test-worker", batch_size=10)

    assert len(exec_count) == 1


def test_async_spawn_with_idempotency_key(db_dsn, queue_name):
    queue = queue_name("async_idem")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run():
        client = AsyncAbsurd(db_dsn, queue_name=queue)

        @client.register_task("async-task")
        async def async_task(params, ctx):
            return {"done": True}

        first = await client.spawn(
            "async-task", {"value": 1}, idempotency_key="async-key"
        )
        second = await client.spawn(
            "async-task", {"value": 2}, idempotency_key="async-key"
        )

        await client.close()
        return first, second

    first, second = asyncio.run(run())

    assert first["task_id"] == second["task_id"]
    assert first["run_id"] == second["run_id"]
