import asyncio
import time
from datetime import datetime, timedelta, timezone

from psycopg import sql
import psycopg

from absurd_sdk import Absurd, AsyncAbsurd


def _fetch_run(conn, queue, run_id):
    query = sql.SQL(
        "select state, nullif(available_at, 'infinity'::timestamptz) as available_at, result "
        "from absurd.{table} where run_id = %s"
    ).format(table=sql.Identifier(f"r_{queue}"))
    return conn.execute(query, (run_id,)).fetchone()


def _fetch_task(conn, queue, task_id):
    query = sql.SQL(
        "select state, attempts, completed_payload from absurd.{table} where task_id = %s"
    ).format(table=sql.Identifier(f"t_{queue}"))
    return conn.execute(query, (task_id,)).fetchone()


def test_sync_worker_processes_task(conn, queue_name):
    queue = queue_name("emails")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    seen = []

    @client.register_task("send_welcome")
    def send_welcome(params, ctx):
        profile = ctx.step("load-profile", lambda: {"user_id": params["user_id"]})
        seen.append(profile)
        return {"status": "sent", "user_id": params["user_id"]}

    spawned = client.spawn("send_welcome", {"user_id": 42})
    client.work_batch(worker_id="sync-worker")

    run_state, available_at, result = _fetch_run(conn, queue, spawned["run_id"])
    assert run_state == "completed"
    assert result == {"status": "sent", "user_id": 42}
    assert seen == [{"user_id": 42}]


def test_sync_worker_rejects_concurrency_above_one(conn, queue_name):
    queue = queue_name("sync_single")
    client = Absurd(conn, queue_name=queue)

    try:
        client.start_worker(concurrency=2)
    except ValueError as exc:
        assert "runs sequentially" in str(exc)
    else:
        raise AssertionError("expected ValueError for concurrency > 1")


def test_sync_worker_suspends_until_alarm(conn, queue_name):
    queue = queue_name("reminders")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("send_reminder")
    def send_reminder(params, ctx):
        ctx.sleep_until("wait", datetime.now(timezone.utc) + timedelta(seconds=2))
        return {"status": "done"}

    spawned = client.spawn("send_reminder", {})

    client.work_batch(worker_id="sleepy")

    run_state, available_at, result = _fetch_run(conn, queue, spawned["run_id"])
    assert run_state == "sleeping"
    assert result is None
    assert available_at is not None

    time.sleep(2.5)
    client.work_batch(worker_id="sleepy")

    run_state, available_at, result = _fetch_run(conn, queue, spawned["run_id"])
    assert run_state == "completed"
    assert result == {"status": "done"}


def test_async_absurd_round_trip(db_dsn, queue_name):
    queue = queue_name("uploads")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run():
        client = AsyncAbsurd(db_dsn, queue_name=queue)

        calls = []

        @client.register_task("process-upload")
        async def process_upload(params, ctx):
            async def double():
                calls.append("called")
                return params["value"] * 2

            value = await ctx.step("double", double)
            return {"processed": value}

        spawned = await client.spawn("process-upload", {"value": 3})
        await client.work_batch(worker_id="async-worker")
        if client._conn is not None:
            await client._conn.commit()
        await client.close()
        return spawned, calls

    spawned, calls = asyncio.run(run())
    assert calls == ["called"]

    with psycopg.connect(db_dsn, autocommit=True) as check_conn:
        run_state, available_at, result = _fetch_run(check_conn, queue, spawned["run_id"])
        assert run_state == "completed"
        assert result == {"processed": 6}


def test_async_begin_complete_step_is_cached_on_retry(db_dsn, queue_name):
    queue = queue_name("uploads_decomposed")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run():
        client = AsyncAbsurd(db_dsn, queue_name=queue)

        executions = []
        attempts = []

        @client.register_task("process-upload-decomposed", default_max_attempts=2)
        async def process_upload(params, ctx):
            attempts.append(len(attempts) + 1)

            handle = await ctx.begin_step("double")
            if handle.done:
                value = handle.state
            else:
                executions.append(len(executions) + 1)
                value = await ctx.complete_step(handle, params["value"] * 2)

            if len(attempts) == 1:
                raise Exception("Intentional failure")

            return {"processed": value, "executions": len(executions)}

        spawned = await client.spawn("process-upload-decomposed", {"value": 3})
        await client.work_batch(worker_id="async-worker")
        await client.work_batch(worker_id="async-worker")

        if client._conn is not None:
            await client._conn.commit()
        await client.close()
        return spawned, executions, attempts

    spawned, executions, attempts = asyncio.run(run())
    assert len(executions) == 1
    assert len(attempts) == 2

    with psycopg.connect(db_dsn, autocommit=True) as check_conn:
        task_state, task_attempts, completed_payload = _fetch_task(
            check_conn, queue, spawned["task_id"]
        )
        assert task_state == "completed"
        assert task_attempts == 2
        assert completed_payload == {"processed": 6, "executions": 1}
