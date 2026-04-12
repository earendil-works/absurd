import asyncio
import time
from datetime import datetime, timedelta, timezone

from psycopg import sql
import psycopg

import absurd_sdk
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


def _fetch_run_with_failure(conn, queue, run_id):
    query = sql.SQL(
        "select state, nullif(available_at, 'infinity'::timestamptz) as available_at, failure_reason "
        "from absurd.{table} where run_id = %s"
    ).format(table=sql.Identifier(f"r_{queue}"))
    return conn.execute(query, (run_id,)).fetchone()


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


def test_unknown_task_is_deferred_not_failed(conn, queue_name):
    queue = queue_name("unknown")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    base_time = datetime(2024, 4, 1, 10, 0, tzinfo=timezone.utc)
    conn.execute(
        "SELECT set_config('absurd.fake_now', %s, false)",
        (base_time.isoformat(),),
    )

    spawned = client.spawn(
        "ghost-task",
        {"value": 1},
        queue=queue,
        max_attempts=1,
    )

    client.work_batch(worker_id="unknown-worker")

    task_state, task_attempts, _ = _fetch_task(conn, queue, spawned["task_id"])
    assert task_state == "sleeping"
    assert task_attempts == 1

    run_state, available_at, failure_reason = _fetch_run_with_failure(
        conn, queue, spawned["run_id"]
    )
    assert run_state == "sleeping"
    assert failure_reason is None
    assert available_at is not None and available_at > base_time


def test_unknown_task_defer_failure_preserves_error_context(conn, queue_name, monkeypatch):
    queue = queue_name("unknown_defer_fail")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    monkeypatch.setattr(absurd_sdk, "_UNKNOWN_TASK_DEFER_BASE_SECONDS", "oops")
    monkeypatch.setattr(absurd_sdk, "_UNKNOWN_TASK_DEFER_JITTER_SECONDS", -1)

    spawned = client.spawn(
        "ghost-task",
        {"value": 1},
        queue=queue,
        max_attempts=1,
    )

    client.work_batch(worker_id="unknown-worker")

    task_state, task_attempts, _ = _fetch_task(conn, queue, spawned["task_id"])
    assert task_state == "failed"
    assert task_attempts == 1

    run_state, _, failure_reason = _fetch_run_with_failure(conn, queue, spawned["run_id"])
    assert run_state == "failed"
    assert failure_reason is not None
    assert failure_reason["name"] == "InvalidTextRepresentation"
    assert "invalid input syntax for type integer" in failure_reason["message"]


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


def test_async_unknown_task_is_deferred_not_failed(db_dsn, queue_name):
    queue = queue_name("unknown_async")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run():
        client = AsyncAbsurd(db_dsn, queue_name=queue)
        spawned = await client.spawn(
            "ghost-task",
            {"value": 1},
            queue=queue,
            max_attempts=1,
        )
        await client.work_batch(worker_id="unknown-worker")
        if client._conn is not None:
            await client._conn.commit()
        await client.close()
        return spawned

    spawned = asyncio.run(run())

    with psycopg.connect(db_dsn, autocommit=True) as check_conn:
        task_state, task_attempts, _ = _fetch_task(check_conn, queue, spawned["task_id"])
        assert task_state == "sleeping"
        assert task_attempts == 1

        run_state, available_at, failure_reason = _fetch_run_with_failure(
            check_conn, queue, spawned["run_id"]
        )
        assert run_state == "sleeping"
        assert failure_reason is None
        assert available_at is not None


def test_async_unknown_task_defer_failure_preserves_error_context(
    db_dsn, queue_name, monkeypatch
):
    queue = queue_name("unknown_async_defer_fail")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    monkeypatch.setattr(absurd_sdk, "_UNKNOWN_TASK_DEFER_BASE_SECONDS", "oops")
    monkeypatch.setattr(absurd_sdk, "_UNKNOWN_TASK_DEFER_JITTER_SECONDS", -1)

    async def run():
        client = AsyncAbsurd(db_dsn, queue_name=queue)
        spawned = await client.spawn(
            "ghost-task",
            {"value": 1},
            queue=queue,
            max_attempts=1,
        )
        await client.work_batch(worker_id="unknown-worker")
        if client._conn is not None:
            await client._conn.commit()
        await client.close()
        return spawned

    spawned = asyncio.run(run())

    with psycopg.connect(db_dsn, autocommit=True) as check_conn:
        task_state, task_attempts, _ = _fetch_task(check_conn, queue, spawned["task_id"])
        assert task_state == "failed"
        assert task_attempts == 1

        run_state, _, failure_reason = _fetch_run_with_failure(
            check_conn, queue, spawned["run_id"]
        )
        assert run_state == "failed"
        assert failure_reason is not None
        assert failure_reason["name"] == "InvalidTextRepresentation"
        assert "invalid input syntax for type integer" in failure_reason["message"]


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
