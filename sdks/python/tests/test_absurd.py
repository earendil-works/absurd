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
