import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import pytest


def test_await_and_emit_event_flow(client):
    queue = "events"
    client.create_queue(queue)

    now = datetime(2024, 2, 1, 12, 0, tzinfo=timezone.utc)
    client.set_fake_now(now)

    spawn = client.spawn_task(queue, "waiter", {"step": 1})

    claimed = client.claim_tasks(queue)[0]
    assert claimed["run_id"] == spawn.run_id

    event_name = "test-event"
    response = client.await_event(
        queue,
        spawn.task_id,
        claimed["run_id"],
        step_name="wait",
        event_name=event_name,
        timeout_seconds=60,
    )
    assert response["should_suspend"] is True
    assert response["payload"] is None

    sleeping_run = client.get_run(queue, claimed["run_id"])
    assert sleeping_run is not None
    assert sleeping_run["state"] == "sleeping"
    assert sleeping_run["wake_event"] == event_name

    payload = {"value": 42}
    client.emit_event(queue, event_name, payload)

    pending_run = client.get_run(queue, claimed["run_id"])
    assert pending_run is not None
    assert pending_run["state"] == "pending"

    replay_claim = client.claim_tasks(queue)[0]
    assert replay_claim["run_id"] == spawn.run_id

    resume = client.await_event(
        queue,
        spawn.task_id,
        replay_claim["run_id"],
        step_name="wait",
        event_name=event_name,
        timeout_seconds=60,
    )
    assert resume["should_suspend"] is False
    assert resume["payload"] == payload

    checkpoint = client.get_checkpoint(queue, spawn.task_id, "wait")
    assert checkpoint is not None
    assert checkpoint["state"] == payload


def test_await_event_timeout_does_not_recreate_wait(client):
    """
    When a timeout expires and the run resumes, await_event() should detect
    the expired timeout and return immediately without creating a new wait
    registration (which would cause an infinite loop).
    """
    queue = "timeout-no-loop"
    client.create_queue(queue)

    base = datetime(2024, 5, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    # Spawn and claim
    spawn = client.spawn_task(queue, "waiter", {"step": 1})
    claim = client.claim_tasks(queue)[0]
    run_id = claim["run_id"]

    event = "foo"
    # First await registers a wait with a 10s timeout and suspends
    resp1 = client.await_event(queue, spawn.task_id, run_id, "wait", event, 10)
    assert resp1["should_suspend"] is True
    assert resp1["payload"] is None

    # Wait table has one row
    wtbl = client.get_table("w", queue)
    wait_count1 = client.conn.execute(
        sql.SQL("select count(*) from absurd.{t}").format(t=wtbl)
    ).fetchone()[0]
    assert wait_count1 == 1

    # Advance time past timeout and resume
    client.set_fake_now(base + timedelta(seconds=15))
    resumed = client.claim_tasks(queue)[0]
    assert resumed["run_id"] == run_id

    # After resume, the expired wait should be cleaned up
    wait_count_after_resume = client.conn.execute(
        sql.SQL("select count(*) from absurd.{t}").format(t=wtbl)
    ).fetchone()[0]
    assert wait_count_after_resume == 0

    # Calling await_event again MUST NOT create a new wait; it should
    # return immediately as a timeout (no suspend).
    resp2 = client.await_event(queue, spawn.task_id, run_id, "wait", event, 10)
    assert resp2["should_suspend"] is False
    assert resp2["payload"] is None

    # Verify the run remains running; no re-sleep occurred
    run = client.get_run(queue, run_id)
    assert run is not None
    assert run["state"] == "running"

    # And the wait table remains empty (no new registration)
    wait_count_final = client.conn.execute(
        sql.SQL("select count(*) from absurd.{t}").format(t=wtbl)
    ).fetchone()[0]
    assert wait_count_final == 0


def test_await_emit_event_race_does_not_lose_wakeup(db_dsn):
    """
    Regression test for the "lost wakeup" race between await_event() and emit_event().

    We make the race deterministic by:
    - pre-creating a dummy wait row for (run_id, step_name)
    - holding a row lock on it so await_event blocks in the UPSERT path
    - trying to emit the event while await_event is blocked (should block too)
    """
    queue = "event-race-gate"
    event_name = "race-event"
    payload = {"value": 42}
    now = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)

    def set_fake_now(conn):
        conn.execute(
            sql.SQL("set absurd.fake_now = {}").format(sql.Literal(now.isoformat()))
        )

    def set_application_name(conn, name):
        conn.execute(sql.SQL("set application_name = {}").format(sql.Literal(name)))

    wtbl = sql.Identifier(f"w_{queue}")
    rtbl = sql.Identifier(f"r_{queue}")

    try:
        with psycopg.connect(db_dsn, autocommit=True) as conn:
            conn.execute("set timezone to 'UTC'")
            set_fake_now(conn)
            set_application_name(conn, "absurd-setup")

            conn.execute("select absurd.create_queue(%s)", (queue,))
            spawn = conn.execute(
                """
                select task_id, run_id
                from absurd.spawn_task(%s, %s, %s, %s)
                """,
                (queue, "waiter", Jsonb({"step": 1}), Jsonb({})),
            ).fetchone()
            assert spawn is not None
            task_id, run_id = spawn

            claim = conn.execute(
                """
                select run_id, task_id
                from absurd.claim_task(%s, %s, %s, %s)
                """,
                (queue, "worker", 60, 1),
            ).fetchone()
            assert claim is not None
            assert claim[0] == run_id
            assert claim[1] == task_id

            # Create a dummy wait row so await_event hits the UPDATE path and can block.
            conn.execute(
                sql.SQL(
                    """
                    insert into absurd.{w} (task_id, run_id, step_name, event_name, timeout_at)
                    values (%s, %s, %s, %s, null)
                    """
                ).format(w=wtbl),
                (task_id, run_id, "wait", "dummy"),
            )

        lock_conn = psycopg.connect(db_dsn)
        lock_conn.execute("set timezone to 'UTC'")
        set_fake_now(lock_conn)
        set_application_name(lock_conn, "absurd-locker")
        lock_conn.execute(
            sql.SQL(
                "select 1 from absurd.{w} where run_id = %s and step_name = %s for update"
            ).format(w=wtbl),
            (run_id, "wait"),
        )

        await_result = {}
        await_done = threading.Event()

        def await_worker():
            try:
                with psycopg.connect(db_dsn, autocommit=True) as await_conn:
                    await_conn.execute("set timezone to 'UTC'")
                    set_fake_now(await_conn)
                    set_application_name(await_conn, "absurd-await-race")
                    row = await_conn.execute(
                        """
                        select should_suspend, payload
                        from absurd.await_event(%s, %s, %s, %s, %s, %s)
                        """,
                        (queue, task_id, run_id, "wait", event_name, None),
                    ).fetchone()
                    await_result["row"] = row
            except Exception as e:
                await_result["exc"] = e
            finally:
                await_done.set()

        t = threading.Thread(target=await_worker, daemon=True)
        t.start()

        # Wait until await_event is blocked on a lock (the w_<queue> row lock).
        with psycopg.connect(db_dsn, autocommit=True) as mon_conn:
            mon_conn.execute("set timezone to 'UTC'")
            set_application_name(mon_conn, "absurd-monitor")
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                row = mon_conn.execute(
                    """
                    select wait_event_type
                    from pg_stat_activity
                    where application_name = %s
                    """,
                    ("absurd-await-race",),
                ).fetchone()
                if row and row[0] == "Lock":
                    break
                time.sleep(0.01)
            else:
                raise AssertionError("await_event did not block as expected")

        # While await_event is blocked, emit_event should block on the event-row lock.
        with psycopg.connect(db_dsn, autocommit=True) as emit_conn:
            emit_conn.execute("set timezone to 'UTC'")
            set_fake_now(emit_conn)
            set_application_name(emit_conn, "absurd-emit")
            emit_conn.execute("set statement_timeout = '200ms'")
            with pytest.raises(psycopg.errors.QueryCanceled):
                emit_conn.execute(
                    "select absurd.emit_event(%s, %s, %s)",
                    (queue, event_name, Jsonb(payload)),
                )
            emit_conn.execute("set statement_timeout = 0")

        # Let await_event proceed; it should suspend (no event delivered yet).
        lock_conn.rollback()
        lock_conn.close()

        assert await_done.wait(5.0), "await_event did not finish"
        if "exc" in await_result:
            raise await_result["exc"]

        should_suspend, got_payload = await_result["row"]
        assert should_suspend is True
        assert got_payload is None

        # Now emit for real; it must wake the sleeping run and create the checkpoint.
        with psycopg.connect(db_dsn, autocommit=True) as emit_conn:
            emit_conn.execute("set timezone to 'UTC'")
            set_fake_now(emit_conn)
            set_application_name(emit_conn, "absurd-emit-final")
            emit_conn.execute(
                "select absurd.emit_event(%s, %s, %s)",
                (queue, event_name, Jsonb(payload)),
            )

        with psycopg.connect(db_dsn, autocommit=True) as check_conn:
            check_conn.execute("set timezone to 'UTC'")
            set_fake_now(check_conn)
            set_application_name(check_conn, "absurd-check")

            state = check_conn.execute(
                sql.SQL("select state from absurd.{r} where run_id = %s").format(
                    r=rtbl
                ),
                (run_id,),
            ).fetchone()[0]
            assert state == "pending"

            claim2 = check_conn.execute(
                """
                select run_id
                from absurd.claim_task(%s, %s, %s, %s)
                """,
                (queue, "worker", 60, 1),
            ).fetchone()
            assert claim2 is not None
            assert claim2[0] == run_id

            resume = check_conn.execute(
                """
                select should_suspend, payload
                from absurd.await_event(%s, %s, %s, %s, %s, %s)
                """,
                (queue, task_id, run_id, "wait", event_name, None),
            ).fetchone()
            assert resume[0] is False
            assert resume[1] == payload
    finally:
        with psycopg.connect(db_dsn, autocommit=True) as cleanup_conn:
            cleanup_conn.execute("set timezone to 'UTC'")
            cleanup_conn.execute("select absurd.drop_queue(%s)", (queue,))
