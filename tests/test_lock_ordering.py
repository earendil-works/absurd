import threading
import time

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb


def _set_application_name(conn, name):
    conn.execute(sql.SQL("set application_name = {}").format(sql.Literal(name)))


def _wait_for_lock_wait(db_dsn, application_name, timeout_seconds=5.0):
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        conn.execute("set timezone to 'UTC'")
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            row = conn.execute(
                """
                select wait_event_type
                from pg_stat_activity
                where application_name = %s
                """,
                (application_name,),
            ).fetchone()
            if row and row[0] == "Lock":
                return
            time.sleep(0.01)

    raise AssertionError(f"{application_name} did not block on a lock")


def _create_running_task(db_dsn, queue, *, options=None):
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        conn.execute("set timezone to 'UTC'")
        conn.execute("select absurd.create_queue(%s)", (queue,))

        spawn_row = conn.execute(
            """
            select task_id, run_id
            from absurd.spawn_task(%s, %s, %s, %s)
            """,
            (queue, "lock-order-task", Jsonb({"value": 1}), Jsonb(options or {})),
        ).fetchone()
        assert spawn_row is not None
        task_id, run_id = spawn_row

        claim_row = conn.execute(
            """
            select run_id
            from absurd.claim_task(%s, %s, %s, %s)
            """,
            (queue, "worker", 60, 1),
        ).fetchone()
        assert claim_row is not None
        assert claim_row[0] == run_id

    return task_id, run_id


def test_cancel_and_complete_run_do_not_deadlock(db_dsn):
    queue = "lock_order_complete"
    task_id, run_id = _create_running_task(db_dsn, queue)

    lock_conn = None
    cancel_done = threading.Event()
    cancel_result = {}

    try:
        lock_conn = psycopg.connect(db_dsn)
        lock_conn.execute("set timezone to 'UTC'")
        _set_application_name(lock_conn, "absurd-lock-order-locker-complete")
        lock_conn.execute("set deadlock_timeout = '100ms'")

        lock_conn.execute(
            sql.SQL("select 1 from absurd.{r} where run_id = %s for update").format(
                r=sql.Identifier(f"r_{queue}")
            ),
            (run_id,),
        )

        def cancel_worker():
            try:
                with psycopg.connect(db_dsn, autocommit=True) as cancel_conn:
                    cancel_conn.execute("set timezone to 'UTC'")
                    _set_application_name(
                        cancel_conn, "absurd-lock-order-cancel-complete"
                    )
                    cancel_conn.execute("set deadlock_timeout = '100ms'")
                    cancel_conn.execute("set statement_timeout = '5s'")
                    cancel_conn.execute(
                        "select absurd.cancel_task(%s, %s)",
                        (queue, task_id),
                    )
            except Exception as exc:  # pragma: no cover - surfaced below
                cancel_result["exc"] = exc
            finally:
                cancel_done.set()

        t = threading.Thread(target=cancel_worker, daemon=True)
        t.start()

        _wait_for_lock_wait(db_dsn, "absurd-lock-order-cancel-complete")

        lock_conn.execute("set statement_timeout = '5s'")
        lock_conn.execute(
            "select absurd.complete_run(%s, %s, %s)",
            (queue, run_id, None),
        )
        lock_conn.commit()

        assert cancel_done.wait(5.0), "cancel_task did not finish"
        if "exc" in cancel_result:
            raise cancel_result["exc"]

        with psycopg.connect(db_dsn, autocommit=True) as check_conn:
            task_state = check_conn.execute(
                sql.SQL("select state from absurd.{t} where task_id = %s").format(
                    t=sql.Identifier(f"t_{queue}")
                ),
                (task_id,),
            ).fetchone()[0]
            run_state = check_conn.execute(
                sql.SQL("select state from absurd.{r} where run_id = %s").format(
                    r=sql.Identifier(f"r_{queue}")
                ),
                (run_id,),
            ).fetchone()[0]

        assert task_state == "completed"
        assert run_state == "completed"
    finally:
        if lock_conn is not None:
            try:
                lock_conn.rollback()
            except Exception:
                pass
            lock_conn.close()

        with psycopg.connect(db_dsn, autocommit=True) as cleanup_conn:
            cleanup_conn.execute("set timezone to 'UTC'")
            cleanup_conn.execute("select absurd.drop_queue(%s)", (queue,))


def test_cancel_and_fail_run_do_not_deadlock(db_dsn):
    queue = "lock_order_fail"
    task_id, run_id = _create_running_task(db_dsn, queue, options={"max_attempts": 1})

    lock_conn = None
    cancel_done = threading.Event()
    cancel_result = {}

    try:
        lock_conn = psycopg.connect(db_dsn)
        lock_conn.execute("set timezone to 'UTC'")
        _set_application_name(lock_conn, "absurd-lock-order-locker-fail")
        lock_conn.execute("set deadlock_timeout = '100ms'")

        lock_conn.execute(
            sql.SQL("select 1 from absurd.{r} where run_id = %s for update").format(
                r=sql.Identifier(f"r_{queue}")
            ),
            (run_id,),
        )

        def cancel_worker():
            try:
                with psycopg.connect(db_dsn, autocommit=True) as cancel_conn:
                    cancel_conn.execute("set timezone to 'UTC'")
                    _set_application_name(cancel_conn, "absurd-lock-order-cancel-fail")
                    cancel_conn.execute("set deadlock_timeout = '100ms'")
                    cancel_conn.execute("set statement_timeout = '5s'")
                    cancel_conn.execute(
                        "select absurd.cancel_task(%s, %s)",
                        (queue, task_id),
                    )
            except Exception as exc:  # pragma: no cover - surfaced below
                cancel_result["exc"] = exc
            finally:
                cancel_done.set()

        t = threading.Thread(target=cancel_worker, daemon=True)
        t.start()

        _wait_for_lock_wait(db_dsn, "absurd-lock-order-cancel-fail")

        lock_conn.execute("set statement_timeout = '5s'")
        lock_conn.execute(
            "select absurd.fail_run(%s, %s, %s, %s)",
            (queue, run_id, Jsonb({"message": "boom"}), None),
        )
        lock_conn.commit()

        assert cancel_done.wait(5.0), "cancel_task did not finish"
        if "exc" in cancel_result:
            raise cancel_result["exc"]

        with psycopg.connect(db_dsn, autocommit=True) as check_conn:
            task_state = check_conn.execute(
                sql.SQL("select state from absurd.{t} where task_id = %s").format(
                    t=sql.Identifier(f"t_{queue}")
                ),
                (task_id,),
            ).fetchone()[0]
            run_state = check_conn.execute(
                sql.SQL("select state from absurd.{r} where run_id = %s").format(
                    r=sql.Identifier(f"r_{queue}")
                ),
                (run_id,),
            ).fetchone()[0]

        assert task_state == "failed"
        assert run_state == "failed"
    finally:
        if lock_conn is not None:
            try:
                lock_conn.rollback()
            except Exception:
                pass
            lock_conn.close()

        with psycopg.connect(db_dsn, autocommit=True) as cleanup_conn:
            cleanup_conn.execute("set timezone to 'UTC'")
            cleanup_conn.execute("select absurd.drop_queue(%s)", (queue,))
