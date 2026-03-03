from datetime import datetime, timedelta, timezone

from psycopg import sql


def _index_exists(conn, table_name, index_name):
    row = conn.execute(
        """
        select 1
        from pg_indexes
        where schemaname = 'absurd'
          and tablename = %s
          and indexname = %s
        """,
        (table_name, index_name),
    ).fetchone()
    return row is not None


def _explain(conn, query, params=()):
    result = conn.execute(query, params)
    return "\n".join(row[0] for row in result.fetchall())


def test_new_queue_creates_performance_indexes(client):
    queue = "index_create"
    client.create_queue(queue)

    assert _index_exists(client.conn, f"r_{queue}", f"r_{queue}_cei")
    assert _index_exists(client.conn, f"w_{queue}", f"w_{queue}_ti")
    assert _index_exists(client.conn, f"e_{queue}", f"e_{queue}_eai")


def test_lease_expiry_scan_uses_claim_expiry_index(client):
    queue = "index_lease"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "task", {"value": 1})
    claim = client.claim_tasks(queue, worker="worker-1", claim_timeout=60)[0]
    assert claim["task_id"] == spawn.task_id

    run = client.get_run(queue, claim["run_id"])
    assert run is not None
    cutoff = run["claim_expires_at"]

    client.conn.execute("set enable_seqscan = off")
    plan = _explain(
        client.conn,
        sql.SQL(
            """
            explain select run_id
            from absurd.{r}
            where state = 'running'
              and claim_expires_at is not null
              and claim_expires_at <= %s
            for update skip locked
            """
        ).format(r=client.get_table("r", queue)),
        (cutoff,),
    )

    assert f"r_{queue}_cei" in plan


def test_event_ttl_ordering_uses_emitted_at_index(client):
    queue = "index_events"
    client.create_queue(queue)

    base = datetime(2024, 7, 1, 12, 0, tzinfo=timezone.utc)
    for offset in range(3):
        client.set_fake_now(base + timedelta(seconds=offset))
        client.emit_event(queue, f"event-{offset}", {"value": offset})

    client.conn.execute("set enable_seqscan = off")
    plan = _explain(
        client.conn,
        sql.SQL(
            """
            explain select event_name
            from absurd.{e}
            where emitted_at < %s
            order by emitted_at
            limit 10
            """
        ).format(e=client.get_table("e", queue)),
        (base + timedelta(minutes=1),),
    )

    assert f"e_{queue}_eai" in plan


def test_wait_cleanup_delete_uses_task_id_index(client):
    queue = "index_wait_cleanup"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "waiter", {"value": 1})
    claim = client.claim_tasks(queue, worker="worker-1", claim_timeout=60)[0]

    response = client.await_event(
        queue,
        spawn.task_id,
        claim["run_id"],
        step_name="wait-step",
        event_name="some-event",
        timeout_seconds=60,
    )
    assert response["should_suspend"] is True

    client.conn.execute("set enable_seqscan = off")
    plan = _explain(
        client.conn,
        sql.SQL(
            """
            explain delete from absurd.{w}
            where task_id = %s
            """
        ).format(w=client.get_table("w", queue)),
        (spawn.task_id,),
    )

    assert f"w_{queue}_ti" in plan
