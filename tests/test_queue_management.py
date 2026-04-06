from datetime import datetime, timedelta, timezone

from psycopg import sql
import pytest


def _get_relkind(conn, relname):
    row = conn.execute(
        """
        select c.relkind
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'absurd'
          and c.relname = %s
        """,
        (relname,),
    ).fetchone()
    return row[0] if row else None


def test_cleanup_tasks_and_events(client):
    queue = "cleanup"
    client.create_queue(queue)

    base = datetime(2024, 3, 1, 8, 0, tzinfo=timezone.utc)
    client.set_fake_now(base)

    client.spawn_task(queue, "to-clean", {"step": "start"})
    claim = client.claim_tasks(queue)[0]

    finish_time = base + timedelta(minutes=10)
    client.set_fake_now(finish_time)
    client.complete_run(queue, claim["run_id"], {"status": "done"})
    client.emit_event(queue, "cleanup-event", {"kind": "notify"})

    run_row = client.get_run(queue, claim["run_id"])
    assert run_row is not None
    assert run_row["claimed_by"] == "worker"
    assert run_row["claim_expires_at"] == base + timedelta(seconds=60)

    # Check that cleanup doesn't happen before TTL expires (TTL is 3600s = 1 hour)
    before_ttl = finish_time + timedelta(minutes=30)
    client.set_fake_now(before_ttl)
    deleted_tasks = client.cleanup_tasks(queue, ttl_seconds=3600, limit=10)
    assert deleted_tasks == 0
    deleted_events = client.cleanup_events(queue, ttl_seconds=3600, limit=10)
    assert deleted_events == 0

    # Now check that cleanup does happen after TTL expires
    later = finish_time + timedelta(hours=26)
    client.set_fake_now(later)
    deleted_tasks = client.cleanup_tasks(queue, ttl_seconds=3600, limit=10)
    assert deleted_tasks == 1
    deleted_events = client.cleanup_events(queue, ttl_seconds=3600, limit=10)
    assert deleted_events == 1

    # Sanity check
    assert client.count_tasks(queue) == 0
    assert client.count_events(queue) == 0


def test_queue_management_round_trip(client):
    client.create_queue("main")
    client.create_queue("main")

    assert client.list_queues() == ["main"]

    client.drop_queue("main")
    assert client.list_queues() == []


def test_queue_storage_mode_defaults_to_unpartitioned(client):
    queue = "mode-default"
    client.create_queue(queue)

    row = client.conn.execute(
        "select storage_mode from absurd.queues where queue_name = %s",
        (queue,),
    ).fetchone()
    assert row is not None
    assert row[0] == "unpartitioned"

    has_idempotency_table = client.conn.execute(
        """
        select 1
        from pg_tables
        where schemaname = 'absurd'
          and tablename = %s
        """,
        (f"i_{queue}",),
    ).fetchone()
    assert has_idempotency_table is None


def test_partitioned_queue_creates_idempotency_registry_table(client):
    queue = "mode-partitioned"
    client.create_queue(queue, storage_mode="partitioned")

    row = client.conn.execute(
        "select storage_mode from absurd.queues where queue_name = %s",
        (queue,),
    ).fetchone()
    assert row is not None
    assert row[0] == "partitioned"

    has_idempotency_table = client.conn.execute(
        """
        select 1
        from pg_tables
        where schemaname = 'absurd'
          and tablename = %s
        """,
        (f"i_{queue}",),
    ).fetchone()
    assert has_idempotency_table is not None


def test_partitioned_queue_creates_partitioned_parents_and_week_partitions(client):
    queue = "mode-partition-ddl"
    client.create_queue(queue, storage_mode="partitioned")

    # Parent tables should be declarative partitioned tables.
    for prefix in ["t", "r", "c", "w"]:
        assert _get_relkind(client.conn, f"{prefix}_{queue}") == "p"

    # Events stay unpartitioned for now.
    assert _get_relkind(client.conn, f"e_{queue}") == "r"

    row = client.conn.execute(
        """
        select
          absurd.partition_week_tag(absurd.current_time()),
          absurd.uuidv7_floor(absurd.week_bucket_utc(absurd.current_time())),
          absurd.uuidv7_floor(absurd.week_bucket_utc(absurd.current_time()) + interval '7 days')
        """
    ).fetchone()
    assert row is not None
    tag, lo, hi = row

    for prefix in ["t", "r", "c", "w"]:
        weekly_name = f"{prefix}_{queue}_{tag}"
        default_name = f"{prefix}_{queue}_d"

        weekly_bound = client.conn.execute(
            """
            select pg_get_expr(c.relpartbound, c.oid)
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = 'absurd'
              and c.relname = %s
            """,
            (weekly_name,),
        ).fetchone()
        assert weekly_bound is not None
        assert str(lo) in weekly_bound[0]
        assert str(hi) in weekly_bound[0]

        default_bound = client.conn.execute(
            """
            select pg_get_expr(c.relpartbound, c.oid)
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = 'absurd'
              and c.relname = %s
            """,
            (default_name,),
        ).fetchone()
        assert default_bound is not None
        assert default_bound[0] == "DEFAULT"


def test_ensure_partitions_can_precreate_future_weeks(client):
    queue = "mode-partition-future"
    base = datetime(2024, 4, 1, 12, 0, tzinfo=timezone.utc)
    future = base + timedelta(days=21)
    client.set_fake_now(base)
    client.create_queue(queue, storage_mode="partitioned")

    future_tag_row = client.conn.execute(
        "select absurd.partition_week_tag(%s)",
        (future,),
    ).fetchone()
    assert future_tag_row is not None
    future_tag = future_tag_row[0]

    # Not created by initial queue setup yet.
    assert _get_relkind(client.conn, f"t_{queue}_{future_tag}") is None

    client.conn.execute("select absurd.ensure_partitions(%s, %s)", (queue, future))

    for prefix in ["t", "r", "c", "w"]:
        assert _get_relkind(client.conn, f"{prefix}_{queue}_{future_tag}") == "r"


def test_partitioned_queue_creation_uses_skew_lookback_window(client):
    queue = "mode-partition-skew"
    boundary = datetime(2024, 4, 1, 0, 30, tzinfo=timezone.utc)
    client.set_fake_now(boundary)
    client.create_queue(queue, storage_mode="partitioned")

    row = client.conn.execute(
        """
        select
          absurd.partition_week_tag(absurd.current_time()),
          absurd.partition_week_tag(absurd.current_time() - interval '1 day')
        """
    ).fetchone()
    assert row is not None
    current_tag, previous_tag = row
    assert previous_tag != current_tag

    for prefix in ["t", "r", "c", "w"]:
        assert _get_relkind(client.conn, f"{prefix}_{queue}_{current_tag}") == "r"
        assert _get_relkind(client.conn, f"{prefix}_{queue}_{previous_tag}") == "r"


def test_create_queue_rejects_existing_partitioned_queue_in_default_mode(client):
    queue = "mode-existing"
    client.create_queue(queue, storage_mode="partitioned")

    with pytest.raises(Exception):
        client.create_queue(queue)


def test_create_queue_with_partitioned_mode_is_idempotent(client):
    queue = "mode-partitioned-idempotent"
    client.create_queue(queue, storage_mode="partitioned")
    client.create_queue(queue, storage_mode="partitioned")

    row = client.conn.execute(
        "select storage_mode from absurd.queues where queue_name = %s",
        (queue,),
    ).fetchone()
    assert row is not None
    assert row[0] == "partitioned"


def test_create_queue_rejects_storage_mode_mismatch(client):
    queue = "mode-mismatch"
    client.create_queue(queue)

    with pytest.raises(Exception):
        client.create_queue(queue, storage_mode="partitioned")


def test_create_queue_rejects_unknown_storage_mode(client):
    with pytest.raises(Exception):
        client.create_queue("mode-unknown", storage_mode="timescale")


def test_queue_name_validation_limits(client):
    max_len_queue = "q" * 57
    client.create_queue(max_len_queue)
    assert max_len_queue in client.list_queues()

    with pytest.raises(Exception):
        client.create_queue("q" * 58)


def test_queue_name_validation_allows_permissive_postgres_names(client):
    for valid_name in [
        "queue-1",
        "UpperCase",
        "bad space",
        "_bad",
        "-bad",
        "bad'quote",
    ]:
        client.create_queue(valid_name)
        assert valid_name in client.list_queues()
        client.drop_queue(valid_name)


def test_queue_name_validation_rejects_only_empty_names(client):
    for invalid_name in ["", "   "]:
        with pytest.raises(Exception):
            client.create_queue(invalid_name)


def test_drop_queue_supports_legacy_overlong_names(client):
    legacy_queue = "q" * 58

    client.conn.execute(
        "insert into absurd.queues (queue_name) values (%s)",
        (legacy_queue,),
    )

    for prefix in ["t", "r", "c", "e", "w"]:
        client.conn.execute(
            sql.SQL("create table absurd.{table} (id integer)").format(
                table=sql.Identifier(f"{prefix}_{legacy_queue}")
            )
        )

    client.drop_queue(legacy_queue)

    assert legacy_queue not in client.list_queues()
