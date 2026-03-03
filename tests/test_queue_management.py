from datetime import datetime, timedelta, timezone

from psycopg import sql
import pytest

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


def test_queue_name_validation_limits(client):
    max_len_queue = "q" * 57
    client.create_queue(max_len_queue)
    assert max_len_queue in client.list_queues()

    with pytest.raises(Exception):
        client.create_queue("q" * 58)


def test_queue_name_validation_allows_permissive_postgres_names(client):
    for valid_name in ["queue-1", "UpperCase", "bad space", "_bad", "-bad", "bad'quote"]:
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
