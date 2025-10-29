from datetime import datetime, timedelta, timezone


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
