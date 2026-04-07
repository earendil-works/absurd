from datetime import datetime, timedelta, timezone


def test_list_detach_candidates_respects_detach_policy(client):
    queue = "detach-policy"
    base = datetime(2024, 4, 1, 12, 0, tzinfo=timezone.utc)

    client.set_fake_now(base)
    client.create_queue(queue, storage_mode="partitioned")

    # Default policy does not emit any detach candidates.
    no_candidates = client.conn.execute(
        "select * from absurd.list_detach_candidates(%s)",
        (queue,),
    ).fetchall()
    assert no_candidates == []

    client.conn.execute(
        "select absurd.set_queue_policy(%s, %s::jsonb)",
        (queue, '{"detach_mode": "empty", "detach_min_age": "30 days"}'),
    )

    # Move time forward so old pre-created partitions become detachable.
    client.set_fake_now(base + timedelta(days=120))

    rows = client.conn.execute(
        """
        select queue_name, parent_table, partition_table, detach_sql, drop_sql
        from absurd.list_detach_candidates(%s)
        order by partition_table
        """,
        (queue,),
    ).fetchall()

    assert rows
    for row in rows:
        assert row[0] == queue
        assert row[2].endswith("_d") is False
        assert "detach partition" in row[3].lower()
        assert "concurrently" not in row[3].lower()
        assert row[4].lower().startswith("drop table if exists absurd.")


def test_list_detach_candidates_uses_concurrently_without_default_partition(client):
    queue = "detach-policy-concurrent"
    base = datetime(2024, 4, 1, 12, 0, tzinfo=timezone.utc)

    client.set_fake_now(base)
    client.create_queue(queue, storage_mode="partitioned")
    client.conn.execute(
        "select absurd.set_queue_policy(%s, %s::jsonb)",
        (
            queue,
            '{"detach_mode": "empty", "detach_min_age": "30 days", "default_partition": "disabled"}',
        ),
    )

    client.set_fake_now(base + timedelta(days=120))
    rows = client.conn.execute(
        """
        select partition_table, detach_sql
        from absurd.list_detach_candidates(%s)
        order by partition_table
        """,
        (queue,),
    ).fetchall()

    assert rows
    assert all("concurrently" in row[1].lower() for row in rows)


def test_list_detach_candidates_skips_non_empty_partitions(client):
    queue = "detach-nonempty"
    base = datetime(2024, 4, 8, 9, 0, tzinfo=timezone.utc)

    client.set_fake_now(base)
    client.create_queue(queue, storage_mode="partitioned")

    current_tag = client.conn.execute(
        "select absurd.partition_week_tag(%s)",
        (base,),
    ).fetchone()[0]

    client.spawn_task(queue, "keep-alive", {"x": 1})

    client.conn.execute(
        "select absurd.set_queue_policy(%s, %s::jsonb)",
        (queue, '{"detach_mode": "empty", "detach_min_age": "0 days"}'),
    )

    client.set_fake_now(base + timedelta(days=60))

    rows = client.conn.execute(
        """
        select parent_table, partition_table
        from absurd.list_detach_candidates(%s)
        where parent_table in (%s, %s)
        """,
        (queue, f"t_{queue}", f"r_{queue}"),
    ).fetchall()

    partition_names = {row[1] for row in rows}

    # The partition holding live task/run rows is not eligible while non-empty.
    assert f"t_{queue}_{current_tag}" not in partition_names
    assert f"r_{queue}_{current_tag}" not in partition_names
