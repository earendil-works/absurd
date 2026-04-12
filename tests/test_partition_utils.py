from datetime import datetime, timezone


def test_uuidv7_timestamp_extracts_timestamp(client):
    frozen = datetime(2024, 4, 1, 10, 20, 30, 123000, tzinfo=timezone.utc)
    client.set_fake_now(frozen)

    row = client.conn.execute(
        "select absurd.uuidv7_timestamp(absurd.portable_uuidv7())"
    ).fetchone()

    assert row is not None
    assert row[0] == frozen


def test_uuidv7_timestamp_returns_null_for_non_v7(client):
    row = client.conn.execute(
        "select absurd.uuidv7_timestamp(uuid_generate_v4())"
    ).fetchone()

    assert row is not None
    assert row[0] is None


def test_uuidv7_floor_rounds_down_to_millisecond(client):
    ts = datetime(2024, 4, 1, 10, 20, 30, 123789, tzinfo=timezone.utc)
    row = client.conn.execute(
        "select absurd.uuidv7_timestamp(absurd.uuidv7_floor(%s))",
        (ts,),
    ).fetchone()

    assert row is not None
    assert row[0] == datetime(2024, 4, 1, 10, 20, 30, 123000, tzinfo=timezone.utc)


def test_uuidv7_floor_is_lower_bound_for_same_millisecond(client):
    frozen = datetime(2024, 4, 1, 10, 20, 30, 123456, tzinfo=timezone.utc)
    client.set_fake_now(frozen)

    row = client.conn.execute(
        """
        with sample as (
          select absurd.portable_uuidv7() as u
        )
        select
          u >= absurd.uuidv7_floor(%s) as ge_lo,
          u < absurd.uuidv7_floor(%s + interval '1 millisecond') as lt_hi
        from sample
        """,
        (frozen, frozen),
    ).fetchone()

    assert row is not None
    assert row[0] is True
    assert row[1] is True


def test_week_bucket_utc_snaps_to_monday_start(client):
    row = client.conn.execute(
        "select absurd.week_bucket_utc(%s)",
        (datetime(2024, 4, 3, 15, 30, tzinfo=timezone.utc),),
    ).fetchone()

    assert row is not None
    assert row[0] == datetime(2024, 4, 1, 0, 0, tzinfo=timezone.utc)


def test_partition_week_tag_uses_iso_year_at_start_boundary(client):
    row = client.conn.execute(
        "select absurd.partition_week_tag(%s)",
        (datetime(2021, 1, 1, 12, 0, tzinfo=timezone.utc),),
    ).fetchone()

    assert row is not None
    assert row[0] == "053"
    assert row[0][1:] != "00"


def test_partition_week_tag_uses_iso_year_at_end_boundary(client):
    row = client.conn.execute(
        "select absurd.partition_week_tag(%s)",
        (datetime(2024, 12, 31, 12, 0, tzinfo=timezone.utc),),
    ).fetchone()

    assert row is not None
    assert row[0] == "501"
