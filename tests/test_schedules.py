from datetime import datetime, timedelta, timezone
from psycopg.sql import SQL


def test_parse_cron_field_wildcard(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('*', 0, 59)"
    ).fetchone()[0]
    assert result == list(range(0, 60))


def test_parse_cron_field_exact(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('5', 0, 59)"
    ).fetchone()[0]
    assert result == [5]


def test_parse_cron_field_step(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('*/15', 0, 59)"
    ).fetchone()[0]
    assert result == [0, 15, 30, 45]


def test_parse_cron_field_range(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('9-12', 0, 23)"
    ).fetchone()[0]
    assert result == [9, 10, 11, 12]


def test_parse_cron_field_range_step(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('9-17/2', 0, 23)"
    ).fetchone()[0]
    assert result == [9, 11, 13, 15, 17]


def test_parse_cron_field_list(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('1,3,5', 0, 6)"
    ).fetchone()[0]
    assert result == [1, 3, 5]


def test_parse_cron_field_list_with_range(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('1,3-5,7', 0, 10)"
    ).fetchone()[0]
    assert result == [1, 3, 4, 5, 7]


def test_parse_cron_field_day_names(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('MON-FRI', 0, 6)"
    ).fetchone()[0]
    assert result == [1, 2, 3, 4, 5]


def test_parse_cron_field_month_names(client):
    result = client.conn.execute(
        "select absurd.parse_cron_field('JAN,JUN,DEC', 1, 12)"
    ).fetchone()[0]
    assert result == [1, 6, 12]
