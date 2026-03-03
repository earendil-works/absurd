import asyncio

import pytest

import absurd_sdk
from absurd_sdk import Absurd, AsyncAbsurd


def test_sync_constructor_fails_before_connect_on_invalid_queue(monkeypatch):
    called = False

    def fake_connect(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("Connection.connect should not be called")

    monkeypatch.setattr(absurd_sdk.Connection, "connect", fake_connect)

    with pytest.raises(ValueError, match="Queue name must be provided"):
        Absurd(None, queue_name="   ")

    assert called is False


def test_sync_queue_validation_is_permissive_but_bounded(conn):
    client = Absurd(conn, queue_name="default")

    permissive_name = "Queue Name-1"
    client.create_queue(permissive_name)
    client.emit_event("event", {"value": 1}, queue_name=permissive_name)
    client.drop_queue(permissive_name)

    with pytest.raises(ValueError, match="Queue name must be provided"):
        client.create_queue("   ")

    with pytest.raises(ValueError, match="too long"):
        client.drop_queue("q" * 58)


def test_async_queue_validation_is_permissive_but_bounded(db_dsn):
    async def run():
        client = AsyncAbsurd(db_dsn, queue_name="default")

        permissive_name = "Queue Name-2"
        await client.create_queue(permissive_name)
        await client.emit_event("event", {"value": 1}, queue_name=permissive_name)
        await client.drop_queue(permissive_name)

        with pytest.raises(ValueError, match="Queue name must be provided"):
            await client.create_queue("\t")

        with pytest.raises(ValueError, match="too long"):
            await client.emit_event("event", {"value": 1}, queue_name="q" * 58)

        await client.close()

    asyncio.run(run())
