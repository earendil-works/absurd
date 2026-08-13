import asyncio

import psycopg

import absurd_sdk


def test_sync_absurd_prefers_absurd_database_url_over_pgdatabase(monkeypatch):
    captured = {}

    def fake_connect(dsn, autocommit=True):
        captured["dsn"] = dsn
        captured["autocommit"] = autocommit
        return object()

    monkeypatch.setenv("ABSURD_DATABASE_URL", "postgresql://localhost/absurd")
    monkeypatch.setenv("PGDATABASE", "postgresql://localhost/other")
    monkeypatch.setattr(absurd_sdk.Connection, "connect", fake_connect)

    absurd_sdk.Absurd()

    assert captured == {
        "dsn": "postgresql://localhost/absurd",
        "autocommit": True,
    }


def test_sync_absurd_falls_back_to_pgdatabase(monkeypatch):
    captured = {}

    def fake_connect(dsn, autocommit=True):
        captured["dsn"] = dsn
        return object()

    monkeypatch.delenv("ABSURD_DATABASE_URL", raising=False)
    monkeypatch.setenv("PGDATABASE", "postgresql://localhost/from-pgdatabase")
    monkeypatch.setattr(absurd_sdk.Connection, "connect", fake_connect)

    absurd_sdk.Absurd()

    assert captured["dsn"] == "postgresql://localhost/from-pgdatabase"


def test_async_absurd_falls_back_to_default_absurd_uri(monkeypatch):
    monkeypatch.delenv("ABSURD_DATABASE_URL", raising=False)
    monkeypatch.delenv("PGDATABASE", raising=False)

    client = absurd_sdk.AsyncAbsurd()

    assert client._conn_string == "postgresql://localhost/absurd"
    assert client._owned_conn is True


def test_async_absurd_reconnects_after_an_interrupted_connection(monkeypatch):
    made = []

    async def connect(dsn, autocommit=True):
        # Connecting to a nonexistent socket fails immediately, producing a
        # real psycopg connection that is `broken`: status BAD without a
        # clean close(), the same state a dropped connection leaves behind.
        made.append(psycopg.AsyncConnection(psycopg.pq.PGconn.connect(b"host=/nonexistent")))
        return made[-1]

    monkeypatch.setattr(absurd_sdk.AsyncConnection, "connect", connect)
    client = absurd_sdk.AsyncAbsurd("postgresql://localhost/absurd")

    async def run():
        await client._ensure_connected()
        await client._ensure_connected()

    asyncio.run(run())

    assert len(made) == 2
    assert client._conn is made[1]
