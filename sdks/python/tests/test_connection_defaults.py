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
