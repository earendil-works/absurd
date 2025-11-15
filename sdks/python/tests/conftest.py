import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

import absurd_sdk


# Ensure Docker socket is discoverable on macOS systems using Docker Desktop.
if "DOCKER_HOST" not in os.environ:
    docker_sock = Path.home() / ".docker" / "run" / "docker.sock"
    if docker_sock.exists():
        os.environ["DOCKER_HOST"] = f"unix://{docker_sock}"


PROJECT_ROOT = Path(__file__).resolve().parents[3]
ABSURD_SQL = PROJECT_ROOT / "sql" / "absurd.sql"


def _normalize_dsn(url: str) -> str:
    if url.startswith("postgresql+psycopg2://"):
        return "postgresql://" + url.split("://", 1)[1]
    return url


@pytest.fixture(scope="session")
def postgres_container():
    container = PostgresContainer("postgres:16-alpine")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def db_dsn(postgres_container):
    dsn = _normalize_dsn(postgres_container.get_connection_url())
    schema = ABSURD_SQL.read_text()
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(schema)
    return dsn


@pytest.fixture
def conn(db_dsn, monkeypatch):
    with psycopg.connect(db_dsn, autocommit=True) as connection:
        connection.execute("set timezone to 'UTC'")
        
        # Store original function
        original_get_current_time = absurd_sdk._get_current_time
        
        # Create a wrapper that checks for fake_now
        def patched_get_current_time():
            result = connection.execute("SELECT current_setting('absurd.fake_now', true)").fetchone()
            fake_now = result[0] if result else None
            if fake_now and fake_now.strip():
                from datetime import datetime
                return datetime.fromisoformat(fake_now)
            return original_get_current_time()
        
        monkeypatch.setattr(absurd_sdk, "_get_current_time", patched_get_current_time)
        
        try:
            yield connection
        finally:
            connection.execute("set absurd.fake_now = default")


@pytest.fixture
def queue_name():
    def factory(prefix="sdk"):
        return f"{prefix}_{uuid4().hex[:8]}"

    return factory
