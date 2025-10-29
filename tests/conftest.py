import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import pytest
from testcontainers.postgres import PostgresContainer


# Set DOCKER_HOST for macOS Docker Desktop if not already set
# This allows testcontainers to find the Docker socket on macOS
# without requiring users to manually set the environment variable.
# Linux users are unaffected as their socket is at the default location.
if "DOCKER_HOST" not in os.environ:
    macos_docker_sock = Path.home() / ".docker" / "run" / "docker.sock"
    if macos_docker_sock.exists():
        os.environ["DOCKER_HOST"] = f"unix://{macos_docker_sock}"


TEST_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = TEST_ROOT.parent
ABSURD_SQL = PROJECT_ROOT / "sql" / "absurd.sql"


def _normalize_dsn(url):
    if url.startswith("postgresql+psycopg2://"):
        return "postgresql://" + url.split("://", 1)[1]
    return url


def _result_columns(result):
    return [col.name for col in result.description]


def _fetchone_dict(result):
    row = result.fetchone()
    if row is None:
        return None
    columns = _result_columns(result)
    return dict(zip(columns, row, strict=False))


def _fetchall_dicts(result):
    rows = result.fetchall()
    if not rows:
        return []
    columns = _result_columns(result)
    return [dict(zip(columns, row, strict=False)) for row in rows]


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
    script = ABSURD_SQL.read_text()
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(script)
    return dsn


@pytest.fixture
def db_connection(db_dsn):
    with psycopg.connect(db_dsn) as conn:
        conn.execute("set timezone to 'UTC'")
        try:
            yield conn
        finally:
            conn.execute("set absurd.fake_now = default")
            conn.rollback()


@pytest.fixture
def client(db_connection):
    helper = AbsurdTestClient(db_connection)
    try:
        yield helper
    finally:
        helper.clear_fake_now()


class SpawnResult:
    def __init__(self, task_id, run_id, attempt):
        self.task_id = task_id
        self.run_id = run_id
        self.attempt = attempt

    def __repr__(self):
        return f"SpawnResult(task_id={self.task_id!r}, run_id={self.run_id!r}, attempt={self.attempt!r})"

    def __eq__(self, other):
        if isinstance(other, SpawnResult):
            return (
                self.task_id == other.task_id
                and self.run_id == other.run_id
                and self.attempt == other.attempt
            )
        return NotImplemented


class AbsurdTestClient:
    def __init__(self, conn):
        self.conn = conn

    def create_queue(self, name):
        self.conn.execute("select absurd.create_queue(%s)", (name,))

    def drop_queue(self, name):
        self.conn.execute("select absurd.drop_queue(%s)", (name,))

    def list_queues(self):
        result = self.conn.execute("select queue_name from absurd.list_queues()")
        return [row[0] for row in result.fetchall()]

    def spawn_task(
        self,
        queue,
        task_name,
        params=None,
        options=None,
    ):
        result = self.conn.execute(
            """
            select task_id, run_id, attempt
            from absurd.spawn_task(%s, %s, %s, %s)
            """,
            (
                queue,
                task_name,
                Jsonb(params),
                Jsonb(options or {}),
            ),
        )
        row = result.fetchone()
        if row is None:
            raise AssertionError("spawn_task returned no rows")
        return SpawnResult(task_id=row[0], run_id=row[1], attempt=row[2])

    def claim_tasks(
        self,
        queue,
        *,
        worker="worker",
        claim_timeout=60,
        qty=1,
    ):
        result = self.conn.execute(
            """
            select run_id, task_id, attempt, task_name, params, retry_strategy,
                   max_attempts, headers, wake_event, event_payload
            from absurd.claim_task(%s, %s, %s, %s)
            """,
            (queue, worker, claim_timeout, qty),
        )
        return _fetchall_dicts(result)

    def complete_run(
        self,
        queue,
        run_id,
        state=None,
    ):
        self.conn.execute(
            "select absurd.complete_run(%s, %s, %s)",
            (queue, run_id, Jsonb(state) if state is not None else None),
        )

    def schedule_run(self, queue, run_id, wake_at):
        self.conn.execute(
            "select absurd.schedule_run(%s, %s, %s)",
            (queue, run_id, wake_at),
        )

    def fail_run(
        self,
        queue,
        run_id,
        reason,
        retry_at=None,
    ):
        self.conn.execute(
            "select absurd.fail_run(%s, %s, %s, %s)",
            (queue, run_id, Jsonb(reason), retry_at),
        )

    def await_event(
        self,
        queue,
        task_id,
        run_id,
        step_name,
        event_name,
        timeout_seconds=None,
    ):
        result = self.conn.execute(
            """
            select should_suspend, payload
            from absurd.await_event(%s, %s, %s, %s, %s, %s)
            """,
            (queue, task_id, run_id, step_name, event_name, timeout_seconds),
        )
        row = _fetchone_dict(result)
        if row is None:
            raise AssertionError("await_event returned no rows")
        return row

    def emit_event(
        self,
        queue,
        event_name,
        payload=None,
    ):
        self.conn.execute(
            "select absurd.emit_event(%s, %s, %s)",
            (queue, event_name, Jsonb(payload)),
        )

    def cleanup_tasks(self, queue, ttl_seconds, limit=1000):
        result = self.conn.execute(
            "select absurd.cleanup_tasks(%s, %s, %s)",
            (queue, ttl_seconds, limit),
        )
        row = result.fetchone()
        return int(row[0]) if row else 0

    def cleanup_events(self, queue, ttl_seconds, limit=1000):
        result = self.conn.execute(
            "select absurd.cleanup_events(%s, %s, %s)",
            (queue, ttl_seconds, limit),
        )
        row = result.fetchone()
        return int(row[0]) if row else 0

    def get_task(self, queue, task_id):
        query = sql.SQL(
            """
            select task_id, task_name, state, attempts, last_attempt_run,
                   completed_payload, cancellation, first_started_at,
                   cancelled_at, enqueue_at
            from absurd.{table}
            where task_id = %s
            """
        ).format(table=self.get_table("t", queue))
        result = self.conn.execute(query, (task_id,))
        return _fetchone_dict(result)

    def get_runs(self, queue, task_id):
        query = sql.SQL(
            """
            select run_id, task_id, attempt, state, available_at, wake_event,
                   event_payload, claimed_by, claim_expires_at, result,
                   failure_reason, started_at, completed_at, failed_at
            from absurd.{table}
            where task_id = %s
            order by attempt
            """
        ).format(table=self.get_table("r", queue))
        result = self.conn.execute(query, (task_id,))
        return _fetchall_dicts(result)

    def get_run(self, queue, run_id):
        query = sql.SQL(
            """
            select run_id, task_id, attempt, state, available_at, wake_event,
                   event_payload, claimed_by, claim_expires_at, result,
                   failure_reason, started_at, completed_at, failed_at
            from absurd.{table}
            where run_id = %s
            """
        ).format(table=self.get_table("r", queue))
        result = self.conn.execute(query, (run_id,))
        return _fetchone_dict(result)

    def get_checkpoint(
        self,
        queue,
        task_id,
        step_name,
    ):
        query = sql.SQL(
            """
            select task_id, checkpoint_name, state, status, owner_run_id, updated_at
            from absurd.{table}
            where task_id = %s and checkpoint_name = %s
            """
        ).format(table=self.get_table("c", queue))
        result = self.conn.execute(query, (task_id, step_name))
        return _fetchone_dict(result)

    def count_tasks(self, queue):
        query = sql.SQL("select count(*) from absurd.{table}").format(
            table=self.get_table("t", queue)
        )
        result = self.conn.execute(query)
        row = result.fetchone()
        return int(row[0]) if row else 0

    def count_events(self, queue):
        query = sql.SQL("select count(*) from absurd.{table}").format(
            table=self.get_table("e", queue)
        )
        result = self.conn.execute(query)
        row = result.fetchone()
        return int(row[0]) if row else 0

    def set_fake_now(self, when):
        if isinstance(when, datetime):
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            when = when.astimezone(timezone.utc).isoformat()
        # SET doesn't support parameterized queries, but the value is already sanitized
        self.conn.execute(sql.SQL("set absurd.fake_now = {}").format(sql.Literal(when)))

    def clear_fake_now(self):
        self.conn.execute("set absurd.fake_now = default")

    def get_table(self, prefix, queue):
        return sql.Identifier(f"{prefix}_{queue}")
