from pathlib import Path
from urllib.error import URLError
import runpy

import pytest


ABSURDCTL_PATH = Path(__file__).resolve().parents[1] / "absurdctl"
MODULE = runpy.run_path(str(ABSURDCTL_PATH))
validate_queue_name = MODULE["validate_queue_name"]
cmd_emit_event = MODULE["cmd_emit_event"]
cmd_retry_task = MODULE["cmd_retry_task"]
cmd_migrate = MODULE["cmd_migrate"]
cmd_schema_version = MODULE["cmd_schema_version"]
parse_migration_filename = MODULE["parse_migration_filename"]
resolve_migration_path = MODULE["resolve_migration_path"]
discover_remote_migrations = MODULE["discover_remote_migrations"]
RemoteMigrationDiscoveryError = MODULE["RemoteMigrationDiscoveryError"]


@pytest.mark.parametrize(
    "queue_name",
    [
        "default",
        "1jobs",
        "queue_1",
        "queue-1",
        "UpperCase",
        "queue123",
        "9",
        "_bad",
        "-bad",
        "bad space",
        "bad'quote",
    ],
)
def test_validate_queue_name_accepts_supported_names(queue_name):
    assert validate_queue_name(queue_name) == queue_name


@pytest.mark.parametrize(
    "queue_name",
    [
        "",
        "   ",
        "a" * 58,
    ],
)
def test_validate_queue_name_rejects_unsupported_names(queue_name):
    with pytest.raises(SystemExit):
        validate_queue_name(queue_name)


def test_emit_event_uses_parameterized_query(monkeypatch):
    captured = {}

    def fake_run_psql(config, query=None, **kwargs):
        captured["query"] = query
        captured["variables"] = kwargs.get("variables")
        return ""

    monkeypatch.setitem(cmd_emit_event.__globals__, "run_psql", fake_run_psql)
    monkeypatch.setitem(
        cmd_emit_event.__globals__, "ensure_queue_exists", lambda *_: None
    )

    cmd_emit_event(["-q", "default", "order.completed", "-P", "note=Bob's"])

    assert (
        captured["query"]
        == "SELECT absurd.emit_event(:'queue', :'event_name', :'payload_json'::jsonb);"
    )
    assert captured["variables"]["queue"] == "default"
    assert captured["variables"]["event_name"] == "order.completed"
    assert captured["variables"]["payload_json"] == '{"note": "Bob\'s"}'


def test_emit_event_validates_queue_name(monkeypatch):
    monkeypatch.setitem(
        cmd_emit_event.__globals__, "ensure_queue_exists", lambda *_: None
    )

    with pytest.raises(SystemExit):
        cmd_emit_event(["-q", "a" * 58, "order.completed"])


def test_retry_task_uses_parameterized_query(monkeypatch):
    captured = {}

    def fake_run_psql_csv(config, query=None, **kwargs):
        captured["query"] = query
        captured["variables"] = kwargs.get("variables")
        return [
            [
                "019a32d3-8425-7ae2-a5af-2f17a6707666",
                "019a32d3-8425-7ae2-a5af-2f17a6707667",
                "2",
                "f",
            ]
        ]

    monkeypatch.setitem(cmd_retry_task.__globals__, "run_psql_csv", fake_run_psql_csv)

    cmd_retry_task(
        [
            "-q",
            "default",
            "019a32d3-8425-7ae2-a5af-2f17a6707666",
            "--max-attempts",
            "5",
        ]
    )

    assert (
        "FROM absurd.retry_task(:'queue', :'task_id', :'options_json'::jsonb)"
        in captured["query"]
    )
    assert captured["variables"]["queue"] == "default"
    assert captured["variables"]["task_id"] == "019a32d3-8425-7ae2-a5af-2f17a6707666"
    assert captured["variables"]["options_json"] == '{"max_attempts": 5}'


def test_retry_task_defaults_to_inplace_retry(monkeypatch):
    captured = {}

    def fake_run_psql_csv(config, query=None, **kwargs):
        captured["variables"] = kwargs.get("variables")
        return [
            [
                "019a32d3-8425-7ae2-a5af-2f17a6707666",
                "019a32d3-8425-7ae2-a5af-2f17a6707667",
                "2",
                "f",
            ]
        ]

    monkeypatch.setitem(cmd_retry_task.__globals__, "run_psql_csv", fake_run_psql_csv)

    cmd_retry_task(["019a32d3-8425-7ae2-a5af-2f17a6707666"])

    assert captured["variables"]["options_json"] == "{}"


def test_retry_task_allows_spawn_new_with_max_attempts(monkeypatch):
    captured = {}

    def fake_run_psql_csv(config, query=None, **kwargs):
        captured["variables"] = kwargs.get("variables")
        return [
            [
                "019a32d3-8425-7ae2-a5af-2f17a6707666",
                "019a32d3-8425-7ae2-a5af-2f17a6707667",
                "1",
                "t",
            ]
        ]

    monkeypatch.setitem(cmd_retry_task.__globals__, "run_psql_csv", fake_run_psql_csv)

    cmd_retry_task(
        [
            "019a32d3-8425-7ae2-a5af-2f17a6707666",
            "--max-attempts",
            "5",
            "--spawn-new",
        ]
    )

    assert (
        captured["variables"]["options_json"]
        == '{"spawn_new": true, "max_attempts": 5}'
    )


def test_parse_migration_filename_handles_supported_names():
    assert parse_migration_filename("0.1.1-main.sql") == ("0.1.1", "main")
    assert parse_migration_filename("0.1.1-0.1.2.sql") == ("0.1.1", "0.1.2")
    assert parse_migration_filename("not-a-migration.txt") is None


def test_resolve_migration_path_finds_chain():
    migrations = {
        ("0.1.0", "0.1.1"): {"name": "0.1.0-0.1.1.sql", "source": "local"},
        ("0.1.1", "main"): {"name": "0.1.1-main.sql", "source": "local"},
    }

    path = resolve_migration_path("0.1.0", "main", migrations)
    assert [item["name"] for item in path] == ["0.1.0-0.1.1.sql", "0.1.1-main.sql"]


def test_resolve_migration_path_allows_gaps_as_null_migrations():
    migrations = {
        ("0.0.3", "0.0.4"): {"name": "0.0.3-0.0.4.sql", "source": "local"},
        ("0.0.4", "0.0.5"): {"name": "0.0.4-0.0.5.sql", "source": "local"},
        ("0.0.8", "0.1.0"): {"name": "0.0.8-0.1.0.sql", "source": "local"},
        ("0.1.0", "0.1.1"): {"name": "0.1.0-0.1.1.sql", "source": "local"},
    }

    path = resolve_migration_path("0.0.1", "0.1.1", migrations)
    assert [item["name"] for item in path] == [
        "0.0.3-0.0.4.sql",
        "0.0.4-0.0.5.sql",
        "0.0.8-0.1.0.sql",
        "0.1.0-0.1.1.sql",
    ]


def test_schema_version_command_prints_unknown(monkeypatch, capsys):
    monkeypatch.setitem(
        cmd_schema_version.__globals__,
        "get_recorded_schema_version",
        lambda config: None,
    )

    cmd_schema_version([])
    out = capsys.readouterr().out.strip()
    assert out == "unknown"


def test_migrate_requires_from_for_legacy_steps(monkeypatch):
    applied = []

    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "get_recorded_schema_version",
        lambda config: "0.1.0",
    )
    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "discover_migrations",
        lambda script_dir, target_ref, include_remote=True: {
            ("0.1.0", "0.1.1"): {
                "name": "0.1.0-0.1.1.sql",
                "source": "local",
                "sql": "-- migration a (legacy, no schema version fn)",
            },
            ("0.1.1", "main"): {
                "name": "0.1.1-main.sql",
                "source": "local",
                "sql": """
                    create or replace function absurd.get_schema_version ()
                      returns text
                      language sql
                    as $$
                      select 'main'::text;
                    $$;
                """,
            },
        },
    )

    def fake_run_psql(config, query=None, **kwargs):
        if kwargs.get("input_data"):
            applied.append(kwargs["input_data"])
        return ""

    monkeypatch.setitem(cmd_migrate.__globals__, "run_psql", fake_run_psql)

    with pytest.raises(SystemExit):
        cmd_migrate([])

    assert applied == []


def test_migrate_applies_migrations_with_from_and_validates_version_progress(
    monkeypatch,
):
    applied = []
    observed_version_checks = []

    def fake_get_recorded_schema_version(config):
        observed_version_checks.append(True)
        # Post-migration validation call should happen only for the migration
        # that defines absurd.get_schema_version().
        return "main"

    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "get_recorded_schema_version",
        fake_get_recorded_schema_version,
    )
    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "discover_migrations",
        lambda script_dir, target_ref, include_remote=True: {
            ("0.1.0", "0.1.1"): {
                "name": "0.1.0-0.1.1.sql",
                "source": "local",
                "sql": "-- migration a (legacy, no schema version fn)",
            },
            ("0.1.1", "main"): {
                "name": "0.1.1-main.sql",
                "source": "local",
                "sql": """
                    create or replace function absurd.get_schema_version ()
                      returns text
                      language sql
                    as $$
                      select 'main'::text;
                    $$;
                """,
            },
        },
    )

    def fake_run_psql(config, query=None, **kwargs):
        if kwargs.get("input_data"):
            applied.append(kwargs["input_data"])
        return ""

    monkeypatch.setitem(cmd_migrate.__globals__, "run_psql", fake_run_psql)

    cmd_migrate(["--from", "0.1.0"])

    assert len(applied) == 2
    # Only post-step validation for the second migration; no initial read when
    # --from is supplied.
    assert len(observed_version_checks) == 1


def test_migrate_applies_sparse_legacy_chain_with_from(monkeypatch):
    applied = []

    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "get_recorded_schema_version",
        lambda config: "0.0.1",
    )
    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "discover_migrations",
        lambda script_dir, target_ref, include_remote=True: {
            ("0.0.3", "0.0.4"): {
                "name": "0.0.3-0.0.4.sql",
                "source": "local",
                "sql": "-- a",
            },
            ("0.0.4", "0.0.5"): {
                "name": "0.0.4-0.0.5.sql",
                "source": "local",
                "sql": "-- b",
            },
            ("0.0.8", "0.1.0"): {
                "name": "0.0.8-0.1.0.sql",
                "source": "local",
                "sql": "-- c",
            },
            ("0.1.0", "0.1.1"): {
                "name": "0.1.0-0.1.1.sql",
                "source": "local",
                "sql": "-- d",
            },
        },
    )

    def fake_run_psql(config, query=None, **kwargs):
        if kwargs.get("input_data"):
            applied.append(kwargs["input_data"])
        return ""

    monkeypatch.setitem(cmd_migrate.__globals__, "run_psql", fake_run_psql)

    cmd_migrate(["--from", "0.0.1", "--to", "0.1.1"])

    assert applied == ["-- a", "-- b", "-- c", "-- d"]


def test_migrate_fails_when_schema_version_function_returns_wrong_target(monkeypatch):
    observed_version_checks = []

    def fake_get_recorded_schema_version(config):
        observed_version_checks.append(True)
        if len(observed_version_checks) == 1:
            return "0.1.0"
        return "0.9.9"

    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "get_recorded_schema_version",
        fake_get_recorded_schema_version,
    )
    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "discover_migrations",
        lambda script_dir, target_ref, include_remote=True: {
            ("0.1.0", "main"): {
                "name": "0.1.0-main.sql",
                "source": "local",
                "sql": """
                    create or replace function absurd.get_schema_version ()
                      returns text
                      language sql
                    as $$
                      select '0.9.9'::text;
                    $$;
                """,
            }
        },
    )
    monkeypatch.setitem(cmd_migrate.__globals__, "run_psql", lambda *a, **k: "")

    with pytest.raises(SystemExit):
        cmd_migrate([])


def test_discover_remote_migrations_reports_fetch_failures(monkeypatch):
    monkeypatch.setitem(
        discover_remote_migrations.__globals__,
        "fetch_text",
        lambda url: (_ for _ in ()).throw(URLError("network down")),
    )

    with pytest.raises(RemoteMigrationDiscoveryError):
        discover_remote_migrations("main")


def test_migrate_falls_back_to_remote_when_local_migrations_missing(monkeypatch):
    applied = []
    discovery_modes = []

    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "get_recorded_schema_version",
        lambda config: "main",
    )

    def fake_discover_migrations(script_dir, target_ref, include_remote=False):
        discovery_modes.append(include_remote)
        if include_remote:
            return {
                ("0.1.0", "main"): {
                    "name": "0.1.0-main.sql",
                    "source": "github",
                    "sql": """
                        create or replace function absurd.get_schema_version ()
                          returns text
                          language sql
                        as $$
                          select 'main'::text;
                        $$;
                    """,
                }
            }
        return {}

    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "discover_migrations",
        fake_discover_migrations,
    )

    def fake_run_psql(config, query=None, **kwargs):
        if kwargs.get("input_data"):
            applied.append(kwargs["input_data"])
        return ""

    monkeypatch.setitem(cmd_migrate.__globals__, "run_psql", fake_run_psql)

    cmd_migrate(["--from", "0.1.0", "--to", "main"])

    assert discovery_modes == [False, True]
    assert len(applied) == 1


def test_migrate_surfaces_remote_discovery_failure(monkeypatch):
    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "get_recorded_schema_version",
        lambda config: "0.1.0",
    )

    def fake_discover_migrations(script_dir, target_ref, include_remote=True):
        if not include_remote:
            return {}
        raise RemoteMigrationDiscoveryError("simulated remote failure")

    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "discover_migrations",
        fake_discover_migrations,
    )

    with pytest.raises(RemoteMigrationDiscoveryError):
        cmd_migrate([])


def test_migrate_requires_known_source_version(monkeypatch):
    monkeypatch.setitem(
        cmd_migrate.__globals__,
        "get_recorded_schema_version",
        lambda config: None,
    )

    with pytest.raises(SystemExit):
        cmd_migrate([])
