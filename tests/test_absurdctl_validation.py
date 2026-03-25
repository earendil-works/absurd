from pathlib import Path
import runpy

import pytest


ABSURDCTL_PATH = Path(__file__).resolve().parents[1] / "absurdctl"
MODULE = runpy.run_path(str(ABSURDCTL_PATH))
validate_queue_name = MODULE["validate_queue_name"]
cmd_emit_event = MODULE["cmd_emit_event"]
cmd_retry_task = MODULE["cmd_retry_task"]


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
    monkeypatch.setitem(cmd_emit_event.__globals__, "ensure_queue_exists", lambda *_: None)

    cmd_emit_event(["-q", "default", "order.completed", "-P", "note=Bob's"])

    assert captured["query"] == "SELECT absurd.emit_event(:'queue', :'event_name', :'payload_json'::jsonb);"
    assert captured["variables"]["queue"] == "default"
    assert captured["variables"]["event_name"] == "order.completed"
    assert captured["variables"]["payload_json"] == '{"note": "Bob\'s"}'


def test_emit_event_validates_queue_name(monkeypatch):
    monkeypatch.setitem(cmd_emit_event.__globals__, "ensure_queue_exists", lambda *_: None)

    with pytest.raises(SystemExit):
        cmd_emit_event(["-q", "a" * 58, "order.completed"])


def test_retry_task_uses_parameterized_query(monkeypatch):
    captured = {}

    def fake_run_psql_csv(config, query=None, **kwargs):
        captured["query"] = query
        captured["variables"] = kwargs.get("variables")
        return [[
            "019a32d3-8425-7ae2-a5af-2f17a6707666",
            "019a32d3-8425-7ae2-a5af-2f17a6707667",
            "2",
            "f",
        ]]

    monkeypatch.setitem(cmd_retry_task.__globals__, "run_psql_csv", fake_run_psql_csv)

    cmd_retry_task([
        "-q",
        "default",
        "019a32d3-8425-7ae2-a5af-2f17a6707666",
        "--max-attempts",
        "5",
    ])

    assert "FROM absurd.retry_task(:'queue', :'task_id', :'options_json'::jsonb)" in captured["query"]
    assert captured["variables"]["queue"] == "default"
    assert captured["variables"]["task_id"] == "019a32d3-8425-7ae2-a5af-2f17a6707666"
    assert captured["variables"]["options_json"] == '{"max_attempts": 5}'


def test_retry_task_defaults_to_inplace_retry(monkeypatch):
    captured = {}

    def fake_run_psql_csv(config, query=None, **kwargs):
        captured["variables"] = kwargs.get("variables")
        return [[
            "019a32d3-8425-7ae2-a5af-2f17a6707666",
            "019a32d3-8425-7ae2-a5af-2f17a6707667",
            "2",
            "f",
        ]]

    monkeypatch.setitem(cmd_retry_task.__globals__, "run_psql_csv", fake_run_psql_csv)

    cmd_retry_task(["019a32d3-8425-7ae2-a5af-2f17a6707666"])

    assert captured["variables"]["options_json"] == "{}"


def test_retry_task_allows_spawn_new_with_max_attempts(monkeypatch):
    captured = {}

    def fake_run_psql_csv(config, query=None, **kwargs):
        captured["variables"] = kwargs.get("variables")
        return [[
            "019a32d3-8425-7ae2-a5af-2f17a6707666",
            "019a32d3-8425-7ae2-a5af-2f17a6707667",
            "1",
            "t",
        ]]

    monkeypatch.setitem(cmd_retry_task.__globals__, "run_psql_csv", fake_run_psql_csv)

    cmd_retry_task(
        [
            "019a32d3-8425-7ae2-a5af-2f17a6707666",
            "--max-attempts",
            "5",
            "--spawn-new",
        ]
    )

    assert captured["variables"]["options_json"] == '{"spawn_new": true, "max_attempts": 5}'
