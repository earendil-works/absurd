import asyncio
import types

import pytest

import absurd_sdk
from absurd_sdk import AsyncTaskContext, TaskContext, TimeoutError


class _Cursor:
    def __init__(self, row):
        self._row = row

    def execute(self, _query, _params):
        return None

    def fetchone(self):
        return self._row


class _AsyncCursor:
    def __init__(self, row):
        self._row = row

    async def execute(self, _query, _params):
        return None

    async def fetchone(self):
        return self._row


class _Conn:
    def __init__(self, row):
        self._row = row

    def cursor(self, row_factory=None):
        return _Cursor(self._row)


class _AsyncConn:
    def __init__(self, row):
        self._row = row

    def cursor(self, row_factory=None):
        return _AsyncCursor(self._row)


def _make_sync_context(event_name: str, row: dict):
    ctx = object.__new__(TaskContext)
    ctx.task_id = "task-1"
    ctx._conn = _Conn(row)
    ctx._queue_name = "default"
    ctx._task = {
        "task_id": "task-1",
        "run_id": "run-1",
        "wake_event": event_name,
        "event_payload": None,
    }
    ctx._checkpoint_cache = {}
    ctx._claim_timeout = 60
    ctx._step_name_counter = {}
    ctx._lookup_checkpoint = lambda _checkpoint: absurd_sdk._CHECKPOINT_NOT_FOUND
    return ctx


def _make_async_context(event_name: str, row: dict):
    ctx = object.__new__(AsyncTaskContext)
    ctx.task_id = "task-1"
    ctx._conn = _AsyncConn(row)
    ctx._queue_name = "default"
    ctx._task = {
        "task_id": "task-1",
        "run_id": "run-1",
        "wake_event": event_name,
        "event_payload": None,
    }
    ctx._checkpoint_cache = {}
    ctx._claim_timeout = 60
    ctx._step_name_counter = {}

    async def _lookup_checkpoint(_self, _checkpoint):
        return absurd_sdk._CHECKPOINT_NOT_FOUND

    ctx._lookup_checkpoint = types.MethodType(_lookup_checkpoint, ctx)
    return ctx


def test_sync_await_event_legacy_timeout_fallback():
    event_name = "legacy-timeout-event"
    ctx = _make_sync_context(
        event_name,
        {
            "should_suspend": False,
            "payload": None,
        },
    )

    with pytest.raises(TimeoutError, match="Timed out waiting for event"):
        ctx.await_event(event_name)


def test_async_await_event_legacy_timeout_fallback():
    event_name = "legacy-timeout-event"
    ctx = _make_async_context(
        event_name,
        {
            "should_suspend": False,
            "payload": None,
        },
    )

    async def run():
        with pytest.raises(TimeoutError, match="Timed out waiting for event"):
            await ctx.await_event(event_name)

    asyncio.run(run())
