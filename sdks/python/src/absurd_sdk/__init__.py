"""
Absurd SDK for Python
"""

from __future__ import annotations

import contextvars
import json
import os
import socket
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    TypedDict,
    TypeVar,
    Union,
    cast,
    overload,
)

from psycopg import AsyncConnection, Connection
from psycopg.rows import dict_row

__all__ = [
    "Absurd",
    "AsyncAbsurd",
    "TaskContext",
    "AsyncTaskContext",
    "SuspendTask",
    "CancelledTask",
    "TimeoutError",
    "RetryStrategy",
    "CancellationPolicy",
    "SpawnOptions",
    "ClaimedTask",
    "AbsurdHooks",
    "get_current_context",
]


# Context variable for accessing the current task context
_current_task_context: contextvars.ContextVar[
    Optional[Union["TaskContext", "AsyncTaskContext"]]
] = contextvars.ContextVar("current_task_context", default=None)


def get_current_context() -> Optional[Union["TaskContext", "AsyncTaskContext"]]:
    """Get the current task context if running inside a task handler.

    Returns None if called outside of a task execution.
    This works with both sync and async task handlers.
    """
    return _current_task_context.get()

JsonValue = Union[
    str, int, float, bool, None, List["JsonValue"], Dict[str, "JsonValue"]
]
JsonObject = Dict[str, JsonValue]

P = TypeVar("P")
R = TypeVar("R")


class RetryStrategy(TypedDict, total=False):
    """Retry strategy configuration"""

    kind: Literal["fixed", "exponential", "none"]
    base_seconds: float
    factor: float
    max_seconds: float


class CancellationPolicy(TypedDict, total=False):
    """Task cancellation policy"""

    max_duration: int
    max_delay: int


class SpawnOptions(TypedDict, total=False):
    """Options for spawning a task"""

    max_attempts: int
    retry_strategy: RetryStrategy
    headers: JsonObject
    queue: str
    cancellation: CancellationPolicy
    idempotency_key: str


class ClaimedTask(TypedDict):
    """A claimed task from the queue"""

    run_id: str
    task_id: str
    task_name: str
    attempt: int
    params: JsonValue
    retry_strategy: JsonValue
    max_attempts: Optional[int]
    headers: Optional[JsonObject]
    wake_event: Optional[str]
    event_payload: Optional[JsonValue]


class SpawnResult(TypedDict):
    """Result of spawning a task"""

    task_id: str
    run_id: str
    attempt: int


# Type aliases for hook callbacks
BeforeSpawnHook = Callable[[str, JsonValue, SpawnOptions], SpawnOptions]
AsyncBeforeSpawnHook = Callable[[str, JsonValue, SpawnOptions], Awaitable[SpawnOptions]]
WrapTaskExecutionHook = Callable[
    [Union["TaskContext", "AsyncTaskContext"], Callable[[], Any]], Any
]
AsyncWrapTaskExecutionHook = Callable[
    [Union["TaskContext", "AsyncTaskContext"], Callable[[], Awaitable[Any]]],
    Awaitable[Any],
]


class AbsurdHooks(TypedDict, total=False):
    """Hooks for customizing Absurd behavior.

    These hooks enable integration with tracing, logging, and context propagation
    systems like OpenTelemetry or custom correlation ID tracking.
    """

    before_spawn: Union[BeforeSpawnHook, AsyncBeforeSpawnHook]
    """Called before spawning a task. Can modify spawn options (including headers).

    Use this to inject trace IDs, correlation IDs, or other context from
    contextvars into the task headers.

    Args:
        task_name: Name of the task being spawned
        params: Parameters being passed to the task
        options: Current spawn options (may be modified and returned)

    Returns:
        Modified spawn options
    """

    wrap_task_execution: Union[WrapTaskExecutionHook, AsyncWrapTaskExecutionHook]
    """Wraps task execution. Must call and return the result of execute().

    Use this to restore context (e.g., into contextvars) before the task handler
    runs, ensuring all code within the task has access to it.

    Args:
        ctx: The task context
        execute: Function to call to execute the task handler

    Returns:
        Result of calling execute()
    """


TaskHandler = Callable[[Any, "TaskContext"], Any]
AsyncTaskHandler = Callable[[Any, "AsyncTaskContext"], Awaitable[Any]]


class SuspendTask(Exception):
    """Internal exception thrown to suspend a run."""


class CancelledTask(Exception):
    """Internal exception thrown when a task is cancelled."""


class TimeoutError(Exception):
    """Error thrown when awaiting an event times out."""


def _serialize_error(err: Any) -> JsonObject:
    """Serialize an exception to JSON"""
    if isinstance(err, Exception):
        formatted = (
            "".join(traceback.format_exception(err.__class__, err, err.__traceback__))
            if err.__traceback__
            else None
        )
        return {
            "name": err.__class__.__name__,
            "message": str(err),
            "traceback": formatted,
        }
    return {"message": str(err)}


def _complete_task_run(
    conn: Connection[Any],
    queue_name: str,
    run_id: str,
    result: Optional[JsonValue],
) -> None:
    conn.cursor().execute(
        "SELECT absurd.complete_run(%s, %s, %s)",
        (queue_name, run_id, json.dumps(result)),
    )


def _fail_task_run(
    conn: Connection[Any],
    queue_name: str,
    run_id: str,
    err: Any,
    fatal_error: Optional[str] = None,
) -> None:
    conn.cursor().execute(
        "SELECT absurd.fail_run(%s, %s, %s, %s)",
        (
            queue_name,
            run_id,
            json.dumps(_serialize_error(err)),
            fatal_error,
        ),
    )


async def _complete_task_run_async(
    conn,
    queue_name,
    run_id,
    result,
) -> None:
    await conn.cursor().execute(
        "SELECT absurd.complete_run(%s, %s, %s)",
        (queue_name, run_id, json.dumps(result)),
    )


async def _fail_task_run_async(
    conn: AsyncConnection[Any],
    queue_name: str,
    run_id: str,
    err: Any,
    fatal_error: Optional[str] = None,
) -> None:
    await conn.cursor().execute(
        "SELECT absurd.fail_run(%s, %s, %s, %s)",
        (
            queue_name,
            run_id,
            json.dumps(_serialize_error(err)),
            fatal_error,
        ),
    )


def _normalize_spawn_options(
    max_attempts: Optional[int] = None,
    retry_strategy: Optional[RetryStrategy] = None,
    headers: Optional[JsonObject] = None,
    cancellation: Optional[CancellationPolicy] = None,
    idempotency_key: Optional[str] = None,
) -> JsonObject:
    normalized: JsonObject = {}

    if headers is not None:
        normalized["headers"] = headers
    if max_attempts is not None:
        normalized["max_attempts"] = max_attempts
    if retry_strategy:
        normalized["retry_strategy"] = _serialize_retry_strategy(retry_strategy)

    cancel = _normalize_cancellation(cancellation)
    if cancel:
        normalized["cancellation"] = cancel

    if idempotency_key is not None:
        normalized["idempotency_key"] = idempotency_key

    return normalized


def _serialize_retry_strategy(strategy: RetryStrategy) -> JsonObject:
    serialized: JsonObject = {"kind": strategy["kind"]}

    if "base_seconds" in strategy:
        serialized["base_seconds"] = strategy["base_seconds"]
    if "factor" in strategy:
        serialized["factor"] = strategy["factor"]
    if "max_seconds" in strategy:
        serialized["max_seconds"] = strategy["max_seconds"]

    return serialized


def _normalize_cancellation(
    policy: Optional[CancellationPolicy],
) -> Optional[JsonObject]:
    if not policy:
        return None

    normalized: JsonObject = {}
    if "max_duration" in policy:
        normalized["max_duration"] = policy["max_duration"]
    if "max_delay" in policy:
        normalized["max_delay"] = policy["max_delay"]

    return normalized if normalized else None


def _get_current_time() -> datetime:
    """Get current time (can be monkeypatched in tests)"""
    return datetime.now(timezone.utc)


def _get_callable_name(fn: Callable[..., Any]) -> str:
    """Best-effort name for a callable, falls back to repr."""
    return getattr(fn, "__name__", repr(fn))


def _create_task_context(
    task_id: str,
    conn: Connection[Any],
    queue_name: str,
    task: ClaimedTask,
    claim_timeout: int,
) -> TaskContext:
    """Create a new task context by loading checkpoints"""
    cursor = conn.cursor(row_factory=dict_row)
    cursor.execute(
        """SELECT checkpoint_name, state, status, owner_run_id, updated_at
            FROM absurd.get_task_checkpoint_states(%s, %s, %s)""",
        (queue_name, task["task_id"], task["run_id"]),
    )
    cache = {row["checkpoint_name"]: row["state"] for row in cursor.fetchall()}
    ctx = object.__new__(TaskContext)
    ctx.task_id = task_id
    ctx._conn = conn
    ctx._queue_name = queue_name
    ctx._task = task
    ctx._checkpoint_cache = cache
    ctx._claim_timeout = claim_timeout
    ctx._step_name_counter = {}
    return ctx


async def _create_async_task_context(
    task_id: str,
    conn: AsyncConnection[Any],
    queue_name: str,
    task: ClaimedTask,
    claim_timeout: int,
) -> AsyncTaskContext:
    """Create a new async task context by loading checkpoints"""
    cursor = conn.cursor(row_factory=dict_row)
    await cursor.execute(
        """SELECT checkpoint_name, state, status, owner_run_id, updated_at
            FROM absurd.get_task_checkpoint_states(%s, %s, %s)""",
        (queue_name, task["task_id"], task["run_id"]),
    )
    rows = await cursor.fetchall()
    cache = {row["checkpoint_name"]: row["state"] for row in rows}
    ctx = object.__new__(AsyncTaskContext)
    ctx.task_id = task_id
    ctx._conn = conn
    ctx._queue_name = queue_name
    ctx._task = task
    ctx._checkpoint_cache = cache
    ctx._claim_timeout = claim_timeout
    ctx._step_name_counter = {}
    return ctx


class TaskContext:
    """Synchronous task execution context"""

    task_id: str
    _conn: Connection[Any]
    _queue_name: str
    _task: ClaimedTask
    _checkpoint_cache: Dict[str, JsonValue]
    _claim_timeout: int
    _step_name_counter: Dict[str, int]

    def __init__(self):
        raise TypeError("Cannot create TaskContext instances")

    @property
    def headers(self) -> Mapping[str, JsonValue]:
        """Returns all headers attached to this task."""
        return self._task.get("headers") or {}

    def step(self, name: str, fn: Callable[[], R]) -> R:
        """Execute an idempotent step identified by name"""
        checkpoint_name = self._get_checkpoint_name(name)
        state = self._lookup_checkpoint(checkpoint_name)
        if state is not None:
            return state  # type: ignore

        rv = fn()
        self._persist_checkpoint(checkpoint_name, rv)
        return rv

    @overload
    def run_step(self, name: Optional[str] = None) -> Callable[[Callable[[], R]], R]:
        """Decorator to run a function as a step and replace it with the return value"""
        ...

    @overload
    def run_step(self, fn: Callable[[], R]) -> R:
        """Decorator to run a function as a step and replace it with the return value"""
        ...

    def run_step(
        self, name_or_fn: Optional[Union[str, Callable[[], R]]] = None
    ) -> Union[R, Callable[[Callable[[], R]], R]]:
        """Decorator to run a function as a step and replace it with the return value.

        Usage:
            @ctx.run_step()
            def step_name():
                return 42
            # step_name is now 42

            @ctx.run_step("custom_name")
            def step_name():
                return 42
            # step_name is now 42, stored as "custom_name"
        """
        # Case 1: @ctx.run_step (no arguments, no parentheses)
        if callable(name_or_fn):
            fn = cast(Callable[[], R], name_or_fn)
            return self.step(_get_callable_name(fn), fn)

        # Case 2: @ctx.run_step() or @ctx.run_step("custom_name")
        custom_name = name_or_fn if isinstance(name_or_fn, str) else None

        def decorator(fn: Callable[[], R]) -> R:
            step_name = (
                custom_name if custom_name is not None else _get_callable_name(fn)
            )
            return self.step(step_name, fn)

        return decorator

    def sleep_for(self, step_name: str, duration: float) -> None:
        """Suspend the task for the given duration in seconds"""
        wake_at = _get_current_time() + timedelta(seconds=duration)
        return self.sleep_until(step_name, wake_at)

    def sleep_until(self, step_name: str, wake_at: Union[datetime, int, float]) -> None:
        """Suspend the task until the specified time"""
        if isinstance(wake_at, (int, float)):
            wake_at = datetime.fromtimestamp(wake_at, timezone.utc)

        checkpoint_name = self._get_checkpoint_name(step_name)
        state = self._lookup_checkpoint(checkpoint_name)

        if state:
            actual_wake_at = (
                datetime.fromisoformat(state) if isinstance(state, str) else wake_at
            )
        else:
            actual_wake_at = wake_at
            self._persist_checkpoint(checkpoint_name, wake_at.isoformat())

        if _get_current_time() < actual_wake_at:
            self._schedule_run(actual_wake_at)
            raise SuspendTask()

    def await_event(
        self,
        event_name: str,
        step_name: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> JsonValue:
        """Wait for an event by name and return its payload"""
        step_name = step_name or f"$awaitEvent:{event_name}"
        checkpoint_name = self._get_checkpoint_name(step_name)
        cached = self._lookup_checkpoint(checkpoint_name)

        if cached is not None:
            return cached

        if (
            self._task["wake_event"] == event_name
            and self._task["event_payload"] is None
        ):
            self._task["wake_event"] = None
            self._task["event_payload"] = None
            raise TimeoutError(f'Timed out waiting for event "{event_name}"')

        cursor = self._conn.cursor(row_factory=dict_row)
        try:
            cursor.execute(
                """SELECT should_suspend, payload
                   FROM absurd.await_event(%s, %s, %s, %s, %s, %s)""",
                (
                    self._queue_name,
                    self._task["task_id"],
                    self._task["run_id"],
                    checkpoint_name,
                    event_name,
                    timeout,
                ),
            )
        except Exception as e:
            if hasattr(e, "pgcode") and e.pgcode == "AB001":  # type: ignore
                raise CancelledTask() from e
            raise

        result = cursor.fetchone()

        if not result:
            raise Exception("Failed to await event")

        if not result["should_suspend"]:
            self._checkpoint_cache[checkpoint_name] = result["payload"]
            self._task["event_payload"] = None
            return result["payload"]

        raise SuspendTask()

    def emit_event(self, event_name: str, payload: Optional[JsonValue] = None) -> None:
        """Emit an event to this task's queue with an optional payload"""
        if not event_name:
            raise ValueError("event_name must be a non-empty string")

        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT absurd.emit_event(%s, %s, %s)",
            (self._queue_name, event_name, json.dumps(payload)),
        )

    def heartbeat(self, seconds: Optional[int] = None) -> None:
        """Extend the current run's lease by the given seconds"""
        cursor = self._conn.cursor()
        try:
            cursor.execute(
                "SELECT absurd.extend_claim(%s, %s, %s)",
                (
                    self._queue_name,
                    self._task["run_id"],
                    seconds or self._claim_timeout,
                ),
            )
        except Exception as e:
            if hasattr(e, "pgcode") and e.pgcode == "AB001":  # type: ignore
                raise CancelledTask() from e
            raise

    def _get_checkpoint_name(self, name: str) -> str:
        """Get unique checkpoint name handling duplicates"""
        count = self._step_name_counter.get(name, 0) + 1
        self._step_name_counter[name] = count
        return name if count == 1 else f"{name}#{count}"

    def _lookup_checkpoint(self, checkpoint_name: str) -> Optional[JsonValue]:
        """Look up a checkpoint by name"""
        if checkpoint_name in self._checkpoint_cache:
            return self._checkpoint_cache[checkpoint_name]

        cursor = self._conn.cursor(row_factory=dict_row)
        cursor.execute(
            """SELECT checkpoint_name, state, status, owner_run_id, updated_at
               FROM absurd.get_task_checkpoint_state(%s, %s, %s)""",
            (self._queue_name, self._task["task_id"], checkpoint_name),
        )
        row = cursor.fetchone()

        if row:
            state = row["state"]
            self._checkpoint_cache[checkpoint_name] = state
            return state

        return None

    def _persist_checkpoint(self, checkpoint_name: str, value: Any) -> None:
        """Persist a checkpoint value"""
        cursor = self._conn.cursor()
        try:
            cursor.execute(
                "SELECT absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s)",
                (
                    self._queue_name,
                    self._task["task_id"],
                    checkpoint_name,
                    json.dumps(value),
                    self._task["run_id"],
                    self._claim_timeout,
                ),
            )
        except Exception as e:
            if hasattr(e, "pgcode") and e.pgcode == "AB001":  # type: ignore
                raise CancelledTask() from e
            raise
        self._checkpoint_cache[checkpoint_name] = value

    def _schedule_run(self, wake_at: datetime) -> None:
        """Schedule a run to wake at a specific time"""
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT absurd.schedule_run(%s, %s, %s)",
            (self._queue_name, self._task["run_id"], wake_at),
        )


class AsyncTaskContext:
    """Asynchronous task execution context"""

    task_id: str
    _conn: AsyncConnection[Any]
    _queue_name: str
    _task: ClaimedTask
    _checkpoint_cache: Dict[str, JsonValue]
    _claim_timeout: int
    _step_name_counter: Dict[str, int]

    def __init__(self):
        raise TypeError("Cannot create AsyncTaskContext instances")

    @property
    def headers(self) -> Mapping[str, JsonValue]:
        """Returns all headers attached to this task."""
        return self._task.get("headers") or {}

    async def step(self, name: str, fn: Callable[[], Awaitable[R]]) -> R:
        """Execute an idempotent step identified by name"""
        checkpoint_name = self._get_checkpoint_name(name)
        state = await self._lookup_checkpoint(checkpoint_name)
        if state is not None:
            return state  # type: ignore

        rv = await fn()
        await self._persist_checkpoint(checkpoint_name, rv)
        return rv

    async def sleep_for(self, step_name: str, duration: float) -> None:
        """Suspend the task for the given duration in seconds"""
        wake_at = _get_current_time() + timedelta(seconds=duration)
        return await self.sleep_until(step_name, wake_at)

    async def sleep_until(
        self, step_name: str, wake_at: Union[datetime, int, float]
    ) -> None:
        """Suspend the task until the specified time"""
        if isinstance(wake_at, (int, float)):
            wake_at = datetime.fromtimestamp(wake_at, timezone.utc)

        checkpoint_name = self._get_checkpoint_name(step_name)
        state = await self._lookup_checkpoint(checkpoint_name)

        if state:
            actual_wake_at = (
                datetime.fromisoformat(state) if isinstance(state, str) else wake_at
            )
        else:
            actual_wake_at = wake_at
            await self._persist_checkpoint(checkpoint_name, wake_at.isoformat())

        if _get_current_time() < actual_wake_at:
            await self._schedule_run(actual_wake_at)
            raise SuspendTask()

    async def await_event(
        self,
        event_name: str,
        step_name: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> JsonValue:
        """Wait for an event by name and return its payload"""
        step_name = step_name or f"$awaitEvent:{event_name}"
        checkpoint_name = self._get_checkpoint_name(step_name)
        cached = await self._lookup_checkpoint(checkpoint_name)

        if cached is not None:
            return cached

        if (
            self._task["wake_event"] == event_name
            and self._task["event_payload"] is None
        ):
            self._task["wake_event"] = None
            self._task["event_payload"] = None
            raise TimeoutError(f'Timed out waiting for event "{event_name}"')

        cursor = self._conn.cursor(row_factory=dict_row)
        try:
            await cursor.execute(
                """SELECT should_suspend, payload
                   FROM absurd.await_event(%s, %s, %s, %s, %s, %s)""",
                (
                    self._queue_name,
                    self._task["task_id"],
                    self._task["run_id"],
                    checkpoint_name,
                    event_name,
                    timeout,
                ),
            )
        except Exception as e:
            if hasattr(e, "pgcode") and e.pgcode == "AB001":  # type: ignore
                raise CancelledTask() from e
            raise

        result = await cursor.fetchone()

        if not result:
            raise Exception("Failed to await event")

        if not result["should_suspend"]:
            self._checkpoint_cache[checkpoint_name] = result["payload"]
            self._task["event_payload"] = None
            return result["payload"]

        raise SuspendTask()

    async def emit_event(
        self, event_name: str, payload: Optional[JsonValue] = None
    ) -> None:
        """Emit an event to this task's queue with an optional payload"""
        if not event_name:
            raise ValueError("event_name must be a non-empty string")

        cursor = self._conn.cursor()
        await cursor.execute(
            "SELECT absurd.emit_event(%s, %s, %s)",
            (self._queue_name, event_name, json.dumps(payload)),
        )

    async def heartbeat(self, seconds: Optional[int] = None) -> None:
        """Extend the current run's lease by the given seconds"""
        cursor = self._conn.cursor()
        try:
            await cursor.execute(
                "SELECT absurd.extend_claim(%s, %s, %s)",
                (
                    self._queue_name,
                    self._task["run_id"],
                    seconds or self._claim_timeout,
                ),
            )
        except Exception as e:
            if hasattr(e, "pgcode") and e.pgcode == "AB001":  # type: ignore
                raise CancelledTask() from e
            raise

    def _get_checkpoint_name(self, name: str) -> str:
        """Get unique checkpoint name handling duplicates"""
        count = self._step_name_counter.get(name, 0) + 1
        self._step_name_counter[name] = count
        return name if count == 1 else f"{name}#{count}"

    async def _lookup_checkpoint(self, checkpoint_name: str) -> Optional[JsonValue]:
        """Look up a checkpoint by name"""
        if checkpoint_name in self._checkpoint_cache:
            return self._checkpoint_cache[checkpoint_name]

        cursor = self._conn.cursor(row_factory=dict_row)
        await cursor.execute(
            """SELECT checkpoint_name, state, status, owner_run_id, updated_at
               FROM absurd.get_task_checkpoint_state(%s, %s, %s)""",
            (self._queue_name, self._task["task_id"], checkpoint_name),
        )
        row = await cursor.fetchone()

        if row:
            state = row["state"]
            self._checkpoint_cache[checkpoint_name] = state
            return state

        return None

    async def _persist_checkpoint(self, checkpoint_name: str, value: Any) -> None:
        """Persist a checkpoint value"""
        cursor = self._conn.cursor()
        try:
            await cursor.execute(
                "SELECT absurd.set_task_checkpoint_state(%s, %s, %s, %s, %s, %s)",
                (
                    self._queue_name,
                    self._task["task_id"],
                    checkpoint_name,
                    json.dumps(value),
                    self._task["run_id"],
                    self._claim_timeout,
                ),
            )
        except Exception as e:
            if hasattr(e, "pgcode") and e.pgcode == "AB001":  # type: ignore
                raise CancelledTask() from e
            raise
        self._checkpoint_cache[checkpoint_name] = value

    async def _schedule_run(self, wake_at: datetime) -> None:
        """Schedule a run to wake at a specific time"""
        cursor = self._conn.cursor()
        await cursor.execute(
            "SELECT absurd.schedule_run(%s, %s, %s)",
            (self._queue_name, self._task["run_id"], wake_at),
        )


class _AbsurdBase:
    """Base class for Absurd clients"""

    def __init__(
        self,
        queue_name: str = "default",
        default_max_attempts: int = 5,
        hooks: Optional[AbsurdHooks] = None,
    ) -> None:
        self._queue_name = queue_name
        self._default_max_attempts = default_max_attempts
        self._hooks: AbsurdHooks = hooks or {}
        self._registry: Dict[str, Dict[str, Any]] = {}
        self._worker_running = False

    def register_task(
        self,
        name: str,
        queue: Optional[str] = None,
        default_max_attempts: Optional[int] = None,
        default_cancellation: Optional[CancellationPolicy] = None,
    ) -> Callable[[TaskHandler], TaskHandler]:
        """Register a task handler by name"""

        def decorator(handler: TaskHandler) -> TaskHandler:
            actual_queue = queue or self._queue_name
            if not actual_queue:
                raise ValueError(
                    f'Task "{name}" must specify a queue or use a client with a default queue'
                )

            self._registry[name] = {
                "name": name,
                "queue": actual_queue,
                "default_max_attempts": default_max_attempts,
                "default_cancellation": default_cancellation,
                "handler": handler,
            }
            return handler

        return decorator

    def _prepare_spawn(
        self,
        task_name: str,
        max_attempts: Optional[int] = None,
        retry_strategy: Optional[RetryStrategy] = None,
        headers: Optional[JsonObject] = None,
        queue: Optional[str] = None,
        cancellation: Optional[CancellationPolicy] = None,
        idempotency_key: Optional[str] = None,
    ) -> tuple[str, JsonObject]:
        """Prepare spawn options for a task"""
        registration = self._registry.get(task_name)

        if registration:
            actual_queue = registration["queue"]
            if queue and queue != actual_queue:
                raise ValueError(
                    f'Task "{task_name}" is registered for queue "{actual_queue}" '
                    f'but spawn requested queue "{queue}".'
                )
        elif not queue:
            raise ValueError(
                f'Task "{task_name}" is not registered. Provide queue when spawning unregistered tasks.'
            )
        else:
            actual_queue = queue

        effective_max_attempts = (
            max_attempts
            if max_attempts is not None
            else (
                registration.get("default_max_attempts")
                if registration
                else self._default_max_attempts
            )
        )

        effective_cancellation = (
            cancellation
            if cancellation is not None
            else registration.get("default_cancellation") if registration else None
        )

        options = _normalize_spawn_options(
            max_attempts=effective_max_attempts,
            retry_strategy=retry_strategy,
            headers=headers,
            cancellation=effective_cancellation,
            idempotency_key=idempotency_key,
        )

        return actual_queue, options


class Absurd(_AbsurdBase):
    """Synchronous Absurd SDK client"""

    def __init__(
        self,
        conn_or_url: Optional[Union[Connection[Any], str]] = None,
        queue_name: str = "default",
        default_max_attempts: int = 5,
        hooks: Optional[AbsurdHooks] = None,
    ) -> None:
        if conn_or_url is None:
            conn_or_url = os.environ.get(
                "ABSURD_DATABASE_URL", "postgresql://localhost/absurd"
            )

        if isinstance(conn_or_url, str):
            self._conn: Connection[Any] = Connection.connect(conn_or_url, autocommit=True)
            self._owned_conn = True
        else:
            self._conn = conn_or_url
            self._owned_conn = False
        super().__init__(queue_name, default_max_attempts, hooks)

    def create_queue(self, queue_name: Optional[str] = None) -> None:
        """Create a queue (defaults to this client's queue)"""
        queue = queue_name or self._queue_name
        cursor = self._conn.cursor()
        cursor.execute("SELECT absurd.create_queue(%s)", (queue,))

    def drop_queue(self, queue_name: Optional[str] = None) -> None:
        """Drop a queue (defaults to this client's queue)"""
        queue = queue_name or self._queue_name
        cursor = self._conn.cursor()
        cursor.execute("SELECT absurd.drop_queue(%s)", (queue,))

    def list_queues(self) -> List[str]:
        """List all queue names"""
        cursor = self._conn.cursor(row_factory=dict_row)
        cursor.execute("SELECT * FROM absurd.list_queues()")
        return [row["queue_name"] for row in cursor.fetchall()]

    def spawn(
        self,
        task_name: str,
        params: Any,
        max_attempts: Optional[int] = None,
        retry_strategy: Optional[RetryStrategy] = None,
        headers: Optional[JsonObject] = None,
        queue: Optional[str] = None,
        cancellation: Optional[CancellationPolicy] = None,
        idempotency_key: Optional[str] = None,
    ) -> SpawnResult:
        """Spawn a task execution by enqueueing it for processing"""
        # Build SpawnOptions and apply before_spawn hook if configured
        spawn_options: SpawnOptions = {
            "max_attempts": max_attempts,
            "retry_strategy": retry_strategy,
            "headers": headers,
            "queue": queue,
            "cancellation": cancellation,
            "idempotency_key": idempotency_key,
        }

        before_spawn = self._hooks.get("before_spawn")
        if before_spawn is not None:
            spawn_options = before_spawn(task_name, params, spawn_options)

        actual_queue, options = self._prepare_spawn(
            task_name,
            max_attempts=spawn_options.get("max_attempts"),
            retry_strategy=spawn_options.get("retry_strategy"),
            headers=spawn_options.get("headers"),
            queue=spawn_options.get("queue"),
            cancellation=spawn_options.get("cancellation"),
            idempotency_key=spawn_options.get("idempotency_key"),
        )
        cursor = self._conn.cursor(row_factory=dict_row)
        cursor.execute(
            """SELECT task_id, run_id, attempt
               FROM absurd.spawn_task(%s, %s, %s, %s)""",
            (actual_queue, task_name, json.dumps(params), json.dumps(options)),
        )
        row = cursor.fetchone()

        if not row:
            raise Exception("Failed to spawn task")

        return {
            "task_id": row["task_id"],
            "run_id": row["run_id"],
            "attempt": row["attempt"],
        }

    def emit_event(
        self,
        event_name: str,
        payload: Optional[JsonValue] = None,
        queue_name: Optional[str] = None,
    ) -> None:
        """Emit an event with an optional payload on the specified or default queue"""
        if not event_name:
            raise ValueError("event_name must be a non-empty string")

        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT absurd.emit_event(%s, %s, %s)",
            (queue_name or self._queue_name, event_name, json.dumps(payload)),
        )

    def cancel_task(self, task_id: str, queue_name: Optional[str] = None) -> None:
        """Cancel a task by ID on the specified or default queue"""
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT absurd.cancel_task(%s, %s)",
            (queue_name or self._queue_name, task_id),
        )

    def claim_tasks(
        self, batch_size: int = 1, claim_timeout: int = 120, worker_id: str = "worker"
    ) -> List[ClaimedTask]:
        """Claim up to batch_size tasks from the queue"""
        cursor = self._conn.cursor(row_factory=dict_row)
        cursor.execute(
            """SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts,
                      headers, wake_event, event_payload
               FROM absurd.claim_task(%s, %s, %s, %s)""",
            (self._queue_name, worker_id, claim_timeout, batch_size),
        )
        return cursor.fetchall()  # type: ignore

    def work_batch(
        self, worker_id: str = "worker", claim_timeout: int = 120, batch_size: int = 1
    ) -> None:
        """Claim and process up to batch_size tasks sequentially"""
        tasks = self.claim_tasks(
            batch_size=batch_size, claim_timeout=claim_timeout, worker_id=worker_id
        )

        for task in tasks:
            self._execute_task(task, claim_timeout)

    def start_worker(
        self,
        worker_id: Optional[str] = None,
        claim_timeout: int = 120,
        concurrency: int = 1,
        batch_size: Optional[int] = None,
        poll_interval: float = 0.25,
    ) -> None:
        """Start a synchronous worker that continuously polls for tasks"""
        if worker_id is None:
            worker_id = f"{socket.gethostname()}:{os.getpid()}"

        effective_batch_size = batch_size or concurrency
        self._worker_running = True

        while self._worker_running:
            tasks = self.claim_tasks(
                batch_size=effective_batch_size,
                claim_timeout=claim_timeout,
                worker_id=worker_id,
            )

            if not tasks:
                time.sleep(poll_interval)
                continue

            for task in tasks:
                self._execute_task(task, claim_timeout)

    def stop_worker(self) -> None:
        """Stop the running worker"""
        self._worker_running = False

    def close(self) -> None:
        """Stop the worker and close the connection if owned"""
        self.stop_worker()
        if self._owned_conn:
            self._conn.close()

    def make_async(self) -> AsyncAbsurd:
        """Create an async client with the same configuration"""
        return AsyncAbsurd(
            self._conn.info.dsn,
            queue_name=self._queue_name,
            default_max_attempts=self._default_max_attempts,
        )

    def _execute_task(self, task: ClaimedTask, claim_timeout: int) -> None:
        """Execute a single task"""
        registration = self._registry.get(task["task_name"])

        if not registration:
            _fail_task_run(
                self._conn,
                self._queue_name,
                task["run_id"],
                Exception("Unknown task"),
            )
            return

        queue_name = registration["queue"]

        if queue_name != self._queue_name:
            _fail_task_run(
                self._conn,
                self._queue_name,
                task["run_id"],
                Exception("Misconfigured task (queue mismatch)"),
            )
            return

        ctx = _create_task_context(
            task["task_id"], self._conn, queue_name, task, claim_timeout
        )

        # Set contextvar and execute with optional wrap hook
        token = _current_task_context.set(ctx)
        try:
            wrap_hook = self._hooks.get("wrap_task_execution")
            if wrap_hook is not None:
                result = wrap_hook(
                    ctx, lambda: registration["handler"](task["params"], ctx)
                )
            else:
                result = registration["handler"](task["params"], ctx)
            _complete_task_run(self._conn, queue_name, task["run_id"], result)
        except (SuspendTask, CancelledTask):
            pass
        except Exception as err:
            _fail_task_run(self._conn, queue_name, task["run_id"], err)
        finally:
            _current_task_context.reset(token)


class AsyncAbsurd(_AbsurdBase):
    """Asynchronous Absurd SDK client"""

    def __init__(
        self,
        conn_or_url: Optional[Union[AsyncConnection[Any], str]] = None,
        queue_name: str = "default",
        default_max_attempts: int = 5,
        hooks: Optional[AbsurdHooks] = None,
    ) -> None:
        if conn_or_url is None:
            conn_or_url = os.environ.get(
                "ABSURD_DATABASE_URL", "postgresql://localhost/absurd"
            )

        if isinstance(conn_or_url, str):
            self._conn_string: Optional[str] = conn_or_url
            self._conn: Optional[AsyncConnection[Any]] = None
            self._owned_conn = True
        else:
            self._conn = conn_or_url
            self._conn_string = None
            self._owned_conn = False
        super().__init__(queue_name, default_max_attempts, hooks)

    async def _ensure_connected(self) -> None:
        """Ensure the connection is established"""
        if self._conn is None and self._conn_string:
            self._conn = await AsyncConnection.connect(self._conn_string, autocommit=True)

    async def create_queue(self, queue_name: Optional[str] = None) -> None:
        """Create a queue (defaults to this client's queue)"""
        await self._ensure_connected()
        assert self._conn is not None
        queue = queue_name or self._queue_name
        cursor = self._conn.cursor()
        await cursor.execute("SELECT absurd.create_queue(%s)", (queue,))

    async def drop_queue(self, queue_name: Optional[str] = None) -> None:
        """Drop a queue (defaults to this client's queue)"""
        await self._ensure_connected()
        assert self._conn is not None
        queue = queue_name or self._queue_name
        cursor = self._conn.cursor()
        await cursor.execute("SELECT absurd.drop_queue(%s)", (queue,))

    async def list_queues(self) -> List[str]:
        """List all queue names"""
        await self._ensure_connected()
        assert self._conn is not None
        cursor = self._conn.cursor(row_factory=dict_row)
        await cursor.execute("SELECT * FROM absurd.list_queues()")
        rows = await cursor.fetchall()
        return [row["queue_name"] for row in rows]

    async def spawn(
        self,
        task_name: str,
        params: Any,
        max_attempts: Optional[int] = None,
        retry_strategy: Optional[RetryStrategy] = None,
        headers: Optional[JsonObject] = None,
        queue: Optional[str] = None,
        cancellation: Optional[CancellationPolicy] = None,
        idempotency_key: Optional[str] = None,
    ) -> SpawnResult:
        """Spawn a task execution by enqueueing it for processing"""
        await self._ensure_connected()
        assert self._conn is not None

        # Build SpawnOptions and apply before_spawn hook if configured
        spawn_options: SpawnOptions = {
            "max_attempts": max_attempts,
            "retry_strategy": retry_strategy,
            "headers": headers,
            "queue": queue,
            "cancellation": cancellation,
            "idempotency_key": idempotency_key,
        }

        before_spawn = self._hooks.get("before_spawn")
        if before_spawn is not None:
            result = before_spawn(task_name, params, spawn_options)
            # Handle both sync and async hooks
            if hasattr(result, "__await__"):
                spawn_options = await result
            else:
                spawn_options = result

        actual_queue, options = self._prepare_spawn(
            task_name,
            max_attempts=spawn_options.get("max_attempts"),
            retry_strategy=spawn_options.get("retry_strategy"),
            headers=spawn_options.get("headers"),
            queue=spawn_options.get("queue"),
            cancellation=spawn_options.get("cancellation"),
            idempotency_key=spawn_options.get("idempotency_key"),
        )
        cursor = self._conn.cursor(row_factory=dict_row)
        await cursor.execute(
            """SELECT task_id, run_id, attempt
               FROM absurd.spawn_task(%s, %s, %s, %s)""",
            (actual_queue, task_name, json.dumps(params), json.dumps(options)),
        )
        row = await cursor.fetchone()

        if not row:
            raise Exception("Failed to spawn task")

        return {
            "task_id": row["task_id"],
            "run_id": row["run_id"],
            "attempt": row["attempt"],
        }

    async def emit_event(
        self,
        event_name: str,
        payload: Optional[JsonValue] = None,
        queue_name: Optional[str] = None,
    ) -> None:
        """Emit an event with an optional payload on the specified or default queue"""
        await self._ensure_connected()
        assert self._conn is not None
        if not event_name:
            raise ValueError("event_name must be a non-empty string")

        cursor = self._conn.cursor()
        await cursor.execute(
            "SELECT absurd.emit_event(%s, %s, %s)",
            (queue_name or self._queue_name, event_name, json.dumps(payload)),
        )

    async def cancel_task(self, task_id: str, queue_name: Optional[str] = None) -> None:
        """Cancel a task by ID on the specified or default queue"""
        await self._ensure_connected()
        assert self._conn is not None
        cursor = self._conn.cursor()
        await cursor.execute(
            "SELECT absurd.cancel_task(%s, %s)",
            (queue_name or self._queue_name, task_id),
        )

    async def claim_tasks(
        self, batch_size: int = 1, claim_timeout: int = 120, worker_id: str = "worker"
    ) -> List[ClaimedTask]:
        """Claim up to batch_size tasks from the queue"""
        await self._ensure_connected()
        assert self._conn is not None
        cursor = self._conn.cursor(row_factory=dict_row)
        await cursor.execute(
            """SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts,
                      headers, wake_event, event_payload
               FROM absurd.claim_task(%s, %s, %s, %s)""",
            (self._queue_name, worker_id, claim_timeout, batch_size),
        )
        return await cursor.fetchall()  # type: ignore

    async def work_batch(
        self, worker_id: str = "worker", claim_timeout: int = 120, batch_size: int = 1
    ) -> None:
        """Claim and process up to batch_size tasks sequentially"""
        tasks = await self.claim_tasks(
            batch_size=batch_size, claim_timeout=claim_timeout, worker_id=worker_id
        )

        for task in tasks:
            await self._execute_task(task, claim_timeout)

    async def start_worker(
        self,
        worker_id: Optional[str] = None,
        claim_timeout: int = 120,
        concurrency: int = 1,
        batch_size: Optional[int] = None,
        poll_interval: float = 0.25,
    ) -> None:
        """Start an asynchronous worker that continuously polls for tasks"""
        import asyncio

        await self._ensure_connected()
        if worker_id is None:
            worker_id = f"{socket.gethostname()}:{os.getpid()}"

        effective_batch_size = batch_size or concurrency
        self._worker_running = True

        while self._worker_running:
            tasks = await self.claim_tasks(
                batch_size=effective_batch_size,
                claim_timeout=claim_timeout,
                worker_id=worker_id,
            )

            if not tasks:
                await asyncio.sleep(poll_interval)
                continue

            executing = set()
            for task in tasks:
                promise = asyncio.create_task(self._execute_task(task, claim_timeout))
                executing.add(promise)
                promise.add_done_callback(executing.discard)

                if len(executing) >= concurrency:
                    await asyncio.wait(executing, return_when=asyncio.FIRST_COMPLETED)

            await asyncio.gather(*executing)

    def stop_worker(self) -> None:
        """Stop the running worker"""
        self._worker_running = False

    async def close(self) -> None:
        """Stop the worker and close the connection if owned"""
        self.stop_worker()
        if self._owned_conn and self._conn:
            await self._conn.close()

    def make_sync(self) -> Absurd:
        """Create a sync client with the same configuration"""
        return Absurd(
            self._conn_string if self._conn_string else self._conn.info.dsn,  # type: ignore
            queue_name=self._queue_name,
            default_max_attempts=self._default_max_attempts,
        )

    async def _execute_task(self, task: ClaimedTask, claim_timeout: int) -> None:
        """Execute a single task"""
        assert self._conn is not None
        registration = self._registry.get(task["task_name"])

        if not registration:
            await _fail_task_run_async(
                self._conn,
                self._queue_name,
                task["run_id"],
                Exception("Unknown task"),
            )
            return

        queue_name = registration["queue"]

        if queue_name != self._queue_name:
            await _fail_task_run_async(
                self._conn,
                self._queue_name,
                task["run_id"],
                Exception("Misconfigured task (queue mismatch)"),
            )
            return

        ctx = await _create_async_task_context(
            task["task_id"], self._conn, queue_name, task, claim_timeout
        )

        # Set contextvar and execute with optional wrap hook
        token = _current_task_context.set(ctx)
        try:
            wrap_hook = self._hooks.get("wrap_task_execution")
            if wrap_hook is not None:

                async def execute():
                    return await registration["handler"](task["params"], ctx)

                hook_result = wrap_hook(ctx, execute)
                # Handle both sync and async wrap hooks
                if hasattr(hook_result, "__await__"):
                    result = await hook_result
                else:
                    result = hook_result
            else:
                result = await registration["handler"](task["params"], ctx)
            await _complete_task_run_async(
                self._conn, queue_name, task["run_id"], result
            )
        except (SuspendTask, CancelledTask):
            pass
        except Exception as err:
            await _fail_task_run_async(
                self._conn,
                queue_name,
                task["run_id"],
                err,
            )
        finally:
            _current_task_context.reset(token)
