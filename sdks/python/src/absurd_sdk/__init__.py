"""
Absurd SDK for Python
"""

import json
import os
import socket
import time
import traceback
from datetime import datetime, timezone
from functools import wraps

from psycopg import AsyncConnection, Connection
from psycopg.rows import dict_row


class SuspendTask(Exception):
    """Internal exception thrown to suspend a run."""

    pass


class TimeoutError(Exception):
    """Error thrown when awaiting an event times out."""

    pass


class TaskContext:
    def __init__(
        self, task_id, conn, queue_name, task, checkpoint_cache, claim_timeout
    ):
        self.task_id = task_id
        self._conn = conn
        self._queue_name = queue_name
        self._task = task
        self._checkpoint_cache = checkpoint_cache
        self._claim_timeout = claim_timeout
        self._step_name_counter = {}

    @classmethod
    def create(cls, task_id, conn, queue_name, task, claim_timeout):
        cursor = conn.cursor(row_factory=dict_row)
        cursor.execute(
            """SELECT checkpoint_name, state, status, owner_run_id, updated_at
               FROM absurd.get_task_checkpoint_states(%s, %s, %s)""",
            (queue_name, task["task_id"], task["run_id"]),
        )
        cache = {row["checkpoint_name"]: row["state"] for row in cursor.fetchall()}
        return cls(task_id, conn, queue_name, task, cache, claim_timeout)

    def step(self, name, fn):
        checkpoint_name = self._get_checkpoint_name(name)
        state = self._lookup_checkpoint(checkpoint_name)
        if state is not None:
            return state

        rv = fn()
        self._persist_checkpoint(checkpoint_name, rv)
        return rv

    def run_step(self, name=None):
        def decorator(fn):
            step_name = name if name is not None else fn.__name__
            return self.step(step_name, fn)

        return decorator

    def sleep_for(self, step_name, duration):
        return self.sleep_until(
            step_name, datetime.now(timezone.utc).timestamp() + duration
        )

    def sleep_until(self, step_name, wake_at):
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

        if datetime.now(timezone.utc) < actual_wake_at:
            self._schedule_run(actual_wake_at)
            raise SuspendTask()

    def await_event(self, event_name, step_name=None, timeout=None):
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
        result = cursor.fetchone()

        if not result:
            raise Exception("Failed to await event")

        if not result["should_suspend"]:
            self._checkpoint_cache[checkpoint_name] = result["payload"]
            self._task["event_payload"] = None
            return result["payload"]

        raise SuspendTask()

    def emit_event(self, event_name, payload=None):
        if not event_name:
            raise ValueError("event_name must be a non-empty string")

        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT absurd.emit_event(%s, %s, %s)",
            (self._queue_name, event_name, json.dumps(payload)),
        )

    def complete(self, result=None):
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT absurd.complete_run(%s, %s, %s)",
            (self._queue_name, self._task["run_id"], json.dumps(result)),
        )

    def fail(self, err):
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT absurd.fail_run(%s, %s, %s, %s)",
            (
                self._queue_name,
                self._task["run_id"],
                json.dumps(_serialize_error(err)),
                None,
            ),
        )

    def _get_checkpoint_name(self, name):
        count = self._step_name_counter.get(name, 0) + 1
        self._step_name_counter[name] = count
        return name if count == 1 else f"{name}#{count}"

    def _lookup_checkpoint(self, checkpoint_name):
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

    def _persist_checkpoint(self, checkpoint_name, value):
        cursor = self._conn.cursor()
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
        self._checkpoint_cache[checkpoint_name] = value

    def _schedule_run(self, wake_at):
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT absurd.schedule_run(%s, %s, %s)",
            (self._queue_name, self._task["run_id"], wake_at),
        )


class AsyncTaskContext:
    def __init__(
        self, task_id, conn, queue_name, task, checkpoint_cache, claim_timeout
    ):
        self.task_id = task_id
        self._conn = conn
        self._queue_name = queue_name
        self._task = task
        self._checkpoint_cache = checkpoint_cache
        self._claim_timeout = claim_timeout
        self._step_name_counter = {}

    @classmethod
    async def create(cls, task_id, conn, queue_name, task, claim_timeout):
        cursor = conn.cursor(row_factory=dict_row)
        await cursor.execute(
            """SELECT checkpoint_name, state, status, owner_run_id, updated_at
               FROM absurd.get_task_checkpoint_states(%s, %s, %s)""",
            (queue_name, task["task_id"], task["run_id"]),
        )
        rows = await cursor.fetchall()
        cache = {row["checkpoint_name"]: row["state"] for row in rows}
        return cls(task_id, conn, queue_name, task, cache, claim_timeout)

    async def step(self, name, fn):
        checkpoint_name = self._get_checkpoint_name(name)
        state = await self._lookup_checkpoint(checkpoint_name)
        if state is not None:
            return state

        rv = await fn()
        await self._persist_checkpoint(checkpoint_name, rv)
        return rv

    def run_step(self, name=None):
        def decorator(fn):
            step_name = name if name is not None else fn.__name__

            @wraps(fn)
            async def wrapper():
                return await self.step(step_name, fn)

            # Create a coroutine from the wrapper and await it immediately
            return asyncio.create_task(wrapper())

        return decorator

    async def sleep_for(self, step_name, duration):
        return await self.sleep_until(
            step_name, datetime.now(timezone.utc).timestamp() + duration
        )

    async def sleep_until(self, step_name, wake_at):
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

        if datetime.now(timezone.utc) < actual_wake_at:
            await self._schedule_run(actual_wake_at)
            raise SuspendTask()

    async def await_event(self, event_name, step_name=None, timeout=None):
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
        result = await cursor.fetchone()

        if not result:
            raise Exception("Failed to await event")

        if not result["should_suspend"]:
            self._checkpoint_cache[checkpoint_name] = result["payload"]
            self._task["event_payload"] = None
            return result["payload"]

        raise SuspendTask()

    async def emit_event(self, event_name, payload=None):
        if not event_name:
            raise ValueError("event_name must be a non-empty string")

        cursor = self._conn.cursor()
        await cursor.execute(
            "SELECT absurd.emit_event(%s, %s, %s)",
            (self._queue_name, event_name, json.dumps(payload)),
        )

    async def complete(self, result=None):
        cursor = self._conn.cursor()
        await cursor.execute(
            "SELECT absurd.complete_run(%s, %s, %s)",
            (self._queue_name, self._task["run_id"], json.dumps(result)),
        )

    async def fail(self, err):
        cursor = self._conn.cursor()
        await cursor.execute(
            "SELECT absurd.fail_run(%s, %s, %s, %s)",
            (
                self._queue_name,
                self._task["run_id"],
                json.dumps(_serialize_error(err)),
                None,
            ),
        )

    def _get_checkpoint_name(self, name):
        count = self._step_name_counter.get(name, 0) + 1
        self._step_name_counter[name] = count
        return name if count == 1 else f"{name}#{count}"

    async def _lookup_checkpoint(self, checkpoint_name):
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

    async def _persist_checkpoint(self, checkpoint_name, value):
        cursor = self._conn.cursor()
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
        self._checkpoint_cache[checkpoint_name] = value

    async def _schedule_run(self, wake_at):
        cursor = self._conn.cursor()
        await cursor.execute(
            "SELECT absurd.schedule_run(%s, %s, %s)",
            (self._queue_name, self._task["run_id"], wake_at),
        )


class _AbsurdBase:
    def __init__(self, queue_name="default", default_max_attempts=5):
        self._queue_name = queue_name
        self._default_max_attempts = default_max_attempts
        self._registry = {}
        self._worker_running = False

    def register_task(
        self, name, queue=None, default_max_attempts=None, default_cancellation=None
    ):
        def decorator(handler):
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
        task_name,
        max_attempts=None,
        retry_strategy=None,
        headers=None,
        queue=None,
        cancellation=None,
    ):
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
        )

        return actual_queue, options


class Absurd(_AbsurdBase):
    def __init__(self, conn_or_url=None, queue_name="default", default_max_attempts=5):
        if conn_or_url is None:
            conn_or_url = os.environ.get(
                "ABSURD_DATABASE_URL", "postgresql://localhost/absurd"
            )

        if isinstance(conn_or_url, str):
            self._conn = Connection.connect(conn_or_url)
            self._owned_conn = True
        else:
            self._conn = conn_or_url
            self._owned_conn = False
        super().__init__(queue_name, default_max_attempts)

    def create_queue(self, queue_name=None):
        queue = queue_name or self._queue_name
        cursor = self._conn.cursor()
        cursor.execute("SELECT absurd.create_queue(%s)", (queue,))

    def drop_queue(self, queue_name=None):
        queue = queue_name or self._queue_name
        cursor = self._conn.cursor()
        cursor.execute("SELECT absurd.drop_queue(%s)", (queue,))

    def list_queues(self):
        cursor = self._conn.cursor(row_factory=dict_row)
        cursor.execute("SELECT * FROM absurd.list_queues()")
        return [row["queue_name"] for row in cursor.fetchall()]

    def spawn(
        self,
        task_name,
        params,
        max_attempts=None,
        retry_strategy=None,
        headers=None,
        queue=None,
        cancellation=None,
    ):
        actual_queue, options = self._prepare_spawn(
            task_name,
            max_attempts=max_attempts,
            retry_strategy=retry_strategy,
            headers=headers,
            queue=queue,
            cancellation=cancellation,
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

    def emit_event(self, event_name, payload=None, queue_name=None):
        if not event_name:
            raise ValueError("event_name must be a non-empty string")

        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT absurd.emit_event(%s, %s, %s)",
            (queue_name or self._queue_name, event_name, json.dumps(payload)),
        )

    def claim_tasks(self, batch_size=1, claim_timeout=120, worker_id="worker"):
        cursor = self._conn.cursor(row_factory=dict_row)
        cursor.execute(
            """SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts,
                      headers, wake_event, event_payload
               FROM absurd.claim_task(%s, %s, %s, %s)""",
            (self._queue_name, worker_id, claim_timeout, batch_size),
        )
        return cursor.fetchall()

    def work_batch(self, worker_id="worker", claim_timeout=120, batch_size=1):
        tasks = self.claim_tasks(
            batch_size=batch_size, claim_timeout=claim_timeout, worker_id=worker_id
        )

        for task in tasks:
            self._execute_task(task, claim_timeout)

    def start_worker(
        self,
        worker_id=None,
        claim_timeout=120,
        concurrency=1,
        batch_size=None,
        poll_interval=0.25,
        on_error=None,
    ):
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

    def stop_worker(self):
        self._worker_running = False

    def close(self):
        self.stop_worker()
        if self._owned_conn:
            self._conn.close()

    def make_async(self):
        return AsyncAbsurd(
            self._conn.info.dsn,
            queue_name=self._queue_name,
            default_max_attempts=self._default_max_attempts,
        )

    def _execute_task(self, task, claim_timeout):
        registration = self._registry.get(task["task_name"])

        if not registration:
            ctx = TaskContext.create(
                task["task_id"], self._conn, "unknown", task, claim_timeout
            )
            ctx.fail(Exception("Unknown task"))
            return

        if registration["queue"] != self._queue_name:
            ctx = TaskContext.create(
                task["task_id"], self._conn, registration["queue"], task, claim_timeout
            )
            ctx.fail(Exception("Misconfigured task (queue mismatch)"))
            return

        ctx = TaskContext.create(
            task["task_id"], self._conn, registration["queue"], task, claim_timeout
        )

        try:
            result = registration["handler"](task["params"], ctx)
            ctx.complete(result)
        except SuspendTask:
            pass
        except Exception as err:
            ctx.fail(err)


class AsyncAbsurd(_AbsurdBase):
    def __init__(self, conn_or_url=None, queue_name="default", default_max_attempts=5):
        # Defer import asyncio
        global asyncio
        import asyncio

        if conn_or_url is None:
            conn_or_url = os.environ.get(
                "ABSURD_DATABASE_URL", "postgresql://localhost/absurd"
            )

        if isinstance(conn_or_url, str):
            self._conn_string = conn_or_url
            self._conn = None
            self._owned_conn = True
        else:
            self._conn = conn_or_url
            self._conn_string = None
            self._owned_conn = False
        super().__init__(queue_name, default_max_attempts)

    async def _ensure_connected(self):
        if self._conn is None and self._conn_string:
            self._conn = await AsyncConnection.connect(self._conn_string)

    async def create_queue(self, queue_name=None):
        await self._ensure_connected()
        queue = queue_name or self._queue_name
        cursor = self._conn.cursor()
        await cursor.execute("SELECT absurd.create_queue(%s)", (queue,))

    async def drop_queue(self, queue_name=None):
        await self._ensure_connected()
        queue = queue_name or self._queue_name
        cursor = self._conn.cursor()
        await cursor.execute("SELECT absurd.drop_queue(%s)", (queue,))

    async def list_queues(self):
        await self._ensure_connected()
        cursor = self._conn.cursor(row_factory=dict_row)
        await cursor.execute("SELECT * FROM absurd.list_queues()")
        rows = await cursor.fetchall()
        return [row["queue_name"] for row in rows]

    async def spawn(
        self,
        task_name,
        params,
        max_attempts=None,
        retry_strategy=None,
        headers=None,
        queue=None,
        cancellation=None,
    ):
        await self._ensure_connected()
        actual_queue, options = self._prepare_spawn(
            task_name,
            max_attempts=max_attempts,
            retry_strategy=retry_strategy,
            headers=headers,
            queue=queue,
            cancellation=cancellation,
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

    async def emit_event(self, event_name, payload=None, queue_name=None):
        await self._ensure_connected()
        if not event_name:
            raise ValueError("event_name must be a non-empty string")

        cursor = self._conn.cursor()
        await cursor.execute(
            "SELECT absurd.emit_event(%s, %s, %s)",
            (queue_name or self._queue_name, event_name, json.dumps(payload)),
        )

    async def claim_tasks(self, batch_size=1, claim_timeout=120, worker_id="worker"):
        await self._ensure_connected()
        cursor = self._conn.cursor(row_factory=dict_row)
        await cursor.execute(
            """SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts,
                      headers, wake_event, event_payload
               FROM absurd.claim_task(%s, %s, %s, %s)""",
            (self._queue_name, worker_id, claim_timeout, batch_size),
        )
        return await cursor.fetchall()

    async def work_batch(self, worker_id="worker", claim_timeout=120, batch_size=1):
        tasks = await self.claim_tasks(
            batch_size=batch_size, claim_timeout=claim_timeout, worker_id=worker_id
        )

        for task in tasks:
            await self._execute_task(task, claim_timeout)

    async def start_worker(
        self,
        worker_id=None,
        claim_timeout=120,
        concurrency=1,
        batch_size=None,
        poll_interval=0.25,
        on_error=None,
    ):
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

    def stop_worker(self):
        self._worker_running = False

    async def close(self):
        self.stop_worker()
        if self._owned_conn and self._conn:
            await self._conn.close()

    def make_sync(self):
        return Absurd(
            self._conn_string if self._conn_string else self._conn.info.dsn,
            queue_name=self._queue_name,
            default_max_attempts=self._default_max_attempts,
        )

    async def _execute_task(self, task, claim_timeout):
        registration = self._registry.get(task["task_name"])

        if not registration:
            ctx = await AsyncTaskContext.create(
                task["task_id"], self._conn, "unknown", task, claim_timeout
            )
            await ctx.fail(Exception("Unknown task"))
            return

        if registration["queue"] != self._queue_name:
            ctx = await AsyncTaskContext.create(
                task["task_id"], self._conn, registration["queue"], task, claim_timeout
            )
            await ctx.fail(Exception("Misconfigured task (queue mismatch)"))
            return

        ctx = await AsyncTaskContext.create(
            task["task_id"], self._conn, registration["queue"], task, claim_timeout
        )

        try:
            result = await registration["handler"](task["params"], ctx)
            await ctx.complete(result)
        except SuspendTask:
            pass
        except Exception as err:
            await ctx.fail(err)


def _serialize_error(err):
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


def _normalize_spawn_options(
    max_attempts=None, retry_strategy=None, headers=None, cancellation=None
):
    normalized = {}

    if headers is not None:
        normalized["headers"] = headers
    if max_attempts is not None:
        normalized["max_attempts"] = max_attempts
    if retry_strategy:
        normalized["retry_strategy"] = _serialize_retry_strategy(retry_strategy)

    cancel = _normalize_cancellation(cancellation)
    if cancel:
        normalized["cancellation"] = cancel

    return normalized


def _serialize_retry_strategy(strategy):
    serialized = {"kind": strategy["kind"]}

    if "base_seconds" in strategy:
        serialized["base_seconds"] = strategy["base_seconds"]
    if "factor" in strategy:
        serialized["factor"] = strategy["factor"]
    if "max_seconds" in strategy:
        serialized["max_seconds"] = strategy["max_seconds"]

    return serialized


def _normalize_cancellation(policy):
    if not policy:
        return None

    normalized = {}
    if "max_duration" in policy:
        normalized["max_duration"] = policy["max_duration"]
    if "max_delay" in policy:
        normalized["max_delay"] = policy["max_delay"]

    return normalized if normalized else None
