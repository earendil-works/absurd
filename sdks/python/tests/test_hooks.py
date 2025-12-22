"""Tests for hooks and contextvars support."""

import asyncio
import contextvars
from typing import Any

import pytest
from psycopg import sql

from absurd_sdk import (
    Absurd,
    AsyncAbsurd,
    TaskContext,
    AsyncTaskContext,
    SpawnOptions,
    get_current_context,
)


def _fetch_run(conn, queue, run_id):
    query = sql.SQL(
        "select state, result from absurd.{table} where run_id = %s"
    ).format(table=sql.Identifier(f"r_{queue}"))
    return conn.execute(query, (run_id,)).fetchone()


class TestBeforeSpawnHook:
    """Tests for the before_spawn hook."""

    def test_sync_before_spawn_can_inject_headers(self, conn, queue_name):
        """before_spawn hook can inject headers into spawn options."""
        queue = queue_name("hooks")
        
        captured_headers = []
        
        def before_spawn(task_name: str, params: Any, options: SpawnOptions) -> SpawnOptions:
            return {
                **options,
                "headers": {
                    **(options.get("headers") or {}),
                    "trace_id": "trace-123",
                    "correlation_id": "corr-456",
                },
            }
        
        client = Absurd(conn, queue_name=queue, hooks={"before_spawn": before_spawn})
        client.create_queue()
        
        @client.register_task("capture-headers")
        def capture_headers(params, ctx: TaskContext):
            captured_headers.append(dict(ctx.headers))
            return "done"
        
        client.spawn("capture-headers", {"test": True})
        client.work_batch(worker_id="worker1")
        
        assert captured_headers == [{"trace_id": "trace-123", "correlation_id": "corr-456"}]

    def test_sync_before_spawn_preserves_existing_headers(self, conn, queue_name):
        """before_spawn hook preserves existing headers when adding new ones."""
        queue = queue_name("hooks")
        
        captured_headers = []
        
        def before_spawn(task_name: str, params: Any, options: SpawnOptions) -> SpawnOptions:
            return {
                **options,
                "headers": {
                    **(options.get("headers") or {}),
                    "injected": "by-hook",
                },
            }
        
        client = Absurd(conn, queue_name=queue, hooks={"before_spawn": before_spawn})
        client.create_queue()
        
        @client.register_task("merge-headers")
        def merge_headers(params, ctx: TaskContext):
            captured_headers.append(dict(ctx.headers))
            return "done"
        
        client.spawn("merge-headers", {}, headers={"existing": "user-provided"})
        client.work_batch(worker_id="worker1")
        
        assert captured_headers == [{"existing": "user-provided", "injected": "by-hook"}]


class TestAsyncBeforeSpawnHook:
    """Tests for the async before_spawn hook."""

    def test_async_before_spawn_can_inject_headers(self, db_dsn, conn, queue_name):
        """Async before_spawn hook can inject headers."""
        queue = queue_name("hooks")
        
        # Create queue with sync client
        Absurd(conn, queue_name=queue).create_queue()
        
        captured_headers = []
        
        async def before_spawn(task_name: str, params: Any, options: SpawnOptions) -> SpawnOptions:
            await asyncio.sleep(0.01)  # Simulate async operation
            return {
                **options,
                "headers": {
                    **(options.get("headers") or {}),
                    "async_header": "fetched-value",
                },
            }
        
        async def run():
            client = AsyncAbsurd(db_dsn, queue_name=queue, hooks={"before_spawn": before_spawn})
            
            @client.register_task("async-header")
            async def async_header(params, ctx: AsyncTaskContext):
                captured_headers.append(dict(ctx.headers))
                return "done"
            
            await client.spawn("async-header", {})
            await client.work_batch(worker_id="worker1")
            await client.close()
        
        asyncio.run(run())
        
        assert captured_headers == [{"async_header": "fetched-value"}]


class TestWrapTaskExecutionHook:
    """Tests for the wrap_task_execution hook."""

    def test_sync_wrap_task_execution(self, conn, queue_name):
        """wrap_task_execution hook wraps task execution."""
        queue = queue_name("hooks")
        
        execution_order = []
        
        def wrap_execution(ctx, execute):
            execution_order.append("before")
            result = execute()
            execution_order.append("after")
            return result
        
        client = Absurd(conn, queue_name=queue, hooks={"wrap_task_execution": wrap_execution})
        client.create_queue()
        
        @client.register_task("wrapped-task")
        def wrapped_task(params, ctx):
            execution_order.append("handler")
            return "done"
        
        client.spawn("wrapped-task", {})
        client.work_batch(worker_id="worker1")
        
        assert execution_order == ["before", "handler", "after"]

    def test_async_wrap_task_execution(self, db_dsn, conn, queue_name):
        """Async wrap_task_execution hook wraps task execution."""
        queue = queue_name("hooks")
        
        Absurd(conn, queue_name=queue).create_queue()
        
        execution_order = []
        
        async def wrap_execution(ctx, execute):
            execution_order.append("before")
            result = await execute()
            execution_order.append("after")
            return result
        
        async def run():
            client = AsyncAbsurd(db_dsn, queue_name=queue, hooks={"wrap_task_execution": wrap_execution})
            
            @client.register_task("wrapped-task")
            async def wrapped_task(params, ctx):
                execution_order.append("handler")
                return "done"
            
            await client.spawn("wrapped-task", {})
            await client.work_batch(worker_id="worker1")
            await client.close()
        
        asyncio.run(run())
        
        assert execution_order == ["before", "handler", "after"]


class TestContextvarsIntegration:
    """Tests for contextvars integration."""

    def test_sync_contextvar_propagation(self, conn, queue_name):
        """Contextvars can be propagated via hooks."""
        queue = queue_name("hooks")
        
        trace_context: contextvars.ContextVar[dict] = contextvars.ContextVar("trace_context")
        
        captured_in_handler = []
        
        def before_spawn(task_name: str, params: Any, options: SpawnOptions) -> SpawnOptions:
            ctx = trace_context.get(None)
            if ctx:
                return {
                    **options,
                    "headers": {
                        **(options.get("headers") or {}),
                        "trace_id": ctx["trace_id"],
                        "span_id": ctx["span_id"],
                    },
                }
            return options
        
        def wrap_execution(ctx, execute):
            headers = ctx.headers
            trace_id = headers.get("trace_id")
            span_id = headers.get("span_id")
            if trace_id and span_id:
                token = trace_context.set({"trace_id": trace_id, "span_id": span_id})
                try:
                    return execute()
                finally:
                    trace_context.reset(token)
            return execute()
        
        client = Absurd(
            conn,
            queue_name=queue,
            hooks={"before_spawn": before_spawn, "wrap_task_execution": wrap_execution},
        )
        client.create_queue()
        
        @client.register_task("contextvar-test")
        def contextvar_test(params, ctx):
            captured_in_handler.append(trace_context.get(None))
            return "done"
        
        # Spawn within a trace context
        trace_context.set({"trace_id": "trace-abc", "span_id": "span-xyz"})
        client.spawn("contextvar-test", {})
        
        # Clear the context before execution (simulating different process)
        trace_context.set(None)
        
        # Execute - should restore context from headers
        client.work_batch(worker_id="worker1")
        
        assert captured_in_handler == [{"trace_id": "trace-abc", "span_id": "span-xyz"}]

    def test_async_contextvar_propagation(self, db_dsn, conn, queue_name):
        """Async contextvars can be propagated via hooks."""
        queue = queue_name("hooks")
        
        Absurd(conn, queue_name=queue).create_queue()
        
        trace_context: contextvars.ContextVar[dict] = contextvars.ContextVar("trace_context")
        
        captured_in_handler = []
        
        def before_spawn(task_name: str, params: Any, options: SpawnOptions) -> SpawnOptions:
            ctx = trace_context.get(None)
            if ctx:
                return {
                    **options,
                    "headers": {
                        **(options.get("headers") or {}),
                        "trace_id": ctx["trace_id"],
                    },
                }
            return options
        
        async def wrap_execution(ctx, execute):
            headers = ctx.headers
            trace_id = headers.get("trace_id")
            if trace_id:
                token = trace_context.set({"trace_id": trace_id})
                try:
                    return await execute()
                finally:
                    trace_context.reset(token)
            return await execute()
        
        async def run():
            client = AsyncAbsurd(
                db_dsn,
                queue_name=queue,
                hooks={"before_spawn": before_spawn, "wrap_task_execution": wrap_execution},
            )
            
            @client.register_task("async-contextvar-test")
            async def contextvar_test(params, ctx):
                captured_in_handler.append(trace_context.get(None))
                return "done"
            
            # Spawn within a trace context
            trace_context.set({"trace_id": "async-trace-123"})
            await client.spawn("async-contextvar-test", {})
            
            # Clear the context
            trace_context.set(None)
            
            # Execute - should restore context
            await client.work_batch(worker_id="worker1")
            await client.close()
        
        asyncio.run(run())
        
        assert captured_in_handler == [{"trace_id": "async-trace-123"}]


class TestGetCurrentContext:
    """Tests for get_current_context() function."""

    def test_sync_get_current_context(self, conn, queue_name):
        """get_current_context() returns TaskContext during execution."""
        queue = queue_name("hooks")
        
        client = Absurd(conn, queue_name=queue)
        client.create_queue()
        
        captured_contexts = []
        
        @client.register_task("context-test")
        def context_test(params, ctx):
            current = get_current_context()
            captured_contexts.append(current)
            return "done"
        
        client.spawn("context-test", {})
        client.work_batch(worker_id="worker1")
        
        assert len(captured_contexts) == 1
        assert isinstance(captured_contexts[0], TaskContext)

    def test_sync_get_current_context_outside_task(self):
        """get_current_context() returns None outside task execution."""
        assert get_current_context() is None

    def test_async_get_current_context(self, db_dsn, conn, queue_name):
        """Async get_current_context() returns AsyncTaskContext during execution."""
        queue = queue_name("hooks")
        
        Absurd(conn, queue_name=queue).create_queue()
        
        captured_contexts = []
        
        async def run():
            client = AsyncAbsurd(db_dsn, queue_name=queue)
            
            @client.register_task("async-context-test")
            async def context_test(params, ctx):
                current = get_current_context()
                captured_contexts.append(current)
                return "done"
            
            await client.spawn("async-context-test", {})
            await client.work_batch(worker_id="worker1")
            await client.close()
        
        asyncio.run(run())
        
        assert len(captured_contexts) == 1
        assert isinstance(captured_contexts[0], AsyncTaskContext)

    def test_async_concurrent_tasks_have_isolated_contexts(self, db_dsn, conn, queue_name):
        """Concurrent async tasks each have their own isolated context."""
        queue = queue_name("hooks")
        
        Absurd(conn, queue_name=queue).create_queue()
        
        captured_task_ids = []
        
        async def run():
            client = AsyncAbsurd(db_dsn, queue_name=queue)
            
            @client.register_task("concurrent-test")
            async def concurrent_test(params, ctx):
                # Simulate some async work
                await asyncio.sleep(0.05)
                current = get_current_context()
                captured_task_ids.append((params["task_num"], current.task_id if current else None))
                return f"done-{params['task_num']}"
            
            # Spawn multiple tasks
            spawned = []
            for i in range(3):
                result = await client.spawn("concurrent-test", {"task_num": i})
                spawned.append(result)
            
            # Process all tasks
            for _ in range(3):
                await client.work_batch(worker_id="worker1")
            
            await client.close()
            return spawned
        
        spawned = asyncio.run(run())
        
        # Each captured context should match the correct task
        assert len(captured_task_ids) == 3
        for task_num, captured_id in captured_task_ids:
            expected_id = spawned[task_num]["task_id"]
            assert captured_id == expected_id, f"Task {task_num} got wrong context"


class TestChildSpawnInheritsContext:
    """Tests that child tasks spawned from within a task inherit context."""

    def test_sync_child_spawn_inherits_context(self, conn, queue_name):
        """Child task spawned from parent inherits trace context via hooks."""
        queue = queue_name("hooks")
        
        trace_context: contextvars.ContextVar[dict] = contextvars.ContextVar("trace_context")
        
        child_trace_id = []
        
        def before_spawn(task_name: str, params: Any, options: SpawnOptions) -> SpawnOptions:
            ctx = trace_context.get(None)
            if ctx:
                return {
                    **options,
                    "headers": {
                        **(options.get("headers") or {}),
                        "trace_id": ctx["trace_id"],
                    },
                }
            return options
        
        def wrap_execution(ctx, execute):
            headers = ctx.headers
            trace_id = headers.get("trace_id")
            if trace_id:
                token = trace_context.set({"trace_id": trace_id})
                try:
                    return execute()
                finally:
                    trace_context.reset(token)
            return execute()
        
        client = Absurd(
            conn,
            queue_name=queue,
            hooks={"before_spawn": before_spawn, "wrap_task_execution": wrap_execution},
        )
        client.create_queue()
        
        @client.register_task("parent-task")
        def parent_task(params, ctx):
            # Spawn child - should inherit trace context via beforeSpawn hook
            client.spawn("child-task", {})
            return "parent-done"
        
        @client.register_task("child-task")
        def child_task(params, ctx):
            headers = ctx.headers
            child_trace_id.append(headers.get("trace_id"))
            return "child-done"
        
        # Spawn parent with trace context
        trace_context.set({"trace_id": "parent-trace"})
        client.spawn("parent-task", {})
        
        # Clear context
        trace_context.set(None)
        
        # Execute parent (which spawns child)
        client.work_batch(worker_id="worker1")
        # Execute child
        client.work_batch(worker_id="worker1")
        
        assert child_trace_id == ["parent-trace"]
