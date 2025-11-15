"""Tests for step functionality"""

from datetime import datetime, timezone, timedelta
from psycopg import sql

from absurd_sdk import Absurd


def _set_fake_now(conn, fake_time):
    """Set the fake timestamp for testing"""
    if fake_time is None:
        conn.execute("set absurd.fake_now = default")
    else:
        # SET doesn't support parameterized queries, need to format as string
        from psycopg import sql
        conn.execute(
            sql.SQL("set absurd.fake_now = '{}'").format(sql.SQL(fake_time.isoformat()))
        )


def _get_task(conn, queue, task_id):
    """Get task details"""
    query = sql.SQL(
        "select state, attempts, completed_payload from absurd.{table} where task_id = %s"
    ).format(table=sql.Identifier(f"t_{queue}"))
    result = conn.execute(query, (task_id,)).fetchone()
    if not result:
        return None
    return {
        "state": result[0],
        "attempts": result[1],
        "completed_payload": result[2],
    }


def _get_run(conn, queue, run_id):
    """Get run details"""
    query = sql.SQL(
        "select state, nullif(available_at, 'infinity'::timestamptz) as available_at from absurd.{table} where run_id = %s"
    ).format(table=sql.Identifier(f"r_{queue}"))
    result = conn.execute(query, (run_id,)).fetchone()
    if not result:
        return None
    return {"state": result[0], "available_at": result[1]}


def test_step_executes_and_returns_value(conn, queue_name):
    """Test that a step executes and returns its value"""
    queue = queue_name("basic_step")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("basic")
    def basic_task(params, ctx):
        result = ctx.step("process", lambda: f"processed-{params['value']}")
        return {"result": result}

    spawned = client.spawn("basic", {"value": 42})
    client.work_batch("worker", 60, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {"result": "processed-42"}


def test_step_result_is_cached_and_not_reexecuted_on_retry(conn, queue_name):
    """Test that step results are cached and not re-executed"""
    queue = queue_name("cache_step")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    execution_count = []
    attempt_count = []

    @client.register_task("cache", default_max_attempts=2)
    def cache_task(params, ctx):
        attempt_count.append(len(attempt_count) + 1)

        def generate_random():
            execution_count.append(len(execution_count) + 1)
            return 42  # fixed value for deterministic testing

        cached = ctx.step("generate-random", generate_random)

        if len(attempt_count) == 1:
            raise Exception("Intentional failure")

        return {"random": cached, "count": len(execution_count)}

    spawned = client.spawn("cache", None)

    client.work_batch("worker", 60, 1)
    assert len(execution_count) == 1

    client.work_batch("worker", 60, 1)
    assert len(execution_count) == 1  # Not re-executed
    assert len(attempt_count) == 2

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"]["count"] == 1
    assert task["attempts"] == 2


def test_multistep_task_only_reexecutes_uncompleted_steps(conn, queue_name):
    """Test that only uncompleted steps are re-executed on retry"""
    queue = queue_name("multistep")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    executed = []
    attempt_count = []

    @client.register_task("multistep-retry", default_max_attempts=2)
    def multistep_task(params, ctx):
        attempt_count.append(len(attempt_count) + 1)

        step1 = ctx.step("step1", lambda: (executed.append("step1"), "result1")[1])
        step2 = ctx.step("step2", lambda: (executed.append("step2"), "result2")[1])

        if len(attempt_count) == 1:
            raise Exception("Fail before step3")

        step3 = ctx.step("step3", lambda: (executed.append("step3"), "result3")[1])

        return {"steps": [step1, step2, step3], "attemptNum": len(attempt_count)}

    spawned = client.spawn("multistep-retry", None)

    client.work_batch("worker", 60, 1)
    assert executed == ["step1", "step2"]

    client.work_batch("worker", 60, 1)
    assert executed == ["step1", "step2", "step3"]

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {
        "steps": ["result1", "result2", "result3"],
        "attemptNum": 2,
    }
    assert task["attempts"] == 2


def test_repeated_step_names_work_correctly(conn, queue_name):
    """Test that repeated step names are handled correctly"""
    queue = queue_name("deduplicate")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("deduplicate")
    def deduplicate_task(params, ctx):
        results = []
        for i in range(3):
            result = ctx.step("loop-step", lambda i=i: i * 10)
            results.append(result)
        return {"results": results}

    spawned = client.spawn("deduplicate", None)
    client.work_batch("worker", 60, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {"results": [0, 10, 20]}


def test_failed_step_does_not_save_checkpoint_and_reexecutes(conn, queue_name):
    """Test that failed steps don't save checkpoints and re-execute"""
    queue = queue_name("fail_step")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    attempt_count = []

    @client.register_task("fail", default_max_attempts=2)
    def fail_task(params, ctx):
        def failing_step():
            attempt_count.append(len(attempt_count) + 1)
            if len(attempt_count) == 1:
                raise Exception("Step fails on first attempt")
            return "success"

        result = ctx.step("fail", failing_step)
        return {"result": result}

    spawned = client.spawn("fail", None)

    client.work_batch("worker", 60, 1)
    assert len(attempt_count) == 1

    client.work_batch("worker", 60, 1)
    assert len(attempt_count) == 2

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {"result": "success"}
    assert task["attempts"] == 2


def test_sleep_for_suspends_until_duration_elapses(conn, queue_name):
    """Test sleepFor suspends until duration elapses"""
    queue = queue_name("sleep_for")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    base = datetime(2024, 5, 5, 10, 0, 0, tzinfo=timezone.utc)
    _set_fake_now(conn, base)

    duration_seconds = 60

    @client.register_task("sleep-for")
    def sleep_for_task(params, ctx):
        ctx.sleep_for("wait-for", duration_seconds)
        return {"resumed": True}

    spawned = client.spawn("sleep-for", None)
    client.work_batch("worker-sleep", 120, 1)

    run = _get_run(conn, queue, spawned["run_id"])
    assert run["state"] == "sleeping"
    wake_time = base + timedelta(seconds=duration_seconds)
    assert run["available_at"] == wake_time

    resume_time = wake_time + timedelta(seconds=5)
    _set_fake_now(conn, resume_time)
    client.work_batch("worker-sleep", 120, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {"resumed": True}


def test_sleep_until_checkpoint_prevents_rescheduling(conn, queue_name):
    """Test sleepUntil checkpoint prevents re-scheduling when wake time passed"""
    queue = queue_name("sleep_until")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    base = datetime(2024, 5, 6, 9, 0, 0, tzinfo=timezone.utc)
    _set_fake_now(conn, base)

    wake_time = base + timedelta(minutes=5)
    executions = []

    @client.register_task("sleep-until")
    def sleep_until_task(params, ctx):
        executions.append(len(executions) + 1)
        ctx.sleep_until("sleep-step", wake_time)
        return {"executions": len(executions)}

    spawned = client.spawn("sleep-until", None)
    client.work_batch("worker-sleep", 120, 1)

    # Check checkpoint was saved
    checkpoint_query = sql.SQL(
        "SELECT checkpoint_name, state, owner_run_id FROM absurd.{table} WHERE task_id = %s"
    ).format(table=sql.Identifier(f"c_{queue}"))
    checkpoint = conn.execute(checkpoint_query, (spawned["task_id"],)).fetchone()
    assert checkpoint[0] == "sleep-step"
    assert checkpoint[1] == wake_time.isoformat()
    assert checkpoint[2] == spawned["run_id"]

    run = _get_run(conn, queue, spawned["run_id"])
    assert run["state"] == "sleeping"

    _set_fake_now(conn, wake_time)
    client.work_batch("worker-sleep", 120, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {"executions": 2}
    assert len(executions) == 2


def test_run_step_decorator_with_default_name(conn, queue_name):
    """Test run_step decorator uses function name as step name"""
    queue = queue_name("run_step_default")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("run-step-default")
    def run_step_task(params, ctx):
        @ctx.run_step()
        def calculate_value():
            return params["x"] * 2

        return {"result": calculate_value}

    spawned = client.spawn("run-step-default", {"x": 21})
    client.work_batch("worker", 60, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {"result": 42}


def test_run_step_decorator_with_custom_name(conn, queue_name):
    """Test run_step decorator with custom step name"""
    queue = queue_name("run_step_custom")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("run-step-custom")
    def run_step_task(params, ctx):
        @ctx.run_step("custom_step_name")
        def calculate_value():
            return params["x"] + params["y"]

        return {"result": calculate_value}

    spawned = client.spawn("run-step-custom", {"x": 10, "y": 32})
    client.work_batch("worker", 60, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {"result": 42}

    # Verify the checkpoint was saved with custom name
    checkpoint_query = sql.SQL(
        "SELECT checkpoint_name, state FROM absurd.{table} WHERE task_id = %s"
    ).format(table=sql.Identifier(f"c_{queue}"))
    checkpoint = conn.execute(checkpoint_query, (spawned["task_id"],)).fetchone()
    assert checkpoint[0] == "custom_step_name"
    assert checkpoint[1] == 42


def test_run_step_decorator_caches_result(conn, queue_name):
    """Test run_step decorator caches result on retry"""
    queue = queue_name("run_step_cache")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    execution_count = []
    attempt_count = []

    @client.register_task("run-step-cache", default_max_attempts=2)
    def run_step_task(params, ctx):
        attempt_count.append(len(attempt_count) + 1)

        @ctx.run_step()
        def expensive_calculation():
            execution_count.append(len(execution_count) + 1)
            return 100

        if len(attempt_count) == 1:
            raise Exception("Intentional failure")

        return {"value": expensive_calculation, "executions": len(execution_count)}

    spawned = client.spawn("run-step-cache", None)

    client.work_batch("worker", 60, 1)
    assert len(execution_count) == 1

    client.work_batch("worker", 60, 1)
    assert len(execution_count) == 1  # Not re-executed
    assert len(attempt_count) == 2

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"]["value"] == 100
    assert task["completed_payload"]["executions"] == 1


def test_run_step_decorator_without_parentheses(conn, queue_name):
    """Test run_step decorator works without parentheses"""
    queue = queue_name("run_step_no_parens")
    client = Absurd(conn, queue_name=queue)
    client.create_queue()

    @client.register_task("run-step-no-parens")
    def run_step_task(params, ctx):
        @ctx.run_step
        def my_step():
            return "success"

        return {"result": my_step}

    spawned = client.spawn("run-step-no-parens", None)
    client.work_batch("worker", 60, 1)

    task = _get_task(conn, queue, spawned["task_id"])
    assert task["state"] == "completed"
    assert task["completed_payload"] == {"result": "success"}
