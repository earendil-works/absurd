# Absurd SDK for Python

Python SDK for [Absurd](https://github.com/earendil-works/absurd): a
PostgreSQL-based durable task execution system.

Absurd is the simplest durable execution workflow system you can think of. It's
entirely based on Postgres and nothing else. It's almost as easy to use as a
queue, but it handles scheduling and retries, and it does all of that without
needing any other services to run in addition to Postgres.

## What is Durable Execution?

Durable execution (or durable workflows) is a way to run long-lived, reliable
functions that can survive crashes, restarts, and network failures without
losing state or duplicating work. Instead of running your logic in memory, a
durable execution system decomposes a task into smaller pieces (step functions)
and records every step and decision.

## Installation

```bash
uv add absurd-sdk
```

## Synchronous API

If you omit the connection argument, the client uses `ABSURD_DATABASE_URL`,
then `PGDATABASE`, then `postgresql://localhost/absurd`.

```python
from absurd_sdk import Absurd

app = Absurd("postgresql://localhost/absurd")

@app.register_task(name="order-fulfillment")
def process_order(params, ctx):
    step = ctx.run_step

    @step("process-payment")
    def payment():
        return {
            "payment_id": f"pay-{params['order_id']}",
            "amount": params["amount"],
        }

    @step("reserve-inventory")
    def inventory():
        return {"reserved_items": params["items"]}

    shipment = ctx.await_event(f"shipment.packed:{params['order_id']}")

    @step("send-notification")
    def notification():
        return {
            "sent_to": params["email"],
            "tracking_number": shipment["tracking_number"],
        }

    return {
        "order_id": params["order_id"],
        "payment": payment,
        "inventory": inventory,
        "tracking_number": shipment["tracking_number"],
        "notification": notification,
    }

app.start_worker()
```

## Asynchronous API

```python
from absurd_sdk import AsyncAbsurd

app = AsyncAbsurd("postgresql://localhost/absurd")

@app.register_task(name="order-fulfillment")
async def process_order(params, ctx):
    async def process_payment():
        return {
            "payment_id": f"pay-{params['order_id']}",
            "amount": params["amount"],
        }

    payment = await ctx.step("process-payment", process_payment)

    async def reserve_inventory():
        return {"reserved_items": params["items"]}

    inventory = await ctx.step("reserve-inventory", reserve_inventory)

    shipment = await ctx.await_event(f"shipment.packed:{params['order_id']}")

    async def send_notification():
        return {
            "sent_to": params["email"],
            "tracking_number": shipment["tracking_number"],
        }

    notification = await ctx.step("send-notification", send_notification)

    return {
        "order_id": params["order_id"],
        "payment": payment,
        "inventory": inventory,
        "tracking_number": shipment["tracking_number"],
        "notification": notification,
    }

await app.start_worker()
```

For async tasks there is no decorator shortcut yet, but the pattern is the
same: define a zero-argument `async def` helper and pass it to
`await ctx.step("step-name", helper)`.

## Using `@step` in synchronous tasks

Because Python `lambda` is limited to a single expression, a nice pattern for
sync code is to alias `ctx.run_step` as `step` and then use `@step(...)`:

```python
@app.register_task(name="my-task")
def my_task(params, ctx):
    step = ctx.run_step

    # Define and run a step in one go
    @step()
    def fetch_data():
        return {"result": 42}

    # fetch_data is now the return value ({"result": 42}), not a function
    print(fetch_data)  # {"result": 42}

    @step("transform-data")
    def transformed():
        return {"value": fetch_data["result"] * 2}

    return transformed
```

The decorator is only implemented for synchronous tasks. In asynchronous tasks
use `async def` helpers with `await ctx.step(...)`.

## Decomposed Steps

When you need to split step handling into two phases (for instance around an
external loop), use `begin_step()` / `complete_step()`:

```python
@app.register_task(name="agent-turn")
def agent_turn(params, ctx):
    handle = ctx.begin_step("persist-turn")
    if handle.done:
        persisted = handle.state
    else:
        payload = {"turn": params["turn"]}
        persisted = ctx.complete_step(handle, payload)

    return {"persisted": persisted}
```

The async API provides the same methods as `await ctx.begin_step(...)` and
`await ctx.complete_step(...)`.

## Task Result Snapshots

You can inspect or await a task's result state. Both methods return a
`TaskResultSnapshot` dataclass:

```python
snapshot = app.fetch_task_result(task_id)
if snapshot is not None:
    print(snapshot.state, snapshot.result, snapshot.failure)

final = app.await_task_result(task_id, timeout=30)
if final.state == "completed":
    print(final.result)
```

From inside a task handler, `TaskContext` / `AsyncTaskContext` also provide
`await_task_result(...)` so parent tasks can durably wait for child tasks.

## License and Links

- [Issue Tracker](https://github.com/earendil-works/absurd/issues)
- License: [Apache-2.0](https://github.com/earendil-works/absurd/blob/main/LICENSE)
