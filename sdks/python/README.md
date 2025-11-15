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
uv add absurd_sdk
```

## Synchronous API

```python
from absurd_sdk import Absurd

app = Absurd("postgresql://localhost/absurd")

@app.register_task(name="order-fulfillment")
def process_order(params, ctx):
    payment = ctx.step("process-payment", lambda: stripe_charge(params["amount"]))
    inventory = ctx.step("reserve-inventory", lambda: reserve_items(params["items"]))
    shipment = ctx.await_event(f"shipment.packed:{params['order_id']}")
    ctx.step("send-notification", lambda: send_email(params["email"], shipment))
    return {"order_id": payment["id"], "tracking_number": shipment["tracking_number"]}

app.start_worker()
```

## Asynchronous API

```python
from absurd_sdk import AsyncAbsurd

app = AsyncAbsurd("postgresql://localhost/absurd")

@app.register_task(name="order-fulfillment")
async def process_order(params, ctx):
    payment = await ctx.step("process-payment", lambda: stripe_charge_async(params["amount"]))
    inventory = await ctx.step("reserve-inventory", lambda: reserve_items_async(params["items"]))
    shipment = await ctx.await_event(f"shipment.packed:{params['order_id']}")
    await ctx.step("send-notification", lambda: send_email_async(params["email"], shipment))
    return {"order_id": payment["id"], "tracking_number": shipment["tracking_number"]}

await app.start_worker()
```

## Using the run_step decorator

Because of limitations with `lambda` to be a single expression, for sync code
you can use the `run_step` decorator:

```python
@app.register_task(name="my-task")
def my_task(params, ctx):
    # Define and run a step in one go
    @ctx.run_step()
    def fetch_data():
        return {"result": 42}

    # fetch_data is now the return value ({"result": 42}), not a function
    print(fetch_data)  # {"result": 42}
```

The decorator is only implemented for synchronous tasks. In asynchronous tasks
use `await ctx.step("step-name", lambda: ...)` directly.

## License and Links

- [Issue Tracker](https://github.com/earendil-works/absurd/issues)
- License: [Apache-2.0](https://github.com/earendil-works/absurd/blob/main/LICENSE)
