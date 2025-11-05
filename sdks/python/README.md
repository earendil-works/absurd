# Absurd Python SDK

Python SDK for Absurd - PostgreSQL-based durable task execution.

## Installation

```bash
pip install absurd_sdk
```

## Usage

### Synchronous API

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

### Asynchronous API

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

### Using the run_step decorator

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

## License

Apache-2.0
