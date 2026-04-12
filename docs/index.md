<div style="text-align: center" align="center">
  <img src="images/logo.jpg" width="350" alt="Une photo d'un éléphant avec le titre : « Ceci n'est pas un éléphant »">
</div>

# Absurd

Absurd is a Postgres-native durable workflow system.  It moves the complexity of
durable execution into the database layer via stored procedures, keeping SDKs
lightweight and language-agnostic.  The core principle is to handle tasks that
may run for minutes, days, or years without losing state.

All you need is a Postgres database and the single
[`absurd.sql`](https://github.com/earendil-works/absurd/blob/main/sql/absurd.sql)
schema file.  No extra services, no message brokers, no coordination layer.
SDKs stay [simple too](https://github.com/earendil-works/absurd/blob/main/sdks/typescript/src/index.ts).

*… because it's absurd how much you can over-design such a simple thing.*

## How It Works

A **task** dispatches onto a **queue** from where a **worker** picks it up.
Tasks are subdivided into **steps** that act as checkpoints.  Once a step
completes successfully its return value is persisted and the step won't execute
again.  If a task fails, it retries from the last checkpoint.

Tasks can also **sleep** (suspend until a time) or **await events** (suspend
until a named event is emitted).  Events are cached — first emit wins — making
them race-free.

=== "TypeScript"

    ```typescript
    import { Absurd } from 'absurd-sdk';

    const app = new Absurd();

    app.registerTask({ name: 'order-fulfillment' }, async (params, ctx) => {
      const payment = await ctx.step('process-payment', async () => {
        return { paymentId: `pay-${params.orderId}`, amount: params.amount };
      });

      const shipment = await ctx.awaitEvent(
        `shipment.packed:${params.orderId}`,
      );

      await ctx.step('send-notification', async () => {
        return {
          sentTo: params.email,
          trackingNumber: shipment.trackingNumber,
        };
      });

      return {
        orderId: params.orderId,
        payment,
        trackingNumber: shipment.trackingNumber,
      };
    });

    await app.startWorker();
    ```

=== "Python"

    ```python
    from absurd_sdk import Absurd

    app = Absurd()


    @app.register_task(name="order-fulfillment")
    def process_order(params, ctx):
        def process_payment():
            return {
                "payment_id": f"pay-{params['order_id']}",
                "amount": params["amount"],
            }

        payment = ctx.step("process-payment", process_payment)

        shipment = ctx.await_event(f"shipment.packed:{params['order_id']}")

        def send_notification():
            return {
                "sent_to": params["email"],
                "tracking_number": shipment["tracking_number"],
            }

        ctx.step("send-notification", send_notification)

        return {
            "order_id": params["order_id"],
            "payment": payment,
            "tracking_number": shipment["tracking_number"],
        }


    app.start_worker()
    ```

=== "Go"

    ```go
    package main

    import (
        "context"
        "log"

        "github.com/earendil-works/absurd/sdks/go/absurd"
        _ "github.com/jackc/pgx/v5/stdlib"
    )

    type OrderFulfillmentParams struct {
        OrderID string `json:"order_id"`
        Amount  int    `json:"amount"`
        Email   string `json:"email"`
    }

    type ShipmentEvent struct {
        TrackingNumber string `json:"tracking_number"`
    }

    var orderFulfillmentTask = absurd.Task(
        "order-fulfillment",
        func(
            ctx context.Context,
            params OrderFulfillmentParams,
        ) (map[string]any, error) {
            payment, err := absurd.Step(
                ctx,
                "process-payment",
                func(ctx context.Context) (map[string]any, error) {
                    return map[string]any{
                        "payment_id": "pay-" + params.OrderID,
                        "amount":     params.Amount,
                    }, nil
                },
            )
            if err != nil {
                return nil, err
            }

            shipment, err := absurd.AwaitEvent[ShipmentEvent](
                ctx,
                "shipment.packed:"+params.OrderID,
            )
            if err != nil {
                return nil, err
            }

            if _, err := absurd.Step(
                ctx,
                "send-notification",
                func(ctx context.Context) (map[string]any, error) {
                    return map[string]any{
                        "sent_to":         params.Email,
                        "tracking_number": shipment.TrackingNumber,
                    }, nil
                },
            ); err != nil {
                return nil, err
            }

            return map[string]any{
                "order_id":        params.OrderID,
                "payment":         payment,
                "tracking_number": shipment.TrackingNumber,
            }, nil
        },
    )

    func main() {
        app, err := absurd.New(absurd.Options{
            QueueName:  "default",
            DriverName: "pgx",
        })
        if err != nil {
            log.Fatal(err)
        }
        defer app.Close()

        app.MustRegister(orderFulfillmentTask)
        if err := app.RunWorker(context.Background()); err != nil {
            log.Fatal(err)
        }
    }
    ```

## Quick Links

- **[Quickstart](./quickstart.md)** — install the schema, create a queue, run your first task
- **[Concepts](./concepts.md)** — what durable execution is, plus tasks, steps, runs, events, and retry semantics
- **[Storage](./storage.md)** — choose unpartitioned vs partitioned queues and automate partition lifecycle
- **[Cleanup and Retention](./cleanup.md)** — set retention policies and automate cleanup with SQL, `absurdctl`, or cron
- **[Comparison](./comparison.md)** — where Absurd fits relative to PGMQ, Cadence, Temporal, Inngest, and DBOS
- **[Patterns](./patterns/)** — practical recipes for common workflow and scheduling setups
- **[Deploying and Rolling Rollouts](./patterns/deployments.md)** — safely introduce new task names during rolling deploys
- **[SDKs](./sdks/)** — available language SDKs
- **[Tools Overview](./tools/)** — utility tools
