# Absurd SDK for Go

Go SDK for [Absurd](https://github.com/earendil-works/absurd): a PostgreSQL-
based durable task execution system.

Absurd is the simplest durable execution workflow system you can think of.
It's entirely based on Postgres and nothing else. It's almost as easy to use
as a queue, but it handles scheduling and retries, and it does all of that
without needing any other services besides Postgres.

**Disclaimer:** _this SDK is new and experimental_

## What is Durable Execution?

Durable execution (or durable workflows) is a way to run long-lived, reliable
functions that can survive crashes, restarts, and network failures without
losing state or duplicating work. Instead of running your logic in memory, a
durable execution system decomposes a task into smaller pieces (steps) and
records every step and decision.

## Installation

```bash
go get github.com/earendil-works/absurd/sdks/go/absurd@latest
```

## Prerequisites

Before using the SDK, initialize Absurd in your PostgreSQL database:

```bash
# One-off usage
uvx absurdctl init -d your-database-name
uvx absurdctl create-queue -d your-database-name default

# Or install it once
uv tool install absurdctl
absurdctl init -d your-database-name
absurdctl create-queue -d your-database-name default
```

See the [absurdctl docs](https://earendil-works.github.io/absurd/tools/absurdctl/) for installation
details and the full CLI reference, including
[`uvx`](https://docs.astral.sh/uv/guides/tools/) usage.

## Quick Start

If you omit `DatabaseURL`, the client uses `ABSURD_DATABASE_URL`, then
`PGDATABASE`, then `postgresql://localhost/absurd`.

```go
package main

import (
	"context"
	"log"

	"github.com/earendil-works/absurd/sdks/go/absurd"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type OrderParams struct {
	OrderID string   `json:"order_id"`
	Amount  int      `json:"amount"`
	Items   []string `json:"items"`
	Email   string   `json:"email"`
}

type ShipmentEvent struct {
	TrackingNumber string `json:"tracking_number"`
}

func main() {
	ctx := context.Background()

	app, err := absurd.New(absurd.Options{
		QueueName:  "default",
		DriverName: "pgx",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	app.MustRegister(absurd.Task(
		"order-fulfillment",
		func(
			ctx context.Context,
			params OrderParams,
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

			inventory, err := absurd.Step(
				ctx,
				"reserve-inventory",
				func(ctx context.Context) (map[string]any, error) {
					return map[string]any{
						"reserved_items": params.Items,
					}, nil
				},
			)
			if err != nil {
				return nil, err
			}

			eventName := "shipment.packed:" + params.OrderID
			shipment, err := absurd.AwaitEvent[ShipmentEvent](ctx, eventName)
			if err != nil {
				return nil, err
			}

			_, err = absurd.Step(
				ctx,
				"send-notification",
				func(ctx context.Context) (map[string]any, error) {
					return map[string]any{
						"sent_to":         params.Email,
						"tracking_number": shipment.TrackingNumber,
					}, nil
				},
			)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				"order_id":        params.OrderID,
				"payment":         payment,
				"inventory":       inventory,
				"tracking_number": shipment.TrackingNumber,
			}, nil
		},
	))

	if err := app.RunWorker(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## Spawning Tasks

```go
result, err := app.Spawn(
	ctx,
	"order-fulfillment",
	OrderParams{
		OrderID: "42",
		Amount:  9999,
		Items:   []string{"widget-1", "gadget-2"},
		Email:   "customer@example.com",
	},
)
if err != nil {
	log.Fatal(err)
}

log.Printf("task=%s run=%s", result.TaskID, result.RunID)
```

If `order-fulfillment` is not registered in this process, pass
`absurd.SpawnOptions{QueueName: "..."}` explicitly. For unregistered tasks,
per-task defaults from registration are not available; spawn options (or client
defaults) are used instead.

## Task Result Snapshots

You can inspect or wait for a task's terminal result:

```go
snapshot, err := app.FetchTaskResult(ctx, app.QueueName(), taskID)
if err != nil {
	log.Fatal(err)
}

final, err := app.AwaitTaskResult(
	ctx,
	app.QueueName(),
	taskID,
	absurd.AwaitTaskResultOptions{Timeout: 30 * time.Second},
)
if err != nil {
	log.Fatal(err)
}
```

Inside a task, you can also wait for child tasks durably:

```go
child, err := app.Spawn(
	ctx,
	"child-task",
	map[string]any{},
	absurd.SpawnOptions{QueueName: "child-workers"},
)
if err != nil {
	return err
}

task := absurd.MustTaskContext(ctx)
childResult, err := task.AwaitTaskResult(
	ctx,
	"child-workers",
	child.TaskID,
	absurd.AwaitTaskResultOptions{Timeout: 30 * time.Second},
)
if err != nil {
	return err
}

_ = childResult
```

## Emitting Events

```go
err := app.EmitEvent(
	ctx,
	app.QueueName(),
	"shipment.packed:42",
	map[string]any{"tracking_number": "TRACK123"},
)
if err != nil {
	log.Fatal(err)
}
```

## Idempotency Keys

Use the task ID to derive idempotency keys for external APIs:

```go
task := absurd.MustTaskContext(ctx)
payment, err := absurd.Step(ctx, "process-payment", func(
	ctx context.Context,
) (map[string]any, error) {
	idempotencyKey := task.TaskID() + ":payment"
	return map[string]any{
		"idempotency_key": idempotencyKey,
		"amount":          params.Amount,
	}, nil
})
if err != nil {
	return nil, err
}

_ = payment
```

## Decomposed Steps

If you need explicit before/after control, split `Step()` into two calls:

```go
handle, err := absurd.BeginStep[map[string]any](ctx, "agent-turn")
if err != nil {
	return err
}

var messages []map[string]any
if handle.Done {
	messages, _ = handle.State["messages"].([]map[string]any)
} else {
	messages = []map[string]any{{
		"role":    "assistant",
		"content": "hello",
	}}
	_, err = handle.CompleteStep(ctx, map[string]any{"messages": messages})
	if err != nil {
		return err
	}
}
```

This is useful when integrating with event-driven loops (for example agent
runtimes) where the checkpoint boundary is not a single inline callback.

## License and Links

- [Examples](https://github.com/earendil-works/absurd/tree/main/sdks/go/absurd/examples)
- [Issue Tracker](https://github.com/earendil-works/absurd/issues)
- License: [Apache-2.0](https://github.com/earendil-works/absurd/blob/main/LICENSE)
