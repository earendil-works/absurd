# Absurd - Rust SDK for Durable Task Execution

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Rust SDK for [Absurd](https://github.com/earendil-works/absurd): a PostgreSQL-based durable task execution system.

Absurd is the simplest durable execution workflow system you can think of. It's entirely based on PostgreSQL and nothing else. It handles scheduling, retries, and state management without needing any other services.

**⚠️ Warning:** This is an early experiment and should not be used in production.

## Features

- 🔄 **Durable Execution**: Tasks survive crashes, restarts, and network failures
- ✅ **Step Checkpointing**: Automatic checkpointing prevents duplicate work
- ⏱️ **Event-Driven**: Tasks can wait for events with race-free semantics
- 🔁 **Retry Strategies**: Configurable exponential backoff and retry limits
- 🎯 **Exactly-Once**: Idempotency keys prevent duplicate execution
- 🐘 **PostgreSQL Native**: No additional infrastructure required
- 🚀 **Async/Await**: Built on Tokio for high performance

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
absurd = "0.1"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

## Prerequisites

You need PostgreSQL with the Absurd schema installed:

```bash
# Install absurdctl from https://github.com/earendil-works/absurd/releases
absurdctl init -d your-database-name
absurdctl create-queue -d your-database-name default
```

## Quick Start

```rust
use absurd::{Absurd, TaskOptions, WorkerOptions};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to database
    let absurd = Absurd::new("postgresql://localhost/absurd").await?;
    
    // Register a task
    absurd.register_task(
        TaskOptions::new("order-fulfillment"),
        |params, mut ctx| Box::pin(async move {
            // Each step is checkpointed
            let payment = ctx.step("process-payment", || Box::pin(async {
                // If process crashes here, this won't run again on retry
                Ok(json!({ "charge_id": "ch_123" }))
            })).await?;
            
            let inventory = ctx.step("reserve-inventory", || Box::pin(async {
                Ok(json!({ "reserved": true }))
            })).await?;
            
            // Wait for an event (task suspends until event arrives)
            let shipment = ctx.await_event(
                "shipment.packed:order-123",
                Default::default()
            ).await?;
            
            // Send notification
            ctx.step("notify", || Box::pin(async {
                Ok(json!({ "sent": true }))
            })).await?;
            
            Ok(json!({
                "status": "completed",
                "payment": payment,
                "shipment": shipment
            }))
        })
    );
    
    // Start worker
    let worker = absurd.start_worker(WorkerOptions::new()).await?;
    
    // Spawn task
    absurd.spawn(
        "order-fulfillment",
        json!({ "order_id": "order-123", "amount": 99.99 }),
        Default::default()
    ).await?;
    
    // Emit event (from another part of your system)
    absurd.emit_event(
        "shipment.packed:order-123",
        Some(json!({ "tracking": "1Z999AA10123456784" })),
        None
    ).await?;
    
    // Graceful shutdown
    worker.close().await?;
    
    Ok(())
}
```

## Core Concepts

### Tasks

A task is a unit of work that can survive failures. Tasks are registered with handlers:

```rust
absurd.register_task(
    TaskOptions::new("my-task"),
    |params, mut ctx| Box::pin(async move {
        // Your task logic here
        Ok(json!({ "result": "success" }))
    })
);
```

### Steps (Checkpointing)

Steps are the secret sauce. Once a step completes, its result is cached:

```rust
let result = ctx.step("expensive-operation", || Box::pin(async {
    // This only runs once, even if the task is retried
    call_external_api().await
})).await?;
```

If your process crashes after this step, on restart the cached result is returned instantly.

### Events

Tasks can wait for events and emit events:

```rust
// Wait for event (suspends task)
let payload = ctx.await_event(
    "payment.confirmed",
    AwaitEventOptions::new().with_timeout(3600)
).await?;

// Emit event (from anywhere)
absurd.emit_event("payment.confirmed", Some(payload), None).await?;
```

Events are **race-free**: if an event is emitted before a task awaits it, the task receives it immediately.

### Sleeping

Tasks can sleep for durations or until specific times:

```rust
// Sleep for 5 minutes
ctx.sleep_for("wait-period", 300).await?;

// Sleep until specific time
ctx.sleep_until("scheduled", Utc::now() + Duration::hours(24)).await?;
```

### Idempotency

Use idempotency keys to prevent duplicate execution:

```rust
absurd.spawn(
    "process-payment",
    json!({ "amount": 100 }),
    SpawnOptions {
        idempotency_key: Some("payment-123".to_string()),
        ..Default::default()
    }
).await?;
```

Spawning with the same key returns the existing task without creating a duplicate.

### Retries

Configure retry behavior:

```rust
use absurd::{SpawnOptions, RetryStrategy};

absurd.spawn(
    "flaky-task",
    params,
    SpawnOptions {
        max_attempts: Some(10),
        retry_strategy: Some(RetryStrategy::Exponential {
            base_seconds: 30.0,
            factor: 2.0,
            max_seconds: Some(3600.0),
        }),
        ..Default::default()
    }
).await?;
```

## Examples

Run the examples (after setting up PostgreSQL):

```bash
export DATABASE_URL="postgresql://localhost/absurd"

# Hello World
cargo run --example hello_world

# Payment Transfer
cargo run --example basic_transfer

# Cross-Border Transfer with Events
cargo run --example cross_border
```

## Architecture

Absurd moves complexity into PostgreSQL stored procedures:

```
Your Rust App
    ↓
Absurd SDK (this crate)
    ↓
PostgreSQL with Absurd Schema
    ├─ Tasks (logical units)
    ├─ Runs (execution attempts)
    ├─ Checkpoints (saved state)
    ├─ Events (emitted signals)
    └─ Workers (claim & execute)
```

Each queue has its own set of tables:
- `t_{queue}` - Tasks
- `r_{queue}` - Runs
- `c_{queue}` - Checkpoints
- `e_{queue}` - Events
- `w_{queue}` - Wait registrations

## Performance

The SDK is built on `tokio` for async I/O and uses connection pooling for database access. Performance is primarily bound by PostgreSQL query latency.
