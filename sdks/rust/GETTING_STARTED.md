# Getting Started with Absurd Rust SDK

This guide will help you get up and running with the Absurd Rust SDK.

## Quick Start

### 1. Create New Project

```bash
cargo new my-absurd-app
cd my-absurd-app
```

### 2. Add Dependencies

Edit `Cargo.toml`:

```toml
[dependencies]
absurd = "0.1"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

### 3. Write Your First Task

Edit `src/main.rs`:

```rust
use absurd::{Absurd, TaskOptions, WorkerOptions};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to database
    let absurd = Absurd::new("postgresql://localhost/absurd").await?;
    
    // Register a task
    absurd.register_task(
        TaskOptions::new("hello-world"),
        |params, mut ctx| Box::pin(async move {
            // This step is checkpointed
            let result = ctx.step("process", || Box::pin(async {
                println!("Processing: {:?}", params);
                Ok(42)
            })).await?;
            
            Ok(json!({ "result": result }))
        })
    );
    
    // Start worker
    let worker = absurd.start_worker(WorkerOptions::new()).await?;
    
    // Spawn task
    absurd.spawn(
        "hello-world",
        json!({ "message": "Hello!" }),
        Default::default()
    ).await?;
    
    // Wait for completion
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Shutdown
    worker.close().await?;
    
    Ok(())
}
```

### 4. Run It

```bash
cargo run
```

You should see:
```
Processing: Object {"message": String("Hello!")}
```

## Next Steps

### Try the Examples

```bash
export DATABASE_URL="postgresql://localhost/absurd"

# Simple example
cargo run --example hello_world

# Payment transfer
cargo run --example basic_transfer

# Cross-border with events
cargo run --example cross_border
```

### Learn Key Concepts

1. **Checkpointing**: Use `ctx.step()` to save progress
2. **Events**: Use `ctx.await_event()` and `absurd.emit_event()`
3. **Retries**: Configure with `SpawnOptions`
4. **Idempotency**: Use idempotency keys for exactly-once

## Common Issues

### Database Connection Failed

```
Error: Database error: connection failed
```

**Solution**: Check PostgreSQL is running and connection string is correct:
```bash
psql -d absurd -c "SELECT 1"  # Should succeed
```

### Schema Not Found

```
Error: relation "absurd.queues" does not exist
```

**Solution**: Install the Absurd schema:
```bash
absurdctl init -d absurd
absurdctl create-queue -d absurd default
```

### Task Not Registered

```
Error: Task 'my-task' not registered
```

**Solution**: Ensure you called `register_task()` before `spawn()`.

## Development Tips

### Enable Logging

```rust
// Add to Cargo.toml
[dependencies]
tracing-subscriber = "0.3"

// Add to main()
tracing_subscriber::fmt::init();
```

### Database Migrations

If the Absurd SQL schema changes, you'll need to apply migrations:

```bash
absurdctl migrate -d absurd
```

### Testing

Run tests with database:
```bash
export DATABASE_URL="postgresql://localhost/absurd_test"
cargo test -- --ignored
```

## Architecture Overview

```
Your App
    ↓
Absurd SDK (Rust)
    ↓
PostgreSQL + Absurd Schema
    ├─ Tasks
    ├─ Runs
    ├─ Checkpoints
    ├─ Events
    └─ Workers
```
