//! # Absurd - Rust SDK for Durable Task Execution
//!
//! Absurd is a PostgreSQL-based durable task execution system. This crate provides
//! a Rust SDK for spawning, executing, and managing long-running, fault-tolerant workflows.
//!
//! ## Features
//!
//! - **Durable Execution**: Tasks survive crashes, restarts, and network failures
//! - **Checkpointing**: Automatic step-level checkpointing prevents duplicate work
//! - **Event-Driven**: Tasks can wait for events with race-free semantics
//! - **Retry Strategies**: Configurable exponential backoff and retry limits
//! - **Exactly-Once Semantics**: Idempotency keys prevent duplicate execution
//! - **PostgreSQL Native**: No additional infrastructure required
//!
//! ## Quick Start
//!
//! ```no_run
//! use absurd::{Absurd, TaskOptions, WorkerOptions};
//! use serde_json::json;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to database
//!     let absurd = Absurd::new("postgresql://localhost/absurd").await?;
//!     
//!     // Create queue
//!     absurd.create_queue(None).await?;
//!     
//!     // Register a task
//!     absurd.register_task(
//!         TaskOptions::new("hello-world"),
//!         |params, mut ctx| Box::pin(async move {
//!             println!("Hello from task!");
//!             
//!             // Checkpointed step
//!             let result = ctx.step("process", || Box::pin(async {
//!                 Ok(42)
//!             })).await?;
//!             
//!             println!("Got result: {}", result);
//!             Ok(json!({ "status": "complete" }))
//!         })
//!     );
//!     
//!     // Start worker
//!     let worker = absurd.start_worker(WorkerOptions::new()).await?;
//!     
//!     // Spawn a task
//!     let spawn_result = absurd.spawn(
//!         "hello-world",
//!         json!({ "user_id": 123 }),
//!         Default::default()
//!     ).await?;
//!     
//!     println!("Task spawned: {}", spawn_result.task_id);
//!     
//!     // Let it run for a bit
//!     tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
//!     
//!     // Clean shutdown
//!     worker.close().await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Core Concepts
//!
//! ### Tasks
//!
//! A task is a unit of work that can be executed durably. Tasks are registered
//! with handlers and can be spawned multiple times with different parameters.
//!
//! ### Steps
//!
//! Steps are checkpointed operations within a task. Once a step completes, its
//! result is cached and won't be re-executed on retry.
//!
//! ```no_run
//! # use absurd::Absurd;
//! # use serde_json::json;
//! # async fn example(mut ctx: absurd::TaskContext) -> Result<(), Box<dyn std::error::Error>> {
//! let result = ctx.step("my-step", || Box::pin(async {
//!     // This code runs only once
//!     Ok(42)
//! })).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Events
//!
//! Tasks can wait for events and emit events. Event handling is race-free due
//! to checkpointing.
//!
//! ```no_run
//! # use absurd::{Absurd, AwaitEventOptions};
//! # use serde_json::json;
//! # async fn example(mut ctx: absurd::TaskContext) -> Result<(), Box<dyn std::error::Error>> {
//! // Wait for an event (suspends task until event arrives)
//! let payload = ctx.await_event(
//!     "payment.completed",
//!     AwaitEventOptions::new().with_timeout(3600)
//! ).await?;
//! 
//! // Emit an event
//! ctx.emit_event("order.processed", Some(json!({ "order_id": 123 }))).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Workers
//!
//! Workers poll the database for tasks and execute them. Multiple workers can
//! run concurrently with automatic task distribution.

pub mod client;
pub mod context;
pub mod error;
pub mod types;
pub mod worker;

// Re-export main types
pub use client::Absurd;
pub use context::{AwaitEventOptions, TaskContext};
pub use error::{Error, Result};
pub use types::{
    CancellationPolicy, ClaimedTask, RetryStrategy, SpawnOptions, SpawnResult,
    TaskOptions, WorkerOptions, Json,
};
pub use worker::Worker;

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::client::Absurd;
    pub use crate::context::{AwaitEventOptions, TaskContext};
    pub use crate::error::{Error, Result};
    pub use crate::types::{
        CancellationPolicy, RetryStrategy, SpawnOptions, TaskOptions, WorkerOptions,
    };
}
