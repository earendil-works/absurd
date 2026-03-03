use absurd::{Absurd, TaskOptions, WorkerOptions};
use serde_json::json;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup logging
    tracing_subscriber::fmt::init();

    // Get database URL from environment
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://localhost/absurd".to_string());

    println!("Connecting to: {}", database_url);

    // Create Absurd client
    let absurd = Absurd::with_queue(&database_url, "default").await?;

    // Create queue if it doesn't exist
    absurd.create_queue(None).await?;
    println!("Queue 'default' ready");

    // Register the hello-world task
    absurd.register_task(
        TaskOptions::new("hello-world"),
        |params, mut ctx| Box::pin(async move {
            println!("\n=== Task Started ===");
            println!("Task ID: {}", ctx.task_id);
            println!("Params: {}", params);

            // Step 1: Simple checkpoint
            let result = ctx.step("step-1", || Box::pin(async {
                println!("Executing step 1...");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                println!("Step 1 complete!");
                Ok("Hello from step 1".to_string())
            })).await?;

            println!("Step 1 result: {}", result);

            // Step 2: Another checkpoint
            let result2 = ctx.step("step-2", || Box::pin(async {
                println!("Executing step 2...");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                println!("Step 2 complete!");
                Ok("World from step 2".to_string())
            })).await?;

            println!("Step 2 result: {}", result2);

            println!("=== Task Complete ===\n");

            Ok(json!({
                "status": "success",
                "message": format!("{} {}", result, result2)
            }))
        })
    );

    println!("Task 'hello-world' registered");

    // Start worker in background
    println!("Starting worker...");
    let worker = absurd.start_worker(
        WorkerOptions::new()
            .with_concurrency(1)
            .with_claim_timeout(30)
    ).await?;

    println!("Worker started");

    // Spawn the task
    println!("\nSpawning task...");
    let spawn_result = absurd.spawn(
        "hello-world",
        json!({
            "name": "Alice",
            "greeting": "Hello World!"
        }),
        Default::default()
    ).await?;

    println!("Task spawned successfully!");
    println!("  Task ID: {}", spawn_result.task_id);
    println!("  Run ID: {}", spawn_result.run_id);
    println!("  Attempt: {}", spawn_result.attempt);

    // Wait for task to complete
    println!("\nWaiting for task to complete...");
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Graceful shutdown
    println!("Shutting down worker...");
    worker.close().await?;

    println!("Done!");

    Ok(())
}
