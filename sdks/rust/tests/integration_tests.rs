// Integration tests for Absurd SDK
// These tests require a running PostgreSQL database with Absurd schema

use absurd::{Absurd, SpawnOptions, TaskOptions};
use serde_json::json;
use std::env;

fn get_test_db_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://localhost/absurd_test".to_string())
}

#[tokio::test]
#[ignore] // Requires database setup
async fn test_basic_task_execution() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test").await?;
    absurd.create_queue(None).await?;

    // Register simple task
    absurd.register_task(
        TaskOptions::new("test-task").with_queue("test"),
        |params, mut ctx| Box::pin(async move {
            let value = params["value"].as_i64().unwrap();
            
            let doubled = ctx.step("double", || {
                let v = value;
                Box::pin(async move { Ok(v * 2) })
            }).await?;
            
            Ok(json!({ "result": doubled }))
        })
    );

    // Spawn task
    let spawn_result = absurd.spawn(
        "test-task",
        json!({ "value": 21 }),
        Default::default()
    ).await?;

    assert!(spawn_result.created);

    // Execute one batch
    let count = absurd.work_batch("test-worker", 30, 1).await?;
    assert_eq!(count, 1);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_idempotency() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test").await?;

    // Spawn with idempotency key
    let result1 = absurd.spawn(
        "test-task",
        json!({ "value": 42 }),
        SpawnOptions {
            idempotency_key: Some("unique-key-123".to_string()),
            ..Default::default()
        }
    ).await?;

    // Spawn again with same key
    let result2 = absurd.spawn(
        "test-task",
        json!({ "value": 42 }),
        SpawnOptions {
            idempotency_key: Some("unique-key-123".to_string()),
            ..Default::default()
        }
    ).await?;

    assert_eq!(result1.task_id, result2.task_id);
    assert!(result1.created);
    assert!(!result2.created); // Second spawn was deduplicated

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_event_emission() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test").await?;

    // Emit event
    absurd.emit_event(
        "test.event",
        Some(json!({ "data": "test" })),
        Some("test")
    ).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_queue_operations() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test-ops").await?;

    // Create queue
    absurd.create_queue(Some("test-ops")).await?;

    // List queues
    let queues = absurd.list_queues().await?;
    assert!(queues.contains(&"test-ops".to_string()));

    // Drop queue
    absurd.drop_queue(Some("test-ops")).await?;

    Ok(())
}
