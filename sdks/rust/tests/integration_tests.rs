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

    let spawn_result = absurd.spawn(
        "test-task",
        json!({ "value": 21 }),
        Default::default()
    ).await?;

    assert!(spawn_result.created);

    let count = absurd.work_batch("test-worker", 30, 1).await?;
    assert_eq!(count, 1);

    // Cleanup after test — TTL 0 removes everything completed
    absurd.cleanup(0, None).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_idempotency() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test").await?;
    absurd.create_queue(None).await?;

    let idempotency_key = format!("unique-key-{}", uuid::Uuid::new_v4());

    let result1 = absurd.spawn(
        "test-task",
        json!({ "value": 42 }),
        SpawnOptions {
            idempotency_key: Some(idempotency_key.clone()),
            queue: Some("test".to_string()),
            ..Default::default()
        }
    ).await?;

    let result2 = absurd.spawn(
        "test-task",
        json!({ "value": 42 }),
        SpawnOptions {
            idempotency_key: Some(idempotency_key),
            queue: Some("test".to_string()),
            ..Default::default()
        }
    ).await?;

    assert_eq!(result1.task_id, result2.task_id);
    assert!(result1.created);
    assert!(!result2.created);

    absurd.cleanup(0, None).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_event_emission() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test").await?;
    absurd.create_queue(None).await?;

    absurd.emit_event(
        "test.event",
        Some(json!({ "data": "test" })),
        Some("test")
    ).await?;

    // Clean up emitted events
    absurd.cleanup(0, None).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_queue_operations() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test-ops").await?;

    absurd.create_queue(Some("test-ops")).await?;

    let queues = absurd.list_queues().await?;
    assert!(queues.contains(&"test-ops".to_string()));

    absurd.drop_queue(Some("test-ops")).await?;

    let queues = absurd.list_queues().await?;
    assert!(!queues.contains(&"test-ops".to_string()));

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let absurd = Absurd::with_queue(&get_test_db_url(), "test").await?;
    absurd.create_queue(None).await?;

    absurd.register_task(
        TaskOptions::new("cleanup-test-task").with_queue("test"),
        |_, _ctx| Box::pin(async move {
            Ok(json!({ "status": "done" }))
        })
    );

    // Spawn and execute a task so there is data to clean up
    absurd.spawn(
        "cleanup-test-task",
        json!({}),
        Default::default()
    ).await?;

    absurd.work_batch("test-worker", 30, 1).await?;

    // TTL 0 should remove all completed tasks and events immediately
    absurd.cleanup(0, None).await?;

    // TTL 30 is a no-op here since nothing is older than 30 days,
    // but it should not error
    absurd.cleanup(30, None).await?;

    Ok(())
}