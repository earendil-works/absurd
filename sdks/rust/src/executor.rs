use crate::client::RegisteredTask;
use crate::context::TaskContext;
use crate::error::{Error, Result};
use crate::types::{ClaimedTask, Json};
use deadpool_postgres::Pool;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Shared task execution logic used by both Worker and work_batch
pub async fn execute_task(
    pool: Pool,
    queue_name: String,
    registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
    task: ClaimedTask,
    claim_timeout: i32,
) -> Result<()> {
    let (handler, task_queue) = {
        let reg = registry.read();
        let registered = reg
            .get(&task.task_name)
            .ok_or_else(|| Error::TaskNotRegistered(task.task_name.clone()))?;
        (registered.handler.clone(), registered.queue.clone())
    };

    let ctx = TaskContext::new(pool.clone(), task_queue, task.clone(), claim_timeout).await?;

    match handler(task.params.clone(), ctx).await {
        Ok(result) => complete_run(&pool, &queue_name, &task.run_id, result).await?,
        Err(Error::Suspended) | Err(Error::Cancelled) => {}
        Err(e) => {
            fail_run(&pool, &queue_name, &task.run_id, &e).await?;
            return Err(e);
        }
    }

    Ok(())
}

/// Wraps execute_task with a hard timeout; optionally terminates the process on breach
pub async fn execute_task_with_timeout(
    pool: Pool,
    queue_name: String,
    registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
    task: ClaimedTask,
    claim_timeout: i32,
    fatal_on_timeout: bool,
) -> Result<()> {
    let timeout_duration = Duration::from_secs((claim_timeout * 2) as u64);
    let fut = execute_task(pool, queue_name, registry, task, claim_timeout);

    if fatal_on_timeout {
        tokio::time::timeout(timeout_duration, fut)
            .await
            .unwrap_or_else(|_| {
                tracing::error!(
                    "Task exceeded claim timeout by 2x ({}s), terminating process",
                    claim_timeout
                );
                std::process::exit(1);
            })
    } else {
        tokio::time::timeout(timeout_duration, fut)
            .await
            .unwrap_or_else(|_| {
                tracing::warn!("Task exceeded claim timeout by 2x ({}s)", claim_timeout);
                Err(Error::Other("Task timeout".to_string()))
            })
    }
}

async fn complete_run(pool: &Pool, queue_name: &str, run_id: &Uuid, result: Json) -> Result<()> {
    pool.get()
        .await?
        .execute(
            "SELECT absurd.complete_run($1, $2, $3)",
            &[&queue_name, run_id, &result],
        )
        .await?;
    Ok(())
}

async fn fail_run(pool: &Pool, queue_name: &str, run_id: &Uuid, error: &Error) -> Result<()> {
    let error_json = serde_json::json!({
        "name": error.to_string(),
        "message": error.to_string(),
    });
    pool.get()
        .await?
        .execute(
            "SELECT absurd.fail_run($1, $2, $3, $4)",
            &[&queue_name, run_id, &error_json, &None::<String>],
        )
        .await?;
    Ok(())
}