use crate::context::TaskContext;
use crate::error::{Error, Result};
use crate::types::{ClaimedTask, WorkerOptions};
use parking_lot::RwLock;
use std::collections::HashMap;
// use std::future::Future;
// use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::Client;
use uuid::Uuid;

// type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
// type TaskHandler = Arc<dyn Fn(serde_json::Value, TaskContext) -> BoxFuture<'static, Result<serde_json::Value>> + Send + Sync>;

// pub(crate) struct RegisteredTask {
//     pub name: String,
//     pub queue: String,
//     pub handler: TaskHandler,
// }

/// Background worker that polls for and executes tasks
pub struct Worker {
    shutdown_tx: mpsc::Sender<()>,
    worker_handle: JoinHandle<()>,
}

impl Worker {
    pub(crate) async fn new(
        client: Arc<Client>,
        queue_name: String,
        registry: Arc<RwLock<HashMap<String, crate::client::RegisteredTask>>>,
        options: WorkerOptions,
    ) -> Result<Self> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        
        let worker_handle = tokio::spawn(async move {
            let mut executing = tokio::task::JoinSet::new();
            let (notify_tx, mut notify_rx) = mpsc::channel::<()>(100);
            
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Worker shutting down");
                        break;
                    }
                    
                    _ = notify_rx.recv() => {
                        // Notified of availability, continue to claim tasks
                    }
                    
                    _ = tokio::time::sleep(Duration::from_secs_f64(options.poll_interval_secs)) => {
                        // Poll interval elapsed
                    }
                }
                
                // Check if we have capacity
                if executing.len() >= options.concurrency {
                    if let Some(result) = executing.join_next().await {
                        if let Err(e) = result {
                            tracing::error!("Task execution panicked: {:?}", e);
                        }
                    }
                    continue;
                }
                
                let available_capacity = options.concurrency - executing.len();
                let to_claim = available_capacity.min(options.batch_size);
                
                if to_claim == 0 {
                    continue;
                }
                
                // Claim tasks
                let tasks = match claim_tasks(
                    &client,
                    &queue_name,
                    &options.worker_id,
                    options.claim_timeout,
                    to_claim as i32,
                ).await {
                    Ok(tasks) => tasks,
                    Err(e) => {
                        tracing::error!("Failed to claim tasks: {:?}", e);
                        continue;
                    }
                };
                
                if tasks.is_empty() {
                    continue;
                }
                
                // Spawn task execution
                for task in tasks {
                    let client_clone = Arc::clone(&client);
                    let queue_clone = queue_name.clone();
                    let registry_clone = registry.clone();
                    let claim_timeout = options.claim_timeout;
                    let fatal_on_timeout = options.fatal_on_lease_timeout;
                    let notify_tx_clone = notify_tx.clone();
                    
                    executing.spawn(async move {
                        if let Err(e) = execute_task_with_timeout(
                            client_clone,
                            queue_clone,
                            registry_clone,
                            task,
                            claim_timeout,
                            fatal_on_timeout,
                        ).await {
                            if !e.is_suspended() && !e.is_cancelled() {
                                tracing::error!("Task execution failed: {:?}", e);
                            }
                        }
                        
                        let _ = notify_tx_clone.send(()).await;
                    });
                }
            }
            
            // Wait for all tasks to complete
            while let Some(result) = executing.join_next().await {
                if let Err(e) = result {
                    tracing::error!("Task execution panicked during shutdown: {:?}", e);
                }
            }
        });
        
        Ok(Self {
            shutdown_tx,
            worker_handle,
        })
    }
    
    /// Stop the worker and wait for all tasks to complete
    pub async fn close(self) -> Result<()> {
        let _ = self.shutdown_tx.send(()).await;
        self.worker_handle.await
            .map_err(|e| Error::Other(format!("Worker task panicked: {}", e)))?;
        Ok(())
    }
}

async fn claim_tasks(
    client: &Client,
    queue_name: &str,
    worker_id: &str,
    claim_timeout: i32,
    batch_size: i32,
) -> Result<Vec<ClaimedTask>> {
    let rows = client
        .query(
            "SELECT run_id, task_id, attempt, task_name, params, retry_strategy, 
                    max_attempts, headers, wake_event, event_payload
             FROM absurd.claim_task($1, $2, $3, $4)",
            &[&queue_name, &worker_id, &claim_timeout, &batch_size],
        )
        .await?;
    
    let tasks = rows
        .iter()
        .map(|row| {
            let headers_json: Option<serde_json::Value> = row.get(7);
            let headers: Option<HashMap<String, serde_json::Value>> = headers_json
                .and_then(|v| serde_json::from_value(v).ok());

            ClaimedTask {
                run_id: row.get(0),
                task_id: row.get(1),
                attempt: row.get(2),
                task_name: row.get(3),
                params: row.get(4),
                retry_strategy: row.get(5),
                max_attempts: row.get(6),
                headers,
                wake_event: row.get(8),
                event_payload: row.get(9),
            }
        })
        .collect();
    
    Ok(tasks)
}

async fn execute_task_with_timeout(
    client: Arc<Client>,
    queue_name: String,
    registry: Arc<RwLock<HashMap<String, crate::client::RegisteredTask>>>,
    task: ClaimedTask,
    claim_timeout: i32,
    fatal_on_timeout: bool,
) -> Result<()> {
    let timeout_duration = Duration::from_secs((claim_timeout * 2) as u64);
    
    let result = if fatal_on_timeout {
        tokio::time::timeout(
            timeout_duration,
            execute_task(client, queue_name, registry, task, claim_timeout),
        )
        .await
        .unwrap_or_else(|_| {
            tracing::error!(
                "Task exceeded claim timeout by 2x ({}s), terminating process",
                claim_timeout
            );
            std::process::exit(1);
        })
    } else {
        tokio::time::timeout(
            timeout_duration,
            execute_task(client, queue_name, registry, task, claim_timeout),
        )
        .await
        .unwrap_or_else(|_| {
            tracing::warn!("Task exceeded claim timeout by 2x ({}s)", claim_timeout);
            Err(Error::Other("Task timeout".to_string()))
        })
    };
    
    result
}

async fn execute_task(
    client: Arc<Client>,
    queue_name: String,
    registry: Arc<RwLock<HashMap<String, crate::client::RegisteredTask>>>,
    task: ClaimedTask,
    claim_timeout: i32,
) -> Result<()> {
    let (handler, task_queue) = {
        let registry_lock = registry.read();
        let registered = registry_lock
            .get(&task.task_name)
            .ok_or_else(|| Error::TaskNotRegistered(task.task_name.clone()))?;
        
        (registered.handler.clone(), registered.queue.clone())
    };
    
    // Create context
    let ctx = TaskContext::new(
        client.clone(),
        task_queue,
        task.clone(),
        claim_timeout,
    ).await?;
    
    // Execute handler
    match handler(task.params.clone(), ctx).await {
        Ok(result) => {
            complete_run(&client, &queue_name, &task.run_id, result).await?;
        }
        Err(Error::Suspended) | Err(Error::Cancelled) => {}
        Err(e) => {
            fail_run(&client, &queue_name, &task.run_id, &e).await?;
            return Err(e);
        }
    }
    
    Ok(())
}

async fn complete_run(
    client: &Client,
    queue_name: &str,
    run_id: &Uuid,
    result: serde_json::Value,
) -> Result<()> {
    client
        .execute(
            "SELECT absurd.complete_run($1, $2, $3)",
            &[&queue_name, run_id, &result],
        )
        .await?;
    Ok(())
}

async fn fail_run(
    client: &Client,
    queue_name: &str,
    run_id: &Uuid,
    error: &Error,
) -> Result<()> {
    let error_json = serde_json::json!({
        "name": error.to_string(),
        "message": error.to_string(),
    });
    
    client
        .execute(
            "SELECT absurd.fail_run($1, $2, $3, $4)",
            &[&queue_name, run_id, &error_json, &None::<String>],
        )
        .await?;
    Ok(())
}
