use crate::client::RegisteredTask;
use crate::error::{Error, Result};
use crate::executor::execute_task_with_timeout;
use crate::types::{ClaimedTask, WorkerOptions};
use deadpool_postgres::Pool;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Background worker that polls for and executes tasks
pub struct Worker {
    shutdown_tx: mpsc::Sender<()>,
    worker_handle: JoinHandle<()>,
}

impl Worker {
    pub(crate) async fn new(
        pool: Pool,
        queue_name: String,
        registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
        options: WorkerOptions,
    ) -> Result<Self> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        let worker_handle = tokio::spawn(async move {
            let mut executing = tokio::task::JoinSet::new();
            let (notify_tx, mut notify_rx) = mpsc::channel::<()>(100);

            loop {
                // Check shutdown first so a pending notify can't starve it
                if shutdown_rx.try_recv().is_ok() {
                    tracing::info!("Worker shutting down");
                    break;
                }

                // Drain any completed tasks before checking capacity
                while executing.len() >= options.concurrency {
                    if let Some(Err(e)) = executing.join_next().await {
                        tracing::error!("Task execution panicked: {:?}", e);
                    }
                }

                // Wait for a notify signal or the poll interval — whichever comes first
                tokio::select! {
                    _ = notify_rx.recv() => {}
                    _ = tokio::time::sleep(Duration::from_secs_f64(options.poll_interval_secs)) => {}
                }

                let available_capacity = options.concurrency - executing.len();
                let to_claim = available_capacity.min(options.batch_size);

                if to_claim == 0 {
                    continue;
                }

                let tasks = match claim_tasks(
                    &pool,
                    &queue_name,
                    &options.worker_id,
                    options.claim_timeout,
                    to_claim as i32,
                )
                .await
                {
                    Ok(tasks) => tasks,
                    Err(e) => {
                        tracing::error!("Failed to claim tasks: {:?}", e);
                        continue;
                    }
                };

                if tasks.is_empty() {
                    continue;
                }

                for task in tasks {
                    let pool_clone = pool.clone();
                    let queue_clone = queue_name.clone();
                    let registry_clone = registry.clone();
                    let claim_timeout = options.claim_timeout;
                    let fatal_on_timeout = options.fatal_on_lease_timeout;
                    let notify_tx_clone = notify_tx.clone();

                    executing.spawn(async move {
                        if let Err(e) = execute_task_with_timeout(
                            pool_clone,
                            queue_clone,
                            registry_clone,
                            task,
                            claim_timeout,
                            fatal_on_timeout,
                        )
                        .await
                        {
                            if !e.is_suspended() && !e.is_cancelled() {
                                tracing::error!("Task execution failed: {:?}", e);
                            }
                        }

                        let _ = notify_tx_clone.send(()).await;
                    });
                }
            }

            // Drain all in-flight tasks before returning
            while let Some(Err(e)) = executing.join_next().await {
                tracing::error!("Task execution panicked during shutdown: {:?}", e);
            }
        });

        Ok(Self {
            shutdown_tx,
            worker_handle,
        })
    }

    /// Stop the worker and wait for all in-flight tasks to complete
    pub async fn close(self) -> Result<()> {
        let _ = self.shutdown_tx.send(()).await;
        self.worker_handle
            .await
            .map_err(|e| Error::Other(format!("Worker task panicked: {}", e)))?;
        Ok(())
    }
}

async fn claim_tasks(
    pool: &Pool,
    queue_name: &str,
    worker_id: &str,
    claim_timeout: i32,
    batch_size: i32,
) -> Result<Vec<ClaimedTask>> {
    let rows = pool
        .get()
        .await?
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
            let headers: Option<HashMap<String, serde_json::Value>> =
                headers_json.and_then(|v| serde_json::from_value(v).ok());

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