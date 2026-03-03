use crate::context::{TaskContext};
use crate::error::{Error, Result};
use crate::types::*;
use crate::worker::Worker;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type TaskHandler = Arc<dyn Fn(Json, TaskContext) -> BoxFuture<'static, Result<Json>> + Send + Sync>;

/// Main Absurd client for interacting with the durable execution system
#[derive(Clone)]
pub struct Absurd {
    client: Arc<Client>,
    queue_name: String,
    default_max_attempts: i32,
    registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
}

pub struct RegisteredTask {
    pub name: String,
    pub queue: String,
    pub default_max_attempts: Option<i32>,
    pub default_cancellation: Option<CancellationPolicy>,
    pub handler: TaskHandler,
}

impl Absurd {
    /// Create a new Absurd client from a database URL
    ///
    /// # Example
    /// ```no_run
    /// # use absurd::Absurd;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let absurd = Absurd::new("postgresql://localhost/absurd").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(database_url: &str) -> Result<Self> {
        Self::with_queue(database_url, "default").await
    }
    
    /// Create a new Absurd client with a specific queue
    pub async fn with_queue(database_url: &str, queue_name: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls)
            .await
            .map_err(|e| Error::Config(format!("Failed to connect to database: {}", e)))?;
        
        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Database connection error: {}", e);
            }
        });
        
        Ok(Self {
            client: Arc::new(client),
            queue_name: queue_name.to_string(),
            default_max_attempts: 5,
            registry: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    
    /// Register a task handler
    ///
    /// # Example
    /// ```no_run
    /// # use absurd::{Absurd, TaskOptions};
    /// # use serde_json::json;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let absurd = Absurd::new("postgresql://localhost/absurd").await?;
    /// absurd.register_task(
    ///     TaskOptions::new("my-task"),
    ///     |params, mut ctx| Box::pin(async move {
    ///         let result = ctx.step("process", || Box::pin(async {
    ///             // Do some work
    ///             Ok(42)
    ///         })).await?;
    ///         
    ///         Ok(json!({ "result": result }))
    ///     })
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn register_task<F>(&self, options: TaskOptions, handler: F)
    where
        F: Fn(Json, TaskContext) -> BoxFuture<'static, Result<Json>> + Send + Sync + 'static,
    {
        let queue = options.queue.clone().unwrap_or_else(|| self.queue_name.clone());
        
        let task = RegisteredTask {
            name: options.name.clone(),
            queue,
            default_max_attempts: options.default_max_attempts,
            default_cancellation: options.default_cancellation,
            handler: Arc::new(handler),
        };
        
        self.registry.write().insert(options.name, task);
    }
    
    /// Spawn a task for execution
    ///
    /// # Example
    /// ```no_run
    /// # use absurd::Absurd;
    /// # use serde_json::json;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let absurd = Absurd::new("postgresql://localhost/absurd").await?;
    /// let result = absurd.spawn(
    ///     "my-task",
    ///     json!({ "user_id": 123 }),
    ///     Default::default()
    /// ).await?;
    ///
    /// println!("Task spawned: {}", result.task_id);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn spawn(
        &self,
        task_name: &str,
        params: Json,
        options: SpawnOptions,
    ) -> Result<SpawnResult> {
        // Get registration info
        let registry = self.registry.read();
        let registration = registry.get(task_name);
        
        let queue = if let Some(reg) = registration {
            if let Some(ref opt_queue) = options.queue {
                if opt_queue != &reg.queue {
                    return Err(Error::Config(format!(
                        "Task '{}' is registered for queue '{}' but spawn requested queue '{}'",
                        task_name, reg.queue, opt_queue
                    )));
                }
            }
            reg.queue.clone()
        } else {
            options.queue.clone().ok_or_else(|| {
                Error::Config(format!(
                    "Task '{}' is not registered. Provide queue in options when spawning unregistered tasks",
                    task_name
                ))
            })?
        };
        
        // Build effective options
        let effective_max_attempts = options.max_attempts
            .or_else(|| registration.and_then(|r| r.default_max_attempts))
            .unwrap_or(self.default_max_attempts);
        
        let effective_cancellation = options.cancellation.clone()
            .or_else(|| registration.and_then(|r| r.default_cancellation.clone()));
        
        let mut spawn_opts = options.clone();
        spawn_opts.max_attempts = Some(effective_max_attempts);
        spawn_opts.cancellation = effective_cancellation;
        
        drop(registry);
        
        // Serialize options
        let options_json = serde_json::to_value(&spawn_opts)?;
        
        // Call spawn_task stored procedure
        let rows = self.client
            .query(
                "SELECT task_id, run_id, attempt, created
                 FROM absurd.spawn_task($1, $2, $3, $4)",
                &[&queue, &task_name, &params, &options_json],
            )
            .await?;
        
        if rows.is_empty() {
            return Err(Error::Other("spawn_task returned no rows".to_string()));
        }
        
        let row = &rows[0];
        Ok(SpawnResult {
            task_id: row.get(0),
            run_id: row.get(1),
            attempt: row.get(2),
            created: row.get(3),
        })
    }
    
    /// Emit an event to the queue
    pub async fn emit_event(
        &self,
        event_name: &str,
        payload: Option<Json>,
        queue_name: Option<&str>,
    ) -> Result<()> {
        let queue = queue_name.unwrap_or(&self.queue_name);
        let payload = payload.unwrap_or(Json::Null);
        
        self.client
            .execute(
                "SELECT absurd.emit_event($1, $2, $3)",
                &[&queue, &event_name, &payload],
            )
            .await?;
        
        Ok(())
    }
    
    /// Cancel a task by ID
    pub async fn cancel_task(&self, task_id: Uuid, queue_name: Option<&str>) -> Result<()> {
        let queue = queue_name.unwrap_or(&self.queue_name);
        
        self.client
            .execute(
                "SELECT absurd.cancel_task($1, $2)",
                &[&queue, &task_id],
            )
            .await?;
        
        Ok(())
    }
    
    /// Create a queue
    pub async fn create_queue(&self, queue_name: Option<&str>) -> Result<()> {
        let queue = queue_name.unwrap_or(&self.queue_name);
        
        self.client
            .execute("SELECT absurd.create_queue($1)", &[&queue])
            .await?;
        
        Ok(())
    }
    
    /// Drop a queue
    pub async fn drop_queue(&self, queue_name: Option<&str>) -> Result<()> {
        let queue = queue_name.unwrap_or(&self.queue_name);
        
        self.client
            .execute("SELECT absurd.drop_queue($1)", &[&queue])
            .await?;
        
        Ok(())
    }
    
    /// List all queues
    pub async fn list_queues(&self) -> Result<Vec<String>> {
        let rows = self.client
            .query("SELECT * FROM absurd.list_queues()", &[])
            .await?;
        
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }
    
    /// Start a worker that processes tasks
    ///
    /// # Example
    /// ```no_run
    /// # use absurd::{Absurd, WorkerOptions};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let absurd = Absurd::new("postgresql://localhost/absurd").await?;
    /// let worker = absurd.start_worker(
    ///     WorkerOptions::new()
    ///         .with_concurrency(5)
    ///         .with_claim_timeout(120)
    /// ).await?;
    ///
    /// // Worker runs until dropped or explicitly closed
    /// worker.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start_worker(&self, options: WorkerOptions) -> Result<Worker> {
        Worker::new(
            self.client.clone(),
            self.queue_name.clone(),
            self.registry.clone(),
            options,
        ).await
    }
    
    /// Claim and execute a single batch of tasks (useful for testing)
    pub async fn work_batch(
        &self,
        worker_id: &str,
        claim_timeout: i32,
        batch_size: i32,
    ) -> Result<usize> {
        let tasks = self.claim_tasks(worker_id, claim_timeout, batch_size).await?;
        let count = tasks.len();
        
        for task in tasks {
            if let Err(e) = self.execute_task(&task, claim_timeout).await {
                if !e.is_suspended() && !e.is_cancelled() {
                    tracing::error!("Task execution failed: {:?}", e);
                }
            }
        }
        
        Ok(count)
    }
    
    // Internal methods
    
    async fn claim_tasks(
        &self,
        worker_id: &str,
        claim_timeout: i32,
        batch_size: i32,
    ) -> Result<Vec<ClaimedTask>> {
        let rows = self.client
            .query(
                "SELECT run_id, task_id, attempt, task_name, params, retry_strategy, 
                        max_attempts, headers, wake_event, event_payload
                 FROM absurd.claim_task($1, $2, $3, $4)",
                &[&self.queue_name, &worker_id, &claim_timeout, &batch_size],
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
    
    async fn execute_task(&self, task: &ClaimedTask, claim_timeout: i32) -> Result<()> {
        // Get the handler
        let registry = self.registry.read();
        let registered = registry.get(&task.task_name)
            .ok_or_else(|| Error::TaskNotRegistered(task.task_name.clone()))?;
        
        let handler = registered.handler.clone();
        let queue_name = registered.queue.clone();
        drop(registry);
        
        // Create context
        let ctx = TaskContext::new(
            self.client.clone(),
            queue_name,
            task.clone(),
            claim_timeout,
        ).await?;
        
        // Execute handler
        match handler(task.params.clone(), ctx).await {
            Ok(result) => {
                self.complete_run(&task.run_id, result).await?;
            }
            Err(Error::Suspended) | Err(Error::Cancelled) => {
                // Normal suspension or cancellation, not an error
            }
            Err(e) => {
                self.fail_run(&task.run_id, &e).await?;
                return Err(e);
            }
        }
        
        Ok(())
    }
    
    async fn complete_run(&self, run_id: &Uuid, result: Json) -> Result<()> {
        self.client
            .execute(
                "SELECT absurd.complete_run($1, $2, $3)",
                &[&self.queue_name, run_id, &result],
            )
            .await?;
        Ok(())
    }
    
    async fn fail_run(&self, run_id: &Uuid, error: &Error) -> Result<()> {
        let error_json = serde_json::json!({
            "name": error.to_string(),
            "message": error.to_string(),
        });
        
        self.client
            .execute(
                "SELECT absurd.fail_run($1, $2, $3, $4)",
                &[&self.queue_name, run_id, &error_json, &None::<String>],
            )
            .await?;
        Ok(())
    }
}
