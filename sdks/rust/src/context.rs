use crate::error::{Error, Result};
use crate::types::{ClaimedTask, Json};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{LazyLock};
use uuid::Uuid;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Context provided to task handlers for checkpointing and event handling
pub struct TaskContext {
    /// Unique task identifier
    pub task_id: Uuid,

    /// Database client (single connection held for the lifetime of this task execution)
    client: deadpool_postgres::Object,

    /// Queue name
    queue_name: String,

    /// Current task details
    task: ClaimedTask,

    /// Checkpoint cache
    checkpoint_cache: HashMap<String, Json>,

    /// Step name counter for automatic deduplication
    step_counter: HashMap<String, usize>,

    /// Claim timeout in seconds
    claim_timeout: i32,
}

impl TaskContext {
    /// Create a new task context
    pub(crate) async fn new(
        pool: Pool,
        queue_name: String,
        task: ClaimedTask,
        claim_timeout: i32,
    ) -> Result<Self> {
        let task_id = task.task_id;

        // Deref to the underlying tokio_postgres::Client and wrap in Arc
        let client = pool.get().await?;

        // Load existing checkpoints
        let rows = client
            .query(
                "SELECT checkpoint_name, state
                 FROM absurd.get_task_checkpoint_states($1, $2, $3)",
                &[&queue_name, &task.task_id, &task.run_id],
            )
            .await?;

        let checkpoint_cache: HashMap<String, Json> = rows
            .iter()
            .map(|row| {
                let name: String = row.get(0);
                let state: Json = row.get(1);
                (name, state)
            })
            .collect();

        Ok(Self {
            task_id,
            client,
            queue_name,
            task,
            checkpoint_cache,
            step_counter: HashMap::new(),
            claim_timeout,
        })
    }

    /// Get headers attached to this task
    pub fn headers(&self) -> &HashMap<String, Json> {
        self.task.headers.as_ref().unwrap_or(&DEFAULT_HEADERS)
    }

    /// Execute an idempotent step with checkpointing.
    ///
    /// If this step has been executed before, returns the cached result.
    /// Otherwise, executes the function and saves the result.
    pub async fn step<F, T>(&mut self, name: &str, f: F) -> Result<T>
    where
        F: FnOnce() -> BoxFuture<'static, Result<T>>,
        T: Serialize + for<'de> Deserialize<'de>,
    {
        // Peek without advancing so cache hits don't shift subsequent step names
        let checkpoint_name = self.peek_checkpoint_name(name);

        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name) {
            let cached_clone = cached.clone();
            // Advance counter to stay in sync even on a cache hit
            self.advance_checkpoint_name(name);
            return Ok(serde_json::from_value(cached_clone)?);
        }

        // Advance counter for the write path
        let checkpoint_name = self.advance_checkpoint_name(name);
        let result = f().await?;
        let value = serde_json::to_value(&result)?;
        self.persist_checkpoint(&checkpoint_name, &value).await?;

        Ok(result)
    }

    /// Sleep for a duration in seconds
    pub async fn sleep_for(&mut self, name: &str, duration_secs: u64) -> Result<()> {
        let wake_at = Utc::now() + chrono::Duration::seconds(duration_secs as i64);
        self.sleep_until(name, wake_at).await
    }

    /// Sleep until a specific time
    pub async fn sleep_until(&mut self, name: &str, wake_at: DateTime<Utc>) -> Result<()> {
        let checkpoint_name = self.peek_checkpoint_name(name);

        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name).cloned() {
            self.advance_checkpoint_name(name);
            let stored_time: DateTime<Utc> = serde_json::from_value(cached)?;
            if Utc::now() >= stored_time {
                return Ok(());
            }
        } else {
            let checkpoint_name = self.advance_checkpoint_name(name);
            let value = serde_json::to_value(wake_at)?;
            self.persist_checkpoint(&checkpoint_name, &value).await?;
        }

        if Utc::now() < wake_at {
            self.schedule_run(wake_at).await?;
            return Err(Error::Suspended);
        }

        Ok(())
    }

    /// Wait for an event to be emitted.
    ///
    /// Returns the event payload when received. If timeout is specified and
    /// exceeded, returns an error.
    pub async fn await_event(
        &mut self,
        event_name: &str,
        options: AwaitEventOptions,
    ) -> Result<Json> {
        let default_step_name = format!("$awaitEvent:{}", event_name);
        let step_name = options.step_name.as_deref().unwrap_or(&default_step_name);

        let checkpoint_name = self.peek_checkpoint_name(step_name);

        if let Some(cached) = self.checkpoint_cache.get(&checkpoint_name).cloned() {
            self.advance_checkpoint_name(step_name);
            return Ok(cached);
        }

        // Check if we resumed due to timeout
        if self.task.wake_event.as_deref() == Some(event_name)
            && self.task.event_payload.is_none()
        {
            return Err(Error::EventTimeout(
                event_name.to_string(),
                options.timeout_secs.unwrap_or(0),
            ));
        }

        let checkpoint_name = self.advance_checkpoint_name(step_name);
        let timeout_secs = options.timeout_secs.map(|t| t as i32);

        let rows = self
            .client
            .query(
                "SELECT should_suspend, payload
                 FROM absurd.await_event($1, $2, $3, $4, $5, $6)",
                &[
                    &self.queue_name,
                    &self.task.task_id,
                    &self.task.run_id,
                    &checkpoint_name,
                    &event_name,
                    &timeout_secs,
                ],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code().code() == "AB001" {
                        return Error::Cancelled;
                    }
                }
                Error::Database(e)
            })?;

        if rows.is_empty() {
            return Err(Error::Other("await_event returned no rows".to_string()));
        }

        let should_suspend: bool = rows[0].get(0);
        let payload: Option<Json> = rows[0].get(1);

        if !should_suspend {
            let payload = payload.unwrap_or(Json::Null);
            self.checkpoint_cache
                .insert(checkpoint_name, payload.clone());
            return Ok(payload);
        }

        Err(Error::Suspended)
    }

    /// Emit an event to the queue
    pub async fn emit_event(&self, event_name: &str, payload: Option<Json>) -> Result<()> {
        let payload = payload.unwrap_or(Json::Null);
        self.client
            .execute(
                "SELECT absurd.emit_event($1, $2, $3)",
                &[&self.queue_name, &event_name, &payload],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code().code() == "AB001" {
                        return Error::Cancelled;
                    }
                }
                Error::Database(e)
            })?;
        Ok(())
    }

    /// Extend the claim timeout to prevent task from being reclaimed
    pub async fn heartbeat(&self, extend_by_secs: Option<i32>) -> Result<()> {
        let extend_by = extend_by_secs.unwrap_or(self.claim_timeout);

        self.client
            .execute(
                "SELECT absurd.extend_claim($1, $2, $3)",
                &[&self.queue_name, &self.task.run_id, &extend_by],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code().code() == "AB001" {
                        return Error::Cancelled;
                    }
                }
                Error::Database(e)
            })?;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /// Returns the name that *would* be used next for `name` without advancing
    /// the counter. Use this for cache lookups.
    fn peek_checkpoint_name(&self, name: &str) -> String {
        let count = self.step_counter.get(name).copied().unwrap_or(0);
        if count == 0 {
            name.to_string()
        } else {
            format!("{}#{}", name, count + 1)
        }
    }

    /// Advances the counter for `name` and returns the checkpoint name to use
    /// for the current invocation. Use this when you are about to write.
    fn advance_checkpoint_name(&mut self, name: &str) -> String {
        let counter = self.step_counter.entry(name.to_string()).or_insert(0);
        *counter += 1;
        if *counter == 1 {
            name.to_string()
        } else {
            format!("{}#{}", name, counter)
        }
    }

    async fn persist_checkpoint(&mut self, name: &str, value: &Json) -> Result<()> {
        self.client
            .execute(
                "SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)",
                &[
                    &self.queue_name,
                    &self.task.task_id,
                    &name,
                    &value,
                    &self.task.run_id,
                    &self.claim_timeout,
                ],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code().code() == "AB001" {
                        return Error::Cancelled;
                    }
                }
                Error::Database(e)
            })?;

        self.checkpoint_cache.insert(name.to_string(), value.clone());
        Ok(())
    }

    async fn schedule_run(&self, wake_at: DateTime<Utc>) -> Result<()> {
        self.client
            .execute(
                "SELECT absurd.schedule_run($1, $2, $3)",
                &[&self.queue_name, &self.task.run_id, &wake_at],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code().code() == "AB001" {
                        return Error::Cancelled;
                    }
                }
                Error::Database(e)
            })?;
        Ok(())
    }
}

/// Options for awaiting an event
#[derive(Debug, Clone, Default)]
pub struct AwaitEventOptions {
    pub step_name: Option<String>,
    pub timeout_secs: Option<u64>,
}

impl AwaitEventOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_step_name(mut self, name: impl Into<String>) -> Self {
        self.step_name = Some(name.into());
        self
    }

    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = Some(timeout_secs);
        self
    }
}

static DEFAULT_HEADERS: LazyLock<HashMap<String, Json>> = LazyLock::new(HashMap::new);