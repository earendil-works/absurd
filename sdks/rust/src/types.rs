use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use uuid::Uuid;

/// JSON value type for task parameters and results
pub type Json = JsonValue;

/// Retry strategy for failed tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum RetryStrategy {
    /// No retries
    None,
    /// Fixed delay between retries
    Fixed {
        #[serde(rename = "base_seconds")]
        base_seconds: f64,
    },
    /// Exponential backoff
    Exponential {
        #[serde(rename = "base_seconds")]
        base_seconds: f64,
        factor: f64,
        #[serde(rename = "max_seconds")]
        max_seconds: Option<f64>,
    },
}

impl Default for RetryStrategy {
    fn default() -> Self {
        Self::Exponential {
            base_seconds: 30.0,
            factor: 2.0,
            max_seconds: Some(3600.0),
        }
    }
}

/// Cancellation policy for tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancellationPolicy {
    /// Maximum duration in seconds from first start
    #[serde(rename = "max_duration", skip_serializing_if = "Option::is_none")]
    pub max_duration: Option<i64>,
    
    /// Maximum delay in seconds before first start
    #[serde(rename = "max_delay", skip_serializing_if = "Option::is_none")]
    pub max_delay: Option<i64>,
}

/// Options for spawning a task
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SpawnOptions {
    /// Maximum number of retry attempts
    #[serde(rename = "max_attempts", skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<i32>,
    
    /// Retry strategy
    #[serde(rename = "retry_strategy", skip_serializing_if = "Option::is_none")]
    pub retry_strategy: Option<RetryStrategy>,
    
    /// Custom headers for the task
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, JsonValue>>,
    
    /// Queue name override
    #[serde(skip)]
    pub queue: Option<String>,
    
    /// Cancellation policy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancellation: Option<CancellationPolicy>,
    
    /// Idempotency key for exactly-once semantics
    #[serde(rename = "idempotency_key", skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

/// Result of spawning a task
#[derive(Debug, Clone)]
pub struct SpawnResult {
    /// Unique task identifier
    pub task_id: Uuid,
    /// Current run identifier
    pub run_id: Uuid,
    /// Attempt number
    pub attempt: i32,
    /// Whether this was a new task (false if deduplicated)
    pub created: bool,
}

/// A claimed task ready for execution
#[derive(Debug, Clone)]
pub struct ClaimedTask {
    pub run_id: Uuid,
    pub task_id: Uuid,
    pub task_name: String,
    pub attempt: i32,
    pub params: JsonValue,
    pub retry_strategy: Option<JsonValue>,
    pub max_attempts: Option<i32>,
    pub headers: Option<HashMap<String, JsonValue>>,
    pub wake_event: Option<String>,
    pub event_payload: Option<JsonValue>,
}

/// Options for registering a task
#[derive(Debug, Clone)]
pub struct TaskOptions {
    /// Task name (required)
    pub name: String,
    /// Queue name override
    pub queue: Option<String>,
    /// Default max attempts
    pub default_max_attempts: Option<i32>,
    /// Default cancellation policy
    pub default_cancellation: Option<CancellationPolicy>,
}

impl TaskOptions {
    /// Create new task options with just a name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            queue: None,
            default_max_attempts: None,
            default_cancellation: None,
        }
    }

    /// Set queue name
    pub fn with_queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Set default max attempts
    pub fn with_max_attempts(mut self, max_attempts: i32) -> Self {
        self.default_max_attempts = Some(max_attempts);
        self
    }

    /// Set default cancellation policy
    pub fn with_cancellation(mut self, cancellation: CancellationPolicy) -> Self {
        self.default_cancellation = Some(cancellation);
        self
    }
}

/// Worker configuration options
#[derive(Debug, Clone)]
pub struct WorkerOptions {
    /// Worker identifier for tracking
    pub worker_id: String,
    /// Task lease duration in seconds
    pub claim_timeout: i32,
    /// Maximum number of tasks to run concurrently
    pub concurrency: usize,
    /// Number of tasks to claim per batch
    pub batch_size: usize,
    /// Seconds to wait between polls when idle
    pub poll_interval_secs: f64,
    /// Terminate process if task exceeds claim timeout by 2x
    pub fatal_on_lease_timeout: bool,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        let hostname = hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "host".to_string());
        
        Self {
            worker_id: format!("{}:{}", hostname, std::process::id()),
            claim_timeout: 120,
            concurrency: 1,
            batch_size: 1,
            poll_interval_secs: 0.25,
            fatal_on_lease_timeout: true,
        }
    }
}

impl WorkerOptions {
    /// Create new worker options with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set worker identifier
    pub fn with_worker_id(mut self, id: impl Into<String>) -> Self {
        self.worker_id = id.into();
        self
    }

    /// Set claim timeout in seconds
    pub fn with_claim_timeout(mut self, timeout: i32) -> Self {
        self.claim_timeout = timeout;
        self
    }

    /// Set concurrency level
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set poll interval in seconds
    pub fn with_poll_interval(mut self, interval: f64) -> Self {
        self.poll_interval_secs = interval;
        self
    }
}
