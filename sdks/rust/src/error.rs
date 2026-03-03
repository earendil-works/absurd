use thiserror::Error;

/// Result type for Absurd operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for Absurd SDK operations
#[derive(Error, Debug)]
pub enum Error {
    /// Database connection or query error
    #[error("Database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    /// Task not found in registry
    #[error("Task '{0}' not registered")]
    TaskNotRegistered(String),

    /// Task execution was suspended (internal)
    #[error("Task suspended")]
    Suspended,

    /// Task was cancelled
    #[error("Task cancelled")]
    Cancelled,

    /// Event await timed out
    #[error("Event '{0}' timed out after {1} seconds")]
    EventTimeout(String, u64),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Invalid configuration
    #[error("Configuration error: {0}")]
    Config(String),

    /// Generic error
    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Check if this error indicates task suspension (not a real error)
    pub fn is_suspended(&self) -> bool {
        matches!(self, Error::Suspended)
    }

    /// Check if this error indicates task cancellation
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Error::Cancelled)
    }
}
