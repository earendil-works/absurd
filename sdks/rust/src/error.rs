use thiserror::Error;
use deadpool_postgres::PoolError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    #[error("Pool error: {0}")]
    Pool(#[from] PoolError),

    #[error("Task '{0}' not registered")]
    TaskNotRegistered(String),

    #[error("Task suspended")]
    Suspended,

    #[error("Task cancelled")]
    Cancelled,

    #[error("Event '{0}' timed out after {1} seconds")]
    EventTimeout(String, u64),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn is_suspended(&self) -> bool {
        matches!(self, Error::Suspended)
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self, Error::Cancelled)
    }
}