use thiserror::Error;

/// Custom error types for MicoDB
#[derive(Error, Debug)]
pub enum MicoError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(String),

    #[error("SQL parse error: {0}")]
    SqlParse(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Query execution error: {0}")]
    QueryExecution(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, MicoError>;
