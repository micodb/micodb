// src/client/error.rs

//! Error types for the MicoDB client library.

use std::io;
use std::fmt;
use thiserror::Error;
use arrow::error::ArrowError;

/// Result type for client operations.
pub type Result<T> = std::result::Result<T, ClientError>;

/// Errors that can occur when using the MicoDB client.
#[derive(Error, Debug)]
pub enum ClientError {
    /// Connection failed to the MicoDB server.
    #[error("Connection error: {0}")]
    ConnectionError(String),

    /// Network I/O error.
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    /// Authentication failed.
    #[error("Authentication failed: {0}")]
    AuthenticationError(String),

    /// Server returned an error.
    #[error("Server error: {0}")]
    ServerError(String),

    /// Error parsing server response.
    #[error("Parse error: {0}")]
    ParseError(String),

    /// Error converting between Arrow and native types.
    #[error("Arrow error: {0}")]
    ArrowError(#[from] ArrowError),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// Transaction error.
    #[error("Transaction error: {0}")]
    TransactionError(String),

    /// Connection pool error.
    #[error("Connection pool error: {0}")]
    PoolError(String),

    /// Query error.
    #[error("Query error: {0}")]
    QueryError(String),
    
    /// Timeout error.
    #[error("Operation timed out after {0:?}")]
    TimeoutError(std::time::Duration),
    
    /// Resource (e.g., database or table) not found.
    #[error("Resource not found: {0}")]
    NotFoundError(String),
    
    /// Schema validation error.
    #[error("Schema validation error: {0}")]
    SchemaError(String),
    
    /// Feature not supported.
    #[error("Feature not supported: {0}")]
    UnsupportedFeatureError(String),
    
    /// General client error.
    #[error("{0}")]
    Other(String),
}

impl ClientError {
    /// Returns true if the error indicates a transient failure that might
    /// succeed on retry (e.g., connection error, timeout).
    pub fn is_retryable(&self) -> bool {
        match self {
            ClientError::ConnectionError(_) => true,
            ClientError::IoError(err) => {
                matches!(
                    err.kind(),
                    io::ErrorKind::ConnectionRefused | 
                    io::ErrorKind::ConnectionReset | 
                    io::ErrorKind::ConnectionAborted | 
                    io::ErrorKind::TimedOut |
                    io::ErrorKind::Interrupted |
                    io::ErrorKind::WouldBlock
                )
            },
            ClientError::TimeoutError(_) => true,
            ClientError::ServerError(msg) => {
                msg.contains("overloaded") || 
                msg.contains("temporary") || 
                msg.contains("retry")
            },
            _ => false,
        }
    }
}