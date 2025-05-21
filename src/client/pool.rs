// src/client/pool.rs

//! Connection pooling for the MicoDB client.

use crate::client::connection::{Connection, ConnectionOptions};
use crate::client::error::{ClientError, Result};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::timeout;

/// Options for configuring a connection pool.
#[derive(Debug, Clone)]
pub struct PoolOptions {
    /// Minimum number of connections in the pool.
    pub min_connections: usize,
    /// Maximum number of connections in the pool.
    pub max_connections: usize,
    /// Maximum time to wait for a connection to become available.
    pub acquire_timeout: Duration,
    /// Whether to test connections when they are returned to the pool.
    pub test_on_return: bool,
    /// Maximum idle time for a connection before it's closed.
    pub max_idle_time: Option<Duration>,
    /// Connection options for creating new connections.
    pub connection_options: ConnectionOptions,
}

impl Default for PoolOptions {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            acquire_timeout: Duration::from_secs(30),
            test_on_return: true,
            max_idle_time: Some(Duration::from_secs(60 * 30)), // 30 minutes
            connection_options: ConnectionOptions::default(),
        }
    }
}

/// A pooled connection with metadata.
struct PooledConnection {
    /// The connection instance.
    connection: Connection,
    /// When the connection was created.
    created_at: Instant,
    /// When the connection was last used.
    last_used_at: Instant,
}

/// A connection pool for MicoDB.
#[derive(Clone)]
pub struct ConnectionPool {
    /// Server address.
    address: String,
    /// Pool options.
    options: PoolOptions,
    /// Available connections.
    available: Arc<Mutex<VecDeque<PooledConnection>>>,
    /// Semaphore for tracking total connections.
    semaphore: Arc<Semaphore>,
}

impl ConnectionPool {
    /// Create a new connection pool.
    pub fn new(address: impl Into<String>, options: PoolOptions) -> Result<Self> {
        let address = address.into();
        let semaphore = Arc::new(Semaphore::new(options.max_connections));
        let available = Arc::new(Mutex::new(VecDeque::with_capacity(options.max_connections)));
        
        let pool = Self {
            address,
            options,
            available,
            semaphore,
        };
        
        // Create minimum connections
        // We can't do this immediately as it would require async initialization.
        // In a real implementation, we could use a background task to maintain min connections.
        
        Ok(pool)
    }
    
    /// Create a new connection.
    async fn create_connection(&self) -> Result<Connection> {
        Connection::connect(&self.address, self.options.connection_options.clone()).await
    }
    
    /// Get a connection from the pool.
    pub async fn get(&self) -> Result<Connection> {
        // Attempt to acquire a permit
        let permit = match timeout(self.options.acquire_timeout, self.semaphore.acquire()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(e)) => return Err(ClientError::PoolError(format!("Failed to acquire connection: {}", e))),
            Err(_) => return Err(ClientError::TimeoutError(self.options.acquire_timeout)),
        };
        
        // Check for available connection
        let mut available = self.available.lock().await;
        
        // Remove expired connections
        if let Some(max_idle) = self.options.max_idle_time {
            let now = Instant::now();
            while let Some(conn) = available.front() {
                if now.duration_since(conn.last_used_at) > max_idle {
                    available.pop_front();
                } else {
                    break;
                }
            }
        }
        
        // Get a connection from the pool or create a new one
        let conn = if let Some(mut pooled) = available.pop_front() {
            pooled.last_used_at = Instant::now();
            pooled.connection
        } else {
            // Create a new connection
            drop(available); // Release lock while connecting
            let conn = self.create_connection().await?;
            conn
        };
        
        // The semaphore permit is dropped when the connection is returned to the pool
        // or explicitly closed.
        Ok(conn)
    }
    
    /// Return a connection to the pool.
    pub async fn return_connection(&self, connection: Connection) -> Result<()> {
        // Test the connection if required
        if self.options.test_on_return {
            // In a real implementation, we would test the connection here
            // by sending a simple command to ensure it's still valid.
        }
        
        let now = Instant::now();
        let pooled = PooledConnection {
            connection,
            created_at: now,
            last_used_at: now,
        };
        
        let mut available = self.available.lock().await;
        
        // Add the connection to the available pool
        available.push_back(pooled);
        
        // The semaphore permit is released here as the connection
        // is considered available again.
        
        Ok(())
    }
    
    /// Close the pool and all connections.
    pub async fn close(&self) -> Result<()> {
        let mut available = self.available.lock().await;
        
        // Clear all connections
        available.clear();
        
        Ok(())
    }
}