// src/client/mod.rs

//! MicoDB client library.
//!
//! This module provides a robust, high-performance client library for connecting to and
//! interacting with a MicoDB server. It includes both sync and async APIs, connection pooling,
//! automatic retry logic, and a fluent query builder interface.

mod error;
mod connection;
mod query_builder;
mod transaction;
mod types;
mod pool;

use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef, DataType, Field};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::time::Duration;

pub use error::{ClientError, Result};
pub use connection::{Connection, ConnectionOptions};
pub use query_builder::{QueryBuilder, SelectBuilder, FilterCondition};
pub use transaction::Transaction;
pub use types::{Value, Row, Table, QueryResult};
pub use pool::{ConnectionPool, PoolOptions};

/// Entry point for creating a connection to a MicoDB server.
pub async fn connect(address: &str) -> Result<Connection> {
    let options = ConnectionOptions::default();
    connect_with_options(address, options).await
}

/// Connect to a MicoDB server with custom options.
pub async fn connect_with_options(address: &str, options: ConnectionOptions) -> Result<Connection> {
    Connection::connect(address, options).await
}

/// Create a new connection pool to a MicoDB server.
pub fn create_pool(address: &str) -> Result<ConnectionPool> {
    let options = PoolOptions::default();
    create_pool_with_options(address, options)
}

/// Create a new connection pool with custom options.
pub fn create_pool_with_options(address: &str, options: PoolOptions) -> Result<ConnectionPool> {
    ConnectionPool::new(address, options)
}

/// A high-level client that encapsulates a connection pool and provides a simplified API.
pub struct Client {
    pool: ConnectionPool,
}

impl Client {
    /// Create a new client with default options.
    pub fn new(address: &str) -> Result<Self> {
        let pool = create_pool(address)?;
        Ok(Self { pool })
    }

    /// Create a new client with custom pool options.
    pub fn with_options(address: &str, options: PoolOptions) -> Result<Self> {
        let pool = create_pool_with_options(address, options)?;
        Ok(Self { pool })
    }

    /// Create a new database.
    pub async fn create_database(&self, name: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.create_database(name).await
    }

    /// Create a new table with the given schema.
    pub async fn create_table(&self, db_name: &str, table_name: &str, schema: &Schema) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.create_table(db_name, table_name, schema).await
    }

    /// Insert data into a table.
    pub async fn append(&self, db_name: &str, table_name: &str, batch: &RecordBatch) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.append(db_name, table_name, batch).await
    }

    /// Read all data from a table.
    pub async fn read_table(&self, db_name: &str, table_name: &str) -> Result<Vec<RecordBatch>> {
        let conn = self.pool.get().await?;
        conn.read_table(db_name, table_name).await
    }

    /// Execute a SQL query.
    pub async fn execute_sql(&self, db_name: &str, query: &str) -> Result<Vec<RecordBatch>> {
        let conn = self.pool.get().await?;
        conn.execute_sql(db_name, query).await
    }

    /// Drop a table.
    pub async fn drop_table(&self, db_name: &str, table_name: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.drop_table(db_name, table_name).await
    }

    /// Begin a new transaction.
    pub async fn begin_transaction(&self, db_name: &str) -> Result<Transaction> {
        let conn = self.pool.get().await?;
        Transaction::begin(conn, db_name).await
    }

    /// Get a query builder for constructing SQL statements.
    pub fn query(&self) -> QueryBuilder {
        QueryBuilder::new(Arc::new(self.pool.clone()))
    }
}

/// Simplified synchronous client API for convenience
#[cfg(feature = "sync")]
pub mod sync {
    use super::*;
    use tokio::runtime::{Runtime, Builder};
    use std::sync::Mutex;
    use once_cell::sync::Lazy;
    
    static RUNTIME: Lazy<Mutex<Runtime>> = Lazy::new(|| {
        Mutex::new(
            Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
        )
    });
    
    /// A synchronous client interface wrapping the async version.
    pub struct SyncClient {
        client: Client,
    }
    
    impl SyncClient {
        /// Create a new synchronous client.
        pub fn new(address: &str) -> Result<Self> {
            let client = Client::new(address)?;
            Ok(Self { client })
        }
        
        /// Create a new database.
        pub fn create_database(&self, name: &str) -> Result<()> {
            let rt = RUNTIME.lock().unwrap();
            rt.block_on(self.client.create_database(name))
        }
        
        /// Create a new table with the given schema.
        pub fn create_table(&self, db_name: &str, table_name: &str, schema: &Schema) -> Result<()> {
            let rt = RUNTIME.lock().unwrap();
            rt.block_on(self.client.create_table(db_name, table_name, schema))
        }
        
        /// Insert data into a table.
        pub fn append(&self, db_name: &str, table_name: &str, batch: &RecordBatch) -> Result<()> {
            let rt = RUNTIME.lock().unwrap();
            rt.block_on(self.client.append(db_name, table_name, batch))
        }
        
        /// Read all data from a table.
        pub fn read_table(&self, db_name: &str, table_name: &str) -> Result<Vec<RecordBatch>> {
            let rt = RUNTIME.lock().unwrap();
            rt.block_on(self.client.read_table(db_name, table_name))
        }
        
        /// Execute a SQL query.
        pub fn execute_sql(&self, db_name: &str, query: &str) -> Result<Vec<RecordBatch>> {
            let rt = RUNTIME.lock().unwrap();
            rt.block_on(self.client.execute_sql(db_name, query))
        }
        
        /// Drop a table.
        pub fn drop_table(&self, db_name: &str, table_name: &str) -> Result<()> {
            let rt = RUNTIME.lock().unwrap();
            rt.block_on(self.client.drop_table(db_name, table_name))
        }
    }
}