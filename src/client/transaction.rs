// src/client/transaction.rs

//! Transaction support for the MicoDB client.

use crate::client::connection::Connection;
use crate::client::error::{ClientError, Result};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Represents the state of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active and operations can be performed.
    Active,
    /// Transaction has been committed.
    Committed,
    /// Transaction has been rolled back.
    RolledBack,
}

impl fmt::Display for TransactionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionState::Active => write!(f, "Active"),
            TransactionState::Committed => write!(f, "Committed"),
            TransactionState::RolledBack => write!(f, "Rolled Back"),
        }
    }
}

/// A transaction in the MicoDB database.
pub struct Transaction {
    /// Connection to the server.
    connection: Arc<Mutex<Connection>>,
    /// Transaction ID.
    tx_id: u64,
    /// Database name.
    db_name: String,
    /// Current state of the transaction.
    state: TransactionState,
}

impl Transaction {
    /// Create a new transaction.
    pub(crate) async fn begin(connection: Connection, db_name: &str) -> Result<Self> {
        let connection = Arc::new(Mutex::new(connection));
        let mut conn_guard = connection.lock().await;
        
        let tx_id = conn_guard.begin_transaction(db_name).await?;
        
        Ok(Self {
            connection,
            tx_id,
            db_name: db_name.to_string(),
            state: TransactionState::Active,
        })
    }
    
    /// Get the transaction ID.
    pub fn id(&self) -> u64 {
        self.tx_id
    }
    
    /// Get the current state of the transaction.
    pub fn state(&self) -> TransactionState {
        self.state
    }
    
    /// Check if the transaction is active.
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }
    
    /// Create a new table within this transaction.
    pub async fn create_table(&mut self, table_name: &str, schema: &Schema) -> Result<()> {
        if !self.is_active() {
            return Err(ClientError::TransactionError(
                format!("Cannot create table in {} transaction", self.state)
            ));
        }
        
        let mut conn = self.connection.lock().await;
        conn.create_table_tx(&self.db_name, table_name, schema, self.tx_id).await
    }
    
    /// Append data to a table within this transaction.
    pub async fn append(&mut self, table_name: &str, batch: &RecordBatch) -> Result<()> {
        if !self.is_active() {
            return Err(ClientError::TransactionError(
                format!("Cannot append data in {} transaction", self.state)
            ));
        }
        
        let mut conn = self.connection.lock().await;
        conn.append_tx(&self.db_name, table_name, batch, self.tx_id).await
    }
    
    /// Drop a table within this transaction.
    pub async fn drop_table(&mut self, table_name: &str) -> Result<()> {
        if !self.is_active() {
            return Err(ClientError::TransactionError(
                format!("Cannot drop table in {} transaction", self.state)
            ));
        }
        
        let mut conn = self.connection.lock().await;
        conn.drop_table_tx(&self.db_name, table_name, self.tx_id).await
    }
    
    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<()> {
        if !self.is_active() {
            return Err(ClientError::TransactionError(
                format!("Cannot commit {} transaction", self.state)
            ));
        }
        
        let mut conn = self.connection.lock().await;
        conn.commit_transaction(&self.db_name, self.tx_id).await?;
        
        self.state = TransactionState::Committed;
        Ok(())
    }
    
    /// Rollback the transaction.
    pub async fn rollback(mut self) -> Result<()> {
        if !self.is_active() {
            return Err(ClientError::TransactionError(
                format!("Cannot rollback {} transaction", self.state)
            ));
        }
        
        let mut conn = self.connection.lock().await;
        conn.rollback_transaction(&self.db_name, self.tx_id).await?;
        
        self.state = TransactionState::RolledBack;
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // If the transaction is still active when dropped, log a warning
        // In a real implementation, we might want to automatically rollback,
        // but that's tricky with async operations in Drop.
        if self.is_active() {
            eprintln!("Warning: Transaction {} dropped while still active. This may lead to a hanging transaction on the server.", self.tx_id);
        }
    }
}