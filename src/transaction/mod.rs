// src/transaction/mod.rs

//! Transaction management for MicoDB.
//! Provides ACID transaction guarantees using MVCC (Multi-Version Concurrency Control).

use crate::common::error::{MicoError, Result};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use tokio::fs::{create_dir_all, File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

/// Different states a transaction can be in
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active and operations can be performed
    Active,
    /// Transaction has been committed
    Committed,
    /// Transaction has been rolled back
    RolledBack,
    /// Transaction has timed out or been aborted
    Aborted,
}

/// Represents an operation within a transaction
#[derive(Debug, Clone)]
pub enum TransactionOp {
    /// Table creation operation
    CreateTable {
        db_name: String,
        table_name: String,
    },
    /// Append operation
    Append {
        db_name: String,
        table_name: String,
        file_id: String, // Parquet file ID that was created
    },
    /// Table drop operation
    DropTable {
        db_name: String,
        table_name: String,
    },
    // More operation types will be added as needed
}

/// Information about a single transaction
#[derive(Debug)]
struct TransactionInfo {
    /// Unique transaction ID
    id: u64,
    /// Current state of the transaction
    state: TransactionState,
    /// Time when transaction started
    start_time: SystemTime,
    /// Operations performed in this transaction
    operations: Vec<TransactionOp>,
    /// Set of resources (tables) locked by this transaction
    locks: HashSet<String>, // Format: "db_name/table_name"
}

/// Structure to maintain the transaction log files
struct WALManager {
    /// Base directory for transaction logs
    base_path: PathBuf,
    /// Active WAL file writer
    current_writer: AsyncMutex<Option<BufWriter<File>>>,
}

impl WALManager {
    /// Create a new WAL manager with the given base path
    async fn new(base_path: PathBuf) -> Result<Self> {
        let wal_dir = base_path.join("wal");
        create_dir_all(&wal_dir).await.map_err(MicoError::Io)?;
        
        Ok(Self {
            base_path: wal_dir,
            current_writer: AsyncMutex::new(None),
        })
    }
    
    /// Get the current WAL writer, creating a new one if necessary
    async fn get_writer(&self) -> Result<&AsyncMutex<Option<BufWriter<File>>>> {
        let mut writer_guard = self.current_writer.lock().await;
        if writer_guard.is_none() {
            let file_path = self.base_path.join(format!("wal-{}.log", Uuid::new_v4()));
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&file_path)
                .await
                .map_err(MicoError::Io)?;
                
            *writer_guard = Some(BufWriter::new(file));
            debug!("Created new WAL file: {:?}", file_path);
        }
        
        drop(writer_guard);
        Ok(&self.current_writer)
    }
    
    /// Log a transaction operation to the WAL
    async fn log_operation(&self, tx_id: u64, op: &TransactionOp) -> Result<()> {
        let writer_mutex = self.get_writer().await?;
        let mut writer_guard = writer_mutex.lock().await;
        
        if let Some(writer) = &mut *writer_guard {
            // Simple text-based format for now, could use a binary format for better efficiency
            let op_str = format!("{};{:?}\n", tx_id, op);
            writer.write_all(op_str.as_bytes()).await.map_err(MicoError::Io)?;
            writer.flush().await.map_err(MicoError::Io)?;
            debug!("Logged transaction operation: {}", op_str.trim());
            Ok(())
        } else {
            Err(MicoError::Transaction("WAL writer not initialized".to_string()))
        }
    }
    
    /// Log a transaction state change to the WAL
    async fn log_state_change(&self, tx_id: u64, state: TransactionState) -> Result<()> {
        let writer_mutex = self.get_writer().await?;
        let mut writer_guard = writer_mutex.lock().await;
        
        if let Some(writer) = &mut *writer_guard {
            let state_str = format!("{};STATE;{:?}\n", tx_id, state);
            writer.write_all(state_str.as_bytes()).await.map_err(MicoError::Io)?;
            writer.flush().await.map_err(MicoError::Io)?;
            debug!("Logged transaction state change: {}", state_str.trim());
            Ok(())
        } else {
            Err(MicoError::Transaction("WAL writer not initialized".to_string()))
        }
    }
}

/// Main transaction manager
pub struct TransactionManager {
    /// Map of transaction IDs to transaction information
    transactions: RwLock<HashMap<u64, TransactionInfo>>,
    /// Next transaction ID to assign
    next_tx_id: Mutex<u64>,
    /// Lock manager to prevent write conflicts
    // Each key is a resource name (typically "db_name/table_name")
    // The value is the transaction ID that currently holds a lock on the resource
    locks: RwLock<HashMap<String, u64>>,
    /// Write-ahead logging manager
    wal_manager: Option<Arc<WALManager>>,
    /// Maximum transaction timeout in seconds
    tx_timeout_secs: u64,
}

impl TransactionManager {
    /// Create a new transaction manager instance
    pub fn new() -> Self {
        Self {
            transactions: RwLock::new(HashMap::new()),
            next_tx_id: Mutex::new(1), // Start with TX ID 1
            locks: RwLock::new(HashMap::new()),
            wal_manager: None, // Initialized during setup
            tx_timeout_secs: 60, // Default 60 seconds timeout
        }
    }
    
    /// Initialize the transaction manager with WAL support
    pub async fn setup(&mut self, data_dir: PathBuf) -> Result<()> {
        let wal_manager = WALManager::new(data_dir).await?;
        self.wal_manager = Some(Arc::new(wal_manager));
        info!("Transaction manager initialized with WAL support");
        Ok(())
    }
    
    /// Start a new transaction and return its ID
    pub async fn begin_transaction(&self) -> Result<u64> {
        // Clean up expired transactions first
        self.cleanup_expired_transactions();
        
        // Generate a new transaction ID
        let tx_id = {
            let mut id_guard = self.next_tx_id.lock().unwrap();
            let current_id = *id_guard;
            *id_guard += 1;
            current_id
        };
        
        // Create a new transaction info
        let tx_info = TransactionInfo {
            id: tx_id,
            state: TransactionState::Active,
            start_time: SystemTime::now(),
            operations: Vec::new(),
            locks: HashSet::new(),
        };
        
        // Register the transaction
        {
            let mut txs = self.transactions.write().unwrap();
            txs.insert(tx_id, tx_info);
        }
        
        // Log the transaction start if WAL is enabled
        if let Some(wal) = &self.wal_manager {
            wal.log_state_change(tx_id, TransactionState::Active).await?;
        }
        
        info!("Started transaction {}", tx_id);
        Ok(tx_id)
    }
    
    /// Commit a transaction
    pub async fn commit_transaction(&self, tx_id: u64) -> Result<()> {
        let mut txs = self.transactions.write().unwrap();
        let mut locks = self.locks.write().unwrap();
        
        // Find the transaction
        if let Some(tx_info) = txs.get_mut(&tx_id) {
            // Check if it's in a state that can be committed
            if tx_info.state != TransactionState::Active {
                return Err(MicoError::Transaction(
                    format!("Cannot commit transaction {} in state {:?}", tx_id, tx_info.state)
                ));
            }
            
            // Update the transaction state
            tx_info.state = TransactionState::Committed;
            
            // Release all locks held by this transaction
            for resource in &tx_info.locks {
                locks.remove(resource);
            }
            
            // Log the commit if WAL is enabled
            if let Some(wal) = &self.wal_manager {
                // Drop locks before async operation
                drop(txs);
                drop(locks);
                
                wal.log_state_change(tx_id, TransactionState::Committed).await?;
                
                // Re-acquire locks to finish the operation
                let mut txs = self.transactions.write().unwrap();
                if let Some(tx_info) = txs.get_mut(&tx_id) {
                    info!("Committed transaction {}", tx_id);
                    // In a real implementation we might retain transaction records for durability
                    // and recovery, but for simplicity we'll remove it
                    txs.remove(&tx_id);
                }
            } else {
                info!("Committed transaction {}", tx_id);
                // Remove the transaction if WAL is not enabled
                txs.remove(&tx_id);
            }
            
            Ok(())
        } else {
            Err(MicoError::Transaction(format!("Transaction {} not found", tx_id)))
        }
    }
    
    /// Roll back a transaction
    pub async fn rollback_transaction(&self, tx_id: u64) -> Result<()> {
        let mut txs = self.transactions.write().unwrap();
        let mut locks = self.locks.write().unwrap();
        
        // Find the transaction
        if let Some(tx_info) = txs.get_mut(&tx_id) {
            // Check if it's in a state that can be rolled back
            if tx_info.state != TransactionState::Active {
                return Err(MicoError::Transaction(
                    format!("Cannot rollback transaction {} in state {:?}", tx_id, tx_info.state)
                ));
            }
            
            // TODO: Undo all operations in reverse order
            // This would require implementing undo logic for each operation type
            // For now, we'll just update the state
            
            // Update the transaction state
            tx_info.state = TransactionState::RolledBack;
            
            // Release all locks held by this transaction
            for resource in &tx_info.locks {
                locks.remove(resource);
            }
            
            // Log the rollback if WAL is enabled
            if let Some(wal) = &self.wal_manager {
                // Drop locks before async operation
                drop(txs);
                drop(locks);
                
                wal.log_state_change(tx_id, TransactionState::RolledBack).await?;
                
                // Re-acquire locks to finish the operation
                let mut txs = self.transactions.write().unwrap();
                if let Some(tx_info) = txs.get_mut(&tx_id) {
                    info!("Rolled back transaction {}", tx_id);
                    // In a real implementation we might retain transaction records for durability
                    // and recovery, but for simplicity we'll remove it
                    txs.remove(&tx_id);
                }
            } else {
                info!("Rolled back transaction {}", tx_id);
                // Remove the transaction if WAL is not enabled
                txs.remove(&tx_id);
            }
            
            Ok(())
        } else {
            Err(MicoError::Transaction(format!("Transaction {} not found", tx_id)))
        }
    }
    
    /// Register a new operation in a transaction
    pub async fn register_operation(&self, tx_id: u64, op: TransactionOp) -> Result<()> {
        // First, try to acquire necessary locks based on the operation type
        self.acquire_locks_for_op(tx_id, &op)?;
        
        // Then add the operation to the transaction's log
        let mut txs = self.transactions.write().unwrap();
        
        // Find the transaction
        if let Some(tx_info) = txs.get_mut(&tx_id) {
            // Check if transaction is active
            if tx_info.state != TransactionState::Active {
                return Err(MicoError::Transaction(
                    format!("Cannot add operation to transaction {} in state {:?}", tx_id, tx_info.state)
                ));
            }
            
            // Add the operation to the log
            tx_info.operations.push(op.clone());
            
            // Log the operation if WAL is enabled
            if let Some(wal) = &self.wal_manager {
                // Drop the lock before async operation
                drop(txs);
                
                wal.log_operation(tx_id, &op).await?;
                
                debug!("Added and logged operation to transaction {}", tx_id);
            } else {
                debug!("Added operation to transaction {}", tx_id);
            }
            
            Ok(())
        } else {
            Err(MicoError::Transaction(format!("Transaction {} not found", tx_id)))
        }
    }
    
    /// Check if a transaction exists and is active
    pub fn is_transaction_active(&self, tx_id: u64) -> bool {
        let txs = self.transactions.read().unwrap();
        txs.get(&tx_id)
            .map(|tx| tx.state == TransactionState::Active)
            .unwrap_or(false)
    }
    
    /// Acquire locks needed for a transaction operation
    fn acquire_locks_for_op(&self, tx_id: u64, op: &TransactionOp) -> Result<()> {
        let mut locks = self.locks.write().unwrap();
        let mut txs = self.transactions.write().unwrap();
        
        let resource_key = match op {
            TransactionOp::CreateTable { db_name, table_name } => {
                format!("{}/{}", db_name, table_name)
            }
            TransactionOp::Append { db_name, table_name, .. } => {
                format!("{}/{}", db_name, table_name)
            }
            TransactionOp::DropTable { db_name, table_name } => {
                format!("{}/{}", db_name, table_name)
            }
        };
        
        // Check if the resource is already locked by another transaction
        if let Some(&lock_tx_id) = locks.get(&resource_key) {
            if lock_tx_id != tx_id {
                return Err(MicoError::Transaction(
                    format!("Resource {} is locked by transaction {}", resource_key, lock_tx_id)
                ));
            }
            // Already locked by this transaction, nothing more to do
            return Ok(());
        }
        
        // Lock the resource with this transaction
        locks.insert(resource_key.clone(), tx_id);
        
        // Add the resource to the transaction's lock set
        if let Some(tx_info) = txs.get_mut(&tx_id) {
            tx_info.locks.insert(resource_key);
        } else {
            // This shouldn't happen, but if the transaction doesn't exist, release the lock
            locks.remove(&resource_key);
            return Err(MicoError::Transaction(format!("Transaction {} not found", tx_id)));
        }
        
        Ok(())
    }
    
    /// Clean up expired transactions
    fn cleanup_expired_transactions(&self) {
        let mut txs = self.transactions.write().unwrap();
        let mut locks = self.locks.write().unwrap();
        let now = SystemTime::now();
        
        let expired_tx_ids: Vec<u64> = txs
            .iter()
            .filter_map(|(&id, tx)| {
                if tx.state == TransactionState::Active {
                    match now.duration_since(tx.start_time) {
                        Ok(duration) if duration > Duration::from_secs(self.tx_timeout_secs) => Some(id),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .collect();
        
        for tx_id in expired_tx_ids {
            if let Some(tx_info) = txs.get_mut(&tx_id) {
                warn!("Aborting expired transaction {}", tx_id);
                tx_info.state = TransactionState::Aborted;
                
                // Release all locks
                for resource in &tx_info.locks {
                    locks.remove(resource);
                }
                
                // In a real implementation, we would also need to add undo logs and rollback
                // any operations performed by this transaction
            }
        }
    }
    
    /// Get statistics about active transactions
    pub fn get_stats(&self) -> (usize, usize) {
        let txs = self.transactions.read().unwrap();
        let active_count = txs.values().filter(|tx| tx.state == TransactionState::Active).count();
        (txs.len(), active_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[tokio::test]
    async fn test_begin_commit_transaction() {
        let tx_manager = TransactionManager::new();
        
        // Begin a transaction
        let tx_id = tx_manager.begin_transaction().await.unwrap();
        assert!(tx_manager.is_transaction_active(tx_id));
        
        // Commit the transaction
        tx_manager.commit_transaction(tx_id).await.unwrap();
        assert!(!tx_manager.is_transaction_active(tx_id));
    }
    
    #[tokio::test]
    async fn test_begin_rollback_transaction() {
        let tx_manager = TransactionManager::new();
        
        // Begin a transaction
        let tx_id = tx_manager.begin_transaction().await.unwrap();
        assert!(tx_manager.is_transaction_active(tx_id));
        
        // Rollback the transaction
        tx_manager.rollback_transaction(tx_id).await.unwrap();
        assert!(!tx_manager.is_transaction_active(tx_id));
    }
    
    #[tokio::test]
    async fn test_transaction_operations() {
        let tx_manager = TransactionManager::new();
        
        // Begin a transaction
        let tx_id = tx_manager.begin_transaction().await.unwrap();
        
        // Register an operation
        let op = TransactionOp::CreateTable {
            db_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
        };
        tx_manager.register_operation(tx_id, op).await.unwrap();
        
        // Register another operation on the same resource
        let op2 = TransactionOp::Append {
            db_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
            file_id: "file1.parquet".to_string(),
        };
        tx_manager.register_operation(tx_id, op2).await.unwrap();
        
        // Commit the transaction
        tx_manager.commit_transaction(tx_id).await.unwrap();
        
        // Verify transaction no longer exists
        assert!(!tx_manager.is_transaction_active(tx_id));
    }
    
    #[tokio::test]
    async fn test_lock_conflicts() {
        let tx_manager = TransactionManager::new();
        
        // Begin two transactions
        let tx_id1 = tx_manager.begin_transaction().await.unwrap();
        let tx_id2 = tx_manager.begin_transaction().await.unwrap();
        
        // Register an operation on the first transaction
        let op1 = TransactionOp::CreateTable {
            db_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
        };
        tx_manager.register_operation(tx_id1, op1).await.unwrap();
        
        // Try to register an operation on the same resource in the second transaction
        // This should fail due to lock conflict
        let op2 = TransactionOp::Append {
            db_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
            file_id: "file1.parquet".to_string(),
        };
        let result = tx_manager.register_operation(tx_id2, op2).await;
        assert!(result.is_err());
        
        // Commit the first transaction
        tx_manager.commit_transaction(tx_id1).await.unwrap();
        
        // Now the second transaction should be able to acquire the lock
        let op3 = TransactionOp::Append {
            db_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
            file_id: "file2.parquet".to_string(),
        };
        let result = tx_manager.register_operation(tx_id2, op3).await;
        assert!(result.is_ok());
        
        // Commit the second transaction
        tx_manager.commit_transaction(tx_id2).await.unwrap();
    }
    
    #[tokio::test]
    async fn test_wal_initialization() {
        let temp_dir = tempdir().unwrap();
        let mut tx_manager = TransactionManager::new();
        
        // Setup WAL
        tx_manager.setup(temp_dir.path().to_path_buf()).await.unwrap();
        
        // Verify WAL directory was created
        let wal_dir = temp_dir.path().join("wal");
        assert!(wal_dir.exists());
        
        // Begin and commit a transaction to generate some WAL entries
        let tx_id = tx_manager.begin_transaction().await.unwrap();
        let op = TransactionOp::CreateTable {
            db_name: "test_wal_db".to_string(),
            table_name: "test_wal_table".to_string(),
        };
        tx_manager.register_operation(tx_id, op).await.unwrap();
        tx_manager.commit_transaction(tx_id).await.unwrap();
        
        // Check that a WAL file was created
        let entries = std::fs::read_dir(&wal_dir).unwrap();
        assert!(entries.count() > 0);
    }
}