// src/cluster/metadata.rs

//! Metadata catalog for MicoDB cluster.
//!
//! The metadata catalog maintains information about databases, tables,
//! partitions, and shards in the cluster.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use crate::common::error::{MicoError, Result};
use super::node::NodeId;
use super::discovery::ServiceDiscovery;

/// Information about a shard in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    /// The shard ID
    pub shard_id: String,
    /// The partition ID
    pub partition_id: String,
    /// The node ID hosting this shard
    pub node_id: NodeId,
    /// The size of the shard in bytes
    pub size_bytes: u64,
    /// The number of rows in the shard
    pub row_count: u64,
    /// The creation time
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// The last update time
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// Whether this is a replica
    pub is_replica: bool,
    /// The replica group ID
    pub replica_group: Option<String>,
    /// The shard status
    pub status: ShardStatus,
}

/// Status of a shard
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardStatus {
    /// Shard is being created
    Creating,
    /// Shard is ready
    Ready,
    /// Shard is being moved
    Moving,
    /// Shard is being deleted
    Deleting,
    /// Shard is in an error state
    Error,
}

/// Information about a partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    /// The partition ID
    pub partition_id: String,
    /// The table ID
    pub table_id: String,
    /// The partition key
    pub partition_key: String,
    /// The shards in this partition
    pub shards: HashMap<String, ShardInfo>,
    /// The creation time
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// The last update time
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// The partition status
    pub status: PartitionStatus,
}

/// Status of a partition
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionStatus {
    /// Partition is being created
    Creating,
    /// Partition is ready
    Ready,
    /// Partition is being split
    Splitting,
    /// Partition is being merged
    Merging,
    /// Partition is being deleted
    Deleting,
    /// Partition is in an error state
    Error,
}

/// Information about a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    /// The table ID
    pub table_id: String,
    /// The database ID
    pub database_id: String,
    /// The table name
    pub name: String,
    /// The table schema (Arrow schema serialized to JSON)
    pub schema_json: String,
    /// The partitions in this table
    pub partitions: HashMap<String, PartitionInfo>,
    /// The creation time
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// The last update time
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// The partition strategy for this table
    pub partition_strategy: String,
    /// The table status
    pub status: TableStatus,
}

/// Status of a table
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableStatus {
    /// Table is being created
    Creating,
    /// Table is ready
    Ready,
    /// Table is being altered
    Altering,
    /// Table is being deleted
    Deleting,
    /// Table is in an error state
    Error,
}

/// Information about a database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    /// The database ID
    pub database_id: String,
    /// The database name
    pub name: String,
    /// The tables in this database
    pub tables: HashMap<String, TableInfo>,
    /// The creation time
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// The last update time
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// The database status
    pub status: DatabaseStatus,
}

/// Status of a database
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseStatus {
    /// Database is being created
    Creating,
    /// Database is ready
    Ready,
    /// Database is being deleted
    Deleting,
    /// Database is in an error state
    Error,
}

/// Mapping of partitions to nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMap {
    /// The mapping of partition IDs to node IDs
    pub map: HashMap<String, Vec<NodeId>>,
    /// The version of this map
    pub version: u64,
    /// The last update time
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// The metadata catalog for a MicoDB cluster
pub struct MetadataCatalog {
    /// The databases in the cluster
    databases: RwLock<HashMap<String, DatabaseInfo>>,
    /// The partitions in the cluster (for quick lookup)
    partitions: RwLock<HashMap<String, (String, String)>>, // partition_id -> (database_id, table_id)
    /// The shards in the cluster (for quick lookup)
    shards: RwLock<HashMap<String, (String, String, String)>>, // shard_id -> (database_id, table_id, partition_id)
    /// The partition map
    partition_map: RwLock<PartitionMap>,
    /// The path to the metadata directory
    metadata_path: String,
    /// The service discovery mechanism
    service_discovery: Arc<dyn ServiceDiscovery>,
}

impl MetadataCatalog {
    /// Create a new metadata catalog
    pub fn new(metadata_path: &str, service_discovery: Arc<dyn ServiceDiscovery>) -> Result<Self> {
        Ok(Self {
            databases: RwLock::new(HashMap::new()),
            partitions: RwLock::new(HashMap::new()),
            shards: RwLock::new(HashMap::new()),
            partition_map: RwLock::new(PartitionMap {
                map: HashMap::new(),
                version: 0,
                updated_at: chrono::Utc::now(),
            }),
            metadata_path: metadata_path.to_string(),
            service_discovery,
        })
    }
    
    /// Load metadata from disk
    pub async fn load(&self) -> Result<()> {
        // In a real implementation, this would load metadata from disk
        Ok(())
    }
    
    /// Save metadata to disk
    pub async fn save(&self) -> Result<()> {
        // In a real implementation, this would save metadata to disk
        Ok(())
    }
    
    /// Create a new database
    pub async fn create_database(&self, name: &str) -> Result<String> {
        // Generate a database ID
        let database_id = format!("db-{}", uuid::Uuid::new_v4());
        
        // Create a new database
        let database = DatabaseInfo {
            database_id: database_id.clone(),
            name: name.to_string(),
            tables: HashMap::new(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            status: DatabaseStatus::Ready,
        };
        
        // Add the database to the catalog
        {
            let mut databases = self.databases.write().await;
            if databases.values().any(|db| db.name == name) {
                return Err(MicoError::Internal(format!("Database '{}' already exists", name)));
            }
            databases.insert(database_id.clone(), database);
        }
        
        // Save the metadata
        self.save().await?;
        
        Ok(database_id)
    }
    
    /// Get a database by ID
    pub async fn get_database(&self, database_id: &str) -> Result<DatabaseInfo> {
        let databases = self.databases.read().await;
        databases.get(database_id).cloned().ok_or_else(|| {
            MicoError::Internal(format!("Database '{}' not found", database_id))
        })
    }
    
    /// Get a database by name
    pub async fn get_database_by_name(&self, name: &str) -> Result<DatabaseInfo> {
        let databases = self.databases.read().await;
        databases.values().find(|db| db.name == name).cloned().ok_or_else(|| {
            MicoError::Internal(format!("Database '{}' not found", name))
        })
    }
    
    /// Create a new table
    pub async fn create_table(
        &self,
        database_id: &str,
        name: &str,
        schema_json: &str,
        partition_strategy: &str,
    ) -> Result<String> {
        // Check if the database exists
        let database = self.get_database(database_id).await?;
        
        // Generate a table ID
        let table_id = format!("table-{}", uuid::Uuid::new_v4());
        
        // Create a new table
        let table = TableInfo {
            table_id: table_id.clone(),
            database_id: database_id.to_string(),
            name: name.to_string(),
            schema_json: schema_json.to_string(),
            partitions: HashMap::new(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            partition_strategy: partition_strategy.to_string(),
            status: TableStatus::Ready,
        };
        
        // Add the table to the database
        {
            let mut databases = self.databases.write().await;
            let db = databases.get_mut(database_id).ok_or_else(|| {
                MicoError::Internal(format!("Database '{}' not found", database_id))
            })?;
            
            if db.tables.values().any(|t| t.name == name) {
                return Err(MicoError::Internal(format!("Table '{}' already exists in database '{}'", name, database.name)));
            }
            
            db.tables.insert(table_id.clone(), table);
            db.updated_at = chrono::Utc::now();
        }
        
        // Save the metadata
        self.save().await?;
        
        Ok(table_id)
    }
    
    /// Get a table by ID
    pub async fn get_table(&self, database_id: &str, table_id: &str) -> Result<TableInfo> {
        let databases = self.databases.read().await;
        let database = databases.get(database_id).ok_or_else(|| {
            MicoError::Internal(format!("Database '{}' not found", database_id))
        })?;
        
        database.tables.get(table_id).cloned().ok_or_else(|| {
            MicoError::Internal(format!("Table '{}' not found in database '{}'", table_id, database.name))
        })
    }
    
    /// Get a table by name
    pub async fn get_table_by_name(&self, database_id: &str, name: &str) -> Result<TableInfo> {
        let databases = self.databases.read().await;
        let database = databases.get(database_id).ok_or_else(|| {
            MicoError::Internal(format!("Database '{}' not found", database_id))
        })?;
        
        database.tables.values().find(|t| t.name == name).cloned().ok_or_else(|| {
            MicoError::Internal(format!("Table '{}' not found in database '{}'", name, database.name))
        })
    }
    
    /// Create a new partition
    pub async fn create_partition(
        &self,
        database_id: &str,
        table_id: &str,
        partition_key: &str,
    ) -> Result<String> {
        // Check if the table exists
        let table = self.get_table(database_id, table_id).await?;
        
        // Generate a partition ID
        let partition_id = format!("partition-{}", uuid::Uuid::new_v4());
        
        // Create a new partition
        let partition = PartitionInfo {
            partition_id: partition_id.clone(),
            table_id: table_id.to_string(),
            partition_key: partition_key.to_string(),
            shards: HashMap::new(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            status: PartitionStatus::Ready,
        };
        
        // Add the partition to the table
        {
            let mut databases = self.databases.write().await;
            let database = databases.get_mut(database_id).ok_or_else(|| {
                MicoError::Internal(format!("Database '{}' not found", database_id))
            })?;
            
            let table = database.tables.get_mut(table_id).ok_or_else(|| {
                MicoError::Internal(format!("Table '{}' not found in database '{}'", table_id, database.name))
            })?;
            
            if table.partitions.values().any(|p| p.partition_key == partition_key) {
                return Err(MicoError::Internal(format!("Partition with key '{}' already exists in table '{}'", partition_key, table.name)));
            }
            
            table.partitions.insert(partition_id.clone(), partition);
            table.updated_at = chrono::Utc::now();
            database.updated_at = chrono::Utc::now();
            
            // Update the partition lookup
            let mut partitions = self.partitions.write().await;
            partitions.insert(partition_id.clone(), (database_id.to_string(), table_id.to_string()));
        }
        
        // Save the metadata
        self.save().await?;
        
        Ok(partition_id)
    }
    
    /// Get a partition by ID
    pub async fn get_partition(&self, partition_id: &str) -> Result<(DatabaseInfo, TableInfo, PartitionInfo)> {
        // Get the database and table IDs
        let partitions = self.partitions.read().await;
        let (database_id, table_id) = partitions.get(partition_id).ok_or_else(|| {
            MicoError::Internal(format!("Partition '{}' not found", partition_id))
        })?;
        
        // Get the database, table, and partition
        let databases = self.databases.read().await;
        let database = databases.get(database_id).ok_or_else(|| {
            MicoError::Internal(format!("Database '{}' not found", database_id))
        })?;
        
        let table = database.tables.get(table_id).ok_or_else(|| {
            MicoError::Internal(format!("Table '{}' not found in database '{}'", table_id, database.name))
        })?;
        
        let partition = table.partitions.get(partition_id).ok_or_else(|| {
            MicoError::Internal(format!("Partition '{}' not found in table '{}'", partition_id, table.name))
        })?;
        
        Ok((database.clone(), table.clone(), partition.clone()))
    }
    
    /// Create a new shard
    pub async fn create_shard(
        &self,
        database_id: &str,
        table_id: &str,
        partition_id: &str,
        node_id: &NodeId,
        is_replica: bool,
        replica_group: Option<String>,
    ) -> Result<String> {
        // Check if the partition exists
        let (database, table, partition) = self.get_partition(partition_id).await?;
        
        // Generate a shard ID
        let shard_id = format!("shard-{}", uuid::Uuid::new_v4());
        
        // Create a new shard
        let shard = ShardInfo {
            shard_id: shard_id.clone(),
            partition_id: partition_id.to_string(),
            node_id: node_id.clone(),
            size_bytes: 0,
            row_count: 0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            is_replica,
            replica_group,
            status: ShardStatus::Ready,
        };
        
        // Add the shard to the partition
        {
            let mut databases = self.databases.write().await;
            let database = databases.get_mut(&database_id).ok_or_else(|| {
                MicoError::Internal(format!("Database '{}' not found", database_id))
            })?;
            
            let table = database.tables.get_mut(&table_id).ok_or_else(|| {
                MicoError::Internal(format!("Table '{}' not found in database '{}'", table_id, database.name))
            })?;
            
            let partition = table.partitions.get_mut(&partition_id).ok_or_else(|| {
                MicoError::Internal(format!("Partition '{}' not found in table '{}'", partition_id, table.name))
            })?;
            
            partition.shards.insert(shard_id.clone(), shard);
            partition.updated_at = chrono::Utc::now();
            table.updated_at = chrono::Utc::now();
            database.updated_at = chrono::Utc::now();
            
            // Update the shard lookup
            let mut shards = self.shards.write().await;
            shards.insert(shard_id.clone(), (database_id.to_string(), table_id.to_string(), partition_id.to_string()));
            
            // Update the partition map
            let mut partition_map = self.partition_map.write().await;
            let nodes = partition_map.map.entry(partition_id.to_string()).or_insert_with(Vec::new);
            if !nodes.contains(node_id) {
                nodes.push(node_id.clone());
            }
            partition_map.version += 1;
            partition_map.updated_at = chrono::Utc::now();
        }
        
        // Save the metadata
        self.save().await?;
        
        Ok(shard_id)
    }
    
    /// Get a shard by ID
    pub async fn get_shard(&self, shard_id: &str) -> Result<(DatabaseInfo, TableInfo, PartitionInfo, ShardInfo)> {
        // Get the database, table, and partition IDs
        let shards = self.shards.read().await;
        let (database_id, table_id, partition_id) = shards.get(shard_id).ok_or_else(|| {
            MicoError::Internal(format!("Shard '{}' not found", shard_id))
        })?;
        
        // Get the database, table, and partition
        let databases = self.databases.read().await;
        let database = databases.get(database_id).ok_or_else(|| {
            MicoError::Internal(format!("Database '{}' not found", database_id))
        })?;
        
        let table = database.tables.get(table_id).ok_or_else(|| {
            MicoError::Internal(format!("Table '{}' not found in database '{}'", table_id, database.name))
        })?;
        
        let partition = table.partitions.get(partition_id).ok_or_else(|| {
            MicoError::Internal(format!("Partition '{}' not found in table '{}'", partition_id, table.name))
        })?;
        
        let shard = partition.shards.get(shard_id).ok_or_else(|| {
            MicoError::Internal(format!("Shard '{}' not found in partition '{}'", shard_id, partition_id))
        })?;
        
        Ok((database.clone(), table.clone(), partition.clone(), shard.clone()))
    }
    
    /// Get the partition map
    pub async fn get_partition_map(&self) -> PartitionMap {
        self.partition_map.read().await.clone()
    }
    
    /// Get the nodes hosting a partition
    pub async fn get_nodes_for_partition(&self, partition_id: &str) -> Result<Vec<NodeId>> {
        let partition_map = self.partition_map.read().await;
        partition_map.map.get(partition_id).cloned().ok_or_else(|| {
            MicoError::Internal(format!("Partition '{}' not found in partition map", partition_id))
        })
    }
    
    /// Get all partitions that could be involved in a query
    pub async fn get_partitions_for_query(&self, database_name: &str, query: &str) -> Result<Vec<String>> {
        // In a real implementation, this would parse the query and determine which partitions are involved
        // For now, we'll just return all partitions for the database
        
        let database = self.get_database_by_name(database_name).await?;
        
        let mut partitions = Vec::new();
        for table in database.tables.values() {
            for partition in table.partitions.keys() {
                partitions.push(partition.clone());
            }
        }
        
        Ok(partitions)
    }
}