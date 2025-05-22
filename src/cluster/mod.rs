// src/cluster/mod.rs

//! Clustering and distributed architecture for MicoDB.
//! 
//! This module provides the components needed for MicoDB to scale horizontally
//! to support petabyte-scale deployments with distributed query processing.

mod node;
mod coordinator;
mod metadata;
mod partition;
mod discovery;

pub use node::{Node, NodeType, NodeStatus, NodeConfig, NodeId};
pub use coordinator::{Coordinator, CoordinatorConfig};
pub use metadata::{MetadataCatalog, PartitionMap, ShardInfo};
pub use partition::{PartitionStrategy, PartitionKey, ShardManager};
pub use discovery::{ServiceDiscovery, NodeRegistry};

use std::sync::Arc;
use crate::common::error::Result;
use crate::storage::StorageManager;
use crate::query::QueryEngine;
use crate::transaction::TransactionManager;

/// Cluster manager that handles distributed operations
pub struct ClusterManager {
    /// Unique identifier for this node
    node_id: NodeId,
    /// The type of this node (Coordinator or Worker)
    node_type: NodeType,
    /// The coordinator instance (if this is a coordinator node)
    coordinator: Option<Coordinator>,
    /// The metadata catalog for the cluster
    metadata_catalog: Arc<MetadataCatalog>,
    /// The service discovery mechanism
    service_discovery: Arc<dyn ServiceDiscovery>,
    /// The partition strategy to use
    partition_strategy: Box<dyn PartitionStrategy>,
    /// The shard manager
    shard_manager: Arc<ShardManager>,
    /// The storage manager
    storage_manager: Arc<StorageManager>,
    /// The query engine
    query_engine: Arc<QueryEngine>,
    /// The transaction manager
    transaction_manager: Arc<TransactionManager>,
}

impl ClusterManager {
    /// Create a new cluster manager
    pub async fn new(
        node_id: NodeId,
        node_type: NodeType,
        storage_manager: Arc<StorageManager>,
        query_engine: Arc<QueryEngine>,
        transaction_manager: Arc<TransactionManager>,
        config_path: &str,
    ) -> Result<Self> {
        // Load configuration from the specified path
        let config = node::NodeConfig::from_file(config_path)?;
        
        // Initialize the service discovery mechanism
        let service_discovery = discovery::create_service_discovery(&config.discovery)?;
        
        // Initialize the metadata catalog
        let metadata_catalog = Arc::new(MetadataCatalog::new(
            &config.metadata_path,
            Arc::clone(&service_discovery),
        )?);
        
        // Initialize the partition strategy
        let partition_strategy = partition::create_partition_strategy(&config.partition_strategy)?;
        
        // Initialize the shard manager
        let shard_manager = Arc::new(ShardManager::new(
            Arc::clone(&metadata_catalog),
            partition_strategy.as_ref(),
        )?);
        
        // Initialize the coordinator if this is a coordinator node
        let coordinator = if node_type == NodeType::Coordinator {
            Some(Coordinator::new(
                node_id.clone(),
                Arc::clone(&metadata_catalog),
                Arc::clone(&service_discovery),
                Arc::clone(&shard_manager),
            )?)
        } else {
            None
        };
        
        Ok(Self {
            node_id,
            node_type,
            coordinator,
            metadata_catalog,
            service_discovery,
            partition_strategy: partition_strategy,
            shard_manager,
            storage_manager,
            query_engine,
            transaction_manager,
        })
    }
    
    /// Start the cluster manager
    pub async fn start(&self) -> Result<()> {
        // Register this node with the service discovery
        self.service_discovery.register_node(
            &self.node_id,
            self.node_type,
            NodeStatus::Starting,
        ).await?;
        
        // If this is a coordinator node, start the coordinator
        if let Some(coordinator) = &self.coordinator {
            coordinator.start().await?;
        }
        
        // Update this node's status to Running
        self.service_discovery.update_node_status(
            &self.node_id,
            NodeStatus::Running,
        ).await?;
        
        Ok(())
    }
    
    /// Stop the cluster manager
    pub async fn stop(&self) -> Result<()> {
        // Update this node's status to Stopping
        self.service_discovery.update_node_status(
            &self.node_id,
            NodeStatus::Stopping,
        ).await?;
        
        // If this is a coordinator node, stop the coordinator
        if let Some(coordinator) = &self.coordinator {
            coordinator.stop().await?;
        }
        
        // Update this node's status to Stopped
        self.service_discovery.update_node_status(
            &self.node_id,
            NodeStatus::Stopped,
        ).await?;
        
        // Unregister this node from the service discovery
        self.service_discovery.unregister_node(&self.node_id).await?;
        
        Ok(())
    }
    
    /// Execute a distributed query
    pub async fn execute_distributed_query(&self, db_name: &str, query: &str) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        match self.node_type {
            NodeType::Coordinator => {
                // If this is a coordinator node, use the coordinator to execute the query
                if let Some(coordinator) = &self.coordinator {
                    coordinator.execute_query(db_name, query).await
                } else {
                    // This should never happen
                    Err(crate::common::error::MicoError::Internal(
                        "Coordinator not initialized on coordinator node".to_string()
                    ))
                }
            }
            NodeType::Worker => {
                // If this is a worker node, forward the query to the coordinator
                Err(crate::common::error::MicoError::Internal(
                    "Worker nodes cannot execute distributed queries directly".to_string()
                ))
            }
        }
    }
    
    /// Get the metadata catalog
    pub fn metadata_catalog(&self) -> Arc<MetadataCatalog> {
        Arc::clone(&self.metadata_catalog)
    }
    
    /// Get the service discovery
    pub fn service_discovery(&self) -> Arc<dyn ServiceDiscovery> {
        Arc::clone(&self.service_discovery)
    }
    
    /// Get the shard manager
    pub fn shard_manager(&self) -> Arc<ShardManager> {
        Arc::clone(&self.shard_manager)
    }
}