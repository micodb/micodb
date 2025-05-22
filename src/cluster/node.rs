// src/cluster/node.rs

//! Node management for MicoDB clustering.
//!
//! This module defines the node types and configuration for the MicoDB cluster.

use std::path::Path;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use serde::{Serialize, Deserialize};
use crate::common::error::{MicoError, Result};

/// Unique identifier for a node in the cluster
pub type NodeId = String;

/// Configuration for a node's network communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// The IP address to bind to
    pub bind_addr: IpAddr,
    /// The port to bind to for client connections
    pub client_port: u16,
    /// The port to bind to for internal cluster communication
    pub cluster_port: u16,
    /// The external hostname for this node (useful in container environments)
    pub external_hostname: Option<String>,
    /// The maximum number of concurrent client connections
    pub max_connections: usize,
    /// The maximum number of concurrent internal connections
    pub max_internal_connections: usize,
}

/// Discovery mechanism configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "config")]
pub enum DiscoveryConfig {
    /// Static list of nodes
    Static {
        /// List of node addresses (host:port)
        nodes: Vec<String>,
    },
    /// ZooKeeper-based discovery
    ZooKeeper {
        /// ZooKeeper connection string
        connection_string: String,
        /// ZooKeeper path for node registration
        path: String,
    },
    /// Kubernetes-based discovery
    Kubernetes {
        /// Kubernetes namespace
        namespace: String,
        /// Kubernetes service name
        service_name: String,
        /// Kubernetes label selector
        label_selector: String,
    },
    /// Consul-based discovery
    Consul {
        /// Consul address
        address: String,
        /// Consul service name
        service_name: String,
    },
}

/// Partition strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "config")]
pub enum PartitionStrategyConfig {
    /// Hash-based partitioning
    Hash {
        /// Number of partitions
        partitions: usize,
        /// Number of replicas
        replicas: usize,
    },
    /// Range-based partitioning
    Range {
        /// Range boundaries
        boundaries: Vec<String>,
        /// Number of replicas
        replicas: usize,
    },
    /// Dynamic partitioning
    Dynamic {
        /// Initial number of partitions
        initial_partitions: usize,
        /// Minimum partition size
        min_partition_size: u64,
        /// Maximum partition size
        max_partition_size: u64,
        /// Number of replicas
        replicas: usize,
    },
}

/// Configuration for a MicoDB node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// The node type (Coordinator or Worker)
    pub node_type: NodeType,
    /// The node's network configuration
    pub network: NetworkConfig,
    /// The discovery mechanism configuration
    pub discovery: DiscoveryConfig,
    /// The partition strategy configuration
    pub partition_strategy: PartitionStrategyConfig,
    /// The path to the metadata directory
    pub metadata_path: String,
    /// The path to the data directory
    pub data_path: String,
    /// The maximum memory to use (in bytes)
    pub max_memory: u64,
    /// The maximum disk space to use (in bytes)
    pub max_disk_space: u64,
    /// The maximum number of CPUs to use
    pub max_cpus: usize,
}

impl NodeConfig {
    /// Load a node configuration from a file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)
            .map_err(|e| MicoError::Internal(format!("Failed to read node config file: {}", e)))?;
        
        let config: NodeConfig = serde_json::from_str(&content)
            .map_err(|e| MicoError::Internal(format!("Failed to parse node config file: {}", e)))?;
        
        Ok(config)
    }
    
    /// Save a node configuration to a file
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| MicoError::Internal(format!("Failed to serialize node config: {}", e)))?;
        
        fs::write(path, content)
            .map_err(|e| MicoError::Internal(format!("Failed to write node config file: {}", e)))?;
        
        Ok(())
    }
    
    /// Get the client socket address
    pub fn client_addr(&self) -> SocketAddr {
        SocketAddr::new(self.network.bind_addr, self.network.client_port)
    }
    
    /// Get the cluster socket address
    pub fn cluster_addr(&self) -> SocketAddr {
        SocketAddr::new(self.network.bind_addr, self.network.cluster_port)
    }
}

/// The type of node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeType {
    /// Coordinator node (manages metadata and orchestrates queries)
    Coordinator,
    /// Worker node (stores data and processes queries)
    Worker,
}

/// The status of a node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is starting up
    Starting,
    /// Node is running
    Running,
    /// Node is stopping
    Stopping,
    /// Node is stopped
    Stopped,
    /// Node is in an error state
    Error,
}

/// Information about a node in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique identifier for this node
    pub id: NodeId,
    /// The type of this node
    pub node_type: NodeType,
    /// The status of this node
    pub status: NodeStatus,
    /// The node's client address
    pub client_addr: SocketAddr,
    /// The node's cluster address
    pub cluster_addr: SocketAddr,
    /// The node's capabilities
    pub capabilities: Vec<String>,
    /// The node's start time
    pub start_time: chrono::DateTime<chrono::Utc>,
    /// The node's last heartbeat time
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
}

/// A node in the MicoDB cluster
pub struct Node {
    /// The node's unique identifier
    id: NodeId,
    /// The node's type (Coordinator or Worker)
    node_type: NodeType,
    /// The node's status
    status: NodeStatus,
    /// The node's configuration
    config: NodeConfig,
}

impl Node {
    /// Create a new node with the given configuration
    pub fn new(id: NodeId, config: NodeConfig) -> Self {
        Self {
            id,
            node_type: config.node_type,
            status: NodeStatus::Stopped,
            config,
        }
    }
    
    /// Get the node's unique identifier
    pub fn id(&self) -> &NodeId {
        &self.id
    }
    
    /// Get the node's type
    pub fn node_type(&self) -> NodeType {
        self.node_type
    }
    
    /// Get the node's status
    pub fn status(&self) -> NodeStatus {
        self.status
    }
    
    /// Set the node's status
    pub fn set_status(&mut self, status: NodeStatus) {
        self.status = status;
    }
    
    /// Get the node's configuration
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }
    
    /// Get the node's information
    pub fn info(&self) -> NodeInfo {
        NodeInfo {
            id: self.id.clone(),
            node_type: self.node_type,
            status: self.status,
            client_addr: self.config.client_addr(),
            cluster_addr: self.config.cluster_addr(),
            capabilities: vec!["query".to_string(), "storage".to_string()],
            start_time: chrono::Utc::now(),
            last_heartbeat: chrono::Utc::now(),
        }
    }
}