mod common;
mod storage;
mod query;
mod transaction;
mod client;
mod server;
mod auth;
mod cluster;

use anyhow::Result;
use std::sync::Arc;
use clap::Parser;
use std::path::PathBuf;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use storage::StorageManager;
use query::QueryEngine;
use transaction::TransactionManager;
use server::Server;
use auth::init_auth_service;
use cluster::{ClusterManager, Node, NodeType, NodeConfig};

/// Command-line arguments for MicoDB
#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    /// The directory where database files will be stored
    #[clap(short, long, default_value = "./data")]
    data_dir: String,

    /// Port to listen on for client connections
    #[clap(short, long, default_value = "3456")]
    port: u16,

    /// Log level (trace, debug, info, warn, error)
    #[clap(long, default_value = "info")]
    log_level: String,
    
    /// JWT secret for authentication (enables JWT auth if provided)
    #[clap(long)]
    jwt_secret: Option<String>,
    
    /// Enable cluster mode
    #[clap(long)]
    cluster: bool,
    
    /// Node configuration file for cluster mode
    #[clap(long)]
    node_config: Option<String>,
    
    /// Node type (coordinator or worker)
    #[clap(long, default_value = "standalone")]
    node_type: String,
    
    /// Node ID (generated automatically if not provided)
    #[clap(long)]
    node_id: Option<String>,
    
    /// Coordinator address for worker nodes
    #[clap(long)]
    coordinator: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize logging
    let log_level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    // Set up the logging subscriber
    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    // Log startup information
    info!("Starting MicoDB v{}", env!("CARGO_PKG_VERSION"));
    info!("Data directory: {}", args.data_dir);
    info!("Listening on port: {}", args.port);

    // Ensure the data directory exists
    tokio::fs::create_dir_all(&args.data_dir).await?;
    info!("Data directory created or already exists");

    // Initialize core components
    let storage_manager = Arc::new(StorageManager::new(PathBuf::from(&args.data_dir)));
    let query_engine = Arc::new(QueryEngine::new(Arc::clone(&storage_manager)));
    
    // Initialize the transaction manager with WAL support
    let mut tx_manager = TransactionManager::new();
    tx_manager.setup(PathBuf::from(&args.data_dir)).await?;
    let transaction_manager = Arc::new(tx_manager);
    
    // Initialize the authentication service
    info!("Initializing authentication service...");
    init_auth_service(&args.data_dir, args.jwt_secret.clone()).await?;
    if args.jwt_secret.is_some() {
        info!("JWT authentication enabled");
    } else {
        info!("JWT authentication disabled (use --jwt-secret to enable)");
    }

    info!("Core components initialized successfully!");

    // Initialize and run the server (with or without clustering)
    if args.cluster {
        // Get the node type
        let node_type = match args.node_type.to_lowercase().as_str() {
            "coordinator" => NodeType::Coordinator,
            "worker" => NodeType::Worker,
            "standalone" => {
                info!("Standalone node specified with --cluster flag, defaulting to coordinator");
                NodeType::Coordinator
            },
            _ => {
                return Err(anyhow::anyhow!("Invalid node type: {}", args.node_type));
            }
        };
        
        // Get the node ID
        let node_id = args.node_id.unwrap_or_else(|| {
            format!("node-{}", uuid::Uuid::new_v4())
        });
        info!("Starting node with ID: {}", node_id);
        
        // Load the node configuration
        let config_path = args.node_config.as_deref().unwrap_or("node_config.json");
        info!("Using node configuration from: {}", config_path);
        
        // Initialize the cluster manager
        let cluster_manager = ClusterManager::new(
            node_id,
            node_type,
            Arc::clone(&storage_manager),
            Arc::clone(&query_engine),
            Arc::clone(&transaction_manager),
            config_path,
        ).await?;
        
        // Start the cluster manager
        info!("Starting cluster manager...");
        cluster_manager.start().await?;
        
        // Initialize and run the server
        let server = Server::new(
            Arc::clone(&storage_manager),
            Arc::clone(&query_engine),
            Arc::clone(&transaction_manager),
            args.port,
        );
        
        info!("Starting MicoDB server in cluster mode...");
        let server_result = server.run().await;
        
        // Stop the cluster manager
        info!("Stopping cluster manager...");
        cluster_manager.stop().await?;
        
        server_result.map_err(Into::into)
    } else {
        // Initialize and run the server in standalone mode
        let server = Server::new(
            Arc::clone(&storage_manager),
            Arc::clone(&query_engine),
            Arc::clone(&transaction_manager),
            args.port,
        );
        
        info!("Starting MicoDB server in standalone mode...");
        server.run().await.map_err(Into::into)
    }
}
