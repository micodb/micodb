mod common;
mod storage;
mod query;
mod transaction;
mod client;
mod server;

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

    info!("Core components initialized successfully!");

    // Initialize and run the server
    let server = Server::new(
        Arc::clone(&storage_manager),
        Arc::clone(&query_engine),
        Arc::clone(&transaction_manager),
        args.port,
    );

    info!("Starting MicoDB server...");
    server.run().await.map_err(Into::into)
}
