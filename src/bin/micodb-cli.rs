// src/bin/micodb-cli.rs

//! Command-line interface for interacting with MicoDB.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use micodb::client::sync::SyncClient;
use micodb::client::ConnectionOptions;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::array::{Int32Array, StringArray, BooleanArray, Float64Array};
use arrow::record_batch::RecordBatch;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::sync::Arc;
use std::time::Duration;
use rustyline::Editor;
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use prettytable::{Table as PrettyTable, Row as PrettyRow, Cell, Attr, color};
use colored::Colorize;

/// The MicoDB command-line interface.
#[derive(Parser, Debug)]
#[clap(name = "micodb", about = "MicoDB CLI", version)]
struct Cli {
    /// Specify the address of the MicoDB server.
    #[clap(short, long, default_value = "localhost:3456")]
    address: String,

    /// Specify the connect timeout in seconds.
    #[clap(long, default_value = "30")]
    connect_timeout: u64,

    /// Specify the operation timeout in seconds.
    #[clap(long, default_value = "60")]
    operation_timeout: u64,

    /// Whether to run in interactive mode.
    #[clap(short, long)]
    interactive: bool,

    /// Specify the command to run.
    #[clap(subcommand)]
    command: Option<Command>,
}

/// MicoDB commands.
#[derive(Subcommand, Debug)]
enum Command {
    /// Create a new database.
    #[clap(name = "create-db")]
    CreateDatabase {
        /// Database name.
        name: String,
    },
    /// Create a new table.
    #[clap(name = "create-table")]
    CreateTable {
        /// Database name.
        db_name: String,
        /// Table name.
        table_name: String,
        /// Schema definition in JSON format.
        schema_file: String,
    },
    /// Insert data into a table.
    #[clap(name = "insert")]
    Insert {
        /// Database name.
        db_name: String,
        /// Table name.
        table_name: String,
        /// CSV file containing data to insert.
        data_file: String,
    },
    /// Execute a SQL query.
    #[clap(name = "query")]
    ExecuteQuery {
        /// Database name.
        db_name: String,
        /// SQL query.
        query: String,
    },
    /// Execute a SQL query from a file.
    #[clap(name = "query-file")]
    ExecuteQueryFile {
        /// Database name.
        db_name: String,
        /// File containing SQL query.
        query_file: String,
    },
    /// Start an interactive SQL session.
    #[clap(name = "sql")]
    SqlSession {
        /// Database name.
        db_name: String,
    },
}

/// Represents a connection to MicoDB.
struct MicoDbConnection {
    /// The MicoDB client.
    client: SyncClient,
    /// The current database.
    current_db: String,
}

impl MicoDbConnection {
    /// Create a new connection to MicoDB.
    fn connect(address: &str, connect_timeout: u64, operation_timeout: u64) -> Result<Self> {
        let mut options = ConnectionOptions::default();
        options.connect_timeout = Duration::from_secs(connect_timeout);
        options.operation_timeout = Duration::from_secs(operation_timeout);

        let client = SyncClient::new(address)?;

        Ok(Self {
            client,
            current_db: "".to_string(),
        })
    }

    /// Set the current database.
    fn use_database(&mut self, db_name: &str) {
        self.current_db = db_name.to_string();
    }

    /// Create a database.
    fn create_database(&self, name: &str) -> Result<()> {
        self.client.create_database(name)?;
        println!("Database '{}' created successfully.", name);
        Ok(())
    }

    /// Create a table.
    fn create_table(&self, db_name: &str, table_name: &str, schema_file: &str) -> Result<()> {
        // Read schema from file
        let file = File::open(schema_file)
            .context(format!("Failed to open schema file: {}", schema_file))?;
        
        let reader = BufReader::new(file);
        let schema_def: serde_json::Value = serde_json::from_reader(reader)
            .context("Failed to parse schema JSON")?;
        
        // Convert JSON schema to Arrow schema
        let fields = if let Some(fields) = schema_def.as_array() {
            fields.iter().map(|field| {
                let name = field["name"].as_str().unwrap().to_string();
                let type_str = field["type"].as_str().unwrap();
                let nullable = field["nullable"].as_bool().unwrap_or(false);
                
                let data_type = match type_str {
                    "Int32" => DataType::Int32,
                    "Utf8" | "String" => DataType::Utf8,
                    "Boolean" | "Bool" => DataType::Boolean,
                    "Float64" | "Double" => DataType::Float64,
                    "Date32" | "Date" => DataType::Date32,
                    "TimestampNanosecond" | "Timestamp" => DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Nanosecond,
                        None,
                    ),
                    _ => panic!("Unsupported data type: {}", type_str),
                };
                
                Field::new(&name, data_type, nullable)
            }).collect::<Vec<Field>>()
        } else {
            return Err(anyhow::anyhow!("Schema definition must be an array"));
        };
        
        let schema = Schema::new(fields);
        
        // Create the table
        self.client.create_table(db_name, table_name, &schema)?;
        println!("Table '{}/{}' created successfully.", db_name, table_name);
        Ok(())
    }

    /// Execute a SQL query.
    fn execute_query(&self, db_name: &str, query: &str) -> Result<()> {
        let batches = self.client.execute_sql(db_name, query)?;
        
        // Display the results
        if batches.is_empty() {
            println!("Query executed successfully. No results returned.");
            return Ok(());
        }
        
        for batch in batches {
            display_record_batch(&batch);
        }
        
        Ok(())
    }
}

/// Display a RecordBatch as a table.
fn display_record_batch(batch: &RecordBatch) {
    let schema = batch.schema();
    let mut table = PrettyTable::new();
    
    // Add header row
    let mut header_row = PrettyRow::new();
    for field in schema.fields() {
        header_row.add_cell(Cell::new(field.name())
            .with_style(Attr::Bold)
            .with_style(Attr::ForegroundColor(color::BLUE)));
    }
    table.add_row(header_row);
    
    // Add data rows
    for row_idx in 0..batch.num_rows() {
        let mut row = PrettyRow::new();
        
        for col_idx in 0..batch.num_columns() {
            let field = schema.field(col_idx);
            let column = batch.column(col_idx);
            
            let value = if !column.is_valid(row_idx) {
                "NULL".to_string()
            } else {
                match field.data_type() {
                    DataType::Int32 => {
                        let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                        array.value(row_idx).to_string()
                    },
                    DataType::Utf8 => {
                        let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                        array.value(row_idx).to_string()
                    },
                    DataType::Boolean => {
                        let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                        array.value(row_idx).to_string()
                    },
                    DataType::Float64 => {
                        let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                        array.value(row_idx).to_string()
                    },
                    _ => format!("<{:?}>", field.data_type()),
                }
            };
            
            row.add_cell(Cell::new(&value));
        }
        
        table.add_row(row);
    }
    
    table.printstd();
    println!("{} rows", batch.num_rows());
}

/// Run in interactive mode.
fn run_interactive_session(conn: &mut MicoDbConnection) -> Result<()> {
    println!("{}", "Welcome to MicoDB Interactive CLI".bright_green());
    println!("Connected to {}", conn.client.server_address());
    println!("Type 'help' for a list of commands or 'exit' to quit.\n");
    
    let mut rl = Editor::<(), FileHistory>::new()?;
    let history_file = dirs::home_dir()
        .map(|p| p.join(".micodb_history"))
        .unwrap_or_else(|| ".micodb_history".into());
    
    let _ = rl.load_history(&history_file);
    
    loop {
        let prompt = if conn.current_db.is_empty() {
            "micodb> ".bright_blue().to_string()
        } else {
            format!("micodb({}):> ", conn.current_db).bright_blue().to_string()
        };
        
        let readline = rl.readline(&prompt);
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str())?;
                
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                
                match line.to_lowercase().as_str() {
                    "exit" | "quit" => break,
                    "help" => {
                        println!("Available commands:");
                        println!("  create database <name>            - Create a new database");
                        println!("  use <database>                    - Switch to a database");
                        println!("  exit, quit                        - Exit the CLI");
                        println!("  help                              - Show this help");
                        println!("\nSQL commands are directly executed on the current database.");
                    },
                    line if line.starts_with("create database ") => {
                        let db_name = line.trim_start_matches("create database ").trim();
                        if let Err(e) = conn.create_database(db_name) {
                            eprintln!("Error: {}", e);
                        }
                    },
                    line if line.starts_with("use ") => {
                        let db_name = line.trim_start_matches("use ").trim();
                        conn.use_database(db_name);
                        println!("Switched to database '{}'", db_name);
                    },
                    _ => {
                        // Assume it's SQL
                        if conn.current_db.is_empty() {
                            eprintln!("No database selected. Use 'use <database>' first.");
                            continue;
                        }
                        
                        if let Err(e) = conn.execute_query(&conn.current_db, line) {
                            eprintln!("Error: {}", e);
                        }
                    },
                }
            },
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                println!("Exiting...");
                break;
            },
            Err(err) => {
                eprintln!("Error: {}", err);
                break;
            }
        }
    }
    
    let _ = rl.save_history(&history_file);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Create a connection to MicoDB
    let mut conn = MicoDbConnection::connect(
        &cli.address,
        cli.connect_timeout,
        cli.operation_timeout,
    )?;
    
    if cli.interactive || cli.command.is_none() {
        // Run in interactive mode
        run_interactive_session(&mut conn)?;
    } else if let Some(cmd) = cli.command {
        // Execute a single command
        match cmd {
            Command::CreateDatabase { name } => {
                conn.create_database(&name)?;
            },
            Command::CreateTable { db_name, table_name, schema_file } => {
                conn.create_table(&db_name, &table_name, &schema_file)?;
            },
            Command::ExecuteQuery { db_name, query } => {
                conn.execute_query(&db_name, &query)?;
            },
            Command::ExecuteQueryFile { db_name, query_file } => {
                let file = File::open(&query_file)
                    .context(format!("Failed to open query file: {}", query_file))?;
                let query = io::read_to_string(file)
                    .context("Failed to read query file")?;
                conn.execute_query(&db_name, &query)?;
            },
            Command::SqlSession { db_name } => {
                conn.use_database(&db_name);
                run_interactive_session(&mut conn)?;
            },
            Command::Insert { .. } => {
                println!("Insert command not implemented yet.");
            },
        }
    }
    
    Ok(())
}