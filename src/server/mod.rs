// src/server/mod.rs

//! MicoDB server implementation.

use crate::common::error::{MicoError, Result as MicoResult};
use crate::query::QueryEngine;
use crate::storage::StorageManager;
use crate::transaction::TransactionManager;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray,
    TimestampNanosecondArray, Date32Array,
    Int32Builder, StringBuilder, BooleanBuilder, Float64Builder, Date32Builder, TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

// Represents a field definition in a client's schema request
#[derive(Serialize, Deserialize, Debug, Clone)]
struct FieldDefJson {
    name: String,
    #[serde(rename = "type")]
    data_type: String, // e.g., "Int32", "Utf8", "Boolean", "Float64", "Date32", "TimestampNanosecond"
    nullable: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct ClientRequest {
    command: String,
    db_name: String,
    table_name: Option<String>,
    rows: Option<Vec<serde_json::Value>>,
    schema: Option<Vec<FieldDefJson>>,
    query: Option<String>, // For SQL command
    tx_id: Option<u64>,    // For transaction operations
}

#[derive(Serialize, Deserialize, Debug)]
struct ServerResponse {
    status: String,
    data: Option<serde_json::Value>,
}

impl ServerResponse {
    fn ok(data: Option<serde_json::Value>) -> Self {
        ServerResponse { status: "ok".to_string(), data }
    }
    fn error(message: String) -> Self {
        ServerResponse { status: "error".to_string(), data: Some(serde_json::Value::String(message)) }
    }
}

pub struct Server {
    storage_manager: Arc<StorageManager>,
    query_engine: Arc<QueryEngine>,
    transaction_manager: Arc<TransactionManager>,
    address: String,
}

fn parse_data_type(type_str: &str) -> MicoResult<DataType> {
    match type_str.to_lowercase().as_str() {
        "int32" => Ok(DataType::Int32),
        "utf8" | "string" => Ok(DataType::Utf8),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "float64" | "double" => Ok(DataType::Float64),
        "date32" | "date" => Ok(DataType::Date32),
        "timestampnanosecond" | "timestamp" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        _ => Err(MicoError::Internal(format!("Unsupported data type: {}", type_str))),
    }
}

fn schema_from_json_defs(json_defs: &[FieldDefJson]) -> MicoResult<SchemaRef> {
    let mut fields = Vec::new();
    for def in json_defs {
        let data_type = parse_data_type(&def.data_type)?;
        fields.push(Field::new(&def.name, data_type, def.nullable));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn json_rows_to_record_batch(rows_json: &[serde_json::Value], schema: SchemaRef) -> MicoResult<RecordBatch> {
    if rows_json.is_empty() {
        let columns = schema.fields().iter().map(|field| {
            arrow::array::new_empty_array(field.data_type())
        }).collect::<Vec<ArrayRef>>();
        return RecordBatch::try_new(schema, columns).map_err(MicoError::Arrow);
    }

    let mut columns_builders: HashMap<String, Box<dyn arrow::array::ArrayBuilder>> = HashMap::new();
    for field in schema.fields() {
        match field.data_type() {
            DataType::Int32 => columns_builders.insert(field.name().clone(), Box::new(Int32Builder::new())),
            DataType::Utf8 => columns_builders.insert(field.name().clone(), Box::new(StringBuilder::new())),
            DataType::Boolean => columns_builders.insert(field.name().clone(), Box::new(BooleanBuilder::new())),
            DataType::Float64 => columns_builders.insert(field.name().clone(), Box::new(Float64Builder::new())),
            DataType::Date32 => columns_builders.insert(field.name().clone(), Box::new(Date32Builder::new())),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => columns_builders.insert(field.name().clone(), Box::new(TimestampNanosecondBuilder::new())),
            dt => return Err(MicoError::Internal(format!("Unsupported data type for RecordBatch conversion: {:?}", dt))),
        };
    }

    for row_val in rows_json {
        if let Some(obj) = row_val.as_object() {
            for field in schema.fields() {
                let builder = columns_builders.get_mut(field.name()).unwrap();
                let json_field_val = obj.get(field.name());
                match field.data_type() {
                    DataType::Int32 => builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap().append_option(json_field_val.and_then(|v| v.as_i64().map(|i| i as i32))),
                    DataType::Utf8 => builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap().append_option(json_field_val.and_then(|v| v.as_str().map(String::from))),
                    DataType::Boolean => builder.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap().append_option(json_field_val.and_then(|v| v.as_bool())),
                    DataType::Float64 => builder.as_any_mut().downcast_mut::<Float64Builder>().unwrap().append_option(json_field_val.and_then(|v| v.as_f64())),
                    DataType::Date32 => builder.as_any_mut().downcast_mut::<Date32Builder>().unwrap().append_option(json_field_val.and_then(|v| v.as_i64().map(|i| i as i32))),
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => builder.as_any_mut().downcast_mut::<TimestampNanosecondBuilder>().unwrap().append_option(json_field_val.and_then(|v| v.as_i64())),
                    _ => {}
                }
            }
        } else { return Err(MicoError::Internal("Invalid row format: expected JSON object".to_string())); }
    }

    let columns = schema.fields().iter().map(|field| columns_builders.remove(field.name()).unwrap().finish()).collect::<Vec<ArrayRef>>();
    RecordBatch::try_new(schema, columns).map_err(MicoError::Arrow)
}

fn record_batches_to_json_rows(batches: Vec<RecordBatch>) -> MicoResult<serde_json::Value> {
    let mut all_rows_json: Vec<serde_json::Value> = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 { continue; }
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut row_map = serde_json::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let column = batch.column(col_idx);
                let value_json = if !column.is_valid(row_idx) {
                    serde_json::Value::Null
                } else {
                    match field.data_type() {
                        DataType::Int32 => serde_json::Value::Number(column.as_any().downcast_ref::<Int32Array>().unwrap().value(row_idx).into()),
                        DataType::Utf8 => serde_json::Value::String(column.as_any().downcast_ref::<StringArray>().unwrap().value(row_idx).to_string()),
                        DataType::Boolean => serde_json::Value::Bool(column.as_any().downcast_ref::<BooleanArray>().unwrap().value(row_idx)),
                        DataType::Float64 => serde_json::json!(column.as_any().downcast_ref::<Float64Array>().unwrap().value(row_idx)),
                        DataType::Date32 => serde_json::json!(column.as_any().downcast_ref::<Date32Array>().unwrap().value(row_idx)),
                        DataType::Timestamp(TimeUnit::Nanosecond, _) => serde_json::json!(column.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(row_idx)),
                        dt => { warn!("Unsupported data type for JSON serialization: {:?}", dt); serde_json::Value::String(format!("<unsupported type: {:?}>", dt)) }
                    }
                };
                row_map.insert(field.name().clone(), value_json);
            }
            all_rows_json.push(serde_json::Value::Object(row_map));
        }
    }
    Ok(serde_json::Value::Array(all_rows_json))
}

async fn handle_connection(
    stream: TcpStream,
    storage_manager: Arc<StorageManager>,
    query_engine: Arc<QueryEngine>,
    transaction_manager: Arc<TransactionManager>,
) -> MicoResult<()> {
    let peer_addr = stream.peer_addr().map_err(MicoError::Io)?;
    info!("Handling connection from {}", peer_addr);

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line_buffer = String::new();

    loop {
        line_buffer.clear();
        match reader.read_line(&mut line_buffer).await {
            Ok(0) => { info!("Connection closed by peer {}", peer_addr); break; }
            Ok(_) => {
                let trimmed_line = line_buffer.trim();
                if trimmed_line.is_empty() { continue; }
                debug!("Received from {}: {}", peer_addr, trimmed_line);

                let response = match serde_json::from_str::<ClientRequest>(trimmed_line) {
                    Ok(request) => match request.command.as_str() {
                        "create_database" => {
                            // Get transaction ID if provided
                            let tx_id = request.tx_id;
                            
                            // If operating within a transaction, validate the transaction exists
                            if let Some(tx_id) = tx_id {
                                if !transaction_manager.is_transaction_active(tx_id) {
                                    return ServerResponse::error(format!("Transaction {} is not active", tx_id));
                                }
                            }
                            
                            match storage_manager.create_database(&request.db_name).await {
                                Ok(_) => {
                                    // If part of a transaction, register the operation
                                    if let Some(tx_id) = tx_id {
                                        // For now, we don't track database creation in transactions
                                        // We could add a CreateDatabase operation type in the future
                                    }
                                    ServerResponse::ok(None)
                                },
                                Err(e) => ServerResponse::error(format!("Failed to create database: {}", e)),
                            }
                        },
                        "create_table" => {
                            if let Some(table_name) = request.table_name {
                                if let Some(schema_defs) = request.schema {
                                    // Get transaction ID if provided
                                    let tx_id = request.tx_id;
                                    
                                    // If operating within a transaction, validate the transaction exists
                                    if let Some(tx_id) = tx_id {
                                        if !transaction_manager.is_transaction_active(tx_id) {
                                            return ServerResponse::error(format!("Transaction {} is not active", tx_id));
                                        }
                                    }
                                    
                                    match schema_from_json_defs(&schema_defs) {
                                        Ok(schema) => match storage_manager.create_table(&request.db_name, &table_name, schema).await {
                                            Ok(_) => {
                                                // If part of a transaction, register the operation
                                                if let Some(tx_id) = tx_id {
                                                    let op = crate::transaction::TransactionOp::CreateTable {
                                                        db_name: request.db_name.clone(),
                                                        table_name: table_name.clone(),
                                                    };
                                                    
                                                    match transaction_manager.register_operation(tx_id, op).await {
                                                        Ok(_) => ServerResponse::ok(None),
                                                        Err(e) => ServerResponse::error(format!("Operation registered but failed to log in transaction: {}", e)),
                                                    }
                                                } else {
                                                    ServerResponse::ok(None)
                                                }
                                            },
                                            Err(e) => ServerResponse::error(format!("Failed to create table: {}", e)),
                                        },
                                        Err(e) => ServerResponse::error(format!("Invalid schema definition: {}", e)),
                                    }
                                } else { ServerResponse::error("Schema not provided for create_table command".to_string()) }
                            } else { ServerResponse::error("Table name not provided for create_table command".to_string()) }
                        }
                        "append" => {
                            if let Some(table_name) = request.table_name {
                                if let Some(rows_json) = request.rows {
                                    // Get transaction ID if provided
                                    let tx_id = request.tx_id;
                                    
                                    // If operating within a transaction, validate the transaction exists
                                    if let Some(tx_id) = tx_id {
                                        if !transaction_manager.is_transaction_active(tx_id) {
                                            return ServerResponse::error(format!("Transaction {} is not active", tx_id));
                                        }
                                    }
                                    
                                    match storage_manager.get_table_schema(&request.db_name, &table_name).await {
                                        Ok(schema) => match json_rows_to_record_batch(&rows_json, schema) {
                                            Ok(batch) => match storage_manager.append_batch(&request.db_name, &table_name, batch).await {
                                                Ok(_) => {
                                                    // If part of a transaction, register the operation
                                                    if let Some(tx_id) = tx_id {
                                                        // In a real implementation, we would get the actual file ID created
                                                        // For now we'll use a placeholder
                                                        let file_id = format!("file-{}.parquet", uuid::Uuid::new_v4());
                                                        
                                                        let op = crate::transaction::TransactionOp::Append {
                                                            db_name: request.db_name.clone(),
                                                            table_name: table_name.clone(),
                                                            file_id,
                                                        };
                                                        
                                                        match transaction_manager.register_operation(tx_id, op).await {
                                                            Ok(_) => ServerResponse::ok(None),
                                                            Err(e) => ServerResponse::error(format!("Operation registered but failed to log in transaction: {}", e)),
                                                        }
                                                    } else {
                                                        ServerResponse::ok(None)
                                                    }
                                                },
                                                Err(e) => ServerResponse::error(format!("Failed to append batch: {}", e)),
                                            },
                                            Err(e) => ServerResponse::error(format!("Failed to convert JSON to RecordBatch: {}", e)),
                                        },
                                        Err(e) => ServerResponse::error(format!("Failed to get table schema for append: {}", e)),
                                    }
                                } else { ServerResponse::error("No rows provided for append command".to_string()) }
                            } else { ServerResponse::error("Table name not provided for append command".to_string()) }
                        }
                            "drop_table" => {
                                if let Some(table_name) = request.table_name {
                                    // Get transaction ID if provided
                                    let tx_id = request.tx_id;
                                    
                                    // If operating within a transaction, validate the transaction exists
                                    if let Some(tx_id) = tx_id {
                                        if !transaction_manager.is_transaction_active(tx_id) {
                                            return ServerResponse::error(format!("Transaction {} is not active", tx_id));
                                        }
                                    }
                                    
                                    match storage_manager.drop_table(&request.db_name, &table_name).await {
                                        Ok(_) => {
                                            // If part of a transaction, register the operation
                                            if let Some(tx_id) = tx_id {
                                                let op = crate::transaction::TransactionOp::DropTable {
                                                    db_name: request.db_name.clone(),
                                                    table_name: table_name.clone(),
                                                };
                                                
                                                match transaction_manager.register_operation(tx_id, op).await {
                                                    Ok(_) => ServerResponse::ok(None),
                                                    Err(e) => ServerResponse::error(format!("Operation registered but failed to log in transaction: {}", e)),
                                                }
                                            } else {
                                                ServerResponse::ok(None)
                                            }
                                        },
                                        Err(e) => ServerResponse::error(format!("Failed to drop table: {}", e)),
                                    }
                                } else {
                                    ServerResponse::error("Table name not provided for drop_table command".to_string())
                                }
                            }
                            "read" => {
                                if let Some(table_name) = request.table_name {
                                    match storage_manager.read_table(&request.db_name, &table_name).await {
                                    Ok(batches) => match record_batches_to_json_rows(batches) {
                                        Ok(json_data) => ServerResponse::ok(Some(json_data)),
                                        Err(e) => ServerResponse::error(format!("Failed to serialize read data: {}", e)),
                                    },
                                    Err(e) => ServerResponse::error(format!("Failed to read table: {}", e)),
                                }
                            } else { ServerResponse::error("Table name not provided for read command".to_string()) }
                        }
                        "sql" => {
                            if let Some(query_str) = request.query {
                                match query_engine.execute_sql(&request.db_name, &query_str).await {
                                    Ok(batches) => match record_batches_to_json_rows(batches) {
                                        Ok(json_data) => ServerResponse::ok(Some(json_data)),
                                        Err(e) => ServerResponse::error(format!("Failed to serialize SQL result: {}", e)),
                                    },
                                    Err(e) => ServerResponse::error(format!("SQL execution error: {}", e)),
                                }
                            } else { ServerResponse::error("No query provided for sql command".to_string()) }
                        }
                        // Transaction commands
                        "begin_transaction" => {
                            match transaction_manager.begin_transaction().await {
                                Ok(tx_id) => ServerResponse::ok(Some(serde_json::json!({ "tx_id": tx_id }))),
                                Err(e) => ServerResponse::error(format!("Failed to begin transaction: {}", e)),
                            }
                        }
                        "commit_transaction" => {
                            if let Some(tx_id) = request.tx_id {
                                match transaction_manager.commit_transaction(tx_id).await {
                                    Ok(_) => ServerResponse::ok(None),
                                    Err(e) => ServerResponse::error(format!("Failed to commit transaction: {}", e)),
                                }
                            } else { ServerResponse::error("No transaction ID provided for commit_transaction command".to_string()) }
                        }
                        "rollback_transaction" => {
                            if let Some(tx_id) = request.tx_id {
                                match transaction_manager.rollback_transaction(tx_id).await {
                                    Ok(_) => ServerResponse::ok(None),
                                    Err(e) => ServerResponse::error(format!("Failed to rollback transaction: {}", e)),
                                }
                            } else { ServerResponse::error("No transaction ID provided for rollback_transaction command".to_string()) }
                        }
                        _ => ServerResponse::error(format!("Unknown command: {}", request.command)),
                    },
                    Err(e) => {
                        warn!("Failed to deserialize request from {}: {} (Line: '{}')", peer_addr, e, trimmed_line);
                        ServerResponse::error(format!("Invalid request format: {}", e))
                    }
                };

                match serde_json::to_string(&response) {
                    Ok(response_json) => {
                        if let Err(e) = write_half.write_all(response_json.as_bytes()).await { error!("Failed to write response to {}: {}", peer_addr, e); break; }
                        if let Err(e) = write_half.write_all(b"\n").await { error!("Failed to write newline to {}: {}", peer_addr, e); break; }
                        if let Err(e) = write_half.flush().await { error!("Failed to flush stream for {}: {}", peer_addr, e); break; }
                        debug!("Sent to {}: {}", peer_addr, response_json);
                    }
                    Err(e) => error!("Failed to serialize response: {}", e),
                }
            }
            Err(e) => { error!("Failed to read from connection {}: {}", peer_addr, e); break; }
        }
    }
    info!("Finished handling connection from {}", peer_addr);
    Ok(())
}

impl Server {
    pub fn new(
        storage_manager: Arc<StorageManager>,
        query_engine: Arc<QueryEngine>, // Added QueryEngine
        transaction_manager: Arc<TransactionManager>,
        port: u16,
    ) -> Self {
        Server { storage_manager, query_engine, transaction_manager, address: format!("127.0.0.1:{}", port) }
    }

    pub async fn run(&self) -> MicoResult<()> {
        info!("MicoDB server listening on {}", self.address);
        let listener = TcpListener::bind(&self.address).await.map_err(|e| MicoError::Network(format!("Failed to bind to address {}: {}", self.address, e)))?;
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    info!("Accepted new connection from: {}", addr);
                    let sm_clone = Arc::clone(&self.storage_manager);
                    let qe_clone = Arc::clone(&self.query_engine); // Clone QueryEngine
                    // let tm_clone = Arc::clone(&self.transaction_manager); // For later use

                    tokio::spawn(async move {
                        debug!("Spawning handler for {}", addr);
                        if let Err(e) = handle_connection(socket, sm_clone, qe_clone, Arc::clone(&self.transaction_manager)).await {
                            error!("Error handling connection from {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => error!("Failed to accept connection: {}", e),
            }
        }
    }
}
