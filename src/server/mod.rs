// src/server/mod.rs

//! MicoDB server implementation.

use crate::common::error::{MicoError, Result as MicoResult};
use crate::query::QueryEngine;
use crate::storage::StorageManager;
use crate::transaction::TransactionManager;
use crate::auth::{get_auth_service, AuthMethod, Permission, ResourceType};
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ResponseFormat {
    /// Standard JSON response (default)
    Json,
    /// Newline-delimited JSON for streaming
    Ndjson,
    /// Arrow Flight protocol (binary)
    ArrowFlight,
}

impl Default for ResponseFormat {
    fn default() -> Self {
        ResponseFormat::Json
    }
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
    
    // Response format options
    #[serde(default)]
    format: ResponseFormat,
    #[serde(default)]
    batch_size: Option<usize>, // For streaming responses, number of rows per batch
    
    // Authentication fields
    auth_method: Option<AuthMethod>,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
    api_key: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ServerResponse {
    status: String,
    data: Option<serde_json::Value>,
    auth_token: Option<String>,
    token_expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl ServerResponse {
    fn ok(data: Option<serde_json::Value>) -> Self {
        ServerResponse { 
            status: "ok".to_string(), 
            data,
            auth_token: None,
            token_expires_at: None,
        }
    }
    
    fn ok_with_auth(data: Option<serde_json::Value>, token: String, expires: chrono::DateTime<chrono::Utc>) -> Self {
        ServerResponse { 
            status: "ok".to_string(), 
            data,
            auth_token: Some(token),
            token_expires_at: Some(expires),
        }
    }
    
    fn error(message: String) -> Self {
        ServerResponse { 
            status: "error".to_string(), 
            data: Some(serde_json::Value::String(message)),
            auth_token: None,
            token_expires_at: None,
        }
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

/// Authentication result with user data
struct AuthContext {
    user: crate::auth::User,
    token: Option<String>,
    token_expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Handle streaming requests (NDJSON or Arrow Flight)
async fn handle_streaming_request(
    request: &ClientRequest,
    write_half: &mut tokio::net::tcp::OwnedWriteHalf,
    storage_manager: &Arc<StorageManager>,
    query_engine: &Arc<QueryEngine>,
    transaction_manager: &Arc<TransactionManager>,
) -> MicoResult<()> {
    // Authenticate the request
    let auth_result = authenticate(request).await?;
    
    match request.command.as_str() {
        "read" => {
            if let Some(table_name) = &request.table_name {
                // Check permission for Read
                let resource_name = format!("{}/{}", request.db_name, table_name);
                if let Ok(false) = check_permission(&auth_result, Permission::Read, ResourceType::Table, &resource_name).await {
                    return Err(MicoError::Internal("Permission denied: Read permission required".to_string()));
                }
                
                match storage_manager.read_table(&request.db_name, table_name).await {
                    Ok(batches) => {
                        match request.format {
                            ResponseFormat::Ndjson => {
                                stream_ndjson_response(write_half, &auth_result, batches, request.batch_size).await?;
                                Ok(())
                            },
                            ResponseFormat::ArrowFlight => {
                                // Will implement Arrow Flight streaming later
                                Err(MicoError::Internal("Arrow Flight streaming not yet implemented".to_string()))
                            },
                            _ => Err(MicoError::Internal("Invalid streaming format requested".to_string())),
                        }
                    },
                    Err(e) => Err(MicoError::Internal(format!("Failed to read table: {}", e))),
                }
            } else {
                Err(MicoError::Internal("Table name not provided for read command".to_string()))
            }
        },
        "sql" => {
            // Check permission for ExecuteQuery
            if let Ok(false) = check_permission(&auth_result, Permission::ExecuteQuery, ResourceType::Database, &request.db_name).await {
                return Err(MicoError::Internal("Permission denied: ExecuteQuery permission required".to_string()));
            }
            
            if let Some(query_str) = &request.query {
                match query_engine.execute_sql(&request.db_name, query_str).await {
                    Ok(batches) => {
                        match request.format {
                            ResponseFormat::Ndjson => {
                                stream_ndjson_response(write_half, &auth_result, batches, request.batch_size).await?;
                                Ok(())
                            },
                            ResponseFormat::ArrowFlight => {
                                // Will implement Arrow Flight streaming later
                                Err(MicoError::Internal("Arrow Flight streaming not yet implemented".to_string()))
                            },
                            _ => Err(MicoError::Internal("Invalid streaming format requested".to_string())),
                        }
                    },
                    Err(e) => Err(MicoError::Internal(format!("SQL execution error: {}", e))),
                }
            } else {
                Err(MicoError::Internal("No query provided for sql command".to_string()))
            }
        },
        _ => Err(MicoError::Internal(format!("Command '{}' does not support streaming", request.command))),
    }
}

/// Authenticate a client request
async fn authenticate(request: &ClientRequest) -> MicoResult<Option<AuthContext>> {
    // Get the auth service
    let auth_service = match get_auth_service() {
        Ok(service) => service,
        Err(e) => {
            warn!("Auth service not available: {}", e);
            return Ok(None); // Auth not configured, allow request
        }
    };
    
    // Check if this is a login command (special handling)
    if request.command == "login" {
        let method = request.auth_method.as_ref().ok_or_else(|| {
            MicoError::Internal("Auth method must be specified for login".to_string())
        })?;
        
        match method {
            AuthMethod::Password => {
                // Get username and password
                let username = request.username.as_ref().ok_or_else(|| {
                    MicoError::Internal("Username must be provided for password login".to_string())
                })?;
                let password = request.password.as_ref().ok_or_else(|| {
                    MicoError::Internal("Password must be provided for password login".to_string())
                })?;
                
                // Authenticate with password
                match auth_service.authenticate_password(username, password).await {
                    Ok(result) => Ok(Some(AuthContext {
                        user: result.user,
                        token: result.token,
                        token_expires_at: result.token_expires_at,
                    })),
                    Err(e) => Err(MicoError::Internal(format!("Authentication failed: {}", e))),
                }
            },
            AuthMethod::Jwt => {
                // Get JWT token
                let token = request.token.as_ref().ok_or_else(|| {
                    MicoError::Internal("Token must be provided for JWT login".to_string())
                })?;
                
                // Authenticate with token
                match auth_service.authenticate_token(token).await {
                    Ok(result) => Ok(Some(AuthContext {
                        user: result.user,
                        token: result.token,
                        token_expires_at: result.token_expires_at,
                    })),
                    Err(e) => Err(MicoError::Internal(format!("Authentication failed: {}", e))),
                }
            },
            AuthMethod::ApiKey => {
                // Get API key
                let key = request.api_key.as_ref().ok_or_else(|| {
                    MicoError::Internal("API key must be provided for API key login".to_string())
                })?;
                
                // Authenticate with API key
                match auth_service.authenticate_api_key(key).await {
                    Ok(result) => Ok(Some(AuthContext {
                        user: result.user,
                        token: result.token,
                        token_expires_at: result.token_expires_at,
                    })),
                    Err(e) => Err(MicoError::Internal(format!("Authentication failed: {}", e))),
                }
            }
        }
    }
    // For non-login commands, check if authentication is provided
    else if let Some(method) = &request.auth_method {
        match method {
            AuthMethod::Password => {
                // Get username and password
                let username = request.username.as_ref().ok_or_else(|| {
                    MicoError::Internal("Username must be provided for password auth".to_string())
                })?;
                let password = request.password.as_ref().ok_or_else(|| {
                    MicoError::Internal("Password must be provided for password auth".to_string())
                })?;
                
                // Authenticate with password
                match auth_service.authenticate_password(username, password).await {
                    Ok(result) => Ok(Some(AuthContext {
                        user: result.user,
                        token: result.token,
                        token_expires_at: result.token_expires_at,
                    })),
                    Err(e) => Err(MicoError::Internal(format!("Authentication failed: {}", e))),
                }
            },
            AuthMethod::Jwt => {
                // Get JWT token
                let token = request.token.as_ref().ok_or_else(|| {
                    MicoError::Internal("Token must be provided for JWT auth".to_string())
                })?;
                
                // Authenticate with token
                match auth_service.authenticate_token(token).await {
                    Ok(result) => Ok(Some(AuthContext {
                        user: result.user,
                        token: result.token,
                        token_expires_at: result.token_expires_at,
                    })),
                    Err(e) => Err(MicoError::Internal(format!("Authentication failed: {}", e))),
                }
            },
            AuthMethod::ApiKey => {
                // Get API key
                let key = request.api_key.as_ref().ok_or_else(|| {
                    MicoError::Internal("API key must be provided for API key auth".to_string())
                })?;
                
                // Authenticate with API key
                match auth_service.authenticate_api_key(key).await {
                    Ok(result) => Ok(Some(AuthContext {
                        user: result.user,
                        token: result.token,
                        token_expires_at: result.token_expires_at,
                    })),
                    Err(e) => Err(MicoError::Internal(format!("Authentication failed: {}", e))),
                }
            }
        }
    } else {
        // No authentication provided, check if authentication is required
        if let Ok(users) = auth_service.get_user("admin").await {
            // If we have users, require authentication
            Err(MicoError::Internal("Authentication required".to_string()))
        } else {
            // No users yet, allow unauthenticated access
            Ok(None)
        }
    }
}

/// Check if the user has permission for an operation
async fn check_permission(
    auth_context: &Option<AuthContext>,
    permission: Permission,
    resource_type: ResourceType,
    resource_name: &str,
) -> MicoResult<bool> {
    // No auth context means no authentication is required yet
    if auth_context.is_none() {
        return Ok(true);
    }
    
    // Get the auth service
    let auth_service = get_auth_service()?;
    
    // Get the user from the auth context
    let user = &auth_context.as_ref().unwrap().user;
    
    // Check if the user has the required permission
    auth_service.check_permission(user, permission, resource_type, resource_name).await
}

/// Handle streaming NDJSON responses for better performance with large data sets
async fn stream_ndjson_response(
    write_half: &mut tokio::net::tcp::OwnedWriteHalf,
    auth_context: &Option<AuthContext>,
    batches: Vec<RecordBatch>,
    batch_size: Option<usize>,
) -> MicoResult<()> {
    // Include auth status in the initial response
    let mut status_response = ServerResponse {
        status: "ok".to_string(),
        data: Some(serde_json::json!({ "streaming": true })),
        auth_token: None,
        token_expires_at: None,
    };
    
    // Include token in the initial response if available
    if let Some(auth_context) = auth_context {
        if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
            status_response.auth_token = Some(token.clone());
            status_response.token_expires_at = Some(expires.clone());
        }
    }
    
    // Send status response
    let status_json = serde_json::to_string(&status_response)
        .map_err(|e| MicoError::Internal(format!("Failed to serialize status response: {}", e)))?;
    write_half.write_all(status_json.as_bytes()).await.map_err(MicoError::Io)?;
    write_half.write_all(b"\n").await.map_err(MicoError::Io)?;
    write_half.flush().await.map_err(MicoError::Io)?;
    
    // Default batch size if not specified
    let rows_per_batch = batch_size.unwrap_or(1000);
    
    // Process each batch
    for batch in batches {
        if batch.num_rows() == 0 { continue; }
        
        let schema = batch.schema();
        let total_rows = batch.num_rows();
        
        // Process rows in chunks
        for chunk_start in (0..total_rows).step_by(rows_per_batch) {
            let chunk_end = std::cmp::min(chunk_start + rows_per_batch, total_rows);
            
            // Process each row in the current chunk
            for row_idx in chunk_start..chunk_end {
                let mut row_map = serde_json::Map::new();
                
                // Process each column in the row
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
                            dt => {
                                warn!("Unsupported data type for JSON serialization: {:?}", dt);
                                serde_json::Value::String(format!("<unsupported type: {:?}>", dt))
                            }
                        }
                    };
                    row_map.insert(field.name().clone(), value_json);
                }
                
                // Serialize and send row
                let row_json = serde_json::to_string(&serde_json::Value::Object(row_map))
                    .map_err(|e| MicoError::Internal(format!("Failed to serialize row: {}", e)))?;
                write_half.write_all(row_json.as_bytes()).await.map_err(MicoError::Io)?;
                write_half.write_all(b"\n").await.map_err(MicoError::Io)?;
            }
            
            // Flush after each chunk
            write_half.flush().await.map_err(MicoError::Io)?;
        }
    }
    
    // Send end of stream marker
    let end_marker = serde_json::to_string(&serde_json::json!({ "end": true }))
        .map_err(|e| MicoError::Internal(format!("Failed to serialize end marker: {}", e)))?;
    write_half.write_all(end_marker.as_bytes()).await.map_err(MicoError::Io)?;
    write_half.write_all(b"\n").await.map_err(MicoError::Io)?;
    write_half.flush().await.map_err(MicoError::Io)?;
    
    Ok(())
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

                let request_result = serde_json::from_str::<ClientRequest>(trimmed_line);
                
                if let Ok(ref request) = request_result {
                    // Check if this is a streaming request before continuing
                    if matches!(request.format, ResponseFormat::Ndjson) && 
                       (request.command == "read" || request.command == "sql") {
                        // Handle streaming response
                        match handle_streaming_request(request, &mut write_half, 
                                                      &storage_manager, &query_engine, &transaction_manager).await {
                            Ok(_) => continue, // Skip normal response handling
                            Err(e) => {
                                error!("Streaming error: {}", e);
                                let error_resp = ServerResponse::error(format!("Streaming error: {}", e));
                                if let Ok(error_json) = serde_json::to_string(&error_resp) {
                                    let _ = write_half.write_all(error_json.as_bytes()).await;
                                    let _ = write_half.write_all(b"\n").await;
                                    let _ = write_half.flush().await;
                                }
                                continue;
                            }
                        }
                    }
                }
                
                let response = match request_result {
                    Ok(request) => {
                        // Authenticate the request
                        let auth_result = match authenticate(&request).await {
                            Ok(context) => context,
                            Err(e) => return ServerResponse::error(format!("Authentication error: {}", e)),
                        };
                        
                        // Handle login command separately
                        if request.command == "login" {
                            if let Some(auth_context) = auth_result {
                                let user_info = serde_json::json!({
                                    "username": auth_context.user.username,
                                    "display_name": auth_context.user.display_name,
                                    "roles": auth_context.user.roles,
                                });
                                
                                if let (Some(token), Some(expires)) = (auth_context.token, auth_context.token_expires_at) {
                                    ServerResponse::ok_with_auth(Some(user_info), token, expires)
                                } else {
                                    ServerResponse::ok(Some(user_info))
                                }
                            } else {
                                ServerResponse::error("Authentication failed".to_string())
                            }
                        } 
                        // Handle other commands
                        else {
                            match request.command.as_str() {
                        "create_database" => {
                            // Check permission
                            if let Ok(false) = check_permission(&auth_result, Permission::CreateDatabase, ResourceType::Global, "").await {
                                return ServerResponse::error("Permission denied: CreateDatabase permission required".to_string());
                            }
                            
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
                                    
                                    // Include token in response if available
                                    if let Some(auth_context) = &auth_result {
                                        if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
                                            return ServerResponse::ok_with_auth(None, token.clone(), expires.clone());
                                        }
                                    }
                                    ServerResponse::ok(None)
                                },
                                Err(e) => ServerResponse::error(format!("Failed to create database: {}", e)),
                            }
                        },
                        "create_table" => {
                            // Check permission for CreateTable
                            if let Ok(false) = check_permission(&auth_result, Permission::CreateTable, ResourceType::Database, &request.db_name).await {
                                return ServerResponse::error("Permission denied: CreateTable permission required".to_string());
                            }
                            
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
                                                        Ok(_) => {
                                                            // Include token in response if available
                                                            if let Some(auth_context) = &auth_result {
                                                                if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
                                                                    return ServerResponse::ok_with_auth(None, token.clone(), expires.clone());
                                                                }
                                                            }
                                                            ServerResponse::ok(None)
                                                        },
                                                        Err(e) => ServerResponse::error(format!("Operation registered but failed to log in transaction: {}", e)),
                                                    }
                                                } else {
                                                    // Include token in response if available
                                                    if let Some(auth_context) = &auth_result {
                                                        if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
                                                            return ServerResponse::ok_with_auth(None, token.clone(), expires.clone());
                                                        }
                                                    }
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
                                // Check permission for Write
                                let resource_name = format!("{}/{}", request.db_name, table_name);
                                if let Ok(false) = check_permission(&auth_result, Permission::Write, ResourceType::Table, &resource_name).await {
                                    return ServerResponse::error("Permission denied: Write permission required".to_string());
                                }
                                
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
                                                            Ok(_) => {
                                                                // Include token in response if available
                                                                if let Some(auth_context) = &auth_result {
                                                                    if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
                                                                        return ServerResponse::ok_with_auth(None, token.clone(), expires.clone());
                                                                    }
                                                                }
                                                                ServerResponse::ok(None)
                                                            },
                                                            Err(e) => ServerResponse::error(format!("Operation registered but failed to log in transaction: {}", e)),
                                                        }
                                                    } else {
                                                        // Include token in response if available
                                                        if let Some(auth_context) = &auth_result {
                                                            if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
                                                                return ServerResponse::ok_with_auth(None, token.clone(), expires.clone());
                                                            }
                                                        }
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
                                    // Check permission for DropTable
                                    let resource_name = format!("{}/{}", request.db_name, table_name);
                                    if let Ok(false) = check_permission(&auth_result, Permission::DropTable, ResourceType::Table, &resource_name).await {
                                        return ServerResponse::error("Permission denied: DropTable permission required".to_string());
                                    }
                                    
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
                                                    Ok(_) => {
                                                        // Include token in response if available
                                                        if let Some(auth_context) = &auth_result {
                                                            if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
                                                                return ServerResponse::ok_with_auth(None, token.clone(), expires.clone());
                                                            }
                                                        }
                                                        ServerResponse::ok(None)
                                                    },
                                                    Err(e) => ServerResponse::error(format!("Operation registered but failed to log in transaction: {}", e)),
                                                }
                                            } else {
                                                // Include token in response if available
                                                if let Some(auth_context) = &auth_result {
                                                    if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
                                                        return ServerResponse::ok_with_auth(None, token.clone(), expires.clone());
                                                    }
                                                }
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
                                    // Check permission for Read
                                    let resource_name = format!("{}/{}", request.db_name, table_name);
                                    if let Ok(false) = check_permission(&auth_result, Permission::Read, ResourceType::Table, &resource_name).await {
                                        return ServerResponse::error("Permission denied: Read permission required".to_string());
                                    }
                                    
                                    match storage_manager.read_table(&request.db_name, &table_name).await {
                                        Ok(batches) => match record_batches_to_json_rows(batches) {
                                            Ok(json_data) => {
                                                // Include token in response if available
                                                if let Some(auth_context) = &auth_result {
                                                    if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
                                                        return ServerResponse::ok_with_auth(Some(json_data), token.clone(), expires.clone());
                                                    }
                                                }
                                                ServerResponse::ok(Some(json_data))
                                            },
                                            Err(e) => ServerResponse::error(format!("Failed to serialize read data: {}", e)),
                                        },
                                        Err(e) => ServerResponse::error(format!("Failed to read table: {}", e)),
                                    }
                                } else { ServerResponse::error("Table name not provided for read command".to_string()) }
                            }
                        "sql" => {
                            // Check permission for ExecuteQuery
                            if let Ok(false) = check_permission(&auth_result, Permission::ExecuteQuery, ResourceType::Database, &request.db_name).await {
                                return ServerResponse::error("Permission denied: ExecuteQuery permission required".to_string());
                            }
                            
                            if let Some(query_str) = request.query {
                                match query_engine.execute_sql(&request.db_name, &query_str).await {
                                    Ok(batches) => match record_batches_to_json_rows(batches) {
                                        Ok(json_data) => {
                                            // Include token in response if available
                                            if let Some(auth_context) = &auth_result {
                                                if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
                                                    return ServerResponse::ok_with_auth(Some(json_data), token.clone(), expires.clone());
                                                }
                                            }
                                            ServerResponse::ok(Some(json_data))
                                        },
                                        Err(e) => ServerResponse::error(format!("Failed to serialize SQL result: {}", e)),
                                    },
                                    Err(e) => ServerResponse::error(format!("SQL execution error: {}", e)),
                                }
                            } else { ServerResponse::error("No query provided for sql command".to_string()) }
                        }
                        // Transaction commands
                        "begin_transaction" => {
                            // Check permission for BeginTransaction
                            if let Ok(false) = check_permission(&auth_result, Permission::BeginTransaction, ResourceType::Database, &request.db_name).await {
                                return ServerResponse::error("Permission denied: BeginTransaction permission required".to_string());
                            }
                            
                            match transaction_manager.begin_transaction().await {
                                Ok(tx_id) => {
                                    let response_data = serde_json::json!({ "tx_id": tx_id });
                                    
                                    // Include token in response if available
                                    if let Some(auth_context) = &auth_result {
                                        if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
                                            return ServerResponse::ok_with_auth(Some(response_data), token.clone(), expires.clone());
                                        }
                                    }
                                    ServerResponse::ok(Some(response_data))
                                },
                                Err(e) => ServerResponse::error(format!("Failed to begin transaction: {}", e)),
                            }
                        }
                        "commit_transaction" => {
                            // Check permission for CommitTransaction
                            if let Ok(false) = check_permission(&auth_result, Permission::CommitTransaction, ResourceType::Database, &request.db_name).await {
                                return ServerResponse::error("Permission denied: CommitTransaction permission required".to_string());
                            }
                            
                            if let Some(tx_id) = request.tx_id {
                                match transaction_manager.commit_transaction(tx_id).await {
                                    Ok(_) => {
                                        // Include token in response if available
                                        if let Some(auth_context) = &auth_result {
                                            if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
                                                return ServerResponse::ok_with_auth(None, token.clone(), expires.clone());
                                            }
                                        }
                                        ServerResponse::ok(None)
                                    },
                                    Err(e) => ServerResponse::error(format!("Failed to commit transaction: {}", e)),
                                }
                            } else { ServerResponse::error("No transaction ID provided for commit_transaction command".to_string()) }
                        }
                        "rollback_transaction" => {
                            // Check permission for RollbackTransaction
                            if let Ok(false) = check_permission(&auth_result, Permission::RollbackTransaction, ResourceType::Database, &request.db_name).await {
                                return ServerResponse::error("Permission denied: RollbackTransaction permission required".to_string());
                            }
                            
                            if let Some(tx_id) = request.tx_id {
                                match transaction_manager.rollback_transaction(tx_id).await {
                                    Ok(_) => {
                                        // Include token in response if available
                                        if let Some(auth_context) = &auth_result {
                                            if let (Some(token), Some(expires)) = (&auth_context.token, &auth_context.token_expires_at) {
                                                return ServerResponse::ok_with_auth(None, token.clone(), expires.clone());
                                            }
                                        }
                                        ServerResponse::ok(None)
                                    },
                                    Err(e) => ServerResponse::error(format!("Failed to rollback transaction: {}", e)),
                                }
                            } else { ServerResponse::error("No transaction ID provided for rollback_transaction command".to_string()) }
                        }
                        _ => ServerResponse::error(format!("Unknown command: {}", request.command)),
                            }
                        }
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
