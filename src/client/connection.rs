// src/client/connection.rs

//! Connection management for the MicoDB client.

use crate::common::error::Result as MicoResult;
use crate::client::error::{ClientError, Result};
use crate::client::types::{QueryResult, Value, Row, Table};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use serde::{Serialize, Deserialize};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time::timeout;

/// Options for configuring a connection to a MicoDB server.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Operation timeout.
    pub operation_timeout: Duration,
    /// Whether to automatically reconnect on connection loss.
    pub auto_reconnect: bool,
    /// Maximum number of reconnection attempts.
    pub max_reconnect_attempts: u32,
    /// Reconnection backoff strategy.
    pub reconnect_backoff: ReconnectBackoff,
    /// TCP keep-alive duration.
    pub tcp_keepalive: Option<Duration>,
    /// TCP no delay (Nagle's algorithm).
    pub tcp_nodelay: bool,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(30),
            operation_timeout: Duration::from_secs(60),
            auto_reconnect: true,
            max_reconnect_attempts: 3,
            reconnect_backoff: ReconnectBackoff::Exponential {
                initial: Duration::from_millis(100),
                max: Duration::from_secs(5),
                multiplier: 2.0,
            },
            tcp_keepalive: Some(Duration::from_secs(60)),
            tcp_nodelay: true,
        }
    }
}

/// Reconnection backoff strategy.
#[derive(Debug, Clone)]
pub enum ReconnectBackoff {
    /// Fixed delay between reconnection attempts.
    Fixed(Duration),
    /// Exponentially increasing delay with a maximum value.
    Exponential {
        /// Initial backoff duration.
        initial: Duration,
        /// Maximum backoff duration.
        max: Duration,
        /// Multiplier for each step.
        multiplier: f64,
    },
}

impl ReconnectBackoff {
    /// Calculate the backoff duration for a given attempt.
    fn duration_for_attempt(&self, attempt: u32) -> Duration {
        match self {
            Self::Fixed(duration) => *duration,
            Self::Exponential {
                initial,
                max,
                multiplier,
            } => {
                let millis = initial.as_millis() as f64 * multiplier.powf(attempt as f64);
                let millis = millis.min(max.as_millis() as f64) as u64;
                Duration::from_millis(millis)
            }
        }
    }
}

/// Request messages sent to the server.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command")]
enum Request {
    #[serde(rename = "create_database")]
    CreateDatabase {
        db_name: String,
        // Authentication
        #[serde(skip_serializing_if = "Option::is_none")]
        auth_method: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
    },
    #[serde(rename = "create_table")]
    CreateTable {
        db_name: String,
        table_name: String,
        schema: Vec<FieldDefJson>,
        // Authentication
        #[serde(skip_serializing_if = "Option::is_none")]
        auth_method: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
    },
    #[serde(rename = "append")]
    Append {
        db_name: String,
        table_name: String,
        rows: Vec<serde_json::Value>,
        tx_id: Option<u64>,
        // Authentication
        #[serde(skip_serializing_if = "Option::is_none")]
        auth_method: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
    },
    #[serde(rename = "read")]
    Read {
        db_name: String,
        table_name: String,
        #[serde(default)]
        format: ResponseFormat,
        #[serde(skip_serializing_if = "Option::is_none")]
        batch_size: Option<usize>,
        // Authentication
        #[serde(skip_serializing_if = "Option::is_none")]
        auth_method: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
    },
    #[serde(rename = "sql")]
    ExecuteSql {
        db_name: String,
        query: String,
        #[serde(default)]
        format: ResponseFormat,
        #[serde(skip_serializing_if = "Option::is_none")]
        batch_size: Option<usize>,
        // Authentication
        #[serde(skip_serializing_if = "Option::is_none")]
        auth_method: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
    },
    #[serde(rename = "drop_table")]
    DropTable {
        db_name: String,
        table_name: String,
        tx_id: Option<u64>,
        // Authentication
        #[serde(skip_serializing_if = "Option::is_none")]
        auth_method: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
    },
    #[serde(rename = "begin_transaction")]
    BeginTransaction {
        db_name: String,
        // Authentication
        #[serde(skip_serializing_if = "Option::is_none")]
        auth_method: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
    },
    #[serde(rename = "commit_transaction")]
    CommitTransaction {
        db_name: String,
        tx_id: u64,
        // Authentication
        #[serde(skip_serializing_if = "Option::is_none")]
        auth_method: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
    },
    #[serde(rename = "rollback_transaction")]
    RollbackTransaction {
        db_name: String,
        tx_id: u64,
        // Authentication
        #[serde(skip_serializing_if = "Option::is_none")]
        auth_method: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
    },
    #[serde(rename = "login")]
    Login {
        db_name: String,
        auth_method: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
    },
}

/// Response format for requests.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ResponseFormat {
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

/// Response from the server.
#[derive(Debug, Serialize, Deserialize)]
struct Response {
    status: String,
    data: Option<serde_json::Value>,
    auth_token: Option<String>,
    token_expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Field definition for JSON schema.
#[derive(Debug, Serialize, Deserialize)]
struct FieldDefJson {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    nullable: bool,
}

/// Authentication credentials for a connection
#[derive(Debug, Clone)]
pub struct AuthCredentials {
    /// Authentication method
    pub auth_method: Option<String>,
    /// Username for password authentication
    pub username: Option<String>,
    /// Password for password authentication
    pub password: Option<String>,
    /// JWT token for token authentication
    pub token: Option<String>,
    /// API key for API key authentication
    pub api_key: Option<String>,
}

impl Default for AuthCredentials {
    fn default() -> Self {
        Self {
            auth_method: None,
            username: None,
            password: None,
            token: None,
            api_key: None,
        }
    }
}

/// Streaming response handler
pub struct StreamingResponse {
    /// Reader for the stream
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    /// Response format
    format: ResponseFormat,
    /// End of stream flag
    is_done: bool,
    /// Authentication token from initial response
    auth_token: Option<String>,
    /// Token expiration time
    token_expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl StreamingResponse {
    /// Check if the stream is done
    pub fn is_done(&self) -> bool {
        self.is_done
    }
    
    /// Get the authentication token
    pub fn auth_token(&self) -> Option<&str> {
        self.auth_token.as_deref()
    }
    
    /// Get the token expiration time
    pub fn token_expires_at(&self) -> Option<&chrono::DateTime<chrono::Utc>> {
        self.token_expires_at.as_ref()
    }
    
    /// Read the next row from the stream
    pub async fn next_row(&mut self) -> Result<Option<serde_json::Value>> {
        if self.is_done {
            return Ok(None);
        }
        
        let mut line = String::new();
        match self.reader.read_line(&mut line).await {
            Ok(0) => {
                self.is_done = true;
                return Ok(None);
            },
            Ok(_) => {
                if line.trim().is_empty() {
                    return self.next_row().await;
                }
                
                let value: serde_json::Value = serde_json::from_str(&line.trim())
                    .map_err(|e| ClientError::ParseError(format!("Failed to parse streaming response: {}", e)))?;
                
                // Check if this is the end marker
                if let serde_json::Value::Object(obj) = &value {
                    if obj.contains_key("end") && obj["end"] == serde_json::Value::Bool(true) {
                        self.is_done = true;
                        return Ok(None);
                    }
                }
                
                Ok(Some(value))
            },
            Err(e) => Err(ClientError::IoError(e)),
        }
    }
    
    /// Collect all rows into a vector
    pub async fn collect(&mut self) -> Result<Vec<serde_json::Value>> {
        let mut rows = Vec::new();
        while let Some(row) = self.next_row().await? {
            rows.push(row);
        }
        Ok(rows)
    }
}

/// A connection to a MicoDB server.
pub struct Connection {
    /// TCP stream to the server.
    stream: TcpStream,
    /// Reader for the stream.
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    /// Writer for the stream.
    writer: tokio::net::tcp::OwnedWriteHalf,
    /// Server address.
    address: String,
    /// Connection options.
    options: ConnectionOptions,
    /// Authentication credentials
    auth: AuthCredentials,
}

impl Connection {
    /// Create a new connection to a MicoDB server.
    pub async fn connect<A: ToSocketAddrs>(address: A, options: ConnectionOptions) -> Result<Self> {
        let address_str = format!("{:?}", address);
        
        // Connect with timeout
        let stream = match timeout(
            options.connect_timeout,
            TcpStream::connect(address),
        ).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return Err(ClientError::ConnectionError(format!("Failed to connect to {}: {}", address_str, e))),
            Err(_) => return Err(ClientError::TimeoutError(options.connect_timeout)),
        };
        
        // Configure TCP options
        if let Some(keepalive) = options.tcp_keepalive {
            stream.set_keepalive(Some(keepalive))?;
        }
        stream.set_nodelay(options.tcp_nodelay)?;
        
        // Split the stream
        let (read_half, write_half) = stream.into_split();
        let reader = BufReader::new(read_half);
        
        Ok(Self {
            stream,
            reader,
            writer: write_half,
            address: address_str,
            options,
            auth: AuthCredentials::default(),
        })
    }
    
    /// Set authentication credentials for the connection
    pub fn set_auth_credentials(&mut self, auth: AuthCredentials) {
        self.auth = auth;
    }
    
    /// Login to the server using authentication credentials
    pub async fn login(&mut self, auth: AuthCredentials, db_name: &str) -> Result<()> {
        let command = Request::Login {
            db_name: db_name.to_string(),
            auth_method: auth.auth_method.clone().unwrap_or_else(|| "password".to_string()),
            username: auth.username.clone(),
            password: auth.password.clone(),
            token: auth.token.clone(),
            api_key: auth.api_key.clone(),
        };
        
        let response = self.send_command(&command).await?;
        match response.status.as_str() {
            "ok" => {
                // Store authentication credentials
                self.auth = auth;
                
                // Store authentication token if provided
                if let Some(token) = response.auth_token {
                    self.auth.auth_method = Some("jwt".to_string());
                    self.auth.token = Some(token);
                }
                
                Ok(())
            },
            "error" => {
                let error_msg = match response.data {
                    Some(serde_json::Value::String(msg)) => msg,
                    _ => "Unknown authentication error".to_string(),
                };
                Err(ClientError::AuthenticationError(error_msg))
            },
            _ => Err(ClientError::ParseError(format!("Unknown response status: {}", response.status))),
        }
    }
    
    /// Send a command to the server and get the response.
    async fn send_command<S: Serialize>(&mut self, command: &S) -> Result<Response> {
        let json = serde_json::to_string(command)
            .map_err(ClientError::JsonError)?;
            
        // Send the command with timeout
        match timeout(
            self.options.operation_timeout,
            async {
                self.writer.write_all(json.as_bytes()).await?;
                self.writer.write_all(b"\n").await?;
                self.writer.flush().await?;
                
                // Read the response
                let mut line = String::new();
                let _bytes_read = self.reader.read_line(&mut line).await?;
                
                if _bytes_read == 0 {
                    return Err(ClientError::ConnectionError("Connection closed by server".to_string()));
                }
                
                // Parse the response
                let response: Response = serde_json::from_str(&line)
                    .map_err(|e| ClientError::ParseError(format!("Failed to parse response: {}", e)))?;
                    
                Ok(response)
            }
        ).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ClientError::TimeoutError(self.options.operation_timeout)),
        }
    }
    
    /// Handle a response, converting errors to ClientErrors.
    fn handle_response(response: Response) -> Result<serde_json::Value> {
        match response.status.as_str() {
            "ok" => Ok(response.data.unwrap_or(serde_json::Value::Null)),
            "error" => {
                let error_msg = match response.data {
                    Some(serde_json::Value::String(msg)) => msg,
                    _ => "Unknown server error".to_string(),
                };
                Err(ClientError::ServerError(error_msg))
            },
            _ => Err(ClientError::ParseError(format!("Unknown response status: {}", response.status))),
        }
    }
    
    /// Create a new database.
    pub async fn create_database(&mut self, name: &str) -> Result<()> {
        let command = Request::CreateDatabase {
            db_name: name.to_string(),
        };
        
        let response = self.send_command(&command).await?;
        Connection::handle_response(response)?;
        Ok(())
    }
    
    /// Convert an Arrow schema to field definitions.
    fn schema_to_field_defs(schema: &Schema) -> Vec<FieldDefJson> {
        schema.fields().iter().map(|field| {
            let data_type = match field.data_type() {
                arrow::datatypes::DataType::Int32 => "Int32",
                arrow::datatypes::DataType::Utf8 => "Utf8",
                arrow::datatypes::DataType::Boolean => "Boolean",
                arrow::datatypes::DataType::Float64 => "Float64",
                arrow::datatypes::DataType::Date32 => "Date32",
                arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => "TimestampNanosecond",
                _ => panic!("Unsupported data type: {:?}", field.data_type()),
            };
            
            FieldDefJson {
                name: field.name().clone(),
                data_type: data_type.to_string(),
                nullable: field.is_nullable(),
            }
        }).collect()
    }
    
    /// Create a new table with the given schema.
    pub async fn create_table(&mut self, db_name: &str, table_name: &str, schema: &Schema) -> Result<()> {
        let field_defs = Self::schema_to_field_defs(schema);
        
        let command = Request::CreateTable {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            schema: field_defs,
        };
        
        let response = self.send_command(&command).await?;
        Connection::handle_response(response)?;
        Ok(())
    }
    
    /// Create a new table with the given schema within a transaction.
    pub async fn create_table_tx(&mut self, db_name: &str, table_name: &str, schema: &Schema, tx_id: u64) -> Result<()> {
        let field_defs = Self::schema_to_field_defs(schema);
        
        let mut command = serde_json::to_value(Request::CreateTable {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            schema: field_defs,
        }).map_err(ClientError::JsonError)?;
        
        // Add tx_id to the command
        if let serde_json::Value::Object(ref mut map) = command {
            map.insert("tx_id".to_string(), serde_json::Value::Number(tx_id.into()));
        }
        
        let response = self.send_command(&command).await?;
        Connection::handle_response(response)?;
        Ok(())
    }
    
    /// Convert a RecordBatch to JSON rows.
    fn record_batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<serde_json::Value>> {
        let mut rows = Vec::with_capacity(batch.num_rows());
        
        for row_idx in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();
            
            for col_idx in 0..batch.num_columns() {
                let field = batch.schema().field(col_idx);
                let column = batch.column(col_idx);
                let field_name = field.name();
                
                let value = if !column.is_valid(row_idx) {
                    serde_json::Value::Null
                } else {
                    match field.data_type() {
                        arrow::datatypes::DataType::Int32 => {
                            let array = column.as_any().downcast_ref::<arrow::array::Int32Array>()
                                .ok_or_else(|| ClientError::ArrowError(
                                    arrow::error::ArrowError::ComputeError("Failed to downcast Int32Array".to_string())
                                ))?;
                            serde_json::Value::Number(array.value(row_idx).into())
                        },
                        arrow::datatypes::DataType::Utf8 => {
                            let array = column.as_any().downcast_ref::<arrow::array::StringArray>()
                                .ok_or_else(|| ClientError::ArrowError(
                                    arrow::error::ArrowError::ComputeError("Failed to downcast StringArray".to_string())
                                ))?;
                            serde_json::Value::String(array.value(row_idx).to_string())
                        },
                        arrow::datatypes::DataType::Boolean => {
                            let array = column.as_any().downcast_ref::<arrow::array::BooleanArray>()
                                .ok_or_else(|| ClientError::ArrowError(
                                    arrow::error::ArrowError::ComputeError("Failed to downcast BooleanArray".to_string())
                                ))?;
                            serde_json::Value::Bool(array.value(row_idx))
                        },
                        arrow::datatypes::DataType::Float64 => {
                            let array = column.as_any().downcast_ref::<arrow::array::Float64Array>()
                                .ok_or_else(|| ClientError::ArrowError(
                                    arrow::error::ArrowError::ComputeError("Failed to downcast Float64Array".to_string())
                                ))?;
                            let value = array.value(row_idx);
                            if value.is_finite() {
                                serde_json::Value::Number(serde_json::Number::from_f64(value)
                                    .ok_or_else(|| ClientError::ParseError(format!("Failed to convert float to JSON: {}", value)))?)
                            } else {
                                serde_json::Value::Null
                            }
                        },
                        // Handle other data types as needed
                        _ => return Err(ClientError::UnsupportedFeatureError(
                            format!("Unsupported data type for JSON conversion: {:?}", field.data_type())
                        )),
                    }
                };
                
                row.insert(field_name.clone(), value);
            }
            
            rows.push(serde_json::Value::Object(row));
        }
        
        Ok(rows)
    }
    
    /// Append data to a table.
    pub async fn append(&mut self, db_name: &str, table_name: &str, batch: &RecordBatch) -> Result<()> {
        let rows = Self::record_batch_to_json_rows(batch)?;
        
        let command = Request::Append {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            rows,
            tx_id: None,
        };
        
        let response = self.send_command(&command).await?;
        Connection::handle_response(response)?;
        Ok(())
    }
    
    /// Append data to a table within a transaction.
    pub async fn append_tx(&mut self, db_name: &str, table_name: &str, batch: &RecordBatch, tx_id: u64) -> Result<()> {
        let rows = Self::record_batch_to_json_rows(batch)?;
        
        let command = Request::Append {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            rows,
            tx_id: Some(tx_id),
        };
        
        let response = self.send_command(&command).await?;
        Connection::handle_response(response)?;
        Ok(())
    }
    
    /// Convert JSON data to RecordBatch.
    fn json_to_record_batches(json_data: serde_json::Value) -> Result<Vec<RecordBatch>> {
        // For now, we'll defer implementing this complex conversion
        // In a real implementation, this would parse the JSON and construct Arrow arrays
        unimplemented!("JSON to RecordBatch conversion is not yet implemented")
    }
    
    /// Read all data from a table.
    pub async fn read_table(&mut self, db_name: &str, table_name: &str) -> Result<Vec<RecordBatch>> {
        let command = Request::Read {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            format: ResponseFormat::Json,
            batch_size: None,
            auth_method: self.auth.auth_method.clone(),
            username: self.auth.username.clone(),
            password: self.auth.password.clone(),
            token: self.auth.token.clone(),
            api_key: self.auth.api_key.clone(),
        };
        
        let response = self.send_command(&command).await?;
        let json_data = Connection::handle_response(response)?;
        
        Self::json_to_record_batches(json_data)
    }
    
    /// Execute a SQL query.
    pub async fn execute_sql(&mut self, db_name: &str, query: &str) -> Result<Vec<RecordBatch>> {
        let command = Request::ExecuteSql {
            db_name: db_name.to_string(),
            query: query.to_string(),
            format: ResponseFormat::Json,
            batch_size: None,
            auth_method: self.auth.auth_method.clone(),
            username: self.auth.username.clone(),
            password: self.auth.password.clone(),
            token: self.auth.token.clone(),
            api_key: self.auth.api_key.clone(),
        };
        
        let response = self.send_command(&command).await?;
        let json_data = Connection::handle_response(response)?;
        
        Self::json_to_record_batches(json_data)
    }
    
    /// Read table data as a streaming response.
    pub async fn read_table_stream(
        &mut self, 
        db_name: &str, 
        table_name: &str, 
        batch_size: Option<usize>
    ) -> Result<StreamingResponse> {
        // Create a new connection for streaming
        let mut stream = TcpStream::connect(&self.address).await
            .map_err(|e| ClientError::ConnectionError(format!("Failed to connect for streaming: {}", e)))?;
            
        // Apply the TCP options
        if let Some(keepalive) = self.options.tcp_keepalive {
            stream.set_keepalive(Some(keepalive))?;
        }
        stream.set_nodelay(self.options.tcp_nodelay)?;
        
        // Create the command
        let command = Request::Read {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            format: ResponseFormat::Ndjson,
            batch_size,
            auth_method: self.auth.auth_method.clone(),
            username: self.auth.username.clone(),
            password: self.auth.password.clone(),
            token: self.auth.token.clone(),
            api_key: self.auth.api_key.clone(),
        };
        
        // Send the command
        let json = serde_json::to_string(&command)
            .map_err(ClientError::JsonError)?;
        stream.write_all(json.as_bytes()).await
            .map_err(ClientError::IoError)?;
        stream.write_all(b"\n").await
            .map_err(ClientError::IoError)?;
        stream.flush().await
            .map_err(ClientError::IoError)?;
            
        // Split the stream
        let (read_half, _write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        
        // Read the initial response with status
        let mut line = String::new();
        reader.read_line(&mut line).await
            .map_err(ClientError::IoError)?;
            
        if line.trim().is_empty() {
            return Err(ClientError::ParseError("Empty response from server".to_string()));
        }
        
        // Parse the response
        let response: Response = serde_json::from_str(&line.trim())
            .map_err(|e| ClientError::ParseError(format!("Failed to parse streaming response status: {}", e)))?;
            
        if response.status != "ok" {
            let error_msg = match response.data {
                Some(serde_json::Value::String(msg)) => msg,
                _ => "Unknown server error".to_string(),
            };
            return Err(ClientError::ServerError(error_msg));
        }
        
        // Create the streaming response handler
        Ok(StreamingResponse {
            reader,
            format: ResponseFormat::Ndjson,
            is_done: false,
            auth_token: response.auth_token,
            token_expires_at: response.token_expires_at,
        })
    }
    
    /// Execute a SQL query as a streaming response.
    pub async fn execute_sql_stream(
        &mut self,
        db_name: &str,
        query: &str,
        batch_size: Option<usize>
    ) -> Result<StreamingResponse> {
        // Create a new connection for streaming
        let mut stream = TcpStream::connect(&self.address).await
            .map_err(|e| ClientError::ConnectionError(format!("Failed to connect for streaming: {}", e)))?;
            
        // Apply the TCP options
        if let Some(keepalive) = self.options.tcp_keepalive {
            stream.set_keepalive(Some(keepalive))?;
        }
        stream.set_nodelay(self.options.tcp_nodelay)?;
        
        // Create the command
        let command = Request::ExecuteSql {
            db_name: db_name.to_string(),
            query: query.to_string(),
            format: ResponseFormat::Ndjson,
            batch_size,
            auth_method: self.auth.auth_method.clone(),
            username: self.auth.username.clone(),
            password: self.auth.password.clone(),
            token: self.auth.token.clone(),
            api_key: self.auth.api_key.clone(),
        };
        
        // Send the command
        let json = serde_json::to_string(&command)
            .map_err(ClientError::JsonError)?;
        stream.write_all(json.as_bytes()).await
            .map_err(ClientError::IoError)?;
        stream.write_all(b"\n").await
            .map_err(ClientError::IoError)?;
        stream.flush().await
            .map_err(ClientError::IoError)?;
            
        // Split the stream
        let (read_half, _write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        
        // Read the initial response with status
        let mut line = String::new();
        reader.read_line(&mut line).await
            .map_err(ClientError::IoError)?;
            
        if line.trim().is_empty() {
            return Err(ClientError::ParseError("Empty response from server".to_string()));
        }
        
        // Parse the response
        let response: Response = serde_json::from_str(&line.trim())
            .map_err(|e| ClientError::ParseError(format!("Failed to parse streaming response status: {}", e)))?;
            
        if response.status != "ok" {
            let error_msg = match response.data {
                Some(serde_json::Value::String(msg)) => msg,
                _ => "Unknown server error".to_string(),
            };
            return Err(ClientError::ServerError(error_msg));
        }
        
        // Create the streaming response handler
        Ok(StreamingResponse {
            reader,
            format: ResponseFormat::Ndjson,
            is_done: false,
            auth_token: response.auth_token,
            token_expires_at: response.token_expires_at,
        })
    }
    
    /// Drop a table.
    pub async fn drop_table(&mut self, db_name: &str, table_name: &str) -> Result<()> {
        let command = Request::DropTable {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            tx_id: None,
        };
        
        let response = self.send_command(&command).await?;
        Connection::handle_response(response)?;
        Ok(())
    }
    
    /// Drop a table within a transaction.
    pub async fn drop_table_tx(&mut self, db_name: &str, table_name: &str, tx_id: u64) -> Result<()> {
        let command = Request::DropTable {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            tx_id: Some(tx_id),
        };
        
        let response = self.send_command(&command).await?;
        Connection::handle_response(response)?;
        Ok(())
    }
    
    /// Begin a transaction.
    pub async fn begin_transaction(&mut self, db_name: &str) -> Result<u64> {
        let command = Request::BeginTransaction {
            db_name: db_name.to_string(),
        };
        
        let response = self.send_command(&command).await?;
        let json_data = Connection::handle_response(response)?;
        
        // Extract the transaction ID from the response
        match json_data {
            serde_json::Value::Object(map) => {
                if let Some(serde_json::Value::Number(tx_id)) = map.get("tx_id") {
                    tx_id.as_u64().ok_or_else(|| ClientError::ParseError("Invalid transaction ID".to_string()))
                } else {
                    Err(ClientError::ParseError("Transaction ID not found in response".to_string()))
                }
            },
            _ => Err(ClientError::ParseError("Invalid response format for begin_transaction".to_string())),
        }
    }
    
    /// Commit a transaction.
    pub async fn commit_transaction(&mut self, db_name: &str, tx_id: u64) -> Result<()> {
        let command = Request::CommitTransaction {
            db_name: db_name.to_string(),
            tx_id,
        };
        
        let response = self.send_command(&command).await?;
        Connection::handle_response(response)?;
        Ok(())
    }
    
    /// Rollback a transaction.
    pub async fn rollback_transaction(&mut self, db_name: &str, tx_id: u64) -> Result<()> {
        let command = Request::RollbackTransaction {
            db_name: db_name.to_string(),
            tx_id,
        };
        
        let response = self.send_command(&command).await?;
        Connection::handle_response(response)?;
        Ok(())
    }
}