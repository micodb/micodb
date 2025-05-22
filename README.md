# MicoDB

<p align="center">
  <img src="https://via.placeholder.com/200x200?text=MicoDB" alt="MicoDB Logo" width="200"/>
</p>

<p align="center">
  <strong>A high-performance columnar database for real-time analytics</strong>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#installation">Installation</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#use-cases">Use Cases</a> •
  <a href="#roadmap">Roadmap</a> •
  <a href="#contributing">Contributing</a> •
  <a href="#license">License</a>
</p>

## What is MicoDB?

MicoDB is a modern, columnar database system designed to bridge the gap between real-time data processing and analytical workloads. Built from the ground up in Rust with Apache Arrow and Parquet, MicoDB delivers exceptional performance, reliability, and a developer-friendly experience.

Unlike traditional databases that force you to choose between real-time capabilities and analytical performance, MicoDB excels at both, making it the ideal solution for companies that need to analyze large volumes of data while simultaneously processing new events.

## Features

- **Columnar Storage**: Optimized for analytical queries with Parquet-based storage
- **Real-time Analytics**: Efficiently process both historical and real-time data
- **ACID Transactions**: Full transaction support with optimistic concurrency control
- **SQL Interface**: Familiar SQL syntax for querying and manipulating data
- **High Performance**: Written in Rust for maximum efficiency and safety
- **Arrow-based**: Ultra-fast in-memory processing using Apache Arrow
- **Distributed Architecture**: Horizontal scaling with coordinator and worker nodes
- **Multiple Data Types**: Support for Int32, String, Boolean, Float64, Date32, and TimestampNanosecond
- **Authentication & Authorization**: Robust security with JWT tokens, API keys, and role-based access control
- **Streaming Responses**: NDJSON streaming for efficient handling of large result sets
- **Partitioning & Sharding**: Automatic data distribution and locality optimization

## Installation

### One-Line Installation

```bash
curl -sSL https://raw.githubusercontent.com/micodb/micodb/main/scripts/install.sh | sh
```

This will automatically detect your OS and architecture, download the appropriate binary, and install it to `~/.micodb/bin`.

### Prerequisites for Manual Installation

- Rust (1.65+)
- Cargo (latest)

### From Source

1. Clone the repository:
   ```bash
   git clone https://github.com/micodb/micodb.git
   cd micodb
   ```

2. Build the project:
   ```bash
   cargo build --release
   ```

3. Run MicoDB:
   ```bash
   ./target/release/micodb
   ```

### From Release Binaries

1. Download the latest release for your platform from the [Releases page](https://github.com/micodb/micodb/releases)
2. Extract the archive: `tar -xzf micodb-vX.Y.Z-os-arch.tar.gz`
3. Add the binary to your PATH: `export PATH=$PATH:/path/to/micodb/bin`

### Configuration Options

MicoDB can be configured using command-line options:

```bash
# Start with a custom data directory
micodb --data-dir /path/to/data

# Specify a different port
micodb --port 8080

# Set log level
micodb --log-level debug

# Enable JWT authentication
micodb --jwt-secret "your-secret-key"

# Start in cluster mode as a coordinator
micodb --cluster --node-type coordinator --node-config /path/to/node_config.json

# Start in cluster mode as a worker
micodb --cluster --node-type worker --node-config /path/to/worker_config.json
```

## Quick Start

### Connecting to MicoDB

MicoDB provides multiple ways to connect:

#### Using the MicoDB Client Library

```rust
use micodb::client::{Client, Result};

async fn main() -> Result<()> {
    // Connect to MicoDB
    let client = Client::new("localhost:3456")?;
    
    // Create a database
    client.create_database("mydb").await?;
    
    // Execute a SQL query
    let results = client.execute_sql("mydb", "SELECT * FROM mytable").await?;
    
    Ok(())
}
```

#### Using the JSON Protocol Directly

MicoDB uses a simple JSON-based protocol over TCP. You can connect to it using any TCP client, such as netcat:

```bash
nc localhost 3456
```

#### Authenticating with MicoDB

```rust
// Connect and authenticate
let mut client = Client::new("localhost:3456")?;

// Using password authentication
let auth = AuthCredentials {
    auth_method: Some("password".to_string()),
    username: Some("admin".to_string()),
    password: Some("admin123".to_string()),
    ..Default::default()
};
client.login(auth, "mydb").await?;

// Using JWT token
let auth = AuthCredentials {
    auth_method: Some("jwt".to_string()),
    token: Some("your-jwt-token".to_string()),
    ..Default::default()
};
client.login(auth, "mydb").await?;
```

### Basic Operations

Here are some example commands to get started:

#### Using Streaming for Large Results

```rust
// Stream results to handle large datasets efficiently
let mut stream = client.execute_sql_stream("mydb", "SELECT * FROM large_table", Some(1000)).await?;

// Process the stream row by row
while let Some(row) = stream.next_row().await? {
    println!("Row: {}", row);
}
```

#### JSON Protocol Examples

#### Create a Database
```json
{"command":"create_database","db_name":"my_database"}
```

#### Create a Table
```json
{
  "command":"create_table",
  "db_name":"my_database",
  "table_name":"users",
  "schema":[
    {"name":"id","type":"Int32","nullable":false},
    {"name":"name","type":"Utf8","nullable":false},
    {"name":"active","type":"Boolean","nullable":true}
  ]
}
```

#### Insert Data
```json
{
  "command":"append",
  "db_name":"my_database",
  "table_name":"users",
  "rows":[
    {"id":1,"name":"Alice","active":true},
    {"id":2,"name":"Bob","active":false},
    {"id":3,"name":"Carol","active":true}
  ]
}
```

#### Query Data with SQL
```json
{
  "command":"sql",
  "db_name":"my_database",
  "query":"SELECT * FROM users WHERE active = true"
}
```

#### Using Transactions

Begin a transaction:
```json
{"command":"begin_transaction","db_name":"my_database"}
```

The response will contain a transaction ID:
```json
{"status":"ok","data":{"tx_id":1}}
```

Create a table within a transaction:
```json
{
  "command":"create_table",
  "db_name":"my_database",
  "table_name":"orders",
  "schema":[
    {"name":"order_id","type":"Int32","nullable":false},
    {"name":"user_id","type":"Int32","nullable":false},
    {"name":"total","type":"Float64","nullable":false}
  ],
  "tx_id":1
}
```

Append data within the transaction:
```json
{
  "command":"append",
  "db_name":"my_database",
  "table_name":"orders",
  "rows":[
    {"order_id":101,"user_id":1,"total":99.99},
    {"order_id":102,"user_id":2,"total":149.95}
  ],
  "tx_id":1
}
```

Commit the transaction:
```json
{"command":"commit_transaction","db_name":"my_database","tx_id":1}
```

To rollback a transaction:
```json
{"command":"rollback_transaction","db_name":"my_database","tx_id":1}
```

## Architecture

MicoDB is built on a modular architecture with several core components:

### Storage Engine

- Parquet-based columnar file storage
- Write-ahead logging (WAL) for durability
- Optimized for high-throughput append operations
- Partition-based data organization

### Query Engine

- SQL parser and validator
- Query planner and optimizer
- Vectorized execution engine based on Apache Arrow
- Distributed query execution (in cluster mode)

### Transaction Manager

- ACID transactional guarantees
- Multi-Version Concurrency Control (MVCC)
- Optimistic locking for concurrent operations
- Distributed transaction coordination

### Server Component

- JSON over TCP protocol
- Connection management
- Command processing
- NDJSON streaming responses

### Cluster Manager

- Coordinator and worker node architecture
- Metadata catalog for distributed data
- Automatic sharding and data placement
- Service discovery for node management

### Authentication Service

- Multiple authentication methods (password, JWT, API key)
- Role-based access control (RBAC)
- Fine-grained permissions (database, table level)
- Token-based session management

## Use Cases

### Real-time Analytics

MicoDB excels at analyzing data as it arrives, making it perfect for:
- Monitoring dashboards
- User behavior analysis
- Business intelligence applications
- Real-time reporting systems

### IoT Data Processing

The columnar storage format and efficient compression make MicoDB ideal for:
- Sensor data collection and analysis
- Smart home device data
- Industrial IoT applications
- Predictive maintenance systems

### Log Analytics

MicoDB's high-speed ingestion and efficient query capabilities are perfect for:
- Application log analysis
- Security event monitoring
- System performance tracking
- Error pattern detection

### Financial Data Analysis

ACID transactions and analytical capabilities make MicoDB suitable for:
- Trading data analysis
- Risk assessment computations
- Financial reporting
- Transaction monitoring

## Comparison to Other Databases

| Feature | MicoDB | ClickHouse | BigQuery | Traditional OLTP DB |
|---------|--------|------------|----------|---------------------|
| Storage Format | Columnar (Parquet) | Columnar | Columnar | Row-based |
| ACID Transactions | ✅ | Limited | Limited | ✅ |
| Query Speed (OLAP) | Fast | Very Fast | Fast | Slow |
| Real-time Ingestion | Fast | Moderate | Slow | Fast |
| Horizontal Scaling | ✅ | ✅ | ✅ | Varies |
| Streaming Results | ✅ (NDJSON) | Limited | Limited | Limited |
| Authentication | ✅ (Multiple) | Basic | ✅ | ✅ |
| Open Source | ✅ (MIT) | ✅ (Apache) | ❌ | Varies |
| Implementation | Rust | C++ | Proprietary | Varies |
| In-memory Format | Arrow | Custom | Proprietary | Varies |

## Roadmap

- **Q3 2025**
  - Implement Arrow Flight protocol for binary streaming
  - Add secondary indexes for faster lookups
  - Improve SQL compatibility with JOINs and complex aggregations

- **Q4 2025**
  - Add high-availability features with automatic failover
  - Implement continuous data ingestion from Kafka and other sources
  - Create client libraries for Python, JavaScript, and Java

- **Q1 2026**
  - Add advanced query optimization for distributed workloads
  - Implement automatic data lifecycle management
  - Add machine learning integrations for in-database analytics

## Contributing

We welcome contributions to MicoDB! To contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature-branch`)
3. Make your changes
4. Add tests for your changes
5. Run the tests (`cargo test`)
6. Commit your changes (`git commit -am 'Add new feature'`)
7. Push to the branch (`git push origin feature-branch`)
8. Create a new Pull Request

Please see our [contributing guidelines](CONTRIBUTING.md) for more details.

## License

MicoDB is licensed under the [GNU Affero General Public License v3.0](LICENSE).

## Contact

- Website: [micodb.com](https://micodb.com)
- GitHub: [github.com/micodb/micodb](https://github.com/micodb/micodb)
- Email: [info@micodb.com](mailto:info@micodb.com)