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
- **Horizontally Scalable**: Design principles that will allow scaling across multiple nodes (in development)
- **Multiple Data Types**: Support for Int32, String, Boolean, Float64, Date32, and TimestampNanosecond

## Installation

### Prerequisites

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

### Configuration Options

MicoDB can be configured using command-line options:

```bash
# Start with a custom data directory
./target/release/micodb --data-dir /path/to/data

# Specify a different port
./target/release/micodb --port 8080

# Set log level
./target/release/micodb --log-level debug
```

## Quick Start

### Connecting to MicoDB

MicoDB uses a simple JSON-based protocol over TCP. You can connect to it using any TCP client, such as netcat:

```bash
nc localhost 3456
```

### Basic Operations

Here are some example commands to get started:

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

### Query Engine

- SQL parser and validator
- Query planner and optimizer
- Vectorized execution engine based on Apache Arrow

### Transaction Manager

- ACID transactional guarantees
- Multi-Version Concurrency Control (MVCC)
- Optimistic locking for concurrent operations

### Server Component

- JSON over TCP protocol
- Connection management
- Command processing

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
| Horizontal Scaling | In Development | ✅ | ✅ | Varies |
| Open Source | ✅ (AGPL) | ✅ (Apache) | ❌ | Varies |
| Implementation | Rust | C++ | Proprietary | Varies |
| In-memory Format | Arrow | Custom | Proprietary | Varies |

## Roadmap

- **Q2 2025**
  - Improve SQL compatibility
  - Enhance transaction performance
  - Implement delta log for real-time updates

- **Q3 2025**
  - Add distributed query execution
  - Implement horizontal scaling
  - Enhance security features (authentication, authorization)

- **Q4 2025**
  - Add high-availability features
  - Develop streaming data ingestion
  - Create client libraries for popular languages

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