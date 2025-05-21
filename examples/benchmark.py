#!/usr/bin/env python3
"""
MicoDB Benchmark Tool - Simple performance testing for MicoDB.

This script performs benchmarks for various operations:
1. Write performance (inserts)
2. Read performance (full table scans)
3. Query performance (filtered selects)
4. Transaction performance

Usage:
  python3 benchmark.py [host] [port] [iterations]

Default:
  host: localhost
  port: 3456
  iterations: 1000
"""

import json
import socket
import sys
import time
import random
import string
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

def random_string(length: int) -> str:
    """Generate a random string of fixed length."""
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))

class MicoDBClient:
    """A simple client for MicoDB."""
    
    def __init__(self, host: str = 'localhost', port: int = 3456):
        """Initialize the client with the given host and port."""
        self.host = host
        self.port = port
        self.socket = None
        
    def connect(self) -> None:
        """Connect to the MicoDB server."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        
    def disconnect(self) -> None:
        """Disconnect from the MicoDB server."""
        if self.socket:
            self.socket.close()
            self.socket = None
            
    def send_command(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """Send a command to the MicoDB server and return the response."""
        if not self.socket:
            raise RuntimeError("Not connected to MicoDB. Call connect() first.")
            
        # Convert command to JSON and send
        command_json = json.dumps(command)
        self.socket.sendall(f"{command_json}\n".encode('utf-8'))
        
        # Read response
        response_data = self.socket.recv(4096).decode('utf-8')
        response = json.loads(response_data)
        
        return response
    
    def create_database(self, db_name: str) -> Dict[str, Any]:
        """Create a new database."""
        command = {
            "command": "create_database",
            "db_name": db_name
        }
        return self.send_command(command)
    
    def create_table(self, db_name: str, table_name: str, schema: List[Dict[str, Any]], 
                    tx_id: Optional[int] = None) -> Dict[str, Any]:
        """Create a new table with the given schema."""
        command = {
            "command": "create_table",
            "db_name": db_name,
            "table_name": table_name,
            "schema": schema
        }
        if tx_id is not None:
            command["tx_id"] = tx_id
        return self.send_command(command)
    
    def append(self, db_name: str, table_name: str, rows: List[Dict[str, Any]],
              tx_id: Optional[int] = None) -> Dict[str, Any]:
        """Append rows to a table."""
        command = {
            "command": "append",
            "db_name": db_name,
            "table_name": table_name,
            "rows": rows
        }
        if tx_id is not None:
            command["tx_id"] = tx_id
        return self.send_command(command)
    
    def read_table(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """Read all data from a table."""
        command = {
            "command": "read",
            "db_name": db_name,
            "table_name": table_name
        }
        return self.send_command(command)
    
    def execute_sql(self, db_name: str, query: str) -> Dict[str, Any]:
        """Execute a SQL query."""
        command = {
            "command": "sql",
            "db_name": db_name,
            "query": query
        }
        return self.send_command(command)
    
    def begin_transaction(self, db_name: str) -> int:
        """Begin a new transaction and return the transaction ID."""
        command = {
            "command": "begin_transaction",
            "db_name": db_name
        }
        response = self.send_command(command)
        if response["status"] == "ok" and response["data"]:
            return response["data"]["tx_id"]
        raise RuntimeError(f"Failed to begin transaction: {response}")
    
    def commit_transaction(self, db_name: str, tx_id: int) -> Dict[str, Any]:
        """Commit a transaction."""
        command = {
            "command": "commit_transaction",
            "db_name": db_name,
            "tx_id": tx_id
        }
        return self.send_command(command)
    
    def rollback_transaction(self, db_name: str, tx_id: int) -> Dict[str, Any]:
        """Rollback a transaction."""
        command = {
            "command": "rollback_transaction",
            "db_name": db_name,
            "tx_id": tx_id
        }
        return self.send_command(command)
    
    def drop_table(self, db_name: str, table_name: str, 
                  tx_id: Optional[int] = None) -> Dict[str, Any]:
        """Drop a table."""
        command = {
            "command": "drop_table",
            "db_name": db_name,
            "table_name": table_name
        }
        if tx_id is not None:
            command["tx_id"] = tx_id
        return self.send_command(command)

def run_write_benchmark(client: MicoDBClient, db_name: str, table_name: str, rows_per_batch: int, num_batches: int) -> float:
    """Run a write benchmark and return the rows per second."""
    schema = [
        {"name": "id", "type": "Int32", "nullable": False},
        {"name": "name", "type": "Utf8", "nullable": False},
        {"name": "value", "type": "Float64", "nullable": True},
        {"name": "active", "type": "Boolean", "nullable": True}
    ]
    
    # Create the table
    client.create_table(db_name, table_name, schema)
    
    total_rows = rows_per_batch * num_batches
    start_time = time.time()
    
    for batch in range(num_batches):
        rows = []
        for i in range(rows_per_batch):
            row_id = batch * rows_per_batch + i
            rows.append({
                "id": row_id,
                "name": random_string(10),
                "value": random.uniform(0, 1000),
                "active": random.choice([True, False])
            })
        
        # Append the batch
        client.append(db_name, table_name, rows)
    
    end_time = time.time()
    duration = end_time - start_time
    rows_per_second = total_rows / duration
    
    return rows_per_second

def run_read_benchmark(client: MicoDBClient, db_name: str, table_name: str) -> float:
    """Run a read benchmark and return the rows per second."""
    start_time = time.time()
    
    # Read the entire table
    response = client.read_table(db_name, table_name)
    
    end_time = time.time()
    duration = end_time - start_time
    
    if response["status"] == "ok" and response["data"]:
        rows = response["data"]
        rows_per_second = len(rows) / duration
        return rows_per_second, len(rows)
    
    return 0, 0

def run_query_benchmark(client: MicoDBClient, db_name: str, table_name: str, num_queries: int) -> float:
    """Run a query benchmark and return the queries per second."""
    start_time = time.time()
    
    for _ in range(num_queries):
        # Generate a random query
        active = random.choice(["true", "false"])
        min_value = random.uniform(0, 500)
        query = f"SELECT * FROM {table_name} WHERE active = {active} AND value > {min_value}"
        
        # Execute the query
        client.execute_sql(db_name, query)
    
    end_time = time.time()
    duration = end_time - start_time
    queries_per_second = num_queries / duration
    
    return queries_per_second

def run_transaction_benchmark(client: MicoDBClient, db_name: str, tx_table_name: str, 
                             operations_per_tx: int, num_transactions: int) -> float:
    """Run a transaction benchmark and return the transactions per second."""
    schema = [
        {"name": "id", "type": "Int32", "nullable": False},
        {"name": "name", "type": "Utf8", "nullable": False},
        {"name": "value", "type": "Float64", "nullable": False}
    ]
    
    # Create the table
    client.create_table(db_name, tx_table_name, schema)
    
    start_time = time.time()
    
    for tx_num in range(num_transactions):
        # Begin a transaction
        tx_id = client.begin_transaction(db_name)
        
        # Perform operations within the transaction
        for op in range(operations_per_tx):
            rows = []
            for i in range(5):  # 5 rows per operation
                row_id = tx_num * operations_per_tx * 5 + op * 5 + i
                rows.append({
                    "id": row_id,
                    "name": random_string(10),
                    "value": random.uniform(0, 1000)
                })
            
            # Append the batch within the transaction
            client.append(db_name, tx_table_name, rows, tx_id)
        
        # Commit the transaction
        client.commit_transaction(db_name, tx_id)
    
    end_time = time.time()
    duration = end_time - start_time
    transactions_per_second = num_transactions / duration
    
    return transactions_per_second

def main():
    # Parse command line arguments
    host = 'localhost'
    port = 3456
    iterations = 1000
    
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    if len(sys.argv) > 3:
        iterations = int(sys.argv[3])
    
    # Create a client and connect
    client = MicoDBClient(host, port)
    client.connect()
    
    try:
        # Create a database for benchmarking
        db_name = f"benchmark_db_{int(time.time())}"
        print(f"Creating benchmark database: {db_name}")
        client.create_database(db_name)
        
        # Write benchmark
        print("\n--- Write Benchmark ---")
        rows_per_batch = 100
        num_batches = max(1, iterations // rows_per_batch)
        write_table = "write_benchmark"
        
        print(f"Running write benchmark with {num_batches} batches of {rows_per_batch} rows each...")
        write_speed = run_write_benchmark(client, db_name, write_table, rows_per_batch, num_batches)
        print(f"Write speed: {write_speed:.2f} rows/second")
        
        # Read benchmark
        print("\n--- Read Benchmark ---")
        print(f"Running read benchmark on table with {rows_per_batch * num_batches} rows...")
        read_speed, total_rows = run_read_benchmark(client, db_name, write_table)
        print(f"Read speed: {read_speed:.2f} rows/second (total rows: {total_rows})")
        
        # Query benchmark
        print("\n--- Query Benchmark ---")
        num_queries = min(100, iterations // 10)
        print(f"Running query benchmark with {num_queries} queries...")
        query_speed = run_query_benchmark(client, db_name, write_table, num_queries)
        print(f"Query speed: {query_speed:.2f} queries/second")
        
        # Transaction benchmark
        print("\n--- Transaction Benchmark ---")
        tx_table = "tx_benchmark"
        operations_per_tx = 5
        num_transactions = min(20, iterations // 50)
        print(f"Running transaction benchmark with {num_transactions} transactions of {operations_per_tx} operations each...")
        tx_speed = run_transaction_benchmark(client, db_name, tx_table, operations_per_tx, num_transactions)
        print(f"Transaction speed: {tx_speed:.2f} transactions/second")
        
        # Summary
        print("\n--- Benchmark Summary ---")
        print(f"Database: {db_name}")
        print(f"Write speed: {write_speed:.2f} rows/second")
        print(f"Read speed: {read_speed:.2f} rows/second")
        print(f"Query speed: {query_speed:.2f} queries/second")
        print(f"Transaction speed: {tx_speed:.2f} transactions/second")
        print("\nNOTE: These are simple benchmarks and may not reflect real-world performance.")
        
    finally:
        # Disconnect from the server
        client.disconnect()

if __name__ == "__main__":
    main()