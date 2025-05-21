#!/usr/bin/env python3
"""
Simple client for MicoDB - Demonstrates basic operations and transactions.

Usage:
  python3 simple_client.py [host] [port]

Default:
  host: localhost
  port: 3456
"""

import json
import socket
import sys
import time
from typing import Any, Dict, List, Optional, Union

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
        print(f"Connected to MicoDB at {self.host}:{self.port}")
        
    def disconnect(self) -> None:
        """Disconnect from the MicoDB server."""
        if self.socket:
            self.socket.close()
            self.socket = None
            print("Disconnected from MicoDB")
            
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

def main():
    host = 'localhost'
    port = 3456
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    
    # Create a client and connect
    client = MicoDBClient(host, port)
    client.connect()
    
    try:
        # Example 1: Basic operations without transactions
        print("\n--- Example 1: Basic operations ---")
        db_name = "example_db"
        
        # Create a database
        print("Creating database...")
        response = client.create_database(db_name)
        print(f"Response: {response}")
        
        # Create a table
        print("Creating table...")
        schema = [
            {"name": "id", "type": "Int32", "nullable": False},
            {"name": "name", "type": "Utf8", "nullable": False},
            {"name": "active", "type": "Boolean", "nullable": True}
        ]
        response = client.create_table(db_name, "users", schema)
        print(f"Response: {response}")
        
        # Append data
        print("Appending data...")
        rows = [
            {"id": 1, "name": "Alice", "active": True},
            {"id": 2, "name": "Bob", "active": False},
            {"id": 3, "name": "Carol", "active": True}
        ]
        response = client.append(db_name, "users", rows)
        print(f"Response: {response}")
        
        # Read data
        print("Reading data...")
        response = client.read_table(db_name, "users")
        print(f"Table data: {json.dumps(response.get('data', []), indent=2)}")
        
        # Execute SQL query
        print("Executing SQL query...")
        response = client.execute_sql(db_name, "SELECT * FROM users WHERE active = true")
        print(f"Query result: {json.dumps(response.get('data', []), indent=2)}")
        
        # Example 2: Using transactions
        print("\n--- Example 2: Using transactions ---")
        
        # Begin a transaction
        print("Beginning transaction...")
        tx_id = client.begin_transaction(db_name)
        print(f"Transaction started with ID: {tx_id}")
        
        # Create a table within the transaction
        print("Creating table within transaction...")
        schema = [
            {"name": "order_id", "type": "Int32", "nullable": False},
            {"name": "user_id", "type": "Int32", "nullable": False},
            {"name": "total", "type": "Float64", "nullable": False}
        ]
        response = client.create_table(db_name, "orders", schema, tx_id)
        print(f"Response: {response}")
        
        # Append data within the transaction
        print("Appending data within transaction...")
        rows = [
            {"order_id": 101, "user_id": 1, "total": 99.99},
            {"order_id": 102, "user_id": 2, "total": 149.95}
        ]
        response = client.append(db_name, "orders", rows, tx_id)
        print(f"Response: {response}")
        
        # Commit the transaction
        print("Committing transaction...")
        response = client.commit_transaction(db_name, tx_id)
        print(f"Response: {response}")
        
        # Read data to verify it was committed
        print("Reading data from committed transaction...")
        response = client.read_table(db_name, "orders")
        print(f"Table data: {json.dumps(response.get('data', []), indent=2)}")
        
        # Example 3: Transaction rollback
        print("\n--- Example 3: Transaction rollback ---")
        
        # Begin another transaction
        print("Beginning transaction...")
        tx_id = client.begin_transaction(db_name)
        print(f"Transaction started with ID: {tx_id}")
        
        # Create a table that will be rolled back
        print("Creating table that will be rolled back...")
        schema = [
            {"name": "product_id", "type": "Int32", "nullable": False},
            {"name": "name", "type": "Utf8", "nullable": False},
            {"name": "price", "type": "Float64", "nullable": False}
        ]
        response = client.create_table(db_name, "products", schema, tx_id)
        print(f"Response: {response}")
        
        # Append data that will be rolled back
        print("Appending data that will be rolled back...")
        rows = [
            {"product_id": 1, "name": "Widget", "price": 19.99},
            {"product_id": 2, "name": "Gadget", "price": 29.99}
        ]
        response = client.append(db_name, "products", rows, tx_id)
        print(f"Response: {response}")
        
        # Rollback the transaction
        print("Rolling back transaction...")
        response = client.rollback_transaction(db_name, tx_id)
        print(f"Response: {response}")
        
        # Try to read data to verify it was rolled back
        print("Trying to read rolled back data (should fail)...")
        try:
            response = client.read_table(db_name, "products")
            print(f"Response: {response}")
        except Exception as e:
            print(f"Expected error (table doesn't exist): {response}")
        
        # Clean up (optional)
        print("\n--- Cleaning up ---")
        print("Dropping tables...")
        response = client.drop_table(db_name, "users")
        print(f"Dropped users table: {response}")
        response = client.drop_table(db_name, "orders")
        print(f"Dropped orders table: {response}")
        
    finally:
        # Disconnect from the server
        client.disconnect()

if __name__ == "__main__":
    main()