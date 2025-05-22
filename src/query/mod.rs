// src/query/mod.rs

//! Query engine for MicoDB.
//! Uses DataFusion for SQL parsing, planning, and execution.

use crate::common::error::{MicoError, Result};
use crate::storage::StorageManager; // To access table data and schemas
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion::datasource::listing::ListingOptions; // Corrected
use datafusion::datasource::file_format::parquet::ParquetFormat; // Corrected
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use std::sync::Arc;
use tracing::{debug, info, error, warn}; // Added warn

pub struct QueryEngine {
    storage_manager: Arc<StorageManager>,
}

impl QueryEngine {
    pub fn new(storage_manager: Arc<StorageManager>) -> Self {
        info!("Initializing QueryEngine");
        QueryEngine { storage_manager }
    }

    // A helper to get the full path for a table's data directory
    fn get_table_data_dir_path(&self, db_name: &str, table_name: &str) -> String {
        // Since StorageManager's base_path is private, and there's no getter method,
        // we'll need to keep the hardcoded approach for now.
        // In a production environment, we would:
        // 1. Add a getter method for the base_path in StorageManager, or
        // 2. Add a method to get the full path for a table directly
        // For now we'll keep using a consistent path that matches StorageManager's structure
        format!("./data/{}/{}", db_name, table_name)
    }


    pub async fn execute_sql(&self, db_name: &str, sql: &str) -> Result<Vec<RecordBatch>> {
        info!("Executing SQL query in database '{}': {}", db_name, sql);

        // Create a new DataFusion session context for each query
        // SessionContext is lightweight and designed to be created per-query or per-session
        let ctx = SessionContext::new();

        // Use DataFusion's SQL parser to properly extract table names from the query
        // This is much more robust than string splitting and handles complex queries
        let table_names = self.extract_table_names_from_sql(sql);
        debug!("Extracted table names from SQL query: {:?}", table_names);

        for table_name in &table_names {
            debug!("Attempting to register table: {}/{}", db_name, table_name);
            match self.storage_manager.get_table_schema(db_name, table_name).await {
                Ok(schema) => {
                    let table_data_path = self.get_table_data_dir_path(db_name, table_name);
                    let table_uri = format!("{}/", table_data_path); // Needs trailing slash for directory
                    
                    info!("Registering table '{}' with path '{}' and schema: {:?}", table_name, table_uri, schema);
                    
                    // Registering a directory of Parquet files as a ListingTable
                    // The schema must be provided.
                    // File extension is important.
                    let list_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                        .with_file_extension(".parquet");
                    
                    ctx.register_listing_table(table_name, &table_uri, list_options, Some(schema), None).await
                        .map_err(|e| MicoError::QueryExecution(format!("Failed to register table {}: {}", table_name, e)))?;
                    info!("Table '{}' registered successfully.", table_name);
                }
                Err(e) => {
                    error!("Failed to get schema for table '{}/{}': {}. Table not registered.", db_name, table_name, e);
                    // Depending on SQL, this might be an error or the table is not needed.
                    // For now, we'll let DataFusion handle "table not found" if query uses it.
                }
            }
        }
        
        if table_names.is_empty() {
            warn!("Could not extract any table names from SQL: {}. Table registration might be incomplete.", sql);
        }


        // Execute the SQL query
        debug!("Executing query with DataFusion: {}", sql);
        let df = ctx.sql(sql).await
            .map_err(|e| MicoError::QueryExecution(format!("DataFusion SQL execution error: {}", e)))?;

        // Collect the results
        let results: Vec<RecordBatch> = df.collect().await
            .map_err(|e| MicoError::QueryExecution(format!("DataFusion result collection error: {}", e)))?;
        
        info!("SQL query executed successfully, {} batches returned.", results.len());
        Ok(results)
    }

    /// Parse SQL and extract table names used in the query
    fn extract_table_names_from_sql(&self, sql: &str) -> Vec<String> {
        let mut table_names = Vec::new();
        
        // Use DataFusion's SQL parser to parse the query
        match DFParser::parse_sql(sql) {
            Ok(statements) => {
                for statement in statements {
                    match statement {
                        DFStatement::Statement(stmt) => {
                            // Extract table names based on statement type
                            match stmt.as_ref() {
                                // Handle SELECT queries
                                datafusion::sql::statement::Statement::Query(query) => {
                                    self.extract_tables_from_query(query, &mut table_names);
                                }
                                // Handle other statement types (INSERT, DELETE, etc.)
                                datafusion::sql::statement::Statement::Insert { table_name, .. } => {
                                    table_names.push(table_name.to_string());
                                }
                                datafusion::sql::statement::Statement::Delete { table_name, .. } => {
                                    table_names.push(table_name.to_string());
                                }
                                datafusion::sql::statement::Statement::Update { table_name, .. } => {
                                    table_names.push(table_name.to_string());
                                }
                                _ => {
                                    // For other statement types, we don't extract table names
                                    debug!("Statement type not handled for table extraction: {:?}", stmt);
                                }
                            }
                        }
                        _ => {
                            // For other statement types, we don't extract table names
                            debug!("Statement variant not handled for table extraction");
                        }
                    }
                }
            }
            Err(e) => {
                // If parsing fails, log the error but don't fail the entire query
                // DataFusion will handle SQL syntax errors later
                warn!("Failed to parse SQL for table name extraction: {}", e);
            }
        }
        
        // Remove duplicates from the list of table names
        table_names.sort();
        table_names.dedup();
        
        table_names
    }
    
    /// Helper to extract table names from a SQL query
    fn extract_tables_from_query(&self, query: &datafusion::sql::query::Query, table_names: &mut Vec<String>) {
        if let Some(body) = &query.body {
            match body.as_ref() {
                datafusion::sql::query::SetExpr::Select(select) => {
                    // Extract from main FROM clause
                    if let Some(from) = &select.from {
                        for table_with_joins in from {
                            // Handle the main table reference
                            self.extract_table_name_from_relation(&table_with_joins.relation, table_names);
                            
                            // Handle any joined tables
                            for join in &table_with_joins.joins {
                                self.extract_table_name_from_relation(&join.relation, table_names);
                            }
                        }
                    }
                }
                datafusion::sql::query::SetExpr::Query(subquery) => {
                    // Recursively extract from subqueries
                    self.extract_tables_from_query(subquery, table_names);
                }
                datafusion::sql::query::SetExpr::SetOperation { left, right, .. } => {
                    // Handle UNION, INTERSECT, EXCEPT operations
                    self.extract_tables_from_query(left, table_names);
                    self.extract_tables_from_query(right, table_names);
                }
                _ => {
                    debug!("Query body type not handled for table extraction");
                }
            }
        }
    }
    
    /// Helper to extract a table name from a SQL relation (table reference)
    fn extract_table_name_from_relation(&self, relation: &datafusion::sql::table_reference::TableReference, table_names: &mut Vec<String>) {
        match relation {
            datafusion::sql::table_reference::TableReference::Table { 
                name, 
                .. 
            } => {
                // Get the last part of the name (which is the table name)
                // For names like "schema.table" or "db.schema.table", we just want "table"
                if let Some(table_name) = name.0.last() {
                    table_names.push(table_name.value.clone());
                }
            }
            datafusion::sql::table_reference::TableReference::SubQuery { query, .. } => {
                // Recursively extract from subqueries
                self.extract_tables_from_query(query, table_names);
            }
            datafusion::sql::table_reference::TableReference::TableFunction { .. } => {
                // Table functions (e.g. unnest) don't have a direct table reference
                debug!("Table function not handled for table extraction");
            }
            datafusion::sql::table_reference::TableReference::Join { left, right, .. } => {
                // Recursively extract from both sides of a join
                self.extract_table_name_from_relation(left, table_names);
                self.extract_table_name_from_relation(right, table_names);
            }
            _ => {
                debug!("Relation type not handled for table extraction");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageManager;
    use std::path::PathBuf;
    use std::sync::Arc;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use tempfile::tempdir;

    // Helper to create a dummy RecordBatch for testing
    fn create_test_batch_for_query_engine() -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("foo"), None, Some("bar")])),
            ],
        ).unwrap();
        (schema, batch)
    }

    #[tokio::test]
    async fn test_simple_select_query() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = Arc::new(StorageManager::new(temp_dir.path().to_path_buf()));
        let query_engine = QueryEngine::new(Arc::clone(&storage_manager));

        let db_name = "test_db_qe";
        let table_name = "test_table_qe";
        let (schema, batch) = create_test_batch_for_query_engine();

        storage_manager.create_database(db_name).await.unwrap();
        storage_manager.create_table(db_name, table_name, schema.clone()).await.unwrap();
        storage_manager.append_batch(db_name, table_name, batch).await.unwrap();

        let sql = format!("SELECT c1, c2 FROM {}", table_name);
        let results = query_engine.execute_sql(db_name, &sql).await.unwrap();
        
        assert_eq!(results.len(), 1);
        let result_batch = &results[0];
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_table_with_where_clause() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = Arc::new(StorageManager::new(temp_dir.path().to_path_buf()));
        let query_engine = QueryEngine::new(Arc::clone(&storage_manager));

        let db_name = "test_db_where";
        let table_name = "test_table_where";
        let (schema, batch) = create_test_batch_for_query_engine();

        storage_manager.create_database(db_name).await.unwrap();
        storage_manager.create_table(db_name, table_name, schema.clone()).await.unwrap();
        storage_manager.append_batch(db_name, table_name, batch).await.unwrap();

        let sql = format!("SELECT c1, c2 FROM {} WHERE c1 > 1", table_name);
        let results = query_engine.execute_sql(db_name, &sql).await.unwrap();
        
        assert_eq!(results.len(), 1);
        let result_batch = &results[0];
        assert_eq!(result_batch.num_rows(), 2); // Should only return rows where c1 > 1
    }

    #[tokio::test]
    async fn test_invalid_sql() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = Arc::new(StorageManager::new(temp_dir.path().to_path_buf()));
        let query_engine = QueryEngine::new(Arc::clone(&storage_manager));

        let db_name = "test_db_invalid";
        let invalid_sql = "SELECT FROM INVALID SYNTAX";
        
        let result = query_engine.execute_sql(db_name, invalid_sql).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nonexistent_table() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = Arc::new(StorageManager::new(temp_dir.path().to_path_buf()));
        let query_engine = QueryEngine::new(Arc::clone(&storage_manager));

        let db_name = "test_db_nonexistent";
        let sql = "SELECT * FROM nonexistent_table";
        
        let result = query_engine.execute_sql(db_name, sql).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_complex_query_with_join() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = Arc::new(StorageManager::new(temp_dir.path().to_path_buf()));
        let query_engine = QueryEngine::new(Arc::clone(&storage_manager));

        let db_name = "test_db_join";
        
        // Create first table
        let table1_name = "test_table1";
        let (schema1, batch1) = create_test_batch_for_query_engine();
        storage_manager.create_database(db_name).await.unwrap();
        storage_manager.create_table(db_name, table1_name, schema1.clone()).await.unwrap();
        storage_manager.append_batch(db_name, table1_name, batch1).await.unwrap();
        
        // Create second table
        let table2_name = "test_table2";
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("c3", DataType::Int32, false),
            Field::new("c4", DataType::Utf8, true),
        ]));
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alpha"), Some("beta"), Some("gamma")])),
            ],
        ).unwrap();
        storage_manager.create_table(db_name, table2_name, schema2.clone()).await.unwrap();
        storage_manager.append_batch(db_name, table2_name, batch2).await.unwrap();

        // Now create a query that joins both tables
        let sql = format!("SELECT t1.c1, t2.c4 FROM {} as t1 JOIN {} as t2 ON t1.c1 = t2.c3", 
            table1_name, table2_name);
            
        // This will test whether our extract_table_names_from_sql method works correctly
        // It should identify both tables in the JOIN query
        let results = query_engine.execute_sql(db_name, &sql).await;
        
        // We should get valid results with the joined data
        assert!(results.is_ok(), "JOIN query should execute successfully");
        let result_batches = results.unwrap();
        assert!(!result_batches.is_empty(), "JOIN query should return results");
    }
    
    #[tokio::test]
    async fn test_extract_table_names() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = Arc::new(StorageManager::new(temp_dir.path().to_path_buf()));
        let query_engine = QueryEngine::new(Arc::clone(&storage_manager));
        
        // Test simple SELECT
        let tables = query_engine.extract_table_names_from_sql(
            "SELECT * FROM users WHERE id > 100");
        assert_eq!(tables, vec!["users"]);
        
        // Test JOIN
        let tables = query_engine.extract_table_names_from_sql(
            "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id");
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
        
        // Test subquery
        let tables = query_engine.extract_table_names_from_sql(
            "SELECT * FROM (SELECT id FROM customers) AS c WHERE c.id > 10");
        assert_eq!(tables, vec!["customers"]);
        
        // Test UNION
        let tables = query_engine.extract_table_names_from_sql(
            "SELECT id FROM table1 UNION SELECT id FROM table2");
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"table1".to_string()));
        assert!(tables.contains(&"table2".to_string()));
    }
}
