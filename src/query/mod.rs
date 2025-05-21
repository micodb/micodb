// src/query/mod.rs

//! Query engine for MicoDB.
//! Uses DataFusion for SQL parsing, planning, and execution.

use crate::common::error::{MicoError, Result};
use crate::storage::StorageManager; // To access table data and schemas
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion::datasource::listing::ListingOptions; // Corrected
use datafusion::datasource::file_format::parquet::ParquetFormat; // Corrected
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
    // This might be better placed in StorageManager or a shared utility if SM exposes it.
    fn get_table_data_dir_path(&self, db_name: &str, table_name: &str) -> String {
        // Assuming storage_manager.base_path is accessible or we have a method for it.
        // For now, let's construct it based on the known structure.
        // This is a simplification; StorageManager should ideally provide this.
        // PathBuf::from("./data").join(db_name).join(table_name).to_string_lossy().into_owned()
        // Let's assume StorageManager's base_path is what we need.
        // However, StorageManager's base_path is private.
        // For now, we'll hardcode the default data path for simplicity in this example.
        // A proper solution would involve QueryEngine having access to this path configuration
        // or StorageManager providing a method to get table paths.
        format!("./data/{}/{}", db_name, table_name)
    }


    pub async fn execute_sql(&self, db_name: &str, sql: &str) -> Result<Vec<RecordBatch>> {
        info!("Executing SQL query in database '{}': {}", db_name, sql);

        // Create a new DataFusion session context for each query
        // SessionContext is lightweight and designed to be created per-query or per-session
        let ctx = SessionContext::new();

        // TODO: Parse SQL to identify table names involved in the query.
        // For this initial version, we'll make a simplifying assumption:
        // The query operates on tables within the specified `db_name`.
        // We would need to register each table mentioned in the SQL.
        // For a simple `SELECT * FROM my_table`, we need to register `my_table`.
        // This part needs a robust way to get table names from SQL.
        // For now, let's assume the SQL query itself contains fully qualified names if needed,
        // or we extract them. A very naive approach for a single table:
        
        // Super naive way to get a table name from a simple SELECT query
        // E.g., "SELECT ... FROM table_name ..."
        // This is NOT robust and only for a quick demo.
        let lowercased_sql = sql.to_lowercase(); // Store the owned String
        let table_name_ref: Option<&str> = lowercased_sql
            .split("from")
            .nth(1)
            .and_then(|s| s.trim().split_whitespace().next())
            .map(|s| s.trim_end_matches(';'));

        // Convert to owned String if found
        let table_name_owned: Option<String> = table_name_ref.map(|s| s.to_string());

        if let Some(table_name) = &table_name_owned { // Borrow the owned String for use
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
        } else {
            warn!("Could not naively extract table name from SQL: {}. Table registration might be incomplete.", sql);
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
}

#[cfg(test)]
mod tests {
    // use super::*;
    // use crate::storage::StorageManager;
    // use std::path::PathBuf;
    // use std::sync::Arc;
    // use arrow::array::{Int32Array, StringArray};
    // use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    // use arrow::record_batch::RecordBatch;
    // use tempfile::tempdir;

    // Helper to create a dummy RecordBatch for testing
    // fn create_test_batch_for_query_engine() -> (SchemaRef, RecordBatch) {
    //     let schema = Arc::new(Schema::new(vec![
    //         Field::new("c1", DataType::Int32, false),
    //         Field::new("c2", DataType::Utf8, true),
    //     ]));
    //     let batch = RecordBatch::try_new(
    //         schema.clone(),
    //         vec![
    //             Arc::new(Int32Array::from(vec![1, 2, 3])),
    //             Arc::new(StringArray::from(vec![Some("foo"), None, Some("bar")])),
    //         ],
    //     ).unwrap();
    //     (schema, batch)
    // }

    // #[tokio::test]
    // async fn test_simple_select_query() {
    //     let temp_dir = tempdir().unwrap();
    //     let storage_manager = Arc::new(StorageManager::new(temp_dir.path().to_path_buf()));
    //     let query_engine = QueryEngine::new(Arc::clone(&storage_manager));

    //     let db_name = "test_db_qe";
    //     let table_name = "test_table_qe";
    //     let (schema, batch) = create_test_batch_for_query_engine();

    //     storage_manager.create_database(db_name).await.unwrap();
    //     storage_manager.create_table(db_name, table_name, schema.clone()).await.unwrap();
    //     storage_manager.append_batch(db_name, table_name, batch).await.unwrap();

    //     let sql = format!("SELECT c1, c2 FROM {}", table_name);
    //     let results = query_engine.execute_sql(db_name, &sql).await.unwrap();
        
    //     assert_eq!(results.len(), 1);
    //     let result_batch = &results[0];
    //     assert_eq!(result_batch.num_rows(), 3);
    //     assert_eq!(result_batch.num_columns(), 2);
    //     // Further assertions on data can be added here
    // }
    // More tests will be needed here, especially for table registration logic
    // and various SQL queries.
}
