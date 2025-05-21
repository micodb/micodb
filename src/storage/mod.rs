// src/storage/mod.rs

//! Storage engine implementation for MicoDB.
//! Handles Parquet file storage, retrieval, and metadata within databases.

use crate::common::error::{MicoError, Result};
use arrow::datatypes::{Schema, SchemaRef}; // SchemaRef for Arc<Schema>
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use std::fs::{File, remove_dir_all}; // Added remove_dir_all
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{create_dir_all, read_dir}; // tokio::fs::remove_dir_all is also an option if we want full async
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const SCHEMA_FILE_NAME: &str = "_schema.json";

pub struct StorageManager {
    base_path: PathBuf,
}

impl StorageManager {
    pub fn new(base_path: PathBuf) -> Self {
        info!("Initializing StorageManager with base path: {:?}", base_path);
        StorageManager { base_path }
    }

    fn validate_name(name: &str, entity_type: &str) -> Result<()> {
        if name.is_empty() || name.contains('/') || name.contains('\\') || name == "." || name == ".." {
            let err_msg = format!(
                "Invalid {} name: '{}'. Must not be empty, contain path separators, or be '.' or '..'.",
                entity_type, name
            );
            error!("{}", err_msg);
            return Err(MicoError::Storage(err_msg));
        }
        Ok(())
    }

    pub async fn create_database(&self, db_name: &str) -> Result<()> {
        Self::validate_name(db_name, "database")?;
        let db_path = self.base_path.join(db_name);
        debug!("Creating database directory for '{}': {:?}", db_name, db_path);
        create_dir_all(&db_path).await.map_err(MicoError::Io)?;
        info!("Database '{}' created or already exists at {}", db_name, db_path.display());
        Ok(())
    }

    pub async fn create_table(&self, db_name: &str, table_name: &str, schema: SchemaRef) -> Result<()> {
        Self::validate_name(db_name, "database")?;
        Self::validate_name(table_name, "table")?;

        let db_path = self.base_path.join(db_name);
        if !db_path.exists() {
            self.create_database(db_name).await?;
        }

        let table_path = db_path.join(table_name);
        if table_path.exists() {
            return Err(MicoError::Storage(format!("Table '{}/{}' already exists.", db_name, table_name)));
        }
        create_dir_all(&table_path).await.map_err(MicoError::Io)?;

        let schema_path = table_path.join(SCHEMA_FILE_NAME);
        let schema_json = serde_json::to_string_pretty(schema.as_ref())
            .map_err(|e| MicoError::Internal(format!("Failed to serialize schema: {}", e)))?;
        
        let mut file = File::create(schema_path).map_err(MicoError::Io)?;
        file.write_all(schema_json.as_bytes()).map_err(MicoError::Io)?;

        info!("Table '{}/{}' created successfully with schema.", db_name, table_name);
        Ok(())
    }

    pub async fn drop_table(&self, db_name: &str, table_name: &str) -> Result<()> {
        Self::validate_name(db_name, "database")?;
        Self::validate_name(table_name, "table")?;

        let table_path = self.base_path.join(db_name).join(table_name);
        if !table_path.exists() || !table_path.is_dir() {
            return Err(MicoError::Storage(format!("Table '{}/{}' does not exist.", db_name, table_name)));
        }

        // Use std::fs::remove_dir_all for synchronous removal.
        // For a fully async version, tokio::fs::remove_dir_all could be used,
        // but it might require more careful error handling or blocking tasks for safety.
        remove_dir_all(&table_path).map_err(|e| MicoError::Io(e))?;
        info!("Table '{}/{}' dropped successfully.", db_name, table_name);
        Ok(())
    }


    pub async fn get_table_schema(&self, db_name: &str, table_name: &str) -> Result<SchemaRef> {
        Self::validate_name(db_name, "database")?;
        Self::validate_name(table_name, "table")?;

        let table_path = self.base_path.join(db_name).join(table_name);
        if !table_path.exists() || !table_path.is_dir() {
            return Err(MicoError::Storage(format!("Table '{}/{}' does not exist.", db_name, table_name)));
        }

        let schema_path = table_path.join(SCHEMA_FILE_NAME);
        if !schema_path.exists() {
            return Err(MicoError::Storage(format!("Schema file not found for table '{}/{}'.", db_name, table_name)));
        }

        let mut file = File::open(schema_path).map_err(MicoError::Io)?;
        let mut schema_json = String::new();
        file.read_to_string(&mut schema_json).map_err(MicoError::Io)?;

        let schema: Schema = serde_json::from_str(&schema_json)
            .map_err(|e| MicoError::Internal(format!("Failed to deserialize schema for table '{}/{}': {}", db_name, table_name, e)))?;
        
        Ok(Arc::new(schema))
    }

    pub async fn append_batch(&self, db_name: &str, table_name: &str, batch: RecordBatch) -> Result<()> {
        Self::validate_name(db_name, "database")?;
        Self::validate_name(table_name, "table")?;

        let table_schema = self.get_table_schema(db_name, table_name).await?;
        if batch.schema() != table_schema {
            warn!("Appending batch with schema different from table schema for '{}/{}'", db_name, table_name);
        }

        let table_path = self.base_path.join(db_name).join(table_name);
        if !table_path.exists() {
            return Err(MicoError::Storage(format!("Table '{}/{}' does not exist. Create table first.", db_name, table_name)));
        }

        let file_name = format!("{}.parquet", Uuid::new_v4());
        let file_path = table_path.join(&file_name);
        debug!("Writing batch to Parquet file: {:?}", file_path);

        let file = File::create(&file_path).map_err(MicoError::Io)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
            .map_err(|e| MicoError::Parquet(e.to_string()))?;
        writer.write(&batch).map_err(|e| MicoError::Parquet(e.to_string()))?;
        writer.close().map_err(|e| MicoError::Parquet(e.to_string()))?;

        info!("Successfully wrote batch to {}", file_path.display());
        Ok(())
    }

    pub async fn read_table(&self, db_name: &str, table_name: &str) -> Result<Vec<RecordBatch>> {
        Self::validate_name(db_name, "database")?;
        Self::validate_name(table_name, "table")?;

        let table_path = self.base_path.join(db_name).join(table_name);
        debug!("Reading table '{}/{}' from path: {:?}", db_name, table_name, table_path);

        if !table_path.exists() {
            info!("Table '{}/{}' directory does not exist, returning empty.", db_name, table_name);
            return Ok(Vec::new());
        }
        if !table_path.is_dir() {
            let err_msg = format!("Table path '{}' is not a directory.", table_path.display());
            error!("{}", err_msg);
            return Err(MicoError::Storage(err_msg));
        }
        
        let mut batches = Vec::new();
        let mut dir_entries = read_dir(&table_path).await.map_err(MicoError::Io)?;

        while let Some(entry) = dir_entries.next_entry().await.map_err(MicoError::Io)? {
            let entry_path = entry.path();
            if entry_path.is_file() && entry_path.extension().map_or(false, |ext| ext == "parquet") {
                debug!("Reading Parquet file: {:?}", entry_path);
                let file = File::open(&entry_path).map_err(MicoError::Io)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|e| MicoError::Parquet(e.to_string()))?;
                let reader = builder.build().map_err(|e| MicoError::Parquet(e.to_string()))?;
                for batch_result in reader {
                    match batch_result {
                        Ok(batch) => batches.push(batch),
                        Err(e) => {
                            let err_msg = format!("Error reading batch from {}: {}", entry_path.display(), e);
                            error!("{}", err_msg);
                            return Err(MicoError::Parquet(err_msg));
                        }
                    }
                }
            }
        }
        info!("Successfully read {} batches for table '{}/{}'", batches.len(), db_name, table_name);
        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, BooleanArray};
    use arrow::datatypes::{DataType, Field};
    use tempfile::tempdir;

    fn test_schema1() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }
    
    fn test_schema2() -> SchemaRef {
         Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("flag", DataType::Boolean, false),
            Field::new("score", DataType::Int32, true),
        ]))
    }

    fn create_record_batch_for_schema(schema: SchemaRef) -> RecordBatch {
        let columns: Vec<Arc<dyn arrow::array::Array>> = schema.fields().iter().map(|field| {
            match field.data_type() {
                DataType::Int32 => {
                    if field.is_nullable() { Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as Arc<dyn arrow::array::Array> } 
                    else { Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array> }
                }
                DataType::Utf8 => {
                    if field.is_nullable() { Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as Arc<dyn arrow::array::Array> } 
                    else { Arc::new(StringArray::from(vec!["a", "b", "c"])) as Arc<dyn arrow::array::Array> }
                }
                DataType::Boolean => {
                    if field.is_nullable() { Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])) as Arc<dyn arrow::array::Array> } 
                    else { Arc::new(BooleanArray::from(vec![true, false, true])) as Arc<dyn arrow::array::Array> }
                }
                _ => panic!("Unsupported data type {} for test batch creation in field {}", field.data_type(), field.name())
            }
        }).collect();
        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[tokio::test]
    async fn test_create_and_drop_table() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = StorageManager::new(temp_dir.path().to_path_buf());
        let db_name = "test_db_drop";
        let table_name = "test_table_drop";
        let schema = test_schema1();

        storage_manager.create_database(db_name).await.unwrap();
        storage_manager.create_table(db_name, table_name, schema.clone()).await.unwrap();
        
        let table_path = temp_dir.path().join(db_name).join(table_name);
        assert!(table_path.exists());

        storage_manager.drop_table(db_name, table_name).await.unwrap();
        assert!(!table_path.exists());
    }

    #[tokio::test]
    async fn test_drop_non_existent_table() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = StorageManager::new(temp_dir.path().to_path_buf());
        let db_name = "test_db_drop_ne";
        let table_name = "non_existent_table_drop";
        storage_manager.create_database(db_name).await.unwrap();

        let result = storage_manager.drop_table(db_name, table_name).await;
        assert!(result.is_err());
        if let Err(MicoError::Storage(msg)) = result {
            assert!(msg.contains("does not exist"));
        } else {
            panic!("Expected MicoError::Storage for dropping non-existent table");
        }
    }

    // ... (other existing tests remain unchanged but are included for completeness) ...
    #[tokio::test]
    async fn test_create_database_and_table() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = StorageManager::new(temp_dir.path().to_path_buf());
        let db_name = "test_db_dyn";
        let table_name = "test_table_dyn";
        let schema = test_schema1();

        storage_manager.create_database(db_name).await.unwrap();
        storage_manager.create_table(db_name, table_name, schema.clone()).await.unwrap();

        let db_path = temp_dir.path().join(db_name);
        assert!(db_path.exists() && db_path.is_dir());
        let table_path = db_path.join(table_name);
        assert!(table_path.exists() && table_path.is_dir());
        let schema_file_path = table_path.join(SCHEMA_FILE_NAME);
        assert!(schema_file_path.exists() && schema_file_path.is_file());
        let loaded_schema = storage_manager.get_table_schema(db_name, table_name).await.unwrap();
        assert_eq!(loaded_schema, schema);
    }
    
    #[tokio::test]
    async fn test_create_table_no_db_should_create_db() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = StorageManager::new(temp_dir.path().to_path_buf());
        let db_name = "new_db_on_table_create";
        let table_name = "my_table";
        let schema = test_schema1();
        assert!(!temp_dir.path().join(db_name).exists());
        storage_manager.create_table(db_name, table_name, schema.clone()).await.unwrap();
        assert!(temp_dir.path().join(db_name).exists());
        let loaded_schema = storage_manager.get_table_schema(db_name, table_name).await.unwrap();
        assert_eq!(loaded_schema, schema);
    }

    #[tokio::test]
    async fn test_append_and_read_with_dynamic_schema() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = StorageManager::new(temp_dir.path().to_path_buf());
        let db_name = "test_db_dyn_append";
        let table_name = "test_table_dyn_append";
        let schema = test_schema1();
        storage_manager.create_table(db_name, table_name, schema.clone()).await.unwrap();
        let batch = create_record_batch_for_schema(schema.clone());
        storage_manager.append_batch(db_name, table_name, batch.clone()).await.unwrap();
        let read_batches = storage_manager.read_table(db_name, table_name).await.unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), batch.num_rows());
        assert_eq!(read_batches[0].schema(), batch.schema());
    }
    
    #[tokio::test]
    async fn test_append_batch_schema_mismatch_warning() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = StorageManager::new(temp_dir.path().to_path_buf());
        let db_name = "db_schema_warn";
        let table_name = "table_schema_warn";
        let schema1 = test_schema1();
        let schema2 = test_schema2();
        storage_manager.create_table(db_name, table_name, schema1.clone()).await.unwrap();
        let batch_with_schema2 = create_record_batch_for_schema(schema2.clone());
        let append_result = storage_manager.append_batch(db_name, table_name, batch_with_schema2.clone()).await;
        assert!(append_result.is_ok()); 
        let read_batches = storage_manager.read_table(db_name, table_name).await.unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].schema(), schema2);
    }

    #[tokio::test]
    async fn test_create_database_main() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = StorageManager::new(temp_dir.path().to_path_buf());
        let db_name = "test_db_main";
        storage_manager.create_database(db_name).await.unwrap();
        let db_path = temp_dir.path().join(db_name);
        assert!(db_path.exists() && db_path.is_dir());
    }

    #[tokio::test]
    async fn test_append_multiple_batches_with_db_dyn() {
        let temp_dir = tempdir().unwrap();
        let storage_manager = StorageManager::new(temp_dir.path().to_path_buf());
        let db_name = "test_db_multi_dyn";
        let table_name = "multi_batch_table_dyn";
        let schema = test_schema1();
        storage_manager.create_table(db_name, table_name, schema.clone()).await.unwrap();
        let batch1 = create_record_batch_for_schema(schema.clone());
        let batch2 = create_record_batch_for_schema(schema.clone());
        storage_manager.append_batch(db_name, table_name, batch1.clone()).await.unwrap();
        storage_manager.append_batch(db_name, table_name, batch2.clone()).await.unwrap();
        let read_batches = storage_manager.read_table(db_name, table_name).await.unwrap();
        assert_eq!(read_batches.len(), 2);
        assert_eq!(read_batches[0].num_rows(), batch1.num_rows());
        assert_eq!(read_batches[1].num_rows(), batch2.num_rows());
    }
}
