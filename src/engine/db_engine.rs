use crate::iceberg::{Catalog, ColumnStats, FileStats, Manifest};
use crate::storage::{self, data, DataFile, DataFileHeader, FileBasedStorage, FileHandle};
use crate::{DatabaseError, Record, Schema, TableChunk, Value};
use crate::TypeID;
use std::any::type_name;
use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::vec;

use crate::engine::SdmsIcebergEngine;

impl SdmsIcebergEngine {
    pub fn new(catalog: Catalog, storage: FileBasedStorage) -> SdmsIcebergEngine {
        SdmsIcebergEngine {
            catalog,
            manifest: Manifest::default(),
            table_id: None,
            storage,
            changed_files: HashSet::new(),
        }
    }

    pub fn start_table_modification(&mut self, table_id: usize) -> Result<(), DatabaseError> {
        if self.table_id.is_some() || !self.catalog.check_table_exists(table_id) {
            return Err(DatabaseError::EngineError);
        }
        self.table_id = Some(table_id);
        Ok(())
    }

    /// Calculate FileStats for a chunk
    fn calculate_statistics(chunk: &TableChunk) -> FileStats {
        // (min, max) for each column
        // columns ordered in the same order as chunk[]
        let mut min_max_vec: Vec<(Value, Value)> = vec![]; 
        
        let no_cols = chunk.len();
        // iterate over each column
        for col_idx in 0..no_cols {
            let mut min: Value = chunk[col_idx][0].clone();
            let mut max: Value = chunk[col_idx][0].clone();
            // iterate over each row in given column, update min/max if necessary
            for row_idx in 0..chunk[col_idx].len() {
                if chunk[col_idx][row_idx] < min {
                    min = chunk[col_idx][row_idx].clone();
                }
                else if chunk[col_idx][row_idx] > max {
                    max = chunk[col_idx][row_idx].clone();
                }
            }
            // add min, max of given column
            min_max_vec.push((min, max));
        }

        // new ColumnStats from each pair of min max
        let stats: Vec<ColumnStats> = min_max_vec.iter().map(|(min, max)| ColumnStats::new(min.clone(), max.clone())).collect();
        FileStats::new(stats)
    }

    /// Insert the chunks into the table
    pub fn insert(&mut self, chunks: Vec<TableChunk>) -> Result<(), DatabaseError> {
        // sanity check: table must be chosen
        if self.table_id.is_none() {
            return Err(DatabaseError::EngineError);
        }

        let schema = self.catalog.get_table_metadata(self.table_id.unwrap()).clone().schema;
        let no_cols = schema.len() as u64;
        let col_info: Vec<(TypeID, usize)> = schema.iter().map(|type_id| (type_id.clone(), 0)).collect();
        // insert each chunk
        for chunk in chunks {
            let stats = SdmsIcebergEngine::calculate_statistics(&chunk);

            let no_bytes_per_row = SdmsIcebergEngine::no_rows_given_schema(&schema);
            let no_rows = chunk[0].len() as u64;
        
            let header = DataFileHeader::new(no_rows, no_cols, col_info.clone());

            // create file
            let file = DataFile::new(header, chunk);
            file.to_bytes();

            // convert data to bytes
            let data = file.to_bytes();
            // write file using data to -> new File Handle
            let fh = self.storage.write_file(&data)?;

            // add_file in manifest using stats and file handle
            self.manifest.add_file(fh, stats);
            self.changed_files.insert(fh);
        }

        Ok(())
    }

    // /// given a TableChunk (without header), converts chunk to its bytes
 fn to_bytes_no_header(chunk: TableChunk) {
    //     let mut result: Vec<u8> = vec![];
        
    //     for col in 0..chunk.len() { // col access
    //         // represents the idx in result where start_idx bytes should be placed            
    //         match &chunk[col][0]    {
    //             Value::Int(_) => {
    //                 for row in 0..chunk[col].len() { // row access
    //                     // simply add int
    //                     if let Value::Int(int) = chunk[col][row] {
    //                         result.extend(int.to_le_bytes());
    //                     }
    //                     else {
    //                         panic!("error")
    //                     }
    //                 }
    //             }

    //             Value::UInt(_) => {
    //                 for row in 0..chunk[col].len() { // row access
    //                     // simply add UInt
    //                     if let Value::UInt(uint) = chunk[col][row] {
    //                         result.extend(uint.to_le_bytes());
    //                     }
    //                     else {
    //                         panic!("error")
    //                     }
    //                 }
    //             }
                
    //             Value::Varchar(_) => {
    //                 for row in 0..chunk[col].len() { // row access
    //                     if let Value::Varchar(s) = &chunk[col][row] {
    //                         let length_bytes: [u8; 8] = s.to_string().len().to_le_bytes();
    //                         // first add length bytes for string
    //                         result.extend(length_bytes);  
                            
    //                         // then add string decoded in utf8                 
    //                         result.extend(s.to_string().as_bytes());
    //                 }
    //                 else {
    //                     panic!("error!!!!")
    //                 }
    //             }
    //             }
    //             Value::RowID(_) => {
    //                 for row in 0..chunk[col].len() { // row access
    //                     // simply add RowID
    //                     if let Value::RowID(rowid) = &chunk[col][row] {
    //                         result.extend(rowid.0.to_le_bytes());
    //                     }
    //                     else {
    //                         panic!("error")
    //                     }
    //                 }
    //             } 
    //         }    
    //     }
    //     result
    // }


    // fn parse_no_header(&mut self, bytes: &Vec<u8>, bytes_per_row: u64, schema: &Schema) -> TableChunk {

    //     let rows = bytes.len() / bytes_per_row as usize;
    //     let mut start_idx = 0;

    //     let mut chunk: Vec<Vec<Value>> = vec![];

    //     // parse each column to table chunk
    //     for type_id in schema {
    //         // used to save values in this column 
    //         let mut column: Vec<Value> = vec![]; 
    //         match type_id {
    //             TypeID::Int => {
    //                 for _idx in 0..rows as usize {
    //                     let int_bytes: [u8; 4] = bytes[start_idx..start_idx+4].try_into().expect("Slice should be exactly 4 bytes");
    //                     // add int to column
    //                     column.push(Value::Int(i32::from_le_bytes(int_bytes)));
    //                     start_idx = start_idx + 4;
    //                 }  
    //             }
    //             TypeID::UInt => {
    //                 for _idx in 0..rows as usize {
    //                     let uint_bytes: [u8; 4] = bytes[start_idx..start_idx+4].try_into().expect("Slice should be exactly 4 bytes");
    //                     // add uint to column
    //                     column.push(Value::UInt(u32::from_le_bytes(uint_bytes)));
    //                     start_idx = start_idx + 4;

    //                 }
    //             }
    //             TypeID::RowID => {
    //                 for _idx in 0..rows as usize {
    //                     let rowid_bytes: [u8; 8] = bytes[start_idx..start_idx+8].try_into().expect("Slice should be exactly 8 bytes");
    //                     // add rowid to column
    //                     column.push(Value::RowID(crate::RowID(u64::from_le_bytes(rowid_bytes))));
    //                     start_idx = start_idx + 8;
    //                 }
    //             }
    //             _ => panic!("i don't update VARCHAR yet :((") // just ignore varchar for now
    //         }
    //         // push materialized column
    //         chunk.push(column);
    //     }

    //     chunk
    // }
 }


    fn no_rows_given_schema(schema : &Vec<TypeID>) -> u64 {
        let mut row_len = 0;
        for typeid in schema {
            match typeid {
                TypeID::Int => {
                    row_len += 4;
                }
                TypeID::UInt => {
                    row_len += 4;
                }
                TypeID::RowID => {
                    row_len += 8;
                }
                _ => panic!("i don't update VARCHAR yet :((")
            }
        }
        row_len
    }

    /// Update the table using `updates`
    /// `updates`: a vector of tuples, where:
    /// * updates.0 contains the filehandle of the file (chunk) to be updates
    /// * updates.1 contains set of changes (row no., new record)
    /// 
    /// 
    /// 

    
    pub fn update(
        &mut self,
        updates: Vec<(FileHandle, Vec<(u32, Record)>)>, // updates on each file record.len() must be equal to schema length
    ) -> Result<(), DatabaseError> {
        // sanity check: table must be chosen
        if self.table_id.is_none() {
            return Err(DatabaseError::EngineError);
        }


        let schema = self.catalog.get_table_metadata(self.table_id.unwrap()).clone().schema;

        // calculate length (in bytes) of a row in the schema
        let row_len = SdmsIcebergEngine::no_rows_given_schema(&schema);

        // Check for duplicate file handles
        let mut seen_file_handles = HashSet::new();
        for (fh, _) in &updates {
            if !seen_file_handles.insert(*fh) {
                // If insert returns false, it means the file handle was already in the set
                return Err(DatabaseError::EngineError);
            }
        }

        for (old_fh, updated_recs) in updates {
            if self.changed_files.contains(&old_fh) {
                return Err(DatabaseError::EngineError);
            }
            
            // load chunk from file
            let mut file = self.storage.read_file(&old_fh).unwrap();
            let mut datafile = DataFile::parse(&mut file)?;
            let mut chunk_data = &mut datafile.data;

            // load chunks to be changed from file


            // update each column
            for col in 0..schema.len() {
                // for each row update
                for (row, record) in &updated_recs {
                    chunk_data[col][*row as usize] = record.record[col].clone(); // update record in place
                }
            }
            let stats = SdmsIcebergEngine::calculate_statistics(&chunk_data);
            // write chunk into new file
            let data= datafile.to_bytes();
            let new_fh = self.storage.write_file(&data)?;

            if self.manifest.added().contains(&new_fh) || self.manifest.deleted().contains(&new_fh) {
                return Err(DatabaseError::EngineError);
            }

            self.manifest.add_file(new_fh, stats);
            self.manifest.delete_file(old_fh);
            
            self.changed_files.insert(old_fh);
        }
        
        Ok(())
    }

    /// Deletes rows in a given `FileHandle` (chunk)
    /// * each element of deletions contains filehandle and the set of row_ids that are to be deleted
    /// 
    /// You can assume that the row indexes in deletions are in ascending order!
    /// 
    /// If a file handle is already contained in the changes of this manifest, do not change files
    /// or the manifest and return DatabaseError::EngineError
    pub fn delete(&mut self, deletions: Vec<(FileHandle, Vec<u32>)>) -> Result<(), DatabaseError> {
        // sanity check: table must be chosen
        if self.table_id.is_none() {
            return Err(DatabaseError::EngineError);
        }

        // Check for duplicate file handles
        let mut seen_file_handles = HashSet::new();
        for (fh, _) in &deletions {
            if !seen_file_handles.insert(*fh) {
                // If insert returns false, it means the file handle was already in the set
                return Err(DatabaseError::EngineError);
            }
        }

        let schema = self.catalog.get_table_metadata(self.table_id.unwrap()).clone().schema;
        let column_info: Vec<(TypeID, usize)> = schema.iter().map(|type_id| (type_id.clone(), 0)).collect();

        // calculate length (in bytes) of a row in the schema

        for (old_fh, deleted_recs) in deletions {
            if self.changed_files.contains(&old_fh) {
                return Err(DatabaseError::EngineError);
            }
            
            // load chunk from file
            let mut file = self.storage.read_file(&old_fh).unwrap();
            let mut datafile = DataFile::parse(&mut file)?;

            
            // update each column by deleting relevant rows
            for col in 0..schema.len() {
                // initialize indizes
                let mut delete_idx = 0; // index of vec containing to-be-deleted indizes
                let mut current_idx = 0; // index which is incremeted in each pass
                
                datafile.data[col].retain(|_| {
                    let keep = delete_idx >= deleted_recs.len() || current_idx as u32 != deleted_recs[delete_idx];
                    if !keep {
                        delete_idx += 1;
                    }
                    current_idx += 1;
                    keep
                });
            }
                        // write chunk into new file
            let deleted: bool = {
                let mut del = true;
                for col in &datafile.data {
                    if col.len() != 0 {
                        // if any column has rows, not fully deleted
                        del = false;
                        break;
                    }
                }
                del
            };   
            
            let new_data_chunk = DataFile::new(DataFileHeader::new(datafile.data[0].len() as u64, datafile.data.len() as u64, column_info.clone()), datafile.data);


            if !deleted { // only create new file if chunk is not empty
                let stats = SdmsIcebergEngine::calculate_statistics(&new_data_chunk.data);

                let data= new_data_chunk.to_bytes();
                let new_fh = self.storage.write_file(&data)?;
                
                self.manifest.add_file(new_fh, stats);
            }

            self.manifest.delete_file(old_fh);
            
            self.changed_files.insert(old_fh);
        }
        
        Ok(())

    }

    /// Update the table by marking the FileHandles in deletions as deleted in the manifest.
    /// If a file handle is already contained in the changes of this manifest, do not change files
    /// or the manifest and return DatabaseError::EngineError
    pub fn delete_chunks(&mut self, deletions: &[FileHandle]) -> Result<(), DatabaseError> {
        for deleted_fh in deletions {
            if self.manifest.added().contains(deleted_fh) || self.manifest.deleted().contains(deleted_fh) {
                return Err(DatabaseError::EngineError);
            }
            
            self.manifest.delete_file(*deleted_fh);
            
            self.changed_files.insert(*deleted_fh);
        }

        Ok(())
        
    }

    pub fn commit(&mut self) -> Result<(), DatabaseError> {
        if let Some(table_id) = self.table_id {
            let table_metadata = self.catalog.get_table_metadata(table_id);

            if !self.changed_files.is_empty() {
                // Moves self.manifest to table_metadata and replaces it with Manifest::default()
                table_metadata.add_version(std::mem::take(&mut self.manifest));
                self.changed_files = HashSet::new();
            }
            self.table_id = None;

            Ok(())
        } else {
            Err(DatabaseError::EngineError)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::db_engine::SdmsIcebergEngine;
    use crate::iceberg::version::Version;
    use crate::iceberg::TableMetadata;
    use crate::storage::{DataFile, FileHandle};
    use crate::{DatabaseError, Record, TableChunk, TypeID, Value};

    #[test]
    fn test_simple_errors() {
        let mut engine = SdmsIcebergEngine::default();
        let table = TableMetadata::new(String::from("Customers"), vec![TypeID::UInt]);
        engine.catalog.add_table(table);

        let insert_result = engine.insert(Vec::new());
        let update_result = engine.update(Vec::new());
        let delete_result = engine.delete(Vec::new());

        for (result, op) in vec![
            (insert_result, "insert"),
            (update_result, "update"),
            (delete_result, "delete"),
        ] {
            match result {
                Ok(_) => {
                    panic!(
                        "Expected error when trying to {} without choosing table!",
                        op
                    )
                }
                Err(x) => match x {
                    DatabaseError::EngineError => {}
                    _ => {
                        panic!(
                            "Expected EngineError when trying to {} without choosing table!",
                            op
                        )
                    }
                },
            }
        }
    }

    pub(crate) fn create_test_table(
    ) -> Result<(SdmsIcebergEngine, usize, Vec<Vec<Value>>, FileHandle), DatabaseError> {
        let mut engine = SdmsIcebergEngine::default();
        let table = TableMetadata::new(String::from("Customers"), vec![TypeID::UInt]);
        let table_id = engine.catalog.add_table(table);

        let mut chunk = TableChunk::default();
        chunk.push((0..20).map(|i| Value::UInt(i)).collect::<Vec<_>>());

        engine.start_table_modification(table_id)?;
        engine.insert(vec![chunk.clone()])?;
        engine.commit()?;

        let files = engine.catalog.get_table_metadata(table_id).files(None);
        let fh = files.iter().next().unwrap();
        Ok((engine, table_id, chunk, *fh))
    }

    #[test]
    fn test_insert() -> Result<(), DatabaseError> {
        let (mut engine, _, chunk, _) = create_test_table()?;

        assert!(engine.catalog.check_table_exists(0));
        let table_metadata = engine.catalog.get_table_metadata(0);

        assert_eq!(table_metadata.snapshot(None).len(), 1);
        assert_eq! {table_metadata.contains(vec![(Value::UInt(5), 0)], None).iter().collect::<Vec<_>>().len(), 1};

        let mut file_reader = engine
            .storage
            .read_file(table_metadata.files(None).iter().next().unwrap())?;
        let data_file = DataFile::parse(&mut file_reader)?;

        let read_chunk = data_file.data;

        assert_eq!(chunk, read_chunk);

        Ok(())
    }

    #[test]
    fn test_update() -> Result<(), DatabaseError> {
        let (mut engine, table_id, mut chunk, fh) = create_test_table()?;

        let updates: Vec<(FileHandle, Vec<(u32, Record)>)> = vec![(
            fh,
            vec![
                (0, Record::new(vec![Value::UInt(1)])),
                (1, Record::new(vec![Value::UInt(2)])),
            ],
        )];

        engine.start_table_modification(table_id)?;
        engine.update(updates)?;
        engine.commit()?;

        assert!(engine.catalog.check_table_exists(0));
        let table_metadata = engine.catalog.get_table_metadata(0);

        // Test that update created a new version
        assert_eq!(table_metadata.snapshot(None).len(), 2);
        assert_eq!(table_metadata.version, Version::new(2));

        // Test that the new version has exactly one valid file
        assert_eq! {table_metadata.files(None).iter().collect::<Vec<_>>().len(), 1};

        // Test that 5 is contained in both versions according to the metadata
        assert_eq! {table_metadata.contains(vec![(Value::UInt(5), 0)], None).iter().collect::<Vec<_>>().len(), 1};
        assert_eq! {table_metadata.contains(vec![(Value::UInt(5), 0)], Some(Version::new(1))).iter().collect::<Vec<_>>().len(), 1};

        // Test that 0 is only contained in version 1 according to the metadata
        assert_eq! {table_metadata.contains(vec![(Value::UInt(0), 0)], None).iter().collect::<Vec<_>>().len(), 0};
        assert_eq! {table_metadata.contains(vec![(Value::UInt(0), 0)], Some(Version::new(1))).iter().collect::<Vec<_>>().len(), 1};

        // Test that the file resulting from the update is correct
        let mut file_reader = engine
            .storage
            .read_file(table_metadata.files(None).iter().next().unwrap())?;
        let data_file = DataFile::parse(&mut file_reader)?;

        let read_chunk = data_file.data;

        chunk[0][0] = Value::UInt(1);
        chunk[0][1] = Value::UInt(2);
        assert_eq!(chunk, read_chunk);

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<(), DatabaseError> {
        let (mut engine, table_id, _, fh) = create_test_table()?;

        let deletions: Vec<(FileHandle, Vec<u32>)> = vec![(fh, (1..20).step_by(2).collect())];

        engine.start_table_modification(table_id)?;
        engine.delete(deletions)?;
        engine.commit()?;

        assert!(engine.catalog.check_table_exists(0));
        let table_metadata = engine.catalog.get_table_metadata(0);

        // Test that delete created a new version
        assert_eq!(table_metadata.snapshot(None).len(), 2);
        assert_eq!(table_metadata.version, Version::new(2));

        // Test that the new version has exactly one valid file
        assert_eq! {table_metadata.files(None).iter().collect::<Vec<_>>().len(),1};

        // Test that 5 is contained in both versions according to the metadata
        assert_eq! {table_metadata.contains(vec![(Value::UInt(5), 0)], None).iter().collect::<Vec<_>>().len(),1};
        assert_eq! {table_metadata.contains(vec![(Value::UInt(5), 0)], Some(Version::new(1))).iter().collect::<Vec<_>>().len(),1};

        // Test that 19 is only contained in version 1 according to the metadata
        assert_eq! {table_metadata.contains(vec![(Value::UInt(19), 0)], None).iter().collect::<Vec<_>>().len(),0};
        assert_eq! {table_metadata.contains(vec![(Value::UInt(19), 0)], Some(Version::new(1))).iter().collect::<Vec<_>>().len(),1};

        // Test that the file resulting from the update is correct
        let mut file_reader = engine
            .storage
            .read_file(table_metadata.files(None).iter().next().unwrap())?;
        let data_file = DataFile::parse(&mut file_reader)?;

        let read_chunk = data_file.data;

        let mut expected_chunk = TableChunk::default();
        expected_chunk.push(
            (0..=18)
                .step_by(2)
                .map(|i| Value::UInt(i))
                .collect::<Vec<_>>(),
        );

        assert_eq! {expected_chunk, read_chunk};

        Ok(())
    }

    #[test]
    fn test_delete_chunks() -> Result<(), DatabaseError> {
        let (mut engine, table_id, _, fh) = create_test_table()?;

        let deletions: Vec<FileHandle> = vec![fh];

        engine.start_table_modification(table_id)?;
        engine.delete_chunks(deletions.as_slice())?;
        engine.commit()?;

        assert! {engine.catalog.check_table_exists(0)};
        let table_metadata = engine.catalog.get_table_metadata(0);

        // Test that delete created a new version
        assert_eq! {table_metadata.snapshot(None).len(), 2};
        assert_eq! {table_metadata.version, Version::new(2)};

        // Test that the new version has no valid files
        assert! {table_metadata.files(None).iter().next().is_none()};

        // Test that 5 is only contained in version 1 according to the metadata
        assert_eq! {table_metadata.contains(vec![(Value::UInt(5), 0)], None).iter().collect::<Vec<_>>().len(), 0};
        assert_eq! {table_metadata.contains(vec![(Value::UInt(5), 0)], Some(Version::new(1))).iter().collect::<Vec<_>>().len(), 1};

        Ok(())
    }
}

// DO NOT REMOVE OR EDIT THE FOLLOWING LINES!
#[cfg(test)]
mod private_tests_db_engine;
