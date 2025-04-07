// This file will be replaced by the runner

mod advanced {
    use crate::engine::db_engine::tests::create_test_table;
    use crate::engine::db_engine::SdmsIcebergEngine;
    use crate::iceberg::version::Version;
    use crate::iceberg::TableMetadata;
    use crate::storage::FileHandle;
    use crate::{DatabaseError, Record, TableChunk, TypeID, Value};

    /// Uses the delete function to delete all rows in a file and checks if the engine behaves
    /// correctly. (Does not add a new empty file.)
    #[test]
    fn test_delete_complete_file() -> Result<(), DatabaseError> {
        unimplemented!();
    }

    /// Checks if the update function correctly rejects double updates to the same file in one
    /// manifest
    #[test]
    fn test_erroneous_update_handling() -> Result<(), DatabaseError> {
        unimplemented!();
    }

    /// Checks if the delete function correctly rejects double deletes to the same file in one
    /// manifest
    #[test]
    fn test_erroneous_delete_handling() -> Result<(), DatabaseError> {
        unimplemented!();
    }

    // TODO tests that change multiple files
    // TODO tests with more complex tables
}
