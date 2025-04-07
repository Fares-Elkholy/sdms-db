// This file will be replaced by the runner
// Author: Jonathan Schild

use super::{Columns, DataFile, DataFileHeader};

mod advanced {
    use super::{Columns, DataFile, DataFileHeader};
    use crate::{DatabaseError, RowID, TypeID, Value};
    use rand::{
        distributions::{Standard, Uniform},
        prelude::Distribution,
        rngs::StdRng,
        thread_rng, Rng, RngCore, SeedableRng,
    };
    use rand_utf8::rand_utf8;
    use std::io::Cursor;
    use std::rc::Rc;

    /// Deserializes random columns of 20_000 rows of (UInt, UInt, UInt) in 1_000 row chunks.
    /// Compare deserialized with reference implementation.
    #[test]
    fn deserialize_column_selection() -> Result<(), DatabaseError> {
        unimplemented!();
    }

    /// Serializes and deserializes 20_000 rows of Int in 1_000 row chunks.
    /// Compare deserialized with original.
    #[test]
    fn serialize_deserialize_int() -> Result<(), DatabaseError> {
        unimplemented!();
    }

    /// Serializes and deserializes 20_000 rows of UInt in 1_000 row chunks.
    /// Compare deserialized with original.
    #[test]
    fn serialize_deserialize_uint() -> Result<(), DatabaseError> {
        unimplemented!();
    }

    /// Serializes and deserializes 20_000 rows of RowID in 1_000 row chunks.
    /// Compare deserialized with original.
    #[test]
    fn serialize_deserialize_rowid() -> Result<(), DatabaseError> {
        unimplemented!();
    }

    /// Serializes and deserializes an empty chunk.
    /// Compare deserialized with original.
    #[test]
    fn serialize_deserialize_empty() -> Result<(), DatabaseError> {
        unimplemented!();
    }

    /// Creates table with 200_000 random entries (RowID, Varchar(80), UInt, Int)
    /// Create 500 rows big chunks
    /// Serialize chunks and compare with reference implementation
    /// Deserialize chunks and compare with reference implementation
    #[test]
    fn serialize_deserialize_all_data_types() -> Result<(), DatabaseError> {
        unimplemented!();
    }
}
