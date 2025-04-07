// This file will be replaced by the runner

use crate::storage::FileHandle;
use crate::{Schema, Value};
use version::Version;

pub mod catalog;
pub mod manifest;
pub mod stats;
pub mod table_metadata;
pub mod version;

#[derive(Debug, Default, Eq, PartialEq)]
pub struct Catalog {
    tables: Vec<TableMetadata>,
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct TableMetadata {
    pub name: String,
    pub schema: Schema,
    pub version: Version,
    manifests: Vec<Manifest>,
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct Manifest {
    added: Vec<FileHandle>,
    deleted: Vec<FileHandle>,
    stats: Vec<FileStats>,
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct FileStats {
    column_stats: Vec<ColumnStats>,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ColumnStats {
    pub min: Value,
    pub max: Value,
}
