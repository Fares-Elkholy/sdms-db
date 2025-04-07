// This file will be replaced by the runner

use crate::{TableChunk, TypeID};
use std::path::PathBuf;
use uuid::Uuid;

pub mod data;
pub mod file_storage;

#[derive(Debug, Clone)]
pub enum Columns {
    All,
    Selection(Vec<usize>),
}

impl From<Option<Vec<usize>>> for Columns {
    fn from(value: Option<Vec<usize>>) -> Self {
        match value {
            Some(v) => Self::Selection(v),
            None => Self::All,
        }
    }
}

impl From<Columns> for Option<Vec<usize>> {
    fn from(value: Columns) -> Self {
        match value {
            Columns::All => None,
            Columns::Selection(v) => Some(v),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct DataFile {
    pub header: DataFileHeader,
    pub data: TableChunk,
}

#[derive(Debug, PartialEq, Eq)]
pub struct DataFileHeader {
    rows: u64,
    columns: u64,
    column_info: Vec<(TypeID, usize)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileHandle {
    file_uuid: Uuid,
}

#[derive(Debug, Clone)]
pub struct FileBasedStorage {
    base_path: PathBuf,
}
