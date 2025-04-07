// This file will be replaced by the runner

pub mod db_engine;
pub mod operators;
pub mod optimizer;

use crate::iceberg::{Catalog, Manifest};
use crate::storage::{FileBasedStorage, FileHandle};
use std::collections::HashSet;

#[derive(Default)]
pub struct SdmsIcebergEngine {
    // The catalog that this engine works on
    pub catalog: Catalog,
    // A manifest that the engine works on from the start of a modification to commit. At commit,
    // this is moved to the catalog and replaced with a new fresh manifest
    manifest: Manifest,
    // The table that is being modified currently. Set by start_table_modification and reset by
    // commit
    table_id: Option<usize>,
    // Storage access struct
    pub storage: FileBasedStorage,
    // Keep track of the files changed in this manifest
    changed_files: HashSet<FileHandle>,
}
