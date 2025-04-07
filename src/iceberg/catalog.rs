// This file will be replaced by the runner

use super::{Catalog, TableMetadata};

impl Catalog {
    pub fn add_table(&mut self, metadata: TableMetadata) -> usize {
        let table_id = self.tables.len();
        self.tables.push(metadata);

        table_id
    }

    pub fn get_table_metadata(&mut self, table_id: usize) -> &mut TableMetadata {
        &mut self.tables[table_id]
    }

    pub fn check_table_exists(&self, table_id: usize) -> bool {
        table_id < self.tables.len()
    }
}
