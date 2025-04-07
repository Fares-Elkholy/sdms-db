use std::collections::VecDeque;
use std::ops::RangeInclusive;

use super::{FileStats, Manifest};
use crate::storage::FileHandle;
use crate::Value;

impl Manifest {
    pub fn new(added: Vec<FileHandle>, deleted: Vec<FileHandle>, stats: Vec<FileStats>) -> Self {
        Manifest {
            added,
            deleted,
            stats,
        }
    }

    pub fn add_file(&mut self, file: FileHandle, stats: FileStats) {
        self.added.push(file);
        self.stats.push(stats);
    }

    pub fn delete_file(&mut self, file: FileHandle) {
        self.deleted.push(file);
    }

    pub fn added(&self) -> &[FileHandle] {
        self.added.as_slice()
    }

    pub fn deleted(&self) -> &[FileHandle] {
        self.deleted.as_slice()
    }

    /// Return an iterator over all file handles of files added in this manifest that have overlap
    /// with all the equality predicates
    pub fn contains(&self, predicates: Vec<(Value, usize)>) -> impl Iterator<Item = &FileHandle> {
        self.stats.iter().enumerate().filter_map(move |(dlt_idx, stat)| {
            if stat.contains(predicates.clone()) {
                Some(&self.added[dlt_idx])
            } else {
                None
            }
        })
    }

    /// Return an iterator over all file handles of files added in this manifest that have overlap
    /// with all the range predicates
    #[allow(redundant_semicolons)]
    pub fn contains_range(
        &self,
        predicates: Vec<(RangeInclusive<Value>, usize)>,
    ) -> impl Iterator<Item = &FileHandle> {
        self.stats.iter().enumerate().filter_map(move |(dlt_idx, stat)| {
            if stat.contains_range(predicates.clone()) {
                Some(&self.added[dlt_idx])
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::iceberg::{ColumnStats, FileStats, Manifest};
    use crate::storage::FileHandle;
    use crate::Value;

    #[test]
    fn test_manifest_containing() {
        let added = vec![FileHandle::default(), FileHandle::default()];
        let deleted = vec![FileHandle::default()];

        let column_stats_1 = ColumnStats::new(Value::UInt(4), Value::UInt(16));
        let column_stats_2 = ColumnStats::new(Value::UInt(16), Value::UInt(64));
        let stats = vec![
            FileStats::new(vec![column_stats_1]),
            FileStats::new(vec![column_stats_2]),
        ];

        let manifest = Manifest::new(added.clone(), deleted, stats);

        let mut contained = manifest.contains(vec![(Value::UInt(5), 0)]);
        assert_eq!(*contained.next().unwrap(), added[0]);
        assert_eq!(contained.next(), None);

        let mut contained = manifest.contains(vec![(Value::UInt(16), 0)]);
        assert_eq!(*contained.next().unwrap(), added[0]);
        assert_eq!(*contained.next().unwrap(), added[1]);
        assert_eq!(contained.next(), None);

        let mut contained = manifest.contains(vec![(Value::UInt(17), 0)]);
        assert_eq!(*contained.next().unwrap(), added[1]);
        assert_eq!(contained.next(), None);

        let mut contained = manifest.contains(vec![(Value::UInt(0), 0)]);
        assert_eq!(contained.next(), None);
    }

    #[test]
    fn test_manifest_containing_range() {
        let added = vec![FileHandle::default(), FileHandle::default()];
        let deleted = vec![FileHandle::default()];

        let column_stats_1 = ColumnStats::new(Value::UInt(4), Value::UInt(16));
        let column_stats_2 = ColumnStats::new(Value::UInt(16), Value::UInt(64));
        let stats = vec![
            FileStats::new(vec![column_stats_1]),
            FileStats::new(vec![column_stats_2]),
        ];

        let manifest = Manifest::new(added.clone(), deleted, stats);

        let mut contained = manifest.contains_range(vec![(Value::UInt(3)..=Value::UInt(10), 0)]);
        assert_eq!(*contained.next().unwrap(), added[0]);
        assert_eq!(contained.next(), None);

        let mut contained = manifest.contains_range(vec![(Value::UInt(10)..=Value::UInt(20), 0)]);
        assert_eq!(*contained.next().unwrap(), added[0]);
        assert_eq!(*contained.next().unwrap(), added[1]);
        assert_eq!(contained.next(), None);

        let mut contained = manifest.contains_range(vec![(Value::UInt(60)..=Value::UInt(60), 0)]);
        assert_eq!(*contained.next().unwrap(), added[1]);
        assert_eq!(contained.next(), None);

        let mut contained = manifest.contains_range(vec![(Value::UInt(0)..=Value::UInt(1), 0)]);
        assert_eq!(contained.next(), None);
    }
}

#[cfg(test)]
mod private_tests_manifest;
