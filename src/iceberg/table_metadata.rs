use super::{version::Version, Manifest, TableMetadata};
use crate::storage::FileHandle;
use crate::{Schema, Value};
use std::collections::HashSet;
use std::fs::File;
use std::ops::RangeInclusive;
use std::result;

impl TableMetadata {
    pub fn new(name: String, schema: Schema) -> Self {
        TableMetadata {
            name,
            schema,
            version: Version::default(),
            manifests: Vec::new(),
        }
    }

    /// Return all manifests (versions) until the input version as a slice. If version is None,
    /// return all manifests (the current version).
    pub fn snapshot(&self, version: Option<Version>) -> &[Manifest] {
            match version {
                None => return self.manifests.as_slice(),
                Some(version) => {
                    let mut version_no: u64 = version.into();
                    if version_no > self.version.into() {
                        version_no = self.version.into();
                    }
                    return &self.manifests[0..version_no as usize];
                }
            }
        }

    /// Return all file handles of files that belong to the table at the specified version.
    pub fn files(&self, version: Option<Version>) -> HashSet<FileHandle> {
        let manifests = self.snapshot(version);

        let mut result: HashSet<FileHandle> = HashSet::new();
        
        for manifest in manifests {
            for added in manifest.added() {
                result.insert(*added);
            }
            for deleted in manifest.deleted() {
                result.remove(deleted);
            }
        }

        result
    }

    /// Return all file handles of files that belong to the table at the specified version and
    /// overlap with all predicates
    pub fn contains(
        &self,
        predicates: Vec<(Value, usize)>,
        version: Option<Version>,
    ) -> HashSet<FileHandle> {
        let manifests = self.snapshot(version);

        let mut result: HashSet<FileHandle> = HashSet::new();
        
        for manifest in manifests {
            for added in manifest.contains(predicates.clone()) {
                result.insert(*added);
            }
            for deleted in manifest.deleted() {
                result.remove(deleted);
            }
        }

        result
    }

    /// Return all file handles of files that belong to the table at the specified version and
    /// overlap with all predicates
    pub fn contains_range(
        &self,
        predicates: Vec<(RangeInclusive<Value>, usize)>,
        version: Option<Version>,
    ) -> HashSet<FileHandle> {
        let manifests = self.snapshot(version);

        let mut result: HashSet<FileHandle> = HashSet::new();
        
        for manifest in manifests {
            for added in manifest.contains_range(predicates.clone()) {
                result.insert(*added);
            }
            for deleted in manifest.deleted() {
                result.remove(deleted);
            }
        }

        result
    }

    /// Add a manifest as a new version to the table and update the table version
    pub fn add_version(&mut self, manifest: Manifest) -> Version {
        self.manifests.push(manifest);
        self.version = self.version.successor();
        self.version
    }
}

#[cfg(test)]
mod tests {
    use crate::iceberg::{version::Version, ColumnStats, FileStats, Manifest, TableMetadata};
    use crate::storage::FileHandle;
    use crate::{TypeID, Value};
    use std::collections::HashSet;

    #[test]
    fn test_add_version() {
        let mut table = TableMetadata::new(String::from("Customers"), vec![TypeID::UInt]);
        let mut manifest_1 = Manifest::default();
        let manifest_2 = Manifest::default();

        manifest_1.add_file(FileHandle::default(), FileStats::default());

        assert_eq!(table.version, Version::default());
        assert_eq!(table.manifests.len(), 0);

        let version_1 = table.add_version(manifest_1.clone());

        assert_eq!(table.version, Version::new(1));
        assert_eq!(version_1, Version::new(1));
        assert_eq!(table.manifests.len(), 1);
        assert_eq!(table.manifests[0], manifest_1);

        let version_2 = table.add_version(manifest_2.clone());

        assert_eq!(table.version, Version::new(2));
        assert_eq!(version_2, Version::new(2));
        assert_eq!(table.manifests.len(), 2);
        assert_eq!(table.manifests[0], manifest_1);
        assert_eq!(table.manifests[1], manifest_2);
    }

    fn create_example_table() -> TableMetadata {
        let mut table = TableMetadata::new(String::from("Customers"), vec![TypeID::UInt]);

        let fh = FileHandle::default();
        let column_stats_1 = ColumnStats::new(Value::UInt(4), Value::UInt(64));
        let stats = FileStats::new(vec![column_stats_1]);
        let mut manifest = Manifest::default();

        manifest.add_file(fh, stats);

        let mut manifest_2 = Manifest::default();
        manifest_2.delete_file(fh);

        let fh2 = FileHandle::default();
        let fh3 = FileHandle::default();

        let column_stats_2 = ColumnStats::new(Value::UInt(0), Value::UInt(16));
        let stats_2 = FileStats::new(vec![column_stats_2]);
        let column_stats_3 = ColumnStats::new(Value::UInt(10), Value::UInt(64));
        let stats_3 = FileStats::new(vec![column_stats_3]);

        manifest_2.add_file(fh2, stats_2);
        manifest_2.add_file(fh3, stats_3);

        table.add_version(manifest);
        table.add_version(manifest_2);

        table
    }

    #[test]
    fn test_table_snapshot() {
        let table = create_example_table();

        let snapshot = table.snapshot(None);
        assert_eq!(snapshot.len(), 2);
        for i in 0..2 {
            assert_eq!(snapshot[i], table.manifests[i])
        }

        let snapshot = table.snapshot(Some(Version::new(1)));
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0], table.manifests[0]);

        let snapshot = table.snapshot(Some(Version::new(2)));
        assert_eq!(snapshot.len(), 2);
        for i in 0..2 {
            assert_eq!(snapshot[i], table.manifests[i])
        }

        let snapshot = table.snapshot(Some(Version::new(3)));
        assert_eq!(snapshot.len(), 2);
        for i in 0..2 {
            assert_eq!(snapshot[i], table.manifests[i])
        }

        let snapshot = table.snapshot(Some(Version::new(0)));
        assert_eq!(snapshot.len(), 0);
    }

    #[test]
    fn test_table_files() {
        let table = create_example_table();
        let fh = table.manifests[0].added[0];
        let fh2 = table.manifests[1].added[0];
        let fh3 = table.manifests[1].added[1];

        let files = table.files(None);
        assert_eq!(files.len(), 2);
        assert!(!files.contains(&fh));
        assert!(files.contains(&fh2));
        assert!(files.contains(&fh3));

        let files = table.files(Some(Version::new(1)));
        assert_eq!(files.len(), 1);
        assert!(files.contains(&fh));

        let files = table.files(Some(Version::new(2)));
        assert_eq!(files.len(), 2);
        assert!(!files.contains(&fh));
        assert!(files.contains(&fh2));
        assert!(files.contains(&fh3));

        let files = table.files(Some(Version::new(3)));
        assert_eq!(files.len(), 2);
        assert!(!files.contains(&fh));
        assert!(files.contains(&fh2));
        assert!(files.contains(&fh3));

        let files = table.files(Some(Version::new(0)));
        assert_eq!(files.len(), 0);
    }

    #[test]
    fn test_table_contains() {
        let table = create_example_table();

        let fh = table.manifests[0].added[0];
        let fh2 = table.manifests[1].added[0];
        let fh3 = table.manifests[1].added[1];

        let files = table.contains(vec![(Value::UInt(18), 0)], Some(Version::new(1)));
        let expected: HashSet<FileHandle> = HashSet::from_iter(vec![fh]);
        assert_eq!(files, expected);

        let files = table.contains(vec![(Value::UInt(0), 0)], Some(Version::new(1)));
        let expected: HashSet<FileHandle> = HashSet::new();
        assert_eq!(files, expected);

        let files = table.contains(vec![(Value::UInt(0), 0)], Some(Version::new(2)));
        let expected: HashSet<FileHandle> = HashSet::from_iter(vec![fh2]);
        assert_eq!(files, expected);

        let files = table.contains(vec![(Value::UInt(21), 0)], Some(Version::new(2)));
        let expected: HashSet<FileHandle> = HashSet::from_iter(vec![fh3]);
        assert_eq!(files, expected);

        let files = table.contains(vec![(Value::UInt(65), 0)], Some(Version::new(2)));
        let expected: HashSet<FileHandle> = HashSet::new();
        assert_eq!(files, expected);

        let files = table.contains(vec![(Value::UInt(12), 0)], Some(Version::new(2)));
        let expected: HashSet<FileHandle> = HashSet::from_iter(vec![fh2, fh3]);
        assert_eq!(files, expected);
    }
}

#[cfg(test)]
mod private_tests_table_metadata;
