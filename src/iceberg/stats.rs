use super::{ColumnStats, FileStats};
use crate::Value;
use std::ops::RangeInclusive;

impl ColumnStats {
    pub fn new(min: Value, max: Value) -> ColumnStats {
        if min > max {
            panic!("ColumnStats min must be less or equal max!");
        }
        ColumnStats { min, max }
    }

    /// Check if the range [self.min, self.max] overlaps with [range.start(), range.end()]
    pub fn contains_range(&self, range: RangeInclusive<Value>) -> bool {
        self.min <= *range.end() && self.max >= *range.start()
    }

    /// Check if value is in [range.start(), range.end()]
    pub fn contains(&self, value: Value) -> bool {
        value >= self.min && value <= self.max
    }
}

impl FileStats {
    pub fn new(column_stats: Vec<ColumnStats>) -> FileStats {
        FileStats { column_stats }
    }

    /// Check if the data in this file overlaps with the predicates
    pub fn contains(&self, predicates: Vec<(Value, usize)>) -> bool {
        for pred in predicates {
            if !self.column_stats[pred.1].contains(pred.0) {
                return false
            }
        }
        
        true
    }

    /// Check if the data in this file overlaps with the predicates
    pub fn contains_range(&self, predicates: Vec<(RangeInclusive<Value>, usize)>) -> bool {
        for pred in predicates {
            if !self.column_stats[pred.1].contains_range(pred.0) {
                return false
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use crate::iceberg::stats::{ColumnStats, FileStats};
    use crate::Value;

    #[test]
    #[should_panic]
    fn test_column_contains_range_panic() {
        let column_stats = ColumnStats::new(Value::UInt(3), Value::UInt(33));
        column_stats.contains_range(Value::Int(4)..=Value::Int(30));
    }

    #[test]
    #[should_panic]
    fn test_column_contains_panic() {
        let column_stats = ColumnStats::new(Value::UInt(3), Value::UInt(33));
        column_stats.contains(Value::Int(4));
    }

    #[test]
    fn test_column_stats_contains_and_contains_range() {
        let column_stats = ColumnStats::new(Value::UInt(3), Value::UInt(33));

        assert!(column_stats.contains_range(Value::UInt(4)..=Value::UInt(30)));
        assert!(column_stats.contains_range(Value::UInt(0)..=Value::UInt(3)));
        assert!(column_stats.contains_range(Value::UInt(33)..=Value::UInt(35)));
        assert!(column_stats.contains_range(Value::UInt(0)..=Value::UInt(35)));
        assert!(!column_stats.contains_range(Value::UInt(0)..=Value::UInt(2)));
        assert!(!column_stats.contains_range(Value::UInt(34)..=Value::UInt(36)));

        let column_stats = ColumnStats::new(Value::Int(3), Value::Int(33));

        assert!(column_stats.contains(Value::Int(4)));
        assert!(column_stats.contains(Value::Int(3)));
        assert!(column_stats.contains(Value::Int(33)));
        assert!(!column_stats.contains(Value::Int(0)));
        assert!(!column_stats.contains(Value::Int(34)));
    }

    #[test]
    fn test_file_contains_range() {
        let column_0_stats = ColumnStats::new(Value::UInt(3), Value::UInt(33));
        let column_1_stats = ColumnStats::new(Value::Int(35), Value::Int(50));

        let file_stats = FileStats::new(vec![column_0_stats, column_1_stats]);

        assert!(file_stats.contains_range(vec![(Value::UInt(4)..=Value::UInt(30), 0)]));
        assert!(file_stats.contains_range(vec![(Value::Int(30)..=Value::Int(40), 1)]));
    }

    #[test]
    fn test_file_contains() {
        let column_0_stats = ColumnStats::new(Value::UInt(3), Value::UInt(33));
        let column_1_stats = ColumnStats::new(Value::Int(35), Value::Int(50));

        let file_stats = FileStats::new(vec![column_0_stats, column_1_stats]);

        assert!(file_stats.contains(vec![(Value::UInt(4), 0)]));
        assert!(file_stats.contains(vec![(Value::Int(40), 1)]));
    }
}

// DO NOT REMOVE OR EDIT THE FOLLOWING LINES!
#[cfg(test)]
mod private_tests_stats;
