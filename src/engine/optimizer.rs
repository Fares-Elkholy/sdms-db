use crate::engine::*;
use crate::DatabaseError;

type TableID = usize;
type ColumnID = usize;
// For Filter:
// the size of the domain (number of distinct values) of the filtered value
type DomainSize = usize;
// 1.0 = full table passes, 0.0 = no tuple passes
type FilterSelectivity = f64;

pub enum QueryPlan {
    TableScan(TableID),
    Filter(
        Box<QueryPlan>,
        Vec<(ColumnID, DomainSize, FilterSelectivity)>,
    ),
    Aggregate(Box<QueryPlan>, Vec<ColumnID>),
    Join(Box<QueryPlan>, Box<QueryPlan>, (ColumnID, ColumnID)),
}

pub struct Optimizer {}

impl Optimizer {
    /**
     * Re-cluster data files to fit the workload.
     * No tests for this one, check benches/basic_bench for a benchmark that might help you for the real benchmark in the pipeline.
     */
    pub fn re_cluster(
        _engine: &mut SdmsIcebergEngine,
        _workload: Vec<QueryPlan>,
    ) -> Result<(), DatabaseError> {
        todo!()
    }
}
