//Note: This file will be replaced by the runner
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use std::cmp::max;
use std::cmp::min;
use std::collections::HashMap;

use lab3::engine::operators::*;
use lab3::engine::optimizer::*;
use lab3::engine::SdmsIcebergEngine;
use lab3::iceberg::TableMetadata;
use lab3::storage::Columns;
use lab3::Operator;
use lab3::{RowID, TableChunk, TypeID, Value};
use rand::distributions::{Distribution, Uniform};
use rand::prelude::StdRng;
use rand::{thread_rng, RngCore, SeedableRng};

/**
 * Tests the optimizer:
 * - Creates data of the structure described below and inserts it into a SdmsIcebergEngine
 * - Creates 3 queries of the desribed structure, as well as 3 Workloads (description) of those
 * - Executes the queries on the engine
 * - Call Optimizer::re_cluster
 * - Executes the queries on the engine again
 *
 *  The data for the benchmark of the optimizer has the following schema:
 *
 *  Customer: vec![TypeID::RowID,TypeID::Int,TypeID::Int,TypeID::Int,TypeID::Int])
 *      Attributes: "cid,age,region,x,y"
 *          e.g. a customer id as primary key, customer age, customer region, and two integer attributes with large domains
 *
 *  Order: vec![TypeID::RowID,TypeID::RowID,TypeID::Int,TypeID::Int]
 *      Attributes: "oid,cid,amount,tax"
 *          e.g. an order ID as primary key, a foreign key reference to a customer, an amount owed for the order and tax payed
 */
fn bench_queries(c: &mut Criterion) {
    unimplemented!();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(std::time::Duration::from_secs(3))
        .measurement_time(std::time::Duration::from_secs(180))
        .sample_size(50);
    targets = bench_queries
}
criterion_main!(benches);
