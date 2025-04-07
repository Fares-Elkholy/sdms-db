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
 * This is a similar data generation than the one the benchmark uses.
 * The schema of the tables for the benchmark is given below.
 *
 * Hint: Change the schema of data generation, the queries and workloads here to check if your optimizer is smart enough to adapt!
 */
pub fn create_basic_customer(
    chunk_size: usize,
    num_records: usize,
) -> (TableMetadata, Vec<TableChunk>) {
    let mut seed_rng = thread_rng();
    let mut seed = [0u8; 32];
    seed_rng.fill_bytes(&mut seed);
    // println!("Seed: {seed:?}");
    let mut rng = StdRng::from_seed(seed);

    let agedist = Uniform::from(10..100);

    let table = TableMetadata::new(String::from("Customer"), vec![TypeID::RowID, TypeID::Int]);
    let mut chunks: Vec<TableChunk> = vec![];

    for cid_start in (0..num_records).step_by(chunk_size) {
        let mut new_chunk = vec![vec![]; table.schema.len()];
        let cid_end = min(cid_start + chunk_size, num_records);

        for cid in cid_start..cid_end {
            new_chunk[0].push(Value::RowID(RowID(cid as u64)));
            new_chunk[1].push(Value::Int(agedist.sample(&mut rng)));
        }
        chunks.push(new_chunk);
    }

    (table, chunks)
}

pub fn create_basic_order(
    chunk_size: usize,
    num_customers: usize,
    num_records: usize,
) -> (TableMetadata, Vec<TableChunk>) {
    let mut seed_rng = thread_rng();
    let mut seed = [0u8; 32];
    seed_rng.fill_bytes(&mut seed);
    // println!("Seed: {seed:?}");
    let mut rng = StdRng::from_seed(seed);

    let table = TableMetadata::new(
        String::from("Order"),
        vec![TypeID::RowID, TypeID::RowID, TypeID::Int],
    );
    let mut chunks: Vec<TableChunk> = vec![];

    let custdist = Uniform::from(0..num_customers);
    let amountdist = Uniform::from(0..20000);

    for oid_start in (0..num_records).step_by(chunk_size) {
        let mut new_chunk = vec![vec![]; table.schema.len()];
        let oid_end = min(oid_start + chunk_size, num_records);

        for oid in oid_start..oid_end {
            let amount = amountdist.sample(&mut rng);
            new_chunk[0].push(Value::RowID(RowID(oid as u64)));
            new_chunk[1].push(Value::RowID(RowID(custdist.sample(&mut rng) as u64)));
            new_chunk[2].push(Value::Int(amount));
        }
        chunks.push(new_chunk);
    }

    (table, chunks)
}

/**
 *  This query computes:
 *     SELECT max(amount) FROM orders,customer WHERE c.cid = o.cid AND c.age >= 20 AND c.age <= 30;
 */
fn get_basic_query(engine: &mut SdmsIcebergEngine) -> Box<dyn Operator> {
    let cust_metadata = engine.catalog.get_table_metadata(0);
    let cust_files = Vec::from_iter(
        cust_metadata.contains_range(vec![(Value::Int(20)..=Value::Int(30), 1)], None),
    );

    let order_metadata = engine.catalog.get_table_metadata(1);
    let order_files = Vec::from_iter(order_metadata.files(None));

    let custscan = ColumnTableScan::new(cust_files.clone(), Columns::All, engine.storage.clone());
    let custfilter = ColumnFilter::new(
        Box::new(custscan),
        HashMap::from([(1 as usize, (Value::Int(20), Value::Int(30)))]),
    );
    let orderscan = ColumnTableScan::new(order_files.clone(), Columns::All, engine.storage.clone());
    let join = ColumnEqJoin::new(Box::new(custfilter), Box::new(orderscan), (0, 1));
    let agg = ColumnAggregate::new(
        Box::new(join),
        HashMap::from([
            // max tax
            (
                4 as usize,
                Box::new(|v: &Vec<Value>| {
                    let mut m: i32 = 0;
                    for value in v.iter() {
                        m = max(m, TryInto::<i32>::try_into(value.clone()).unwrap());
                    }
                    Value::Int(m)
                }) as AggFunc,
            ),
        ]),
    );

    Box::new(agg)
}

/**
 * Use the information contained in the query plan in the optimizer to re-cluster the tables for better pruning.
 */
fn get_basic_workload() -> QueryPlan {
    let w = QueryPlan::Aggregate(
        Box::new(QueryPlan::Join(
            Box::new(QueryPlan::Filter(
                Box::new(QueryPlan::TableScan(0)),
                vec![(1, 90, 1.0 / 9.0)],
            )),
            Box::new(QueryPlan::TableScan(1)),
            (0, 1),
        )),
        vec![8],
    );
    w
}

fn bench_basic_query(c: &mut Criterion) {
    let mut engine = SdmsIcebergEngine::default();

    let (custtable, customer_chunks) = create_basic_customer(1000, 4500);
    let custtable_id = engine.catalog.add_table(custtable);

    let (ordertable, order_chunks) = create_basic_order(1000, 4500, 5235);
    let ordertable_id = engine.catalog.add_table(ordertable);

    engine.start_table_modification(custtable_id).unwrap();
    engine.insert(customer_chunks).unwrap();
    engine.commit().unwrap();

    engine.start_table_modification(ordertable_id).unwrap();
    engine.insert(order_chunks).unwrap();
    engine.commit().unwrap();

    let mut before_op = get_basic_query(&mut engine);
    // before reclustering
    before_op.open();
    let mut expected_result = vec![];
    while let Some(chunk) = before_op.next() {
        // println!("Before: {:?}",chunk);
        expected_result = chunk;
    }
    before_op.close();
    let _ = Optimizer::re_cluster(&mut engine, vec![get_basic_workload()]);

    c.bench_function("Basic Query after Re-clustering", |b| {
        b.iter(|| {
            // after reclustering
            let mut after_op = get_basic_query(&mut engine);

            // after reclustering
            after_op.open();
            while let Some(chunk) = after_op.next() {
                // println!("After: {:?}",chunk);
                assert_eq! {chunk,expected_result};
            }
            after_op.close();
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(std::time::Duration::from_secs(3))
        .measurement_time(std::time::Duration::from_secs(60))
        .sample_size(100);
    targets = bench_basic_query
}
criterion_main!(benches);
