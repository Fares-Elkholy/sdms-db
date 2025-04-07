use crate::storage::{data, Columns, DataFile};
use crate::storage::{FileBasedStorage, FileHandle};
use crate::Operator;
use crate::{TableChunk, Value};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;
use std::iter::Filter;
use std::{default, result, vec};

pub struct ColumnTableScan {
    opened: bool,
    files: VecDeque<FileHandle>, 
    col_project: Columns, 
    storage: FileBasedStorage
}

impl ColumnTableScan {
    pub fn new(files: Vec<FileHandle>, col_project: Columns, storage: FileBasedStorage) -> Self {
        ColumnTableScan {
            opened: false,
            files: VecDeque::from(files),
            col_project,
            storage,
        }
    }
}

impl Operator for ColumnTableScan {
    // Initialise child operators and itself.
    fn open(&mut self) {
        self.opened = true; 
    }
    // Get next chunk (part of table), in this case the columns of one file.
    // Returns [None] if there are no more files to read.
    fn next(&mut self) -> Option<TableChunk> {
        if !self.opened {
            return None;
        }
        
        let curr_fh = self.files.pop_front()?;
        let mut file = self.storage.read_file(&curr_fh).ok()?;
        
        // Use DataFile's parse_columns which already handles projection
        let datafile = DataFile::parse_columns(&mut file, self.col_project.clone()).ok()?;
        
        // No need for additional column processing, parse_columns already handles it
        Some(datafile.data)
    }
    // Cleans up child and itself.
    fn close(&mut self) {
        self.opened = false;
    }
}

/**
 * An operator for conjunctive filters.
 */
pub struct ColumnFilter {
    opened: bool,
    // vec to store current chunks

    child: Box<dyn Operator>,
    filters: Vec<(usize, (Value, Value))>,
}
impl ColumnFilter {
    pub fn new(child: Box<dyn Operator>, filters: HashMap<usize, (Value, Value)>) -> Self {
        ColumnFilter {
            opened: false,
            child: child,
            filters: filters.into_iter().collect(),
        }        
    }
}
impl Operator for ColumnFilter {
    // Initialise child operators and itself.
    fn open(&mut self) {
        self.child.open();
        self.opened = true;
    }
    // Get next chunk (part of table), filtered down to respective rows.
    // Returns [None] if there are no more chunks to read.
    // Preserve the order of rows per chunk.
    fn next(&mut self) -> Option<TableChunk> {
        if !self.opened {
            self.child.open();
            self.opened = true;
        }
    
        if let Some(mut table_chunk) = self.child.next() {
            if table_chunk.is_empty() {
                return Some(table_chunk);
            }

            // Find the maximum row count across all columns
            let row_count = table_chunk.iter().map(|col| col.len()).max().unwrap_or(0);
            if row_count == 0 {
                return Some(table_chunk);
            }
            
            // Bitmap of rows to keep (true = keep)
            let mut rows_to_keep = vec![true; row_count];
            
            // Fast path: check if we have any filters at all
            if !self.filters.is_empty() {
                // Apply filters with early termination for each row
                'row_loop: for row_idx in 0..row_count {
                    for (col_idx, (min, max)) in &self.filters {
                        if *col_idx >= table_chunk.len() {
                            continue;
                        }
                        
                        // Early termination if column is shorter than row_idx
                        if row_idx >= table_chunk[*col_idx].len() {
                            continue;
                        }
                        
                        let value = &table_chunk[*col_idx][row_idx];
                        if value < min || value > max {
                            rows_to_keep[row_idx] = false;
                            continue 'row_loop; // Skip remaining filter checks for this row
                        }
                    }
                }
            }
            
            // Count how many rows will remain to pre-allocate vectors
            let remaining_rows = rows_to_keep.iter().filter(|&&keep| keep).count();
            
            // Apply filtering to all columns with pre-allocated vectors
            for col in &mut table_chunk {
                if col.is_empty() {
                    continue;
                }
                
                let mut new_col = Vec::with_capacity(remaining_rows);
                let col_len = col.len();
                
                for row_idx in 0..row_count {
                    if row_idx < col_len && rows_to_keep[row_idx] {
                        new_col.push(col[row_idx].clone());
                    }
                }
                
                *col = new_col;
            }
            
            Some(table_chunk)
        } else {
            None
        }
    }
    // Cleans up child and itself.
    fn close(&mut self) {
        self.child.close();
        self.opened = false;
    }
}

pub type AggFunc = Box<dyn Fn(&Vec<Value>) -> Value>;

pub struct ColumnAggregate {
    opened: bool,

    child: Box<dyn Operator>,

    aggregates: Vec<(usize, AggFunc)>,

}
impl ColumnAggregate {
    pub fn new(child: Box<dyn Operator>, aggregates: HashMap<usize, AggFunc>) -> Self {
        // Convert the HashMap to a Vec and sort it by column index
        let mut agg_vec: Vec<(usize, AggFunc)> = aggregates.into_iter().collect();
        agg_vec.sort_by(|a, b| a.0.cmp(&b.0));
        ColumnAggregate {
            opened: false,
            child,
            aggregates: agg_vec,
        }
    }
}
impl Operator for ColumnAggregate {
    // Initialise child operators and itself.
    fn open(&mut self) {
        self.child.open();
        self.opened = true;
    }
    // Returns chunk of the projected and aggregated column values.
    // Preserve the order of aggregation columns.
    fn next(&mut self) -> Option<TableChunk> {
        if !self.opened {
            return None;
        }
        
        if self.aggregates.is_empty() {
            self.opened = false;
            return Some(vec![]);
        }
        
        // Prepare result vectors - one per aggregate function
        let mut result_chunk: TableChunk = vec![];
        for _ in &self.aggregates {
            result_chunk.push(vec![]);
        }
        
        // Process all chunks from child
        let mut has_data = false;
        while let Some(chunk) = self.child.next() {
            has_data = true;
            
            // For each aggregate, collect the values from this chunk
            for (idx, (col_idx, _)) in self.aggregates.iter().enumerate() {
                if *col_idx < chunk.len() && !chunk[*col_idx].is_empty() {
                    // Append values to corresponding result vector
                    if result_chunk[idx].is_empty() {
                        // First chunk with data for this column
                        result_chunk[idx] = chunk[*col_idx].clone();
                    } else {
                        // Append to existing data
                        result_chunk[idx].extend(chunk[*col_idx].clone());
                    }
                }
            }
        }
        
        // Apply aggregation functions to collected values
        let mut final_result: TableChunk = vec![];
        for (idx, (_, agg_fn)) in self.aggregates.iter().enumerate() {
            if !result_chunk[idx].is_empty() {
                let agg = agg_fn(&result_chunk[idx]);
                final_result.push(vec![agg]);
            } else {
                // No data for this column
                final_result.push(vec![]);
            }
        }
        
        self.opened = false;
        
        if has_data {
            Some(final_result)
        } else {
            Some(vec![])
        }
    }
    // Cleans up child and itself.
    fn close(&mut self) {
        self.child.close();
        self.opened = false;
    }
}

pub struct ColumnEqJoin {
    child0: Box<dyn Operator>,
    child1: Box<dyn Operator>,
    join_column_idxs: (usize, usize),
    hash_table: HashMap<Value, Vec<Vec<Value>>>,
    child1_chunks: Vec<TableChunk>,
    current_chunk_idx: usize,
    opened: bool,
}
impl ColumnEqJoin {
    pub fn new(
        child0: Box<dyn Operator>,
        child1: Box<dyn Operator>,
        join_column_idxs: (usize, usize),
    ) -> Self {
        ColumnEqJoin {
            child0,
            child1,
            join_column_idxs,
            hash_table: HashMap::new(),
            child1_chunks: Vec::new(),
            current_chunk_idx: 0,
            opened: false,
        }
    }
}
impl Operator for ColumnEqJoin {
    // Initialise child operators and itself.
    fn open(&mut self) {
        self.child0.open();
        self.child1.open();
        
        // Build hash table for child0 chunks
        let (child0_col_idx, _) = self.join_column_idxs;
        
        // Process all chunks from child0
        while let Some(chunk) = self.child0.next() {
            // Skip if chunk is empty or doesn't have the join column
            if chunk.is_empty() || chunk.len() <= child0_col_idx || chunk[child0_col_idx].is_empty() {
                continue;
            }
            
            // For each row in the chunk
            let rows_count = chunk[0].len();
            for row_idx in 0..rows_count {
                // Get the join key
                let join_key = chunk[child0_col_idx].get(row_idx);
                if join_key.is_none() {
                    continue;
                }
                
                // Build a row with values from all columns
                let mut row = Vec::new();
                for col in &chunk {
                    if let Some(value) = col.get(row_idx) {
                        row.push(value.clone());
                    }
                }
                
                // Add row to hash table
                self.hash_table
                    .entry(join_key.unwrap().clone())
                    .or_insert_with(Vec::new)
                    .push(row);
            }
        }
        
        // Reset child1 to get all chunks
        self.child1.close();
        self.child1.open();
        
        // Store all child1 chunks
        self.child1_chunks.clear();
        while let Some(chunk) = self.child1.next() {
            self.child1_chunks.push(chunk);
        }
        
        self.current_chunk_idx = 0;
        self.opened = true;
    }
    // Get next chunk of child0 rows joined with child1 rows.
    // Preserve the order of rows of each of child1's chunks (between chunks and inside chunks).
    fn next(&mut self) -> Option<TableChunk> {
        if !self.opened {
            self.open();
        }
        
        // Return None if we've processed all child1 chunks
        if self.current_chunk_idx >= self.child1_chunks.len() {
            return None;
        }
        
        let child1_chunk = &self.child1_chunks[self.current_chunk_idx];
        self.current_chunk_idx += 1;
        
        // If child1_chunk is empty or doesn't have the join column, return empty result
        let (_, child1_col_idx) = self.join_column_idxs;
        if child1_chunk.is_empty() || child1_chunk.len() <= child1_col_idx || child1_chunk[child1_col_idx].is_empty() {
            // Return a chunk with empty columns for each column in both children
            let total_cols = if child1_chunk.is_empty() { 0 } else { child1_chunk.len() * 2 };
            let empty_chunk = vec![vec![]; total_cols];
            return Some(empty_chunk);
        }
        
        // Create result chunk with all columns from both children
        let mut result_chunk = Vec::new();
        
        // Calculate how many rows we'll have after join
        let mut row_count = 0;
        let rows_in_child1 = child1_chunk[0].len();
        for row_idx in 0..rows_in_child1 {
            let join_key = &child1_chunk[child1_col_idx][row_idx];
            if let Some(matching_rows) = self.hash_table.get(join_key) {
                row_count += matching_rows.len();
            }
        }
        
        // If no matches found, return empty columns
        if row_count == 0 {
            let total_cols = child1_chunk.len() * 2; // Assuming both sides have same number of columns
            let empty_chunk = vec![vec![]; total_cols];
            return Some(empty_chunk);
        }
        
        // Initialize result columns (child0 columns + child1 columns)
        let mut child0_col_count = 0;
        for rows in self.hash_table.values() {
            if !rows.is_empty() {
                child0_col_count = rows[0].len();
                break;
            }
        }
        
        // Initialize result columns
        for _ in 0..child0_col_count {
            result_chunk.push(Vec::with_capacity(row_count));
        }
        for _ in 0..child1_chunk.len() {
            result_chunk.push(Vec::with_capacity(row_count));
        }
        
        // Perform the join by iterating through child1 rows
        for row_idx in 0..rows_in_child1 {
            let join_key = &child1_chunk[child1_col_idx][row_idx];
            
            if let Some(matching_rows) = self.hash_table.get(join_key) {
                for child0_row in matching_rows {
                    // Add child0 column values
                    for (col_idx, value) in child0_row.iter().enumerate() {
                        result_chunk[col_idx].push(value.clone());
                    }
                    
                    // Add child1 column values
                    for (col_idx, col) in child1_chunk.iter().enumerate() {
                        result_chunk[child0_col_count + col_idx].push(col[row_idx].clone());
                    }
                }
            }
        }
        
        Some(result_chunk)
    }

    // Cleans up child and itself.
    fn close(&mut self) {
        self.child0.close();
        self.child1.close();
        self.hash_table.clear();
        self.child1_chunks.clear();
        self.current_chunk_idx = 0;
        self.opened = false; 
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::operators::*;
    use crate::Operator;
    use crate::{TableChunk, Value};
    use std::cmp::max;
    use uuid::Uuid;

    pub struct DummyTableScan {
        chunks: Vec<TableChunk>,
        chunks_idx: usize,
        opened: bool,
    }

    impl DummyTableScan {
        pub fn new(chunks: Vec<TableChunk>) -> Self {
            Self {
                chunks: chunks.clone(),
                chunks_idx: 0,
                opened: false,
            }
        }
    }

    impl Operator for DummyTableScan {
        fn open(&mut self) {
            self.chunks_idx = 0;
            self.opened = true;
        }
        fn next(&mut self) -> Option<TableChunk> {
            if !self.opened {
                panic!("Operator not open")
            }

            let result = if self.chunks_idx < self.chunks.len() {
                Some(self.chunks[self.chunks_idx].clone())
            } else {
                None
            };

            self.chunks_idx += 1;

            result
        }
        fn close(&mut self) {
            self.chunks_idx = 0;
            self.opened = false;
        }
    }

    #[test]
    fn test_column_filter_empty() {
        let data = vec![
            vec![vec![], vec![Value::UInt(3), Value::UInt(4), Value::UInt(5)]],
            vec![vec![], vec![Value::UInt(6), Value::UInt(7), Value::UInt(8)]],
            vec![vec![], vec![]],
        ];
        let dts = DummyTableScan::new(data.clone());
        let mut colf = ColumnFilter::new(
            Box::new(dts),
            HashMap::from([(1 as usize, (Value::UInt(40), Value::UInt(70)))]),
        );
        colf.open();

        let mut results = vec![];

        while let Some(chunk) = colf.next() {
            results.push(chunk);
        }

        let expected_data: Vec<Vec<Vec<Value>>> = vec![
            vec![vec![], vec![]],
            vec![vec![], vec![]],
            vec![vec![], vec![]],
        ];

        assert_eq! {results.len(), expected_data.len(), "Wrong number of result chunks"};
        for (idx, item) in results.iter().enumerate() {
            assert_eq! {item, &expected_data[idx], "Wrong chunk data"};
        }
    }

    #[test]
    fn test_column_filter() {
        let data = vec![
            vec![
                vec![Value::Int(-3), Value::Int(-4), Value::Int(-5)],
                vec![Value::UInt(3), Value::UInt(4), Value::UInt(5)],
            ],
            vec![
                vec![Value::Int(-6), Value::Int(-7), Value::Int(-8)],
                vec![Value::UInt(6), Value::UInt(7), Value::UInt(8)],
            ],
        ];
        let dts = DummyTableScan::new(data.clone());
        let mut colf = ColumnFilter::new(
            Box::new(dts),
            HashMap::from([(1 as usize, (Value::UInt(4), Value::UInt(7)))]),
        );
        colf.open();

        let mut results = vec![];

        while let Some(chunk) = colf.next() {
            results.push(chunk);
        }

        let expected_data = vec![
            vec![
                vec![Value::Int(-4), Value::Int(-5)],
                vec![Value::UInt(4), Value::UInt(5)],
            ],
            vec![
                vec![Value::Int(-6), Value::Int(-7)],
                vec![Value::UInt(6), Value::UInt(7)],
            ],
        ];

        assert_eq! {results.len(), expected_data.len(), "Wrong number of result chunks"};
        for (idx, item) in results.iter().enumerate() {
            assert_eq! {item, &expected_data[idx], "Wrong chunk data"};
        }
    }

    #[test]
    fn test_column_filter_string() {
        let data = vec![
            vec![
                vec![Value::Int(-3), Value::Int(-4), Value::Int(-5)],
                vec![
                    Value::Varchar(String::from("3").into()),
                    Value::Varchar(String::from("4").into()),
                    Value::Varchar(String::from("5").into()),
                ],
            ],
            vec![
                vec![Value::Int(-6), Value::Int(-7), Value::Int(-8)],
                vec![
                    Value::Varchar(String::from("6").into()),
                    Value::Varchar(String::from("7").into()),
                    Value::Varchar(String::from("8").into()),
                ],
            ],
        ];
        let dts = DummyTableScan::new(data.clone());
        let mut colf = ColumnFilter::new(
            Box::new(dts),
            HashMap::from([(
                1 as usize,
                (
                    Value::Varchar(String::from("4").into()),
                    Value::Varchar(String::from("7").into()),
                ),
            )]),
        );
        colf.open();

        let mut results = vec![];

        while let Some(chunk) = colf.next() {
            results.push(chunk);
        }

        let expected_data = vec![
            vec![
                vec![Value::Int(-4), Value::Int(-5)],
                vec![
                    Value::Varchar(String::from("4").into()),
                    Value::Varchar(String::from("5").into()),
                ],
            ],
            vec![
                vec![Value::Int(-6), Value::Int(-7)],
                vec![
                    Value::Varchar(String::from("6").into()),
                    Value::Varchar(String::from("7").into()),
                ],
            ],
        ];

        assert_eq! {results.len(), expected_data.len(), "Wrong number of result chunks"};
        for (idx, item) in results.iter().enumerate() {
            assert_eq! {item, &expected_data[idx], "Wrong chunk data"};
        }
    }

    #[test]
    fn test_column_aggregate_empty() {
        let data = vec![
            vec![vec![Value::Int(-3), Value::Int(-4), Value::Int(-5)], vec![]],
            vec![vec![Value::Int(-6), Value::Int(-7), Value::Int(-8)], vec![]],
        ];
        let dts = DummyTableScan::new(data.clone());
        let mut cola = ColumnAggregate::new(Box::new(dts), HashMap::from([]));
        cola.open();

        let mut results = vec![];

        while let Some(chunk) = cola.next() {
            results.push(chunk);
        }

        let expected_data: Vec<Vec<Vec<Value>>> = vec![vec![]];

        assert_eq! {results.len(), 1, "Wrong number of result chunks"};
        for (idx, item) in results.iter().enumerate() {
            assert_eq! {item, &expected_data[idx], "Wrong chunk data"};
        }
    }

    #[test]
    fn test_column_aggregate() {
        let data = vec![
            vec![
                vec![Value::Int(-3), Value::Int(-4), Value::Int(-5)],
                vec![Value::UInt(3), Value::UInt(4), Value::UInt(5)],
            ],
            vec![
                vec![Value::Int(-6), Value::Int(-7), Value::Int(-8)],
                vec![Value::UInt(6), Value::UInt(7), Value::UInt(8)],
            ],
        ];
        let dts = DummyTableScan::new(data.clone());
        let mut cola = ColumnAggregate::new(
            Box::new(dts),
            HashMap::from([
                // sum column 0
                (
                    0 as usize,
                    Box::new(|v: &Vec<Value>| {
                        let mut sum: i32 = 0;
                        for value in v.iter() {
                            sum += TryInto::<i32>::try_into(value.clone()).unwrap();
                        }
                        Value::Int(sum)
                    }) as AggFunc,
                ),
                // max
                (
                    1 as usize,
                    Box::new(|v: &Vec<Value>| {
                        let mut m: u32 = 0;
                        for value in v.iter() {
                            m = max(m, TryInto::<u32>::try_into(value.clone()).unwrap());
                        }
                        Value::UInt(m)
                    }) as AggFunc,
                ),
            ]),
        );
        cola.open();

        let mut results = vec![];

        while let Some(chunk) = cola.next() {
            results.push(chunk);
        }

        let expected_data = vec![vec![vec![Value::Int(-33)], vec![Value::UInt(8)]]];

        assert_eq! {results.len(), expected_data.len(),"Wrong number of result chunks"};
        for (idx, item) in results.iter().enumerate() {
            assert_eq! {item, &expected_data[idx], "Wrong chunk data"};
        }
    }

    #[test]
    fn test_column_aggregate_string() {
        let data = vec![
            vec![
                vec![Value::Int(-3), Value::Int(-4), Value::Int(-5)],
                vec![
                    Value::Varchar(String::from("a").into()),
                    Value::Varchar(String::from("b").into()),
                    Value::Varchar(String::from("c").into()),
                ],
            ],
            vec![
                vec![Value::Int(-6), Value::Int(-7), Value::Int(-8)],
                vec![
                    Value::Varchar(String::from("d").into()),
                    Value::Varchar(String::from("e").into()),
                    Value::Varchar(String::from("f").into()),
                ],
            ],
        ];
        let dts = DummyTableScan::new(data.clone());
        let mut cola = ColumnAggregate::new(
            Box::new(dts),
            HashMap::from([
                //max
                (
                    1 as usize,
                    Box::new(|v: &Vec<Value>| {
                        let mut m = Value::Varchar(String::from("\0").into());
                        for value in v.iter() {
                            m = max(m, value.clone());
                        }
                        m
                    }) as AggFunc,
                ),
            ]),
        );
        cola.open();

        let mut results = vec![];

        while let Some(chunk) = cola.next() {
            results.push(chunk);
        }

        let expected_data = vec![vec![vec![Value::Varchar(String::from("f").into())]]];

        assert_eq! {results.len(), expected_data.len(), "Wrong number of result chunks"};
        for (idx, item) in results.iter().enumerate() {
            assert_eq! {item, &expected_data[idx], "Wrong chunk data"};
        }
    }

    #[test]
    fn test_column_join_empty() {
        let data1 = vec![
            vec![
                vec![Value::Int(-3), Value::Int(-4), Value::Int(-5)],
                vec![Value::UInt(3), Value::UInt(4), Value::UInt(5)],
            ],
            vec![
                vec![Value::Int(-6), Value::Int(-7), Value::Int(-8)],
                vec![Value::UInt(6), Value::UInt(7), Value::UInt(8)],
            ],
        ];
        let data2 = vec![
            vec![
                vec![Value::UInt(300), Value::UInt(308), Value::UInt(400)],
                vec![Value::UInt(13), Value::UInt(15), Value::UInt(14)],
            ],
            vec![vec![], vec![]],
        ];

        let dts1 = DummyTableScan::new(data1.clone());
        let dts2 = DummyTableScan::new(data2.clone());
        let mut colj = ColumnEqJoin::new(Box::new(dts1), Box::new(dts2), (1, 0));
        colj.open();

        let mut results = vec![];

        while let Some(chunk) = colj.next() {
            results.push(chunk);
        }

        let expected_data: Vec<Vec<Vec<Value>>> = vec![
            vec![vec![], vec![], vec![], vec![]],
            vec![vec![], vec![], vec![], vec![]],
        ];

        assert_eq! {results.len(), expected_data.len(), "Wrong number of result chunks"};
        for (idx, item) in results.iter().enumerate() {
            assert_eq! {item, &expected_data[idx], "Wrong data after join"};
        }
    }

    #[test]
    fn test_column_join() {
        let data1 = vec![
            vec![
                vec![Value::Int(-3), Value::Int(-4), Value::Int(-5)],
                vec![Value::UInt(3), Value::UInt(4), Value::UInt(5)],
            ],
            vec![
                vec![Value::Int(-6), Value::Int(-7), Value::Int(-8)],
                vec![Value::UInt(6), Value::UInt(7), Value::UInt(8)],
            ],
        ];
        let data2 = vec![
            vec![
                vec![Value::UInt(3), Value::UInt(8), Value::UInt(4)],
                vec![Value::UInt(13), Value::UInt(15), Value::UInt(14)],
            ],
            vec![
                vec![Value::UInt(3), Value::UInt(8), Value::UInt(4)],
                vec![Value::UInt(13), Value::UInt(15), Value::UInt(14)],
            ],
        ];

        let dts1 = DummyTableScan::new(data1.clone());
        let dts2 = DummyTableScan::new(data2.clone());
        let mut colj = ColumnEqJoin::new(Box::new(dts1), Box::new(dts2), (1, 0));
        colj.open();

        let mut results = vec![];

        while let Some(chunk) = colj.next() {
            results.push(chunk);
        }

        let expected_data = vec![
            vec![
                vec![Value::Int(-3), Value::Int(-8), Value::Int(-4)],
                vec![Value::UInt(3), Value::UInt(8), Value::UInt(4)],
                vec![Value::UInt(3), Value::UInt(8), Value::UInt(4)],
                vec![Value::UInt(13), Value::UInt(15), Value::UInt(14)],
            ],
            vec![
                vec![Value::Int(-3), Value::Int(-8), Value::Int(-4)],
                vec![Value::UInt(3), Value::UInt(8), Value::UInt(4)],
                vec![Value::UInt(3), Value::UInt(8), Value::UInt(4)],
                vec![Value::UInt(13), Value::UInt(15), Value::UInt(14)],
            ],
        ];

        assert_eq! {results.len(), expected_data.len(), "Wrong number of result chunks"};
        for (idx, item) in results.iter().enumerate() {
            assert_eq! {item, &expected_data[idx], "Wrong data after join"};
        }
    }

    #[test]
    fn test_column_tablescan() {
        let data = vec![
            vec![
                vec![Value::Int(-3), Value::Int(-4), Value::Int(-5)],
                vec![Value::UInt(3), Value::UInt(4), Value::UInt(5)],
            ],
            vec![
                vec![Value::Int(-6), Value::Int(-7), Value::Int(-8)],
                vec![Value::UInt(6), Value::UInt(7), Value::UInt(8)],
            ],
        ];

        let fbs = FileBasedStorage::new("./test_files/".into());
        let fhs = vec![
            FileHandle::new(Uuid::parse_str("329c8bbf1a084117bd83a711e7827b94").unwrap()),
            FileHandle::new(Uuid::parse_str("92e96717b94943b087d5e421e29d1017").unwrap()),
        ];

        let mut dts = ColumnTableScan::new(fhs, Columns::All, fbs);
        dts.open();

        let mut results = vec![];
        while let Some(chunk) = dts.next() {
            results.push(chunk);
        }

        assert_eq! {results.len(), data.len(), "Wrong number of result chunks"};
        for (idx, item) in results.iter().enumerate() {
            assert_eq! {item, &data[idx], "Wrong data after table scan"};
        }
    }

    #[test]
    fn test_column_tablescan_with_projection() {
        let fbs = FileBasedStorage::new("./test_files/".into());
        let fhs = vec![
            FileHandle::new(Uuid::parse_str("329c8bbf1a084117bd83a711e7827b94").unwrap()),
            FileHandle::new(Uuid::parse_str("92e96717b94943b087d5e421e29d1017").unwrap()),
        ];

        let mut dts = ColumnTableScan::new(fhs, Columns::from(Some(vec![1])), fbs);
        dts.open();

        let mut results = vec![];

        while let Some(chunk) = dts.next() {
            results.push(chunk);
        }

        let expected_data = vec![
            vec![vec![], vec![Value::UInt(3), Value::UInt(4), Value::UInt(5)]],
            vec![vec![], vec![Value::UInt(6), Value::UInt(7), Value::UInt(8)]],
        ];

        assert_eq! {results.len(), expected_data.len(), "Wrong number of result chunks"};
        for (idx, item) in results.iter().enumerate() {
            assert_eq! {item, &expected_data[idx], "Wrong data after table scan"};
        }
    }
}

// DO NOT REMOVE OR EDIT THE FOLLOWING LINES!
#[cfg(test)]
mod private_tests_operators;
