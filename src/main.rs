use sdms_db::engine::SdmsIcebergEngine;
use sdms_db::iceberg::{Catalog, TableMetadata};
use sdms_db::storage::FileBasedStorage;
use sdms_db::{RowID, TypeID, Value};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::rc::Rc;

use sdms_db::engine::operators::{AggFunc, ColumnAggregate, ColumnEqJoin, ColumnFilter, ColumnTableScan};
use sdms_db::Operator;
use sdms_db::storage::Columns;

fn main() {
    let data_path = PathBuf::from("./sdms_data");
    let storage = FileBasedStorage::new(data_path);
    let catalog = Catalog::default();
    let mut engine = SdmsIcebergEngine::new(catalog, storage);

    println!("Welcome to SDMS-DB CLI");
    println!("Type 'help' for commands.");

    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();
        match parts[0] {
            "exit" => break,
            "help" => print_help(),
            "create_table" => handle_create_table(&mut engine, &parts[1..]),
            "insert" => handle_insert(&mut engine, &parts[1..]),
            "scan" => handle_scan(&mut engine, &parts[1..]),
            "filter" => handle_filter(&mut engine, &parts[1..]),
            "aggregate" => handle_aggregate(&mut engine, &parts[1..]),
            "join" => handle_join(&mut engine, &parts[1..]),
            "populate" => handle_populate(&mut engine),
            _ => println!("Unknown command. Type 'help' for available commands."),
        }
    }
}

fn print_help() {
    println!("Available commands:");
    println!("  create_table <name> <type1> <type2> ...  - Create a new table. Types: Int, UInt, RowID, Varchar");
    println!("  insert <table_id> <val1> <val2> ...      - Insert a row into a table");
    println!("  scan <table_id>                          - Scan and print all rows in a table");
    println!("  filter <table_id> <col_idx> <min> <max>  - Filter rows where min <= col <= max");
    println!("  aggregate <table_id> <col_idx> <func>    - Aggregate column. Funcs: count, sum, min, max");
    println!("  join <t1_id> <c1_idx> <t2_id> <c2_idx>   - Inner join two tables on specified columns");
    println!("  populate                                 - Create sample tables and insert sample data");
    println!("  exit                                     - Exit the CLI");
}

fn parse_value(type_id: TypeID, val_str: &str) -> Option<Value> {
    match type_id {
        TypeID::Int => val_str.parse::<i32>().ok().map(Value::Int),
        TypeID::UInt => val_str.parse::<u32>().ok().map(Value::UInt),
        TypeID::RowID => val_str.parse::<u64>().ok().map(|v| Value::RowID(RowID(v))),
        TypeID::Varchar => Some(Value::Varchar(Rc::new(val_str.to_string()))),
    }
}

fn execute_operator(mut op: Box<dyn Operator>) {
    op.open();
    let mut total_rows = 0;
    while let Some(chunk) = op.next() {
        if chunk.is_empty() { continue; }
        let num_rows = chunk[0].len();
        total_rows += num_rows;
        for row_idx in 0..num_rows {
            let mut row_str = String::new();
            for col in &chunk {
                row_str.push_str(&format!("{} | ", col[row_idx]));
            }
            println!("{}", row_str.trim_end_matches(" | "));
        }
    }
    op.close();
    println!("Total rows: {}", total_rows);
}





fn handle_create_table(engine: &mut SdmsIcebergEngine, args: &[&str]) {
    if args.len() < 2 {
        println!("Usage: create_table <name> <type1> <type2> ...");
        return;
    }

    let name = args[0].to_string();
    let mut schema = Vec::new();

    for type_str in &args[1..] {
        match *type_str {
            "Int" => schema.push(TypeID::Int),
            "UInt" => schema.push(TypeID::UInt),
            "RowID" => schema.push(TypeID::RowID),
            "Varchar" => schema.push(TypeID::Varchar),
            _ => {
                println!("Unknown type: {}", type_str);
                return;
            }
        }
    }

    let metadata = TableMetadata::new(name.clone(), schema);
    let id = engine.catalog.add_table(metadata);
    println!("Table '{}' created with ID: {}", name, id);
}

fn handle_insert(engine: &mut SdmsIcebergEngine, args: &[&str]) {
    if args.len() < 2 {
        println!("Usage: insert <table_id> <val1> <val2> ...");
        return;
    }

    let table_id: usize = match args[0].parse() {
        Ok(id) => id,
        Err(_) => {
            println!("Invalid table ID");
            return;
        }
    };

    if !engine.catalog.check_table_exists(table_id) {
        println!("Table ID {} does not exist", table_id);
        return;
    }

    let schema = engine.catalog.get_table_metadata(table_id).schema.clone();
    if args.len() - 1 != schema.len() {
        println!(
            "Column count mismatch. Expected {}, got {}",
            schema.len(),
            args.len() - 1
        );
        return;
    }

    let mut row_values = Vec::new();
    for (i, val_str) in args[1..].iter().enumerate() {
        let val = match schema[i] {
            TypeID::Int => match val_str.parse::<i32>() {
                Ok(v) => Value::Int(v),
                Err(_) => {
                    println!("Invalid Int value: {}", val_str);
                    return;
                }
            },
            TypeID::UInt => match val_str.parse::<u32>() {
                Ok(v) => Value::UInt(v),
                Err(_) => {
                    println!("Invalid UInt value: {}", val_str);
                    return;
                }
            },
            TypeID::RowID => match val_str.parse::<u64>() {
                Ok(v) => Value::RowID(RowID(v)),
                Err(_) => {
                    println!("Invalid RowID value: {}", val_str);
                    return;
                }
            },
            TypeID::Varchar => Value::Varchar(Rc::new(val_str.to_string())),
        };
        row_values.push(val);
    }

    // Convert row to column-based chunk (1 row)
    let mut chunk = Vec::new();
    for val in row_values {
        chunk.push(vec![val]);
    }

    if let Err(e) = engine.start_table_modification(table_id) {
        println!("Error starting modification: {:?}", e);
        return;
    }

    if let Err(e) = engine.insert(vec![chunk]) {
        println!("Error inserting data: {:?}", e);
        // Try to commit anyway to clean up or just return?
        // In this simple engine, we might need to reset state if insert fails, but let's just try commit.
    }

    if let Err(e) = engine.commit() {
        println!("Error committing transaction: {:?}", e);
    } else {
        println!("Insert successful");
    }
}

fn handle_scan(engine: &mut SdmsIcebergEngine, args: &[&str]) {
    if args.len() < 1 {
        println!("Usage: scan <table_id>");
        return;
    }

    let table_id: usize = match args[0].parse() {
        Ok(id) => id,
        Err(_) => {
            println!("Invalid table ID");
            return;
        }
    };

    if !engine.catalog.check_table_exists(table_id) {
        println!("Table ID {} does not exist", table_id);
        return;
    }

    let metadata = engine.catalog.get_table_metadata(table_id);
    let files = metadata.files(None);
    
    println!("Scanning table {} ({} files)...", table_id, files.len());
    
    let mut total_rows = 0;

    for file_handle in files {
        match engine.storage.read_file(&file_handle) {
            Ok(mut reader) => {
                match sdms_db::storage::DataFile::parse(&mut reader) {
                    Ok(data_file) => {
                        let chunk = data_file.data;
                        if chunk.is_empty() { continue; }
                        
                        let num_rows = chunk[0].len();
                        total_rows += num_rows;

                        // Print rows
                        for row_idx in 0..num_rows {
                            let mut row_str = String::new();
                            for col in &chunk {
                                row_str.push_str(&format!("{} | ", col[row_idx]));
                            }
                            println!("{}", row_str.trim_end_matches(" | "));
                        }
                    }
                    Err(e) => println!("Error parsing file: {:?}", e),
                }
            }
            Err(e) => println!("Error reading file: {:?}", e),
        }
    }
    println!("Total rows: {}", total_rows);
}

fn handle_filter(engine: &mut SdmsIcebergEngine, args: &[&str]) {
    if args.len() < 4 {
        println!("Usage: filter <table_id> <col_idx> <min> <max>");
        return;
    }
    let table_id: usize = match args[0].parse() {
        Ok(id) => id,
        Err(_) => { println!("Invalid table ID"); return; }
    };
    let col_idx: usize = match args[1].parse() {
        Ok(id) => id,
        Err(_) => { println!("Invalid column index"); return; }
    };

    if !engine.catalog.check_table_exists(table_id) {
        println!("Table ID {} does not exist", table_id);
        return;
    }

    let metadata = engine.catalog.get_table_metadata(table_id);
    let schema = &metadata.schema;
    
    if col_idx >= schema.len() {
        println!("Column index out of bounds");
        return;
    }

    let min_val = match parse_value(schema[col_idx].clone(), args[2]) {
        Some(v) => v,
        None => { println!("Invalid min value"); return; }
    };
    let max_val = match parse_value(schema[col_idx].clone(), args[3]) {
        Some(v) => v,
        None => { println!("Invalid max value"); return; }
    };

    let files: Vec<_> = metadata.files(None).into_iter().collect();
    let scan = ColumnTableScan::new(files, Columns::All, engine.storage.clone());
    
    let mut filters = HashMap::new();
    filters.insert(col_idx, (min_val, max_val));
    
    let filter_op = ColumnFilter::new(Box::new(scan), filters);
    println!("Executing filter...");
    execute_operator(Box::new(filter_op));
}

fn handle_aggregate(engine: &mut SdmsIcebergEngine, args: &[&str]) {
    if args.len() < 3 {
        println!("Usage: aggregate <table_id> <col_idx> <func>");
        return;
    }
    let table_id: usize = match args[0].parse() {
        Ok(id) => id,
        Err(_) => { println!("Invalid table ID"); return; }
    };
    let col_idx: usize = match args[1].parse() {
        Ok(id) => id,
        Err(_) => { println!("Invalid column index"); return; }
    };
    let func_name = args[2];

    if !engine.catalog.check_table_exists(table_id) {
        println!("Table ID {} does not exist", table_id);
        return;
    }

    let metadata = engine.catalog.get_table_metadata(table_id);
    let files: Vec<_> = metadata.files(None).into_iter().collect();
    let scan = ColumnTableScan::new(files, Columns::All, engine.storage.clone());

    let agg_func: AggFunc = match func_name {
        "count" => Box::new(|col: &Vec<Value>| Value::Int(col.len() as i32)),
        "sum" => Box::new(|col: &Vec<Value>| {
            let mut sum_i = 0;
            let mut sum_u = 0;
            let mut is_int = false;
            for val in col {
                match val {
                    Value::Int(v) => { sum_i += v; is_int = true; },
                    Value::UInt(v) => { sum_u += v; },
                    _ => {}
                }
            }
            if is_int { Value::Int(sum_i) } else { Value::UInt(sum_u) }
        }),
        "min" => Box::new(|col: &Vec<Value>| {
            if col.is_empty() { return Value::Int(0); }
            let mut min = &col[0];
            for val in col {
                if val < min { min = val; }
            }
            min.clone()
        }),
        "max" => Box::new(|col: &Vec<Value>| {
            if col.is_empty() { return Value::Int(0); }
            let mut max = &col[0];
            for val in col {
                if val > max { max = val; }
            }
            max.clone()
        }),
        _ => {
            println!("Unknown aggregate function: {}", func_name);
            return;
        }
    };

    let mut aggregates = HashMap::new();
    aggregates.insert(col_idx, agg_func);

    let agg_op = ColumnAggregate::new(Box::new(scan), aggregates);
    println!("Executing aggregate...");
    execute_operator(Box::new(agg_op));
}

fn handle_join(engine: &mut SdmsIcebergEngine, args: &[&str]) {
    if args.len() < 4 {
        println!("Usage: join <t1_id> <c1_idx> <t2_id> <c2_idx>");
        return;
    }
    let t1_id: usize = match args[0].parse() { Ok(id) => id, Err(_) => return };
    let c1_idx: usize = match args[1].parse() { Ok(id) => id, Err(_) => return };
    let t2_id: usize = match args[2].parse() { Ok(id) => id, Err(_) => return };
    let c2_idx: usize = match args[3].parse() { Ok(id) => id, Err(_) => return };

    if !engine.catalog.check_table_exists(t1_id) || !engine.catalog.check_table_exists(t2_id) {
        println!("Table does not exist");
        return;
    }

    let meta1 = engine.catalog.get_table_metadata(t1_id);
    let files1: Vec<_> = meta1.files(None).into_iter().collect();
    let scan1 = ColumnTableScan::new(files1, Columns::All, engine.storage.clone());

    let meta2 = engine.catalog.get_table_metadata(t2_id);
    let files2: Vec<_> = meta2.files(None).into_iter().collect();
    let scan2 = ColumnTableScan::new(files2, Columns::All, engine.storage.clone());

    let join_op = ColumnEqJoin::new(Box::new(scan1), Box::new(scan2), (c1_idx, c2_idx));
    println!("Executing join...");
    execute_operator(Box::new(join_op));
}

fn handle_populate(engine: &mut SdmsIcebergEngine) {
    // Create Students table
    // Schema: RowID, Name (Varchar), Age (UInt), Score (Int)
    let students_schema = vec![TypeID::RowID, TypeID::Varchar, TypeID::UInt, TypeID::Int];
    let students_meta = TableMetadata::new("Students".to_string(), students_schema);
    let students_id = engine.catalog.add_table(students_meta);
    println!("Created 'Students' table with ID: {}", students_id);

    // Insert sample students
    let students_data = vec![
        vec![Value::RowID(RowID(1)), Value::Varchar(Rc::new("Alice".to_string())), Value::UInt(20), Value::Int(95)],
        vec![Value::RowID(RowID(2)), Value::Varchar(Rc::new("Bob".to_string())), Value::UInt(21), Value::Int(88)],
        vec![Value::RowID(RowID(3)), Value::Varchar(Rc::new("Charlie".to_string())), Value::UInt(22), Value::Int(75)],
        vec![Value::RowID(RowID(4)), Value::Varchar(Rc::new("David".to_string())), Value::UInt(20), Value::Int(92)],
    ];

    insert_rows(engine, students_id, students_data);

    // Create Courses table
    // Schema: RowID, Title (Varchar), Credits (UInt)
    let courses_schema = vec![TypeID::RowID, TypeID::Varchar, TypeID::UInt];
    let courses_meta = TableMetadata::new("Courses".to_string(), courses_schema);
    let courses_id = engine.catalog.add_table(courses_meta);
    println!("Created 'Courses' table with ID: {}", courses_id);

    // Insert sample courses
    let courses_data = vec![
        vec![Value::RowID(RowID(101)), Value::Varchar(Rc::new("Database Systems".to_string())), Value::UInt(6)],
        vec![Value::RowID(RowID(102)), Value::Varchar(Rc::new("Operating Systems".to_string())), Value::UInt(6)],
        vec![Value::RowID(RowID(103)), Value::Varchar(Rc::new("Algorithms".to_string())), Value::UInt(8)],
    ];

    insert_rows(engine, courses_id, courses_data);
}

fn insert_rows(engine: &mut SdmsIcebergEngine, table_id: usize, rows: Vec<Vec<Value>>) {
    if rows.is_empty() { return; }
    
    let num_cols = rows[0].len();
    let mut chunk = vec![Vec::new(); num_cols];

    for row in rows {
        for (i, val) in row.into_iter().enumerate() {
            chunk[i].push(val);
        }
    }

    engine.start_table_modification(table_id).unwrap();
    engine.insert(vec![chunk]).unwrap();
    engine.commit().unwrap();
    println!("Inserted {} rows into table {}", num_cols, table_id);
}
