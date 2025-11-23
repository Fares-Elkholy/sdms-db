use lab3::engine::SdmsIcebergEngine;
use lab3::iceberg::{Catalog, TableMetadata};
use lab3::storage::FileBasedStorage;
use lab3::{Record, RowID, TypeID, Value};
use std::io::{self, Write};
use std::path::PathBuf;
use std::rc::Rc;

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
    println!("  populate                                 - Create sample tables and insert sample data");
    println!("  exit                                     - Exit the CLI");
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
                match lab3::storage::DataFile::parse(&mut reader) {
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
