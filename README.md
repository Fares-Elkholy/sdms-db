# SDMS-DB

This project implements a simplified database engine with support for the [Iceberg table format](https://iceberg.apache.org/spec/#overview). It includes a catalog, manifest files, and file-based storage.

## Quick Start

To interact with the database engine directly, you can explore the interactive CLI or run the test suite.

### Running the CLI

```bash
cargo run
```

**Available Commands:**
- `populate`: Creates sample "Students" and "Courses" tables with data.
- `scan <table_id>`: Prints all rows in a table (e.g., `scan 0`).
- `create_table <name> <type1> ...`: Creates a new table.
- `insert <table_id> <val1> ...`: Inserts a row.
- `help`: Lists all commands.

### Running Tests

To run the unit tests:

```bash
cargo test
```

## Project Structure

- `src/engine`: Core database engine logic, including operators and optimizer.
- `src/iceberg`: Implementation of Iceberg metadata (Catalog, Manifest, TableMetadata).
- `src/storage`: File-based storage handling.
- `src/value_cmp.rs`: Value comparison logic.

## Prerequisites

- Rust (latest stable version)
- Cargo


## Data Storage Format

The database uses a custom binary format for storing table chunks. Files are stored with a `.bin` extension and follow a columnar layout.

### File Header
The header contains metadata about the file's content. Here's how the data is serialized:
1.  **Magic Bytes** (8 bytes)
2.  **Row Count** (8 bytes): Number of rows in the chunk (column-based).
3.  **Column Count** (8 bytes): Number of columns in the file.
4.  **Column Info** (per column):
    - **Type ID** (8 bytes)
    - **Start Index** (8 bytes): Byte offset where the column data begins in file.

## Features
- Data is stored column by column for efficient aggregation (Columnar Storage).
- Supports metadata through manifests and catalogs (similar to Apache Iceberg).
- Data is stored in chunks with column-based statistics (min/max) for fast pruning.
- Basic support for optimistic concurrency updates.
