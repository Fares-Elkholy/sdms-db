# Scalable Datamanagement Systems - DB

This project implements a simplified database engine with support for the Iceberg table format. It includes a catalog, manifest files, and file-based storage.

## Quick Start for Interviewers

To verify the project's functionality, you can run the test suite or explore the interactive CLI.

### Run Interactive CLI
```bash
cargo run
```
Once inside the CLI, type `populate` to generate sample data, then `scan 0` to view it.

### Run Tests
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

## How to Run

You can run the interactive CLI to interact with the database engine directly.

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

## Data Storage Format

The database uses a custom binary format for storing table chunks. Files are stored with a `.bin` extension and follow a columnar layout.

### File Header
The header contains metadata about the file's content:
1.  **Magic Bytes** (8 bytes): `SDMS\x19\x03JS` (0x53, 0x44, 0x4d, 0x53, 0x19, 0x03, 0x4a, 0x53)
2.  **Row Count** (8 bytes, u64): Number of rows in the chunk.
3.  **Column Count** (8 bytes, u64): Number of columns.
4.  **Column Info**: For each column:
    - **Type ID** (8 bytes, u64): 0=Int, 1=UInt, 2=RowID, 3=Varchar.
    - **Start Index** (8 bytes, u64): Byte offset where the column data begins.

### Data Layout
Data is stored column by column (Columnar Storage).
- **Int/UInt**: Stored as 4-byte little-endian integers.
- **RowID**: Stored as 8-byte little-endian integers.
- **Varchar**: Stored as an 8-byte length followed by the UTF-8 string bytes.

## Features

- **Iceberg Table Format**: Supports metadata management through manifests and catalogs.
- **Columnar Storage**: Data is stored in chunks with column-based statistics (min/max).
- **Optimistic Concurrency**: Basic support for optimistic updates (implied by `OptimisticFail` error).