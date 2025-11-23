# Scalable Datamanagement Systems - Lab 3

This project implements a simplified database engine with support for the Iceberg table format. It includes a catalog, manifest files, and file-based storage.

## Quick Start for Interviewers

To verify the project's functionality, you can run the test suite or explore the interactive CLI.

### Run Tests
```bash
cargo test
```

### Run Interactive CLI
```bash
cargo run
```
Once inside the CLI, type `populate` to generate sample data, then `scan 0` to view it.

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

### Running Benchmarks

To run the performance benchmarks:

```bash
cargo bench
```

## Features

- **Iceberg Table Format**: Supports metadata management through manifests and catalogs.
- **Columnar Storage**: Data is stored in chunks with column-based statistics (min/max).
- **Optimistic Concurrency**: Basic support for optimistic updates (implied by `OptimisticFail` error).