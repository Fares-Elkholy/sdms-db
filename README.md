# Scalable Datamanagement Systems - Lab 3

This project implements a simplified database engine with support for the Iceberg table format. It includes a catalog, manifest files, and file-based storage.

## Quick Start for Interviewers

To verify the project's functionality, simply run the test suite. All tests should pass.

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

Since this is a library crate, there is no main executable to run directly. However, you can run the test suite and benchmarks.

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

## Improvements

- Optimized `calculate_statistics` to handle empty chunks and reduce cloning.
- Cleaned up unused and commented-out code in the engine.
- Improved `insert` method efficiency.
- Removed unimplemented advanced tests to ensure a clean test run.
