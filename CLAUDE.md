# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`postgres_to_polars` is a Rust library that streams PostgreSQL query results directly into Polars DataFrames via sqlx. It provides a `#[derive(IntoDataFrame)]` proc macro and a `.to_dataframe()` extension trait on sqlx streams.

## Common Commands

### Building
```bash
cargo build
cargo build --release
```

### Testing
```bash
# Requires DATABASE_URL in .env pointing to a PostgreSQL database with the test schema
# Tests use #[sqlx::test] which creates temporary databases per test

cargo test                          # Run all tests
cargo test --test query             # Run query tests only
cargo test test_simple_query        # Run specific test
```

### Benchmarks
```bash
# Creates its own temporary database (_bench_postgres_to_polars)
cargo bench
```

### Linting & Formatting
```bash
cargo fmt
cargo clippy
```

## Architecture

### Workspace structure

```
postgres_to_polars/                     (workspace root)
├── Cargo.toml                          (workspace config)
├── postgres_to_polars/                 (library crate)
│   ├── src/lib.rs                      (traits, VecToColumn impls, StreamToDataFrame)
│   ├── tests/query.rs                  (integration tests with #[sqlx::test])
│   ├── benches/query_bench.rs          (criterion benchmarks)
│   └── migrations/                     (sqlx migrations for test DB)
└── postgres_to_polars_derive/          (proc-macro crate)
    └── src/lib.rs                      (derive IntoDataFrame)
```

### Core components

1. **`IntoDataFrame` derive macro** (`postgres_to_polars_derive/src/lib.rs`):
   Generates for each struct:
   - A builder struct with `Vec<T>` per field (columnar storage)
   - `dataframe_builder(capacity)` constructor with pre-allocated Vecs
   - `push(row)` to decompose a row into columns
   - `build()` to convert Vecs into Polars Series/DataFrame via `VecToColumn`
   - Trait impls for `HasDataFrameBuilder` and `DataFrameBuilder`

2. **`StreamToDataFrame` trait** (`postgres_to_polars/src/lib.rs`):
   Extension trait on any `Stream<Item = Result<T, sqlx::Error>>`:
   - `.to_dataframe(capacity)` — streams rows into builder, returns DataFrame
   - `.to_dataframe_default()` — same with default capacity (1024)

3. **`VecToColumn` trait** (`postgres_to_polars/src/lib.rs`):
   Converts `Vec<T>` to Polars `Column`. Implemented for:
   - Scalar types: i32, i64, f32, f64, bool, String (+ Option variants)
   - Chrono types: NaiveDate, NaiveDateTime, NaiveTime (+ Option variants)
   - List types: Vec<String>, Option<Vec<String>> (via ListStringChunkedBuilder)

### Key design decisions

- **Streaming**: Rows are consumed one-by-one from the sqlx stream and pushed into columnar Vecs. No intermediate `Vec<Struct>` — memory usage is proportional to data, not 2x.
- **Capacity hint**: `to_dataframe(n)` pre-allocates Vecs to avoid reallocations. For 500K rows this saves ~9% on string-heavy queries.
- **VecToColumn dispatch**: The derive macro generates `VecToColumn::to_column(name, vec)` calls. This avoids type detection in the proc macro — the trait dispatch handles type-specific conversion at compile time.

## Test setup

Tests require a running PostgreSQL instance. Connection configured via `DATABASE_URL` in `.env`.

The test database needs the schema from `postgres_to_polars/migrations/`:
- `users` table with: id, first_name, last_name, email, tags (text[]), birth_date, created_at, login_time
- 500K rows of generated data

`#[sqlx::test]` creates temporary databases per test and applies migrations automatically.

Benchmarks create their own `_bench_postgres_to_polars` database and clean it up after.

## Supported PostgreSQL → Polars type mapping

| Rust type | PostgreSQL | Polars |
|-----------|-----------|--------|
| i32 | int4 | Int32 |
| i64 | int8 | Int64 |
| f32 | float4 | Float32 |
| f64 | float8 | Float64 |
| bool | bool | Boolean |
| String | text/varchar | String |
| NaiveDate | date | Date |
| NaiveDateTime | timestamp | Datetime |
| NaiveTime | time | Time |
| Vec\<String\> | text[] | List(String) |
