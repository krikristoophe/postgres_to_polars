# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`postgres_to_polars` is a Rust library for direct and performant conversion of PostgreSQL data to Polars DataFrames. It implements a custom PostgreSQL binary protocol client using raw TCP connections and `postgres-protocol` for message parsing, avoiding the overhead of traditional PostgreSQL drivers.

## Common Commands

### Building
```bash
cargo build
cargo build --release
```

### Testing
```bash
# Run all tests
cargo test

# Run specific test file
cargo test --test query
cargo test --test load

# Run specific test by name
cargo test test_simple_query
cargo test test_pool_concurrent_error_handling_retry_loop
```

### Running the Binary
```bash
# The bin/test.rs contains a benchmark/test binary
cargo run --bin postgres_to_polars
```

### Linting & Formatting
```bash
cargo fmt
cargo clippy
```

## Architecture

### Core Protocol Implementation

The library implements PostgreSQL's binary protocol directly over TCP:

1. **Client** (`src/models/client.rs`): Core client managing TCP connection, prepared statement cache, and protocol state machine
   - Handles authentication (MD5, cleartext)
   - Implements 3-step query flow: Parse → Bind → Execute
   - Maintains prepared statement cache (keyed by MD5 hash of query text)
   - Tracks connection health via `AtomicBool` flag
   - Uses `BytesMut` for efficient buffer management
   - Portal-based query execution with unique portal names

2. **Connection Pool** (`src/models/pool.rs`): bb8-based connection pooling with health checks
   - `ClientManager` implements bb8's `ManageConnection` trait
   - Health validation via `ping()` and `has_broken()` checks
   - Configurable timeouts and connection limits

3. **Binary Data Conversion** (`src/models/column_result.rs`): PostgreSQL binary format → Polars types
   - `ColumnStorage` enum represents different PostgreSQL column types
   - Supports: int4, int8, text, varchar, bool, date, timestamptz, timestamp, time, float8, text[]
   - Handles epoch conversions (PostgreSQL 2000-01-01 → Unix 1970-01-01)
   - Pre-allocated column buffers grow in 1024-element chunks

### Key Design Patterns

**Prepared Statement Caching**: Statements are hashed (MD5) and cached in `Client.prepared_statements`. The cache stores parameter types and column metadata, allowing reuse across executions with the same query text.

**Error Recovery**: When a query fails, the client is marked "unhealthy" (`mark_unhealthy()`). The pool's health check mechanism will detect this and either replace the connection or let it recover via `ping()`. The client can recover from errors without disconnection.

**Message Loop Pattern**: All protocol interactions follow a read loop pattern:
```rust
loop {
    read into buffer
    while let Some(message) = backend::Message::parse(&mut buffer) {
        match message {
            ReadyForQuery => break loop,
            ErrorResponse => store error,
            // ... handle other messages
        }
    }
}
```

**Buffer Reuse**: `read_buffer` is a `BytesMut` that persists across message reads within a single query, avoiding allocations. It's only cleared between independent operations.

### Type Mapping

| PostgreSQL Type | OID  | Polars Type | Notes |
|-----------------|------|-------------|-------|
| int4 | 23 | Int32 | Direct binary conversion |
| int8 | 20 | Int64 | Direct binary conversion |
| text/varchar/name | 25/1043/19 | String | UTF-8 decode with lossy conversion |
| bool | 16 | Boolean | Single byte: 0/1 |
| date | 1082 | Date | Days since 2000-01-01 → Unix epoch |
| timestamptz | 1184 | Datetime(Microseconds) | Microseconds since 2000-01-01 → Unix epoch |
| timestamp | 1114 | Datetime(Microseconds) | Same conversion as timestamptz |
| time | 1083 | Time | Microseconds since midnight |
| float8 | 701 | Float64 | Direct binary conversion |
| text[] | 1009 | List(String) | Custom parser in `text_array.rs` |

Unknown types fall back to `Bytes` storage with a warning printed.

### Module Structure

- `models/client.rs`: TCP connection, protocol state machine, query execution
- `models/pool.rs`: bb8 connection pool integration
- `models/column_result.rs`: Binary data parsing and Polars conversion
- `models/params.rs`: Parameter encoding (Rust types → PostgreSQL binary)
- `models/client_options.rs`: Connection configuration
- `models/pool_options.rs`: Pool configuration
- `utils/error.rs`: Error types and Result alias
- `utils/text_array.rs`: PostgreSQL array format parser
- `utils/mod.rs`: MD5 helpers, statement naming, error formatting

## Test Setup

Tests require a running PostgreSQL instance with:
- Username: `POSTGRES_USER`
- Password: `pgpassword`
- Database: `pg-database`
- Host: `127.0.0.1:5432`
- Tables: `users`, `time_entries` (with `tags text[]` column)

The `tests/` directory contains:
- `query.rs`: Unit tests for query execution, prepared statement caching, error handling
- `load.rs`: Load tests with concurrent queries (100+ concurrent connections)

## Important Implementation Details

### Portal Management
Each query execution creates a unique portal using an atomic counter. Portals are not explicitly closed; they're implicitly cleaned up at transaction boundaries (after `ReadyForQuery`).

### Error Handling Flow
- Errors set `error_to_return` variable but don't immediately break loops
- Loop continues until `ReadyForQuery` to fully drain server messages
- Only then is the error returned
- Client health flag is updated based on error type

### Unsafe Buffer Operations
The code uses unsafe buffer manipulation for performance:
```rust
let dst = read_buffer.chunk_mut();
let buf: &mut [u8] = unsafe {
    std::slice::from_raw_parts_mut(dst.as_mut_ptr(), dst.len())
};
```
This is safe because we call `advance_mut(n)` immediately after reading `n` bytes.

### Parameter Type Consistency
If you reuse a prepared statement with different parameter types, you'll get a `ParamTypeMismatch` error. Parameter types are part of the cache key validation.

## Common Pitfalls

1. **Text Arrays**: The text array parser expects PostgreSQL's text format (e.g., `{foo,bar,"baz,qux"}`). Make sure columns are actually `text[]` type, not JSON arrays.

2. **Timestamp Epochs**: PostgreSQL uses 2000-01-01 as epoch for timestamps; Polars/Unix use 1970-01-01. The conversions add/subtract 946684800 seconds (or 946684800000000 microseconds).

3. **Connection Health**: After an error, test if `client.has_broken()` before reusing. The pool will handle this automatically, but manual client usage needs explicit checks.

4. **Buffer Clearing**: Don't clear `read_buffer` between Parse and Execute phases of the same query—leftover bytes from one message phase may be needed for the next.

5. **Column Count Mismatches**: If the query returns more or fewer columns than expected, you'll get a "Too many/few fields" error. This shouldn't happen in normal usage but indicates a protocol desync.
