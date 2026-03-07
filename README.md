# postgres_to_polars

A Rust library for streaming PostgreSQL query results directly into [Polars](https://pola.rs/) DataFrames using [sqlx](https://github.com/launchbadge/sqlx).

Derive `IntoDataFrame` on your struct, use sqlx's `query_as!` macro, and call `.to_dataframe()` on the stream. That's it.

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
postgres_to_polars = "1.0"
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "chrono"] }
tokio = { version = "1", features = ["full"] }
polars = "0.53"
```

### Basic example

```rust
use sqlx::PgPool;
use postgres_to_polars::{IntoDataFrame, StreamToDataFrame};

#[derive(sqlx::FromRow, IntoDataFrame)]
struct User {
    id: i32,
    name: Option<String>,
    email: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect("postgres://user:pass@localhost/db").await?;

    // With estimated row count (avoids reallocations)
    let df = sqlx::query_as!(User, "SELECT id, name, email FROM users")
        .fetch(&pool)
        .to_dataframe(100_000)
        .await?;

    println!("{}", df);
    Ok(())
}
```

### With default capacity

When you don't know the row count upfront:

```rust
let df = sqlx::query_as!(User, "SELECT id, name, email FROM users WHERE active = true")
    .fetch(&pool)
    .to_dataframe_default()
    .await?;
```

## Supported types

| Rust type | PostgreSQL type | Polars type |
|-----------|----------------|-------------|
| `i32` | int4 | Int32 |
| `i64` | int8 | Int64 |
| `f32` | float4 | Float32 |
| `f64` | float8 | Float64 |
| `bool` | bool | Boolean |
| `String` | text, varchar | String |
| `chrono::NaiveDate` | date | Date |
| `chrono::NaiveDateTime` | timestamp | Datetime |
| `chrono::NaiveTime` | time | Time |
| `Vec<String>` | text[] | List(String) |

All types support `Option<T>` for nullable columns.

## How it works

1. `#[derive(IntoDataFrame)]` generates a columnar builder for your struct
2. `.to_dataframe(capacity)` streams rows from PostgreSQL and pushes each row into the builder (no intermediate `Vec<Struct>`)
3. The builder converts each column `Vec` into a Polars `Series` and assembles the `DataFrame`

This approach is memory-efficient: rows are consumed one at a time from the stream and stored directly in columnar format.

## Performance

Benchmarks on 500K rows (PostgreSQL running locally in Docker):

| Query | Time |
|-------|------|
| 500K rows × 1 column (i32) | ~100ms |
| 500K rows × 4 columns (i32 + 3×String) | ~173ms |
| 100K rows × 4 columns | ~33ms |
| 10K rows × 4 columns | ~3.7ms |

## License

Apache-2.0
