use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use polars::prelude::SchemaExt;
use postgres_to_polars::{IntoDataFrame, StreamToDataFrame};
use sqlx::PgPool;

#[derive(sqlx::FromRow, IntoDataFrame)]
struct UserRow {
    id: i32,
}

#[derive(sqlx::FromRow, IntoDataFrame)]
struct ValRow {
    val: Option<i32>,
}

#[derive(sqlx::FromRow, IntoDataFrame)]
struct CountRow {
    count: Option<i64>,
}

#[derive(sqlx::FromRow, IntoDataFrame)]
struct UserFullRow {
    id: i32,
    first_name: Option<String>,
    last_name: Option<String>,
    email: Option<String>,
}

#[derive(sqlx::FromRow, IntoDataFrame)]
struct TagsRow {
    tags: Option<Vec<String>>,
}

#[derive(sqlx::FromRow, IntoDataFrame)]
struct DateRow {
    birth_date: Option<NaiveDate>,
}

#[derive(sqlx::FromRow, IntoDataFrame)]
struct DateTimeRow {
    created_at: Option<NaiveDateTime>,
}

#[derive(sqlx::FromRow, IntoDataFrame)]
struct TimeRow {
    login_time: Option<NaiveTime>,
}

#[sqlx::test]
async fn test_simple_query(pool: PgPool) {
    let df = sqlx::query_as!(UserRow, "SELECT id FROM users LIMIT 10")
        .fetch(&pool)
        .to_dataframe(10)
        .await
        .expect("Query failed");

    assert!(df.height() <= 10, "Should have at most 10 rows");
    assert_eq!(df.width(), 1);
}

#[sqlx::test]
async fn test_count_query(pool: PgPool) {
    let df = sqlx::query_as!(CountRow, "SELECT COUNT(*) as count FROM users")
        .fetch(&pool)
        .to_dataframe(1)
        .await
        .expect("Query failed");

    assert_eq!(df.height(), 1, "Should have 1 row");
}

#[sqlx::test]
async fn test_query_with_params(pool: PgPool) {
    let df = sqlx::query_as!(UserRow, "SELECT id FROM users WHERE id = $1", 1i32)
        .fetch(&pool)
        .to_dataframe(1)
        .await
        .expect("Query failed");

    assert!(df.height() <= 1, "Should have at most 1 row");
}

#[sqlx::test]
async fn test_prepared_statement_cache(pool: PgPool) {
    let df1 = sqlx::query_as!(UserRow, "SELECT id FROM users WHERE id = $1", 1i32)
        .fetch(&pool)
        .to_dataframe(1)
        .await;
    assert!(df1.is_ok(), "First query should succeed");

    let df2 = sqlx::query_as!(UserRow, "SELECT id FROM users WHERE id = $1", 2i32)
        .fetch(&pool)
        .to_dataframe(1)
        .await;
    assert!(
        df2.is_ok(),
        "Second query should succeed (cached statement)"
    );
}

#[sqlx::test]
async fn test_error_handling(pool: PgPool) {
    // query_as::<_, T> (not macro) because the table doesn't exist — would fail at compile time with query_as!
    let result = sqlx::query_as::<_, UserRow>("SELECT id FROM table_qui_nexiste_pas")
        .fetch(&pool)
        .to_dataframe_default()
        .await;

    assert!(result.is_err(), "Query should have failed");
}

#[sqlx::test]
async fn test_multiple_columns(pool: PgPool) {
    let df = sqlx::query_as!(
        UserFullRow,
        "SELECT id, first_name, last_name, email FROM users LIMIT 5"
    )
    .fetch(&pool)
    .to_dataframe(5)
    .await
    .expect("Query failed");

    assert!(df.height() <= 5);
    assert_eq!(df.width(), 4);

    let schema = df.schema();
    assert!(schema.get_field("id").is_some());
    assert!(schema.get_field("first_name").is_some());
    assert!(schema.get_field("last_name").is_some());
    assert!(schema.get_field("email").is_some());
}

#[sqlx::test]
async fn test_empty_result(pool: PgPool) {
    let df = sqlx::query_as!(UserRow, "SELECT id FROM users WHERE FALSE")
        .fetch(&pool)
        .to_dataframe_default()
        .await
        .expect("Query failed");

    assert_eq!(df.height(), 0, "Should have 0 rows");
}

#[sqlx::test]
async fn test_parameterized_int(pool: PgPool) {
    let df = sqlx::query_as!(ValRow, "SELECT $1::int as val", 42i32)
        .fetch(&pool)
        .to_dataframe(1)
        .await
        .expect("Query failed");

    assert_eq!(df.height(), 1);

    let val = df.column("val").unwrap();
    assert_eq!(val.get(0).unwrap().to_string(), "42");
}

#[sqlx::test]
async fn test_large_result(pool: PgPool) {
    let df = sqlx::query_as!(
        UserFullRow,
        "SELECT id, first_name, last_name, email FROM users"
    )
    .fetch(&pool)
    .to_dataframe(500_000)
    .await
    .expect("Query failed");

    assert_eq!(df.height(), 500_000);
    assert_eq!(df.width(), 4);
}

#[sqlx::test]
async fn test_concurrent_queries(pool: PgPool) {
    let pool = std::sync::Arc::new(pool);

    let mut handles = tokio::task::JoinSet::new();

    for i in 0..50i32 {
        let pool = pool.clone();
        handles.spawn(async move {
            let df = sqlx::query_as!(ValRow, "SELECT $1::int as val", i)
                .fetch(pool.as_ref())
                .to_dataframe(1)
                .await
                .expect("Query failed");

            assert_eq!(df.height(), 1);
        });
    }

    while let Some(result) = handles.join_next().await {
        result.expect("Task panicked");
    }
}

#[sqlx::test]
async fn test_naive_date(pool: PgPool) {
    let df = sqlx::query_as!(DateRow, "SELECT birth_date FROM users LIMIT 5")
        .fetch(&pool)
        .to_dataframe(5)
        .await
        .expect("Query failed");

    assert_eq!(df.height(), 5);
    assert_eq!(df.width(), 1);

    let schema = df.schema();
    assert!(schema.get_field("birth_date").is_some());
}

#[sqlx::test]
async fn test_naive_datetime(pool: PgPool) {
    let df = sqlx::query_as!(DateTimeRow, "SELECT created_at FROM users LIMIT 5")
        .fetch(&pool)
        .to_dataframe(5)
        .await
        .expect("Query failed");

    assert_eq!(df.height(), 5);
    assert_eq!(df.width(), 1);

    let schema = df.schema();
    assert!(schema.get_field("created_at").is_some());
}

#[sqlx::test]
async fn test_naive_time(pool: PgPool) {
    let df = sqlx::query_as!(TimeRow, "SELECT login_time FROM users LIMIT 5")
        .fetch(&pool)
        .to_dataframe(5)
        .await
        .expect("Query failed");

    assert_eq!(df.height(), 5);
    assert_eq!(df.width(), 1);

    let schema = df.schema();
    assert!(schema.get_field("login_time").is_some());
}

#[sqlx::test]
async fn test_text_array(pool: PgPool) {
    let df = sqlx::query_as!(TagsRow, "SELECT tags FROM users LIMIT 5")
        .fetch(&pool)
        .to_dataframe(5)
        .await
        .expect("Query failed");

    assert_eq!(df.height(), 5);
    assert_eq!(df.width(), 1);

    let schema = df.schema();
    assert!(schema.get_field("tags").is_some());
}
