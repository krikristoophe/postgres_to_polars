use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use polars::prelude::SchemaExt;
use postgres_to_polars::{IntoDataFrame, StreamToDataFrame};
use sqlx::PgPool;

#[derive(sqlx::FromRow, IntoDataFrame)]
struct TagsRow {
    tags: Option<Vec<String>>,
}

#[derive(sqlx::FromRow, IntoDataFrame)]
struct ColumnNameRow {
    #[column_name = "user_id"]
    id: i32,
    #[column_name = "user_email"]
    email: Option<String>,
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

#[sqlx::test]
async fn test_column_name_attribute(pool: PgPool) {
    let df = sqlx::query_as!(ColumnNameRow, "SELECT id, email FROM users LIMIT 5")
        .fetch(&pool)
        .to_dataframe(5)
        .await
        .expect("Query failed");

    assert_eq!(df.height(), 5);
    assert_eq!(df.width(), 2);

    let schema = df.schema();
    assert!(
        schema.get_field("user_id").is_some(),
        "Column should be renamed to 'user_id'"
    );
    assert!(
        schema.get_field("user_email").is_some(),
        "Column should be renamed to 'user_email'"
    );
    assert!(
        schema.get_field("id").is_none(),
        "Original field name 'id' should not appear"
    );
    assert!(
        schema.get_field("email").is_none(),
        "Original field name 'email' should not appear"
    );
}
