use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use polars::prelude::{DataType, SchemaExt};
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
struct SqlxRenameRow {
    #[sqlx(rename = "user_id")]
    id: i32,
    #[sqlx(rename = "user_email")]
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
    let df = sqlx::query_as!(
        DateRow,
        "SELECT CASE WHEN value = 1 THEN DATE '2024-06-15' END as birth_date FROM generate_series(1, 2) AS value"
    )
    .fetch(&pool)
    .to_dataframe(5)
    .await
    .expect("Query failed");

    assert_eq!(df.height(), 2);
    assert_eq!(df.width(), 1);

    let schema = df.schema();
    assert!(schema.get_field("birth_date").is_some());
    let column = df.column("birth_date").unwrap();
    assert_eq!(column.dtype(), &DataType::Date);
    assert_eq!(column.get(0).unwrap().to_string(), "2024-06-15");
    assert!(column.get(1).unwrap().is_null());
}

#[sqlx::test]
async fn test_naive_datetime(pool: PgPool) {
    let df = sqlx::query_as!(
        DateTimeRow,
        "SELECT CASE WHEN value = 1 THEN TIMESTAMP '2024-06-15 09:30:00' END as created_at FROM generate_series(1, 2) AS value"
    )
    .fetch(&pool)
    .to_dataframe(5)
    .await
    .expect("Query failed");

    assert_eq!(df.height(), 2);
    assert_eq!(df.width(), 1);

    let schema = df.schema();
    assert!(schema.get_field("created_at").is_some());
    let column = df.column("created_at").unwrap();
    assert!(matches!(column.dtype(), DataType::Datetime(_, _)));
    assert!(
        column
            .get(0)
            .unwrap()
            .to_string()
            .contains("2024-06-15 09:30:00")
    );
    assert!(column.get(1).unwrap().is_null());
}

#[sqlx::test]
async fn test_naive_time(pool: PgPool) {
    let df = sqlx::query_as!(
        TimeRow,
        "SELECT CASE WHEN value = 1 THEN TIME '09:30:00' END as login_time FROM generate_series(1, 2) AS value"
    )
    .fetch(&pool)
    .to_dataframe(5)
    .await
    .expect("Query failed");

    assert_eq!(df.height(), 2);
    assert_eq!(df.width(), 1);

    let schema = df.schema();
    assert!(schema.get_field("login_time").is_some());
    let column = df.column("login_time").unwrap();
    assert_eq!(column.dtype(), &DataType::Time);
    assert_eq!(column.get(0).unwrap().to_string(), "09:30:00");
    assert!(column.get(1).unwrap().is_null());
}

#[sqlx::test]
async fn test_text_array(pool: PgPool) {
    let df = sqlx::query_as!(
        TagsRow,
        "SELECT CASE WHEN value = 1 THEN ARRAY['alpha', 'beta']::text[] END as tags FROM generate_series(1, 2) AS value"
    )
    .fetch(&pool)
    .to_dataframe(5)
    .await
    .expect("Query failed");

    assert_eq!(df.height(), 2);
    assert_eq!(df.width(), 1);

    let schema = df.schema();
    assert!(schema.get_field("tags").is_some());
    let column = df.column("tags").unwrap();
    assert!(matches!(column.dtype(), DataType::List(_)));
    assert_eq!(column.get(0).unwrap().to_string(), "[\"alpha\", \"beta\"]");
    assert!(column.get(1).unwrap().is_null());
}

#[sqlx::test]
async fn test_column_name_attribute(pool: PgPool) {
    let df = sqlx::query_as!(
        ColumnNameRow,
        "SELECT id as \"id!\", NULL::text as email FROM generate_series(1, 5) AS users(id)"
    )
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

#[sqlx::test]
async fn test_sqlx_rename_as_column_name(pool: PgPool) {
    // query_as::<_, T> (not macro) because #[sqlx(rename)] works with FromRow derive, not query_as! macro
    let df = sqlx::query_as::<_, SqlxRenameRow>(
        r#"SELECT id as "user_id", NULL::text as "user_email" FROM generate_series(1, 5) AS users(id)"#,
    )
    .fetch(&pool)
    .to_dataframe(5)
    .await
    .expect("Query failed");

    assert_eq!(df.height(), 5);
    assert_eq!(df.width(), 2);

    let schema = df.schema();
    assert!(
        schema.get_field("user_id").is_some(),
        "Column should use sqlx rename 'user_id'"
    );
    assert!(
        schema.get_field("user_email").is_some(),
        "Column should use sqlx rename 'user_email'"
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
