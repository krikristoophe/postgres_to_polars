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
struct UserFullRow {
    id: i32,
    first_name: Option<String>,
    last_name: Option<String>,
    email: Option<String>,
}

#[sqlx::test]
async fn test_large_result(pool: PgPool) {
    let df = sqlx::query_as!(
        UserFullRow,
        "SELECT id as \"id!\", format('first-%s', id) as first_name, format('last-%s', id) as last_name, format('user-%s@example.test', id) as email FROM generate_series(1, 500000) AS users(id)"
    )
    .fetch(&pool)
    .to_dataframe(500_000)
    .await
    .expect("Query failed");

    assert_eq!(df.height(), 500_000);
    assert_eq!(df.width(), 4);
    assert_eq!(
        df.column("first_name").unwrap().get(0).unwrap().to_string(),
        "\"first-1\""
    );
    assert_eq!(
        df.column("email")
            .unwrap()
            .get(499_999)
            .unwrap()
            .to_string(),
        "\"user-500000@example.test\""
    );
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
