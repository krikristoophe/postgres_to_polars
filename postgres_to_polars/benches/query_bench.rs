use criterion::{Criterion, criterion_group, criterion_main};
use postgres_to_polars::{IntoDataFrame, StreamToDataFrame};
use sqlx::{Executor, PgPool};
use tokio::runtime::Runtime;

const BENCH_DB: &str = "_bench_postgres_to_polars";

#[derive(sqlx::FromRow, IntoDataFrame)]
struct UserRow {
    id: i32,
}

#[derive(sqlx::FromRow, IntoDataFrame)]
struct UserFullRow {
    id: i32,
    first_name: Option<String>,
    last_name: Option<String>,
    email: Option<String>,
}

async fn setup() -> PgPool {
    dotenvy::from_path("../.env").ok();
    let base_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let base_pool = PgPool::connect(&base_url).await.unwrap();

    let _ = base_pool
        .execute(format!("DROP DATABASE IF EXISTS \"{BENCH_DB}\"").as_str())
        .await;
    base_pool
        .execute(format!("CREATE DATABASE \"{BENCH_DB}\"").as_str())
        .await
        .unwrap();
    base_pool.close().await;

    let bench_url = base_url.rsplit_once('/').unwrap().0;
    let bench_url = format!("{bench_url}/{BENCH_DB}");
    let pool = PgPool::connect(&bench_url).await.unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool
}

async fn teardown() {
    let base_url = std::env::var("DATABASE_URL").unwrap();
    let base_pool = PgPool::connect(&base_url).await.unwrap();
    let _ = base_pool
        .execute(format!("DROP DATABASE IF EXISTS \"{BENCH_DB}\"").as_str())
        .await;
    base_pool.close().await;
}

fn bench_queries(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let pool = rt.block_on(setup());

    c.bench_function("select_id_500k", |b| {
        b.to_async(&rt).iter(|| async {
            sqlx::query_as!(UserRow, "SELECT id FROM users")
                .fetch(&pool)
                .to_dataframe(500_000)
                .await
                .unwrap()
        });
    });

    c.bench_function("select_4cols_500k", |b| {
        b.to_async(&rt).iter(|| async {
            sqlx::query_as!(
                UserFullRow,
                "SELECT id, first_name, last_name, email FROM users"
            )
            .fetch(&pool)
            .to_dataframe(500_000)
            .await
            .unwrap()
        });
    });

    c.bench_function("select_4cols_10k", |b| {
        b.to_async(&rt).iter(|| async {
            sqlx::query_as!(
                UserFullRow,
                "SELECT id, first_name, last_name, email FROM users LIMIT 10000"
            )
            .fetch(&pool)
            .to_dataframe(500_000)
            .await
            .unwrap()
        });
    });

    c.bench_function("select_4cols_100k", |b| {
        b.to_async(&rt).iter(|| async {
            sqlx::query_as!(
                UserFullRow,
                "SELECT id, first_name, last_name, email FROM users LIMIT 100000"
            )
            .fetch(&pool)
            .to_dataframe(500_000)
            .await
            .unwrap()
        });
    });

    rt.block_on(pool.close());
    rt.block_on(teardown());
}

criterion_group!(benches, bench_queries);
criterion_main!(benches);
