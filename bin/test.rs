use std::time::{Duration, Instant};

use postgres_to_polars::{
    BinaryParam, ClientOptions, PgToPlError, PgToPlResult, PoolOptions, build_pool,
};
use tokio::task::JoinSet;

const USERNAME: &str = "POSTGRES_USER";
const PASSWORD: &str = "pgpassword";
const DATABASE: &str = "pg-database";

#[tokio::main]
async fn main() -> PgToPlResult<()> {
    let client_options = ClientOptions::new(
        String::from(USERNAME),
        String::from(PASSWORD),
        String::from(DATABASE),
        String::from("127.0.0.1"),
        5432,
    );
    let pool_options = PoolOptions::new(client_options, 10);
    let pool = build_pool(pool_options).await?;

    let query = "
        SELECT
            *
        from users
        where
            id = $1;
        ";

    let params = vec![Some(BinaryParam::Int4(24))];

    let mut set: JoinSet<Result<Duration, PgToPlError>> = JoinSet::new();

    for _ in 0..40 {
        let query = query.to_string();
        let params = params.clone();
        let pool = pool.clone();

        set.spawn(async move {
            let t0 = Instant::now();
            let client = pool
                .get()
                .await
                .map_err(|e| PgToPlError::PoolError(format!("bb8 get failed: {e:?}")))?;
            let _df = client.query(&query, params).await?;
            let t1 = Instant::now();
            Ok(t1.duration_since(t0))
        });
    }

    let mut results = Vec::new();
    while let Some(res) = set.join_next().await {
        results.push(res.unwrap()); // unwrap le JoinError, puis tu as ton Result<Duration, ...>
    }

    let avg = results
        .iter()
        .map(|r| r.as_ref().unwrap())
        .sum::<Duration>()
        / results.len() as u32;
    println!("Average query time: {:?}", avg);

    Ok(())
}
