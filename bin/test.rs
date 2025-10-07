use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use postgres_to_polars::{BinaryParam, ClientOptions, Pool, PoolOptions};

const USERNAME: &str = "POSTGRES_USER";
const PASSWORD: &str = "pgpassword";
const DATABASE: &str = "pg-database";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client_options = ClientOptions::new(
        String::from(USERNAME),
        String::from(PASSWORD),
        String::from(DATABASE),
        String::from("127.0.0.1"),
        5432,
    );
    let pool_options = PoolOptions::new(client_options, 10);
    let pool = Arc::new(Pool::new(pool_options).await?);

    let query = "
        SELECT
            *
        from users
        where
            id = $1;
        ";

    let params = vec![Some(BinaryParam::Int4(24))];

    let results: Vec<anyhow::Result<Duration>> = futures::future::join_all((0..400).map(|_| {
        let query = query.to_string();
        let params = params.clone();
        let pool = pool.clone();

        async move {
            let t0 = Instant::now();
            let mut client_ref = pool.acquire().await?;
            let client = client_ref.client();
            let _df = client.query(&query, params).await?;
            let t1 = Instant::now();
            Ok(t1.duration_since(t0))
        }
    }))
    .await;

    let avg = results
        .iter()
        .map(|r| r.as_ref().unwrap())
        .sum::<Duration>()
        / results.len() as u32;
    println!("Average query time: {:?}", avg);

    Ok(())
}
