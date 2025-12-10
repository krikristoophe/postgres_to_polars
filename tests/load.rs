use postgres_to_polars::{BinaryParam, ClientOptions, PoolOptions, build_pool};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::{Duration, Instant, sleep};

const USERNAME: &str = "POSTGRES_USER";
const PASSWORD: &str = "pgpassword";
const DATABASE: &str = "pg-database";

fn create_test_client_option() -> ClientOptions {
    ClientOptions::new(
        String::from(USERNAME),
        String::from(PASSWORD),
        String::from(DATABASE),
        String::from("127.0.0.1"),
        5432,
        true,
    )
}

#[tokio::test]
async fn load_test_simple() {
    let pool = build_pool(PoolOptions {
        client_options: create_test_client_option(),
        max_connections: 10,
        acquire_timeout: 5,
    })
    .await
    .unwrap();

    let num_tasks = 100;
    let num_queries_per_task = 50;

    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    let mut handles = vec![];

    for task_id in 0..num_tasks {
        let pool = pool.clone();
        let success = success_count.clone();
        let errors = error_count.clone();

        let handle = tokio::spawn(async move {
            for i in 0..num_queries_per_task {
                match pool.get().await {
                    Ok(conn) => {
                        match conn
                            .query(
                                "SELECT $1::int as val",
                                [Some(BinaryParam::Int4(task_id * 1000 + i))],
                            )
                            .await
                        {
                            Ok(_df) => {
                                success.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(e) => {
                                eprintln!("❌ Query error task {}: {:?}", task_id, e);
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("❌ Pool timeout task {}: {:?}", task_id, e);
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Petite pause entre requêtes (optionnel)
                sleep(Duration::from_millis(10)).await;
            }
        });

        handles.push(handle);
    }

    // Attendre toutes les tâches
    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start.elapsed();
    let total_queries = num_tasks * num_queries_per_task;
    let successes = success_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);

    println!("✅ Durée: {:?}", duration);
    println!("✅ Succès: {} / {}", successes, total_queries);
    println!("❌ Erreurs: {}", errors);
    println!(
        "📊 QPS: {:.2}",
        total_queries as f64 / duration.as_secs_f64()
    );

    assert_eq!(errors, 0, "Il y a eu des erreurs !");
}

#[tokio::test]
async fn load_test_mixed_queries() {
    let pool = build_pool(PoolOptions {
        client_options: create_test_client_option(),
        max_connections: 10,
        acquire_timeout: 5,
    })
    .await
    .unwrap();

    let queries = vec![
        "SELECT $1::int",
        "SELECT $1::text, $2::int",
        "SELECT NOW()",
        "SELECT * FROM pg_tables LIMIT 10",
    ];

    let num_concurrent = 50;
    let duration_secs = 30;

    let success = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = vec![];

    for task_id in 0..num_concurrent {
        let pool = pool.clone();
        let success = success.clone();
        let errors = errors.clone();
        let queries = queries.clone();

        let handle = tokio::spawn(async move {
            let mut counter = 0;
            while start.elapsed().as_secs() < duration_secs {
                let query_idx = counter % queries.len();
                let query = queries[query_idx];

                match pool.get().await {
                    Ok(conn) => {
                        let result = match query_idx {
                            0 => {
                                conn.query(query, [Some(BinaryParam::Int4(counter as i32))])
                                    .await
                            }
                            1 => {
                                conn.query(
                                    query,
                                    [
                                        Some(BinaryParam::Text("test".to_string())),
                                        Some(BinaryParam::Int4(42)),
                                    ],
                                )
                                .await
                            }
                            _ => {
                                conn.query(query, std::iter::empty::<Option<BinaryParam>>())
                                    .await
                            }
                        };

                        match result {
                            Ok(_) => success.fetch_add(1, Ordering::Relaxed),
                            Err(e) => {
                                eprintln!("❌ Task {} error: {:?}", task_id, e);
                                errors.fetch_add(1, Ordering::Relaxed)
                            }
                        };
                    }
                    Err(e) => {
                        eprintln!("❌ Task {} pool error: {:?}", task_id, e);
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }

                counter += 1;
                sleep(Duration::from_millis(50)).await;
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let total = success.load(Ordering::Relaxed);
    let errs = errors.load(Ordering::Relaxed);

    println!("✅ Total réussi: {}", total);
    println!("❌ Total erreurs: {}", errs);
    println!(
        "📊 Taux d'erreur: {:.2}%",
        (errs as f64 / (total + errs) as f64) * 100.0
    );
}
