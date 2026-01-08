use postgres_to_polars::ClientOptions;

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
    )
    .with_monkey_chaos_already_prepare()
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use polars::prelude::{DataType, SchemaExt};
    use postgres_to_polars::{BinaryParam, Client, PoolOptions, build_pool, init_logger};
    use tokio::task::JoinSet;

    use crate::create_test_client_option;

    #[tokio::test]
    async fn test_simple_query() {
        init_logger();
        // Configuration du client
        let options = create_test_client_option();

        // Création et connexion du client
        let client = Client::new(options).await.expect("Failed to create client");
        client.connect().await.expect("Failed to connect");

        // Exécution de la requête
        let result = client.query("SELECT * FROM users LIMIT 10", vec![]).await;

        match result {
            Ok(df) => {
                // Assertions basiques
                assert!(df.height() <= 10, "Should have at most 10 rows");
            }
            Err(e) => {
                panic!("Query failed: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_no_data_query() {
        init_logger();
        // Configuration du client
        let options = create_test_client_option();

        // Création et connexion du client
        let client = Client::new(options).await.expect("Failed to create client");
        client.connect().await.expect("Failed to connect");

        // Exécution de la requête
        let result = client.query("SELECT 1;", vec![]).await;

        match result {
            Ok(df) => {
                // Assertions basiques
                assert!(df.height() == 1, "Should have 1 rows");
            }
            Err(e) => {
                panic!("Query failed: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_query_with_params() {
        init_logger();
        let options = create_test_client_option();

        let client = Client::new(options).await.expect("Failed to create client");
        client.connect().await.expect("Failed to connect");

        // Requête avec paramètre
        let user_id = 1i32;
        let param = Some(BinaryParam::Int4(user_id));

        let result = client
            .query("SELECT * FROM users WHERE id = $1;", vec![param])
            .await;

        match result {
            Ok(df) => {
                assert!(df.height() <= 1, "Should have at most 1 row");
            }
            Err(e) => {
                panic!("Query failed: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_prepared_statement_cache() {
        init_logger();
        let options = create_test_client_option();

        let client = Client::new(options).await.expect("Failed to create client");
        client.connect().await.expect("Failed to connect");

        let query = "SELECT * FROM users WHERE id = $1";

        // Première exécution (prépare le statement)
        let result1 = client.query(query, vec![Some(BinaryParam::Int4(1))]).await;
        assert!(result1.is_ok(), "First query should succeed");

        // Deuxième exécution (utilise le statement caché)
        let result2 = client.query(query, vec![Some(BinaryParam::Int4(2))]).await;
        assert!(
            result2.is_ok(),
            "Second query should succeed and use cached statement"
        );
    }

    #[tokio::test]
    async fn test_error_handling() {
        init_logger();
        let options = create_test_client_option();

        let client = Client::new(options).await.expect("Failed to create client");
        client.connect().await.expect("Failed to connect");

        // Requête invalide (table inexistante)
        let result = client
            .query("SELECT * FROM table_qui_nexiste_pas", vec![])
            .await;

        match result {
            Ok(_) => panic!("Query should have failed"),
            Err(_) => {
                assert!(client.has_broken(), "Client should be marked unhealthy");
            }
        }
    }

    #[tokio::test]
    async fn test_pool_error_handling_retry_loop() {
        init_logger();
        let client_options = create_test_client_option();

        let pool_options = PoolOptions::new(client_options, 10, 5);
        let pool = build_pool(pool_options).await.expect("Pool failed");

        for _ in 0..1000 {
            let client = pool.get().await.expect("Fail to get client");
            // Requête invalide (table inexistante)
            let result = client
                .query("SELECT * FROM table_qui_nexiste_pas", vec![])
                .await;

            assert!(result.is_err());

            assert!(client.has_broken());
        }
    }

    #[tokio::test]
    async fn test_pool_concurrent_error_handling_retry_loop() {
        init_logger();
        let client_options = create_test_client_option();

        let pool_options = PoolOptions::new(client_options, 10, 5);
        let pool = build_pool(pool_options).await.expect("Pool failed");
        let pool = Arc::new(pool);

        let mut join_set = JoinSet::new();

        for i in 0..1000 {
            let pool = Arc::clone(&pool);

            join_set.spawn(async move {
                let client = pool.get().await.expect("Fail to get client");

                let result = client
                    .query("SELECT * FROM table_qui_nexiste_pas", vec![])
                    .await;
                assert!(result.is_err(), "Iteration {}: Expected error", i);

                assert!(client.has_broken());
            });
        }

        // Attend que toutes les tâches se terminent
        while let Some(result) = join_set.join_next().await {
            result.expect("Task panicked");
        }
    }

    #[tokio::test]
    async fn test_array_col_query() {
        init_logger();
        // Configuration du client
        let options = create_test_client_option();

        // Création et connexion du client
        let client = Client::new(options).await.expect("Failed to create client");
        client.connect().await.expect("Failed to connect");

        // Exécution de la requête
        let result = client
            .query("SELECT tags from time_entries limit 1;", vec![])
            .await;

        match result {
            Ok(df) => {
                // Assertions basiques
                assert!(df.height() == 1, "Should have 1 rows");

                let schema = df.schema();

                let tag_field = schema.get_field("tags").unwrap();

                assert_eq!(tag_field.dtype, DataType::List(Box::new(DataType::String)));
            }
            Err(e) => {
                panic!("Query failed: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_array_col_no_row_query() {
        init_logger();
        // Configuration du client
        let options = create_test_client_option();

        // Création et connexion du client
        let client = Client::new(options).await.expect("Failed to create client");
        client.connect().await.expect("Failed to connect");

        // Exécution de la requête
        let result = client
            .query("SELECT tags from time_entries WHERE FALSE;", vec![])
            .await;

        match result {
            Ok(df) => {
                // Assertions basiques
                assert!(df.height() == 0, "Should have 0 rows");

                let schema = df.schema();

                let tag_field = schema.get_field("tags").unwrap();

                assert_eq!(tag_field.dtype, DataType::List(Box::new(DataType::String)));
            }
            Err(e) => {
                panic!("Query failed: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_query_cancellation_with_select() {
        init_logger();
        let options = create_test_client_option();
        let client = Arc::new(Client::new(options).await.expect("Failed to create client"));
        client.connect().await.expect("Failed to connect");

        for i in 0..10 {
            println!("loop {:?}", i);
            let result = tokio::select! {
                res = client.query(
                    "SELECT i, md5(i::text) FROM generate_series(1, 1000000) i",
                    vec![]
                ) => {
                    panic!("Query should have been cancelled, got: {:?}", res);
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    "cancelled"
                }
            };

            assert_eq!(result, "cancelled");

            let result = client
                .query(
                    "SELECT i, md5(i::text) FROM generate_series(1, 1000000) i",
                    vec![],
                )
                .await;

            println!("{:?}", result);
        }
    }
}
