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
        true,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use postgres_to_polars::{BinaryParam, Client, PoolOptions, build_pool};
    use tokio::task::JoinSet;

    use crate::create_test_client_option;

    #[tokio::test]
    async fn test_simple_query() {
        // Configuration du client
        let options = create_test_client_option();

        // Création et connexion du client
        let client = Client::new(options).await;
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
    async fn test_query_with_params() {
        let options = create_test_client_option();

        let client = Client::new(options).await;
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
        let options = create_test_client_option();

        let client = Client::new(options).await;
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
        let options = create_test_client_option();

        let client = Client::new(options).await;
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
    async fn test_error_handling_retry() {
        let options = create_test_client_option();

        let client = Client::new(options).await;
        client.connect().await.expect("Failed to connect");

        // Requête invalide (table inexistante)
        let result = client
            .query("SELECT * FROM table_qui_nexiste_pas", vec![])
            .await;

        assert!(result.is_err());

        let result = client.query("SELECT * FROM users limit 10;", vec![]).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_error_handling_retry_loop() {
        let options = create_test_client_option();

        let client = Client::new(options).await;
        client.connect().await.expect("Failed to connect");

        for _ in 0..100 {
            // Requête invalide (table inexistante)
            let result = client
                .query("SELECT * FROM table_qui_nexiste_pas", vec![])
                .await;

            assert!(result.is_err());

            let result = client.query("SELECT * FROM users limit 10;", vec![]).await;

            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_pool_error_handling_retry_loop() {
        let client_options = create_test_client_option();

        let pool_options = PoolOptions::new(client_options, 10);
        let pool = build_pool(pool_options).await.expect("Pool failed");

        for _ in 0..1000 {
            let client = pool.get().await.expect("Fail to get client");
            // Requête invalide (table inexistante)
            let result = client
                .query("SELECT * FROM table_qui_nexiste_pas", vec![])
                .await;

            assert!(result.is_err());

            let result = client.query("SELECT * FROM users limit 10;", vec![]).await;

            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_pool_concurrent_error_handling_retry_loop() {
        let client_options = create_test_client_option();

        let pool_options = PoolOptions::new(client_options, 10);
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

                let result = client.query("SELECT * FROM users limit 10;", vec![]).await;
                assert!(result.is_ok(), "Iteration {}: Expected success", i);
            });
        }

        // Attend que toutes les tâches se terminent
        while let Some(result) = join_set.join_next().await {
            result.expect("Task panicked");
        }
    }
}
