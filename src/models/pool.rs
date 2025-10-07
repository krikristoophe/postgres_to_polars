use futures::future::join_all;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::Client;

use super::pool_options::PoolOptions;

pub struct Pool {
    available: Arc<Mutex<Vec<Arc<Client>>>>,
    semaphore: Arc<Semaphore>,
}

pub struct ClientRef {
    client: Arc<Client>,
    pool: Arc<Mutex<Vec<Arc<Client>>>>,
    _permit: OwnedSemaphorePermit, // RAII: libéré au drop
}

impl ClientRef {
    pub fn client(&mut self) -> Arc<Client> {
        self.client.clone()
    }
}

impl Drop for ClientRef {
    fn drop(&mut self) {
        let client = Arc::clone(&self.client);
        let pool = Arc::clone(&self.pool);
        tokio::spawn(async move {
            let is_healthy = { client.is_healthy.lock().await.clone() };
            let client = if !is_healthy {
                Arc::new(client.replace().await)
            } else {
                client
            };

            let mut locked = pool.lock().await;
            locked.push(client);
        });
    }
}

impl Pool {
    pub async fn new(options: PoolOptions) -> anyhow::Result<Self> {
        let client_options = options.client_options;

        let shared_opts = Arc::new(client_options);

        // Crée tous les clients en parallèle
        let client_futures = (0..options.max_connections).map(|_| {
            let opts = shared_opts.clone();
            async move {
                let client = Client::new((*opts).clone()).await;
                client.connect().await?;
                anyhow::Ok(Arc::new(client))
            }
        });

        // Exécution parallèle
        let clients: Vec<Arc<Client>> = join_all(client_futures)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;

        Ok(Self {
            available: Arc::new(Mutex::new(clients)),
            semaphore: Arc::new(Semaphore::new(options.max_connections)),
        })
    }

    pub async fn acquire(&self) -> anyhow::Result<ClientRef> {
        let permit = self.semaphore.clone().acquire_owned().await?;

        // On attend qu’un client soit dispo
        let client = loop {
            let mut available = self.available.lock().await;
            if let Some(client) = available.pop() {
                break client;
            }
            // Mutex est relâché ici avant await
            drop(available);
            tokio::task::yield_now().await; // évite de bloquer inutilement
        };

        Ok(ClientRef {
            client,
            pool: self.available.clone(),
            _permit: permit,
        })
    }
}
