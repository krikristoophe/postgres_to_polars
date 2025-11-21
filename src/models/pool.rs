use std::time::Duration;

use bb8::{ManageConnection, Pool};

use crate::{Client, ClientOptions, PgToPlError, PoolOptions, utils::error::PgToPlResult};

pub struct ClientManager {
    pub options: ClientOptions,
}

//#[async_trait]
impl ManageConnection for ClientManager {
    type Connection = Client;
    type Error = PgToPlError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let client = Client::new(self.options.clone()).await;
        client.connect().await?;
        Ok(client)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.ping().await
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.has_broken()
    }
}

pub async fn build_pool(opts: PoolOptions) -> PgToPlResult<Pool<ClientManager>> {
    let mgr = ClientManager {
        options: opts.client_options,
    };
    let pool = Pool::builder()
        .max_size(opts.max_connections)
        .connection_timeout(Duration::from_secs(opts.acquire_timeout))
        .idle_timeout(Some(Duration::from_secs(60)))
        .max_lifetime(Some(Duration::from_secs(30 * 60)))
        .test_on_check_out(true)
        .build(mgr)
        .await?;
    Ok(pool)
}

pub type PgToPlPool = Pool<ClientManager>;
