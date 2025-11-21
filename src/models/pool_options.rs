use crate::ClientOptions;

#[derive(Debug, Clone)]
pub struct PoolOptions {
    pub max_connections: u32,
    pub client_options: ClientOptions,
    pub acquire_timeout: u64,
}

impl PoolOptions {
    pub fn new(client_options: ClientOptions, max_connections: u32, acquire_timeout: u64) -> Self {
        Self {
            max_connections,
            client_options,
            acquire_timeout,
        }
    }
}
