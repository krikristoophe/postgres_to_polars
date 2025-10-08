use crate::ClientOptions;

#[derive(Debug, Clone)]
pub struct PoolOptions {
    pub max_connections: u32,
    pub client_options: ClientOptions,
}

impl PoolOptions {
    pub fn new(client_options: ClientOptions, max_connections: u32) -> Self {
        Self {
            max_connections,
            client_options,
        }
    }
}
