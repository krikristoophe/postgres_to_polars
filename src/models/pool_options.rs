use crate::ClientOptions;

#[derive(Debug, Clone)]
pub struct PoolOptions {
    pub max_connections: usize,
    pub client_options: ClientOptions,
}

impl PoolOptions {
    pub fn new(client_options: ClientOptions, max_connections: usize) -> Self {
        Self {
            max_connections,
            client_options,
        }
    }
}
