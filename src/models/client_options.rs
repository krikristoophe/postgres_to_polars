#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub user: String,
    pub password: String,
    pub database: String,
    pub host: String,
    pub port: u16,
    pub prepare: bool,
}

impl ClientOptions {
    pub fn new(
        user: String,
        password: String,
        database: String,
        host: String,
        port: u16,
        prepare: bool,
    ) -> Self {
        ClientOptions {
            user,
            password,
            database,
            host,
            port,
            prepare,
        }
    }

    pub fn connect_url(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
