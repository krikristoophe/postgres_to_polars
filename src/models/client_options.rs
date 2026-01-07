#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub user: String,
    pub password: String,
    pub database: String,
    pub host: String,
    pub port: u16,
    pub monkey_chaos_already_prepare: bool,
}

impl ClientOptions {
    pub fn new(user: String, password: String, database: String, host: String, port: u16) -> Self {
        ClientOptions {
            user,
            password,
            database,
            host,
            port,
            monkey_chaos_already_prepare: false,
        }
    }

    pub fn with_monkey_chaos_already_prepare(mut self) -> Self {
        self.monkey_chaos_already_prepare = true;
        self
    }

    pub fn connect_url(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
