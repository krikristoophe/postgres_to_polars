mod models;

mod utils;

#[cfg(feature = "execution")]
pub use models::client::Client;
pub use models::client_options::ClientOptions;
pub use models::params::count_placeholders;
pub use models::params::{BinaryParam, IntoBinaryParam};
#[cfg(feature = "execution")]
pub use models::pool::PgToPlPool;
#[cfg(feature = "execution")]
pub use models::pool::build_pool;
pub use models::pool_options::PoolOptions;

pub use utils::error::{PgToPlError, PgToPlResult};

pub use utils::logger::init_logger;

pub use utils::statement_name;
