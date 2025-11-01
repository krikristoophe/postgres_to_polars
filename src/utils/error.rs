use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgToPlError {
    #[error("I/O error {0}")]
    Io(#[from] std::io::Error),
    #[error("Only 1D arrays supported")]
    OnlyOneDimensionArraySupported,
    #[error("Not enough bytes")]
    NotEnoughBytes,
    #[error("Bind error")]
    BindError,
    #[error("Row has fewer fields ({0}) than expected ({1})")]
    TooFewField(usize, usize),
    #[error("Row has more fields than expected ({0})")]
    TooManyField(usize),
    #[error("Ping failed")]
    PingFailed,
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Pool error {0}")]
    PoolError(String),
}

pub type PgToPlResult<T> = Result<T, PgToPlError>;
