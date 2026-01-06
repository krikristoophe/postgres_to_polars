use std::array::TryFromSliceError;

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
    #[error("Ping failed : {0}")]
    PingFailed(String),
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Pool error {0}")]
    PoolError(String),
    #[error("Parameter type mismatch")]
    ParamTypeMismatch,
    #[error("Query error: {0}")]
    QueryError(String),
    #[error("Connection broken")]
    ConnectionBroken,
    #[error("Polars error: {0}")]
    PolarsError(#[from] polars::error::PolarsError),
    #[error("Column type error: {type_name}<{bytes:?}> {error}")]
    ColumnTypeError {
        bytes: Vec<u8>,
        type_name: String,
        error: TryFromSliceError,
    },
}

pub type PgToPlResult<T> = Result<T, PgToPlError>;
