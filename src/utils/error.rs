use std::array::TryFromSliceError;

use postgres_protocol::message::backend;
use thiserror::Error;

use crate::models::postgres_error::PostgresError;

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
    QueryError(PostgresError),
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
    #[error("Timeout")]
    Timeout,
}

pub type PgToPlResult<T> = Result<T, PgToPlError>;

pub trait MessageX {
    fn message_name(&self) -> &'static str;
}

impl MessageX for backend::Message {
    fn message_name(&self) -> &'static str {
        match self {
            backend::Message::AuthenticationCleartextPassword => "AuthenticationCleartextPassword",
            backend::Message::AuthenticationGss => "AuthenticationGss",
            backend::Message::AuthenticationKerberosV5 => "AuthenticationKerberosV5",
            backend::Message::AuthenticationMd5Password(_) => "AuthenticationMd5Password",
            backend::Message::AuthenticationOk => "AuthenticationOk",
            backend::Message::AuthenticationScmCredential => "AuthenticationScmCredential",
            backend::Message::AuthenticationSspi => "AuthenticationSspi",
            backend::Message::AuthenticationGssContinue(_) => "AuthenticationGssContinue",
            backend::Message::AuthenticationSasl(_) => "AuthenticationSasl",
            backend::Message::AuthenticationSaslContinue(_) => "AuthenticationSaslContinue",
            backend::Message::AuthenticationSaslFinal(_) => "AuthenticationSaslFinal",
            backend::Message::BackendKeyData(_) => "BackendKeyData",
            backend::Message::BindComplete => "BindComplete",
            backend::Message::CloseComplete => "CloseComplete",
            backend::Message::CommandComplete(_) => "CommandComplete",
            backend::Message::CopyData(_) => "CopyData",
            backend::Message::CopyDone => "CopyDone",
            backend::Message::CopyInResponse(_) => "CopyInResponse",
            backend::Message::CopyOutResponse(_) => "CopyOutResponse",
            backend::Message::DataRow(_) => "DataRow",
            backend::Message::EmptyQueryResponse => "EmptyQueryResponse",
            backend::Message::ErrorResponse(_) => "ErrorResponse",
            backend::Message::NoData => "NoData",
            backend::Message::NoticeResponse(_) => "NoticeResponse",
            backend::Message::NotificationResponse(_) => "NotificationResponse",
            backend::Message::ParameterDescription(_) => "ParameterDescription",
            backend::Message::ParameterStatus(_) => "ParameterStatus",
            backend::Message::ParseComplete => "ParseComplete",
            backend::Message::PortalSuspended => "PortalSuspended",
            backend::Message::ReadyForQuery(_) => "ReadyForQuery",
            backend::Message::RowDescription(_) => "RowDescription",
            _ => "UnknownMessage",
        }
    }
}
