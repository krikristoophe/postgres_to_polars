use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::client_options::ClientOptions;
use super::params::BinaryParam;
use crate::PgToPlError;
use crate::models::column_result::{
    ColumnStorage, clone_storages, column_from_field, column_to_series, push_column_value,
};
use crate::models::params::format_params;
use crate::utils::error::PgToPlResult;
use crate::utils::{error_to_string, md5_hash, print_error, statement_name};
use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use nanoid::nanoid;
use polars::prelude::*;
use postgres_protocol::IsNull;
use postgres_protocol::message::backend;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct PreparedStatementInfo {
    param_types: Vec<u32>,
    columns: Vec<ColumnStorage>,
}

pub struct Client {
    healthy: AtomicBool,
    options: ClientOptions,
    stream: Arc<Mutex<TcpStream>>,
    prepared_statements: Mutex<HashMap<String, PreparedStatementInfo>>,
    portal_count: Mutex<i32>,
}

impl Client {
    pub async fn new(options: ClientOptions) -> Self {
        let stream = TcpStream::connect(options.connect_url()).await.unwrap();
        Client {
            healthy: AtomicBool::new(false),
            options,
            stream: Arc::new(Mutex::new(stream)),
            prepared_statements: Mutex::new(HashMap::new()),
            portal_count: Mutex::new(0),
        }
    }

    pub async fn replace(&self) -> Self {
        Client::new(self.options.clone()).await
    }

    pub async fn connect(&self) -> PgToPlResult<()> {
        let mut stream = self.stream.lock().await;
        // Handshake initial
        let mut buf = BytesMut::new(); // <-- au lieu de Vec<u8>
        frontend::startup_message(
            [
                ("user", self.options.user.as_str()),
                ("database", self.options.database.as_str()),
            ],
            &mut buf,
        )?;
        stream.write_all(&buf).await?;

        // Lecture des messages d'accueil jusqu'à ReadyForQuery

        let mut read_buffer = BytesMut::with_capacity(8192);

        loop {
            read_buffer.reserve(8192);
            let n = {
                read_buffer.reserve(8192);
                let dst = read_buffer.chunk_mut();
                let buf: &mut [u8] =
                    unsafe { std::slice::from_raw_parts_mut(dst.as_mut_ptr(), dst.len()) };
                let n = stream.read(buf).await?;
                unsafe {
                    read_buffer.advance_mut(n);
                }
                n
            };

            if n == 0 {
                break; // Connexion fermée
            }

            let mut ready = false;

            while let Some(message) = backend::Message::parse(&mut read_buffer)? {
                match message {
                    backend::Message::ReadyForQuery(_) => {
                        ready = true;
                        break;
                    }
                    backend::Message::ErrorResponse(error) => {
                        print_error(&error);
                    }
                    backend::Message::AuthenticationCleartextPassword => {
                        println!("Authentication: Cleartext password requested");
                    }
                    backend::Message::AuthenticationMd5Password(salt) => {
                        let mut buf = BytesMut::new(); // <-- au lieu de Vec<u8>
                        frontend::password_message(
                            md5_hash(
                                self.options.user.as_str(),
                                self.options.password.as_str(),
                                &salt.salt(),
                            )
                            .as_bytes(),
                            &mut buf,
                        )?;
                        stream.write_all(&buf).await?;
                    }
                    _ => {}
                }
            }
            if ready {
                break;
            }
        }

        self.mark_healthy();

        Ok(())
    }

    pub async fn query<P>(&self, query: &str, params: P) -> PgToPlResult<DataFrame>
    where
        P: IntoIterator<Item = Option<BinaryParam>>,
    {
        let portal_count = {
            let mut count = self.portal_count.lock().await;
            *count += 1;
            *count
        };
        let portal_name = format!("portal_{}", portal_count);

        let mut buf = BytesMut::new(); // <-- au lieu de Vec<u8>

        let (param_types, param_values) = format_params(params);

        let name = if self.options.prepare {
            statement_name(query)
        } else {
            nanoid!()
        };
        let mut prepared_statements = self.prepared_statements.lock().await;

        let (prepare, mut columns) = match prepared_statements.get(&name) {
            Some(info) => {
                if info.param_types != param_types {
                    return Err(PgToPlError::ParamTypeMismatch);
                }
                (false, info.columns.clone())
            }
            None => (true, Vec::new()),
        };

        let mut stream = self.stream.lock().await;

        if prepare {
            frontend::parse(&name, query, param_types.iter().copied(), &mut buf)?;
            stream.write_all(&buf).await?;

            buf.clear();
            frontend::describe(b'S', &name, &mut buf)?;
            stream.write_all(&buf).await?;
        }

        // Étape 2 : Bind avec result_format = binaire
        buf.clear();
        frontend::bind(
            &portal_name,
            &name,
            std::iter::repeat(1).take(param_values.len()), // format binaire
            param_values.iter(),
            |val, buf| match val {
                Some(bytes) => {
                    buf.put_slice(bytes);
                    Ok(IsNull::No)
                }
                None => Ok(IsNull::Yes),
            },
            [1],
            &mut buf,
        )
        .map_err(|_| PgToPlError::BindError)?;
        stream.write_all(&buf).await?;

        // Étape 3 : Execute
        buf.clear();
        frontend::execute(&portal_name, 0, &mut buf)?;
        stream.write_all(&buf).await?;

        frontend::close(b'P', &portal_name, &mut buf)?;
        stream.write_all(&buf).await?;

        // Étape 4 : Sync
        buf.clear();
        frontend::sync(&mut buf);
        stream.write_all(&buf).await?;

        // Lire les messages de réponse
        let mut read_buffer = BytesMut::with_capacity(8192);

        let mut done = false;

        let mut error_to_return: Option<String> = None;

        while !done {
            let n = {
                read_buffer.reserve(8192);
                let dst = read_buffer.chunk_mut();
                let buf: &mut [u8] =
                    unsafe { std::slice::from_raw_parts_mut(dst.as_mut_ptr(), dst.len()) };
                let n = stream.read(buf).await?;
                unsafe {
                    read_buffer.advance_mut(n);
                }
                n
            };

            if n == 0 {
                self.mark_unhealthy();
                return Err(PgToPlError::ConnectionClosed);
            }
            while let Some(message) = backend::Message::parse(&mut read_buffer)? {
                match message {
                    backend::Message::RowDescription(desc) => {
                        columns.clear();
                        let fields = desc.fields().iterator();
                        for field in fields {
                            let f = field?;

                            columns.push(column_from_field(&f))
                        }
                    }
                    backend::Message::DataRow(row) => {
                        let buf = row.buffer();
                        let mut ranges = row.ranges(); // FallibleIterator

                        for (i, col) in columns.iter_mut().enumerate() {
                            let next = ranges.next()?; // Result<Option<Option<Range>>>
                            match next {
                                Some(Some(r)) => push_column_value(col, Some(&buf[r])),
                                Some(None) => push_column_value(col, None),
                                None => {
                                    prepared_statements.remove(&name);
                                    // trop peu de champs côté serveur
                                    return Err(PgToPlError::TooFewField(i, columns.len()));
                                }
                            }
                        }
                        // champs en trop ?
                        if ranges.next()?.is_some() {
                            prepared_statements.remove(&name);
                            return Err(PgToPlError::TooManyField(columns.len()));
                        }
                    }
                    backend::Message::NoData => {
                        done = true;
                        self.mark_healthy();
                        break;
                    }
                    backend::Message::ReadyForQuery(_) => {
                        done = true;
                        if let Some(err_msg) = error_to_return {
                            self.mark_unhealthy();
                            return Err(PgToPlError::QueryError(err_msg));
                        }

                        self.mark_healthy();
                        break;
                    }
                    backend::Message::ErrorResponse(error) => {
                        let error_msg = error_to_string(&error);

                        if error_to_return.is_none() {
                            error_to_return = Some(error_msg);
                        }
                    }
                    _ => {}
                }
            }
        }

        if prepare && self.options.prepare {
            prepared_statements.insert(
                name.clone(),
                PreparedStatementInfo {
                    param_types: param_types.clone(),
                    columns: clone_storages(&columns),
                },
            );
        }

        Ok(DataFrame::from_iter(
            columns.into_iter().map(|col| column_to_series(col)),
        ))
    }

    pub fn has_broken(&self) -> bool {
        !self.healthy.load(Ordering::Relaxed)
    }

    fn mark_unhealthy(&self) {
        self.healthy.store(false, Ordering::Relaxed);
    }
    fn mark_healthy(&self) {
        self.healthy.store(true, Ordering::Relaxed);
    }

    pub async fn ping(&self) -> PgToPlResult<()> {
        let mut stream = self.stream.lock().await;
        let mut buf = BytesMut::new();
        frontend::query("/* ping */ SELECT 1", &mut buf)?;
        stream.write_all(&buf).await?;

        // Lire jusqu'à ReadyForQuery (drain complet)
        let mut read_buffer = BytesMut::with_capacity(4096);
        loop {
            read_buffer.reserve(4096);
            let dst = read_buffer.chunk_mut();
            let buf: &mut [u8] =
                unsafe { std::slice::from_raw_parts_mut(dst.as_mut_ptr(), dst.len()) };
            let n = stream.read(buf).await?;
            unsafe {
                read_buffer.advance_mut(n);
            }
            if n == 0 {
                return Err(PgToPlError::ConnectionClosed);
            }

            while let Some(m) = backend::Message::parse(&mut read_buffer)? {
                match m {
                    backend::Message::ReadyForQuery(_) => {
                        self.mark_healthy();
                        return Ok(());
                    }
                    backend::Message::ErrorResponse(e) => {
                        self.mark_unhealthy();
                        return Err(PgToPlError::PingFailed(error_to_string(&e)));
                    }
                    _ => {}
                }
            }
        }
    }
}
