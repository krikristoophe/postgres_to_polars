use std::collections::HashMap;

use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};

use super::client_options::ClientOptions;
use super::params::BinaryParam;
use crate::PgToPlError;
use crate::models::column_result::{
    ColumnStorage, clone_storages, column_from_field, column_to_series, push_column_value,
};
use crate::models::params::format_params;
use crate::utils::error::{MessageX, PgToPlResult};
use crate::utils::{md5_hash, statement_name};
use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use polars::prelude::*;
use postgres_protocol::IsNull;
use postgres_protocol::message::backend;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, error, warn};

#[derive(Debug, Clone)]
struct PreparedStatementInfo {
    param_types: Vec<u32>,
    columns: Vec<ColumnStorage>,
}

pub struct Client {
    healthy: AtomicBool,
    ready: AtomicBool,
    options: ClientOptions,
    stream: Mutex<TcpStream>,
    prepared_statements: Mutex<HashMap<String, PreparedStatementInfo>>,
    portal_count: Mutex<i32>,
    monkey_chaos_already_prepare: bool,
}

impl Client {
    pub async fn new(options: ClientOptions) -> PgToPlResult<Self> {
        let stream = TcpStream::connect(options.connect_url()).await?;
        Ok(Client {
            monkey_chaos_already_prepare: options.monkey_chaos_already_prepare,
            healthy: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            options,
            stream: Mutex::new(stream),
            prepared_statements: Mutex::new(HashMap::new()),
            portal_count: Mutex::new(0),
        })
    }

    pub fn with_monkey_chaos_already_prepare(mut self) -> Self {
        self.monkey_chaos_already_prepare = true;
        self
    }

    pub async fn connect(&self) -> PgToPlResult<()> {
        let mut stream = self.stream.lock().await;
        self._connect(&mut stream).await
    }

    pub async fn _connect(&self, stream: &mut TcpStream) -> PgToPlResult<()> {
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

        let mut error_to_return: Option<PgToPlError> = None;

        let mut done = false;

        while !done {
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
                self.mark_unhealthy();
                return Err(PgToPlError::ConnectionClosed);
            }

            loop {
                let message = backend::Message::parse(&mut read_buffer);

                match message {
                    Ok(Some(backend::Message::ReadyForQuery(_))) => {
                        done = true;
                        if error_to_return.is_some() {
                            self.mark_unhealthy();
                        }

                        break;
                    }
                    Ok(Some(backend::Message::ErrorResponse(error))) => {
                        self.mark_unhealthy();

                        if error_to_return.is_none() {
                            error_to_return = Some(error.into());
                        }
                    }
                    Ok(Some(backend::Message::AuthenticationCleartextPassword)) => {
                        warn!("Authentication: Cleartext password requested");
                    }
                    Ok(Some(backend::Message::AuthenticationMd5Password(salt))) => {
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
                    //
                    Ok(Some(backend::Message::ParameterStatus(body))) => {
                        // ✅ Normal : le serveur nous envoie les paramètres de session
                        let name = match body.name() {
                            Ok(name) => name,
                            Err(_) => "fail to parse parameter name",
                        };
                        let value = match body.value() {
                            Ok(value) => value,
                            Err(_) => "fail to parse parameter value",
                        };

                        debug!("Parameter status: {} = {}", name, value);
                    }
                    Ok(Some(backend::Message::AuthenticationOk))
                    | Ok(Some(backend::Message::BackendKeyData(_))) => {
                        // Skip
                    }
                    //
                    Ok(Some(other)) => {
                        warn!("[Connect] Unhandled message: {:?}", other.message_name());
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        error_to_return = Some(e.into());
                        break;
                    }
                }
            }
        }

        if let Some(error_to_return) = error_to_return {
            return Err(error_to_return);
        } else {
            self.healthy.store(true, Ordering::Relaxed);
            self.ready.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    pub async fn reset(&self, stream: &mut TcpStream) -> PgToPlResult<()> {
        warn!("Resetting client connection");
        // 1. Fermer proprement l'ancienne connexion si possible
        let _ = stream.shutdown().await; // Ignore les erreurs

        {
            let _ = stream.shutdown().await;

            *stream = TcpStream::connect(self.options.connect_url()).await?;

            // 4. Réinitialiser l'état
            self.healthy.store(false, Ordering::SeqCst);
            self.ready.store(false, Ordering::SeqCst);
            self.prepared_statements.lock().await.clear();
            *self.portal_count.lock().await = 0;
        }

        self._connect(stream).await?;

        Ok(())
    }

    pub async fn query<P>(&self, query: &str, params: P) -> PgToPlResult<DataFrame>
    where
        P: IntoIterator<Item = Option<BinaryParam>> + Clone + Debug,
    {
        if self.has_broken() {
            return Err(PgToPlError::ConnectionBroken);
        }

        let mut stream = self.stream.lock().await;

        if !self.ready.load(Ordering::Relaxed) {
            self.reset(&mut stream).await?;
        }

        self.ready.store(false, Ordering::Relaxed);

        let res = self._query(query, params.clone(), &mut stream).await;

        if let Err(error) = &res {
            self.mark_unhealthy();
            error!(
                "Failed to execute query '{}' with params: {:?}: {}",
                query, params, error
            );
        }

        if !self.has_broken() {
            self.ready.store(true, Ordering::Relaxed);
        }

        res
    }

    pub async fn _query<P>(
        &self,
        query: &str,
        params: P,
        stream: &mut TcpStream,
    ) -> PgToPlResult<DataFrame>
    where
        P: IntoIterator<Item = Option<BinaryParam>> + Clone,
    {
        if self.has_broken() {
            self.mark_unhealthy();
            return Err(PgToPlError::ConnectionBroken);
        }

        let portal_count = {
            let mut count = self.portal_count.lock().await;
            *count += 1;
            *count
        };

        let portal_name = format!("portal_{}", portal_count);

        let (param_types, param_values) = format_params(params.clone());

        let name = statement_name(query);
        let mut prepared_statements = self.prepared_statements.lock().await;

        let (prepare, mut columns) = match prepared_statements.get(&name) {
            Some(info) => {
                if info.param_types != param_types {
                    return Err(PgToPlError::ParamTypeMismatch);
                }
                if self.monkey_chaos_already_prepare && false {
                    debug!("Monkey chaos already prepare");
                    (true, Vec::new())
                } else {
                    (false, info.columns.clone())
                }
            }
            None => (true, Vec::new()),
        };

        // Step 1 : Prepare the statement if not already prepared
        if prepare {
            columns = self
                .prepare_query(&name, query, &param_types, stream)
                .await?;
            prepared_statements.insert(
                name.clone(),
                PreparedStatementInfo {
                    param_types: param_types.clone(),
                    columns: clone_storages(&columns),
                },
            );
        }

        // Step 2 : Bind prepared statement to portal
        {
            let mut buf = BytesMut::new(); // <-- au lieu de Vec<u8>
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

            // Step 3 : Execute

            frontend::execute(&portal_name, 0, &mut buf)?;
            frontend::sync(&mut buf);
            stream.write_all(&buf).await?;
        }
        let mut error_to_return: Option<PgToPlError> = None;

        // Step 4 : read response
        {
            let mut read_buffer = BytesMut::with_capacity(8192);
            let mut done = false;

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

                loop {
                    let message = backend::Message::parse(&mut read_buffer);

                    match message {
                        Ok(Some(backend::Message::DataRow(row))) => {
                            let buf = row.buffer();
                            let mut ranges = row.ranges(); // FallibleIterator

                            for (i, col) in columns.iter_mut().enumerate() {
                                let next = ranges.next(); // Result<Option<Option<Range>>>
                                match next {
                                    Ok(Some(Some(r))) => {
                                        let res = push_column_value(col, Some(&buf[r]));

                                        if let Err(e) = res {
                                            self.mark_unhealthy();
                                            error_to_return = Some(e);
                                        }
                                    }
                                    Ok(Some(None)) => {
                                        let res = push_column_value(col, None);
                                        if let Err(e) = res {
                                            self.mark_unhealthy();
                                            error_to_return = Some(e);
                                        }
                                    }
                                    Ok(None) => {
                                        self.mark_unhealthy();
                                        // trop peu de champs côté serveur
                                        error_to_return =
                                            Some(PgToPlError::TooFewField(columns.len(), i));
                                        break;
                                    }
                                    Err(e) => {
                                        self.mark_unhealthy();
                                        error_to_return = Some(e.into());
                                    }
                                }
                            }
                            // champs en trop ?
                            if error_to_return.is_none() && ranges.next()?.is_some() {
                                self.mark_unhealthy();
                                error_to_return = Some(PgToPlError::TooManyField(columns.len()));
                            }
                        }
                        Ok(Some(backend::Message::ReadyForQuery(_))) => {
                            done = true;
                            if error_to_return.is_some() {
                                self.mark_unhealthy();
                            }
                        }
                        Ok(Some(backend::Message::ErrorResponse(error))) => {
                            self.mark_unhealthy();

                            if error_to_return.is_none() {
                                error_to_return = Some(error.into());
                            }
                        }
                        //
                        Ok(Some(backend::Message::CommandComplete(body))) => match body.tag() {
                            Ok(tag) => {
                                debug!("Command completed: {}", tag);
                            }
                            Err(err) => {
                                warn!("Error parsing command tag: {}", err);
                            }
                        },
                        Ok(Some(backend::Message::BindComplete)) => {}
                        Ok(Some(backend::Message::EmptyQueryResponse)) => {
                            debug!("Empty query response");
                        }
                        //
                        Ok(Some(other)) => {
                            warn!("[Read] Unhandled message: {:?}", other.message_name());
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(e) => {
                            error_to_return = Some(e.into());
                            break;
                        }
                    }
                }
            }
        }

        if let Some(error_to_return) = error_to_return {
            self.mark_unhealthy();
            Err(error_to_return)
        } else {
            drop(prepared_statements);

            let series = columns
                .into_iter()
                .map(|col| column_to_series(col))
                .collect::<PgToPlResult<Vec<_>>>()?;

            Ok(DataFrame::from_iter(series))
        }
    }

    pub fn has_broken(&self) -> bool {
        !self.healthy.load(Ordering::Relaxed)
    }

    fn mark_unhealthy(&self) {
        self.healthy.store(false, Ordering::Relaxed);
    }

    pub async fn ping(&self) -> PgToPlResult<()> {
        if self.has_broken() {
            return Err(PgToPlError::ConnectionBroken);
        }

        let mut stream = self.stream.lock().await;

        let mut buf = BytesMut::new();
        frontend::query("/* ping */ SELECT 1;", &mut buf)?;
        stream.write_all(&buf).await?;

        // Lire jusqu'à ReadyForQuery (drain complet)
        let mut read_buffer = BytesMut::with_capacity(4096);
        let mut error_to_return: Option<PgToPlError> = None;
        let mut done = false;

        while !done {
            read_buffer.reserve(4096);
            let dst = read_buffer.chunk_mut();
            let buf: &mut [u8] =
                unsafe { std::slice::from_raw_parts_mut(dst.as_mut_ptr(), dst.len()) };
            let n = stream.read(buf).await?;
            unsafe {
                read_buffer.advance_mut(n);
            }
            if n == 0 {
                self.mark_unhealthy();
                return Err(PgToPlError::ConnectionClosed);
            }

            loop {
                let message = backend::Message::parse(&mut read_buffer);

                match message {
                    Ok(Some(backend::Message::ReadyForQuery(_))) => {
                        done = true;
                        break;
                    }
                    Ok(Some(backend::Message::ErrorResponse(error))) => {
                        self.mark_unhealthy();

                        if error_to_return.is_none() {
                            error_to_return = Some(error.into());
                        }
                    }
                    Ok(Some(backend::Message::CommandComplete(body))) => match body.tag() {
                        Ok(tag) => {
                            debug!("Command completed: {}", tag);
                        }
                        Err(err) => {
                            warn!("Error parsing command tag: {}", err);
                        }
                    },
                    Ok(Some(backend::Message::RowDescription(_)))
                    | Ok(Some(backend::Message::DataRow(_))) => {
                        // Skip
                    }

                    Ok(Some(other)) => {
                        warn!("[Ping] Unhandled message: {:?}", other.message_name());
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        error_to_return = Some(e.into());
                        break;
                    }
                }
            }
        }

        if let Some(err) = error_to_return {
            self.mark_unhealthy();
            Err(err)
        } else {
            Ok(())
        }
    }

    async fn prepare_query(
        &self,
        name: &str,
        query: &str,
        param_types: &Vec<u32>,
        stream: &mut TcpStream,
    ) -> PgToPlResult<Vec<ColumnStorage>> {
        let res = self._prepare_query(name, query, param_types, stream).await;
        if let Ok(columns) = res {
            Ok(columns)
        } else {
            self.close_statement(name, stream).await?;
            // discard + reprepare
            self._prepare_query(name, query, param_types, stream).await
        }
    }

    async fn _prepare_query(
        &self,
        name: &str,
        query: &str,
        param_types: &Vec<u32>,
        stream: &mut TcpStream,
    ) -> PgToPlResult<Vec<ColumnStorage>> {
        let mut buf = BytesMut::new();
        let mut read_buffer = BytesMut::with_capacity(4096);
        frontend::parse(&name, query, param_types.iter().copied(), &mut buf)?;
        frontend::describe(b'S', &name, &mut buf)?;
        frontend::sync(&mut buf);
        stream.write_all(&buf).await?;

        let mut done = false;

        let mut error_to_return: Option<PgToPlError> = None;

        let mut columns = vec![];

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

            loop {
                let message = backend::Message::parse(&mut read_buffer);

                match message {
                    Ok(Some(backend::Message::RowDescription(desc))) => {
                        columns.clear();
                        let fields = desc.fields().iterator();
                        for field in fields {
                            let f = field?;

                            columns.push(column_from_field(&f))
                        }
                    }

                    Ok(Some(backend::Message::ReadyForQuery(_))) => {
                        done = true;

                        break;
                    }
                    Ok(Some(backend::Message::ErrorResponse(error))) => {
                        if error_to_return.is_none() {
                            error_to_return = Some(error.into());
                        }
                    }
                    Ok(Some(backend::Message::ParameterDescription(body))) => {
                        let parameters = body.parameters().iterator();
                        let mut index = 0;
                        for parameter in parameters {
                            if let Ok(parameter) = parameter {
                                if let Some(param_type) = param_types.get(index) {
                                    if parameter != *param_type {
                                        warn!(
                                            "Parameter type mismatch for stmt '{}': Provided {}, expected {}",
                                            name, parameter, param_types[index]
                                        );
                                    }
                                } else {
                                    warn!(
                                        "Unexpected parameter type for stmt '{}'. Bad number of parameters will occur. Expected parameter {} at index {}",
                                        name, parameter, index
                                    );
                                }
                            } else {
                                warn!("Failed to parse parameter description for stmt '{}'", name);
                            }
                            index += 1;
                        }
                        if index != param_types.len() {
                            warn!(
                                "Parameter description mismatch for stmt '{}': Provided {}, expected {}",
                                name,
                                param_types.len(),
                                index,
                            );
                        }
                    }
                    //
                    Ok(Some(backend::Message::ParseComplete)) => {
                        // ✅ C'est normal, le Parse a réussi
                        debug!("Statement '{}' parsed successfully", name);
                    }

                    //
                    Ok(Some(other)) => {
                        warn!("[Prepare] Unhandled message: {:?}", other.message_name());
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        error_to_return = Some(e.into());
                        break;
                    }
                }
            }
        }

        if let Some(error_to_return) = error_to_return {
            self.mark_unhealthy();
            Err(error_to_return)
        } else {
            Ok(columns)
        }
    }

    pub async fn close_statement(&self, name: &str, stream: &mut TcpStream) -> PgToPlResult<()> {
        let mut buf = BytesMut::new();

        // Close message : type 'S' pour Statement (ou 'P' pour Portal)
        frontend::close(b'S', name, &mut buf)?;
        frontend::sync(&mut buf);

        stream.write_all(&buf).await?;

        // Lire la réponse jusqu'à ReadyForQuery
        let mut read_buffer = BytesMut::with_capacity(4096);

        let mut error_to_return: Option<PgToPlError> = None;
        let mut done = false;

        while !done {
            let n = {
                read_buffer.reserve(4096);
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
                return Err(PgToPlError::ConnectionClosed);
            }

            loop {
                let message = backend::Message::parse(&mut read_buffer);

                match message {
                    Ok(Some(backend::Message::CloseComplete)) => {}
                    Ok(Some(backend::Message::ReadyForQuery(_))) => {
                        done = true;
                        break;
                    }
                    Ok(Some(backend::Message::ErrorResponse(error))) => {
                        error_to_return = Some(error.into());
                    }
                    Ok(Some(other)) => {
                        warn!("[CloseStmt] Unhandled message: {:?}", other.message_name());
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        error_to_return = Some(e.into());
                        break;
                    }
                }
            }
        }

        if let Some(error_to_return) = error_to_return {
            self.mark_unhealthy();
            Err(error_to_return)
        } else {
            Ok(())
        }
    }
}
