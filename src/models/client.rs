use super::client_options::ClientOptions;
use super::params::BinaryParam;
use crate::models::column_result::{
    ColumnStorage, column_from_field, column_to_series, push_column_value,
};
use crate::models::params::format_params;
use crate::utils::{md5_hash, print_error, statement_name};
use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use polars::prelude::*;
use postgres_protocol::IsNull;
use postgres_protocol::message::backend;
use postgres_protocol::message::backend::DataRowRanges;
use postgres_protocol::message::frontend;
use std::io::{Read, Write};
use std::net::TcpStream;

pub struct Client {
    options: ClientOptions,
    stream: TcpStream,
    portal_name: String,
}

impl Client {
    pub fn new(options: ClientOptions) -> Self {
        let portal_name = format!("portal_{}", uuid::Uuid::new_v4());
        let stream = TcpStream::connect(options.connect_url()).unwrap();
        Client {
            options,
            stream,
            portal_name,
        }
    }

    pub fn connect(&self) -> anyhow::Result<()> {
        let mut stream = &self.stream;
        // Handshake initial
        let mut buf = BytesMut::new(); // <-- au lieu de Vec<u8>
        frontend::startup_message(
            [
                ("user", self.options.user.as_str()),
                ("database", self.options.database.as_str()),
            ],
            &mut buf,
        )?;
        stream.write_all(&buf)?;

        // Lecture des messages d'accueil jusqu'à ReadyForQuery

        let mut read_buffer = BytesMut::with_capacity(8192);

        loop {
            read_buffer.reserve(8192);
            let n = {
                read_buffer.reserve(8192);
                let dst = read_buffer.chunk_mut();
                let buf: &mut [u8] =
                    unsafe { std::slice::from_raw_parts_mut(dst.as_mut_ptr(), dst.len()) };
                let n = stream.read(buf)?;
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
                        print_error(error);
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
                        stream.write_all(&buf)?;
                    }
                    _ => {}
                }
            }
            if ready {
                break;
            }
        }

        Ok(())
    }

    pub fn query<P>(&self, query: &str, params: P) -> anyhow::Result<DataFrame>
    where
        P: IntoIterator<Item = Option<BinaryParam>>,
    {
        let mut stream = &self.stream;
        let mut buf = BytesMut::new(); // <-- au lieu de Vec<u8>

        let (param_types, param_values) = format_params(params);

        let name = statement_name(query);

        frontend::parse(&name, query, param_types.iter().copied(), &mut buf)?;
        stream.write_all(&buf)?;

        buf.clear();
        frontend::describe(b'S', &name, &mut buf)?;

        stream.write_all(&buf)?;

        // Étape 2 : Bind avec result_format = binaire
        buf.clear();
        frontend::bind(
            &self.portal_name,
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
        .map_err(|_| anyhow::anyhow!("bind error"))?;
        stream.write_all(&buf)?;

        // Étape 3 : Execute
        buf.clear();
        frontend::execute(&self.portal_name, 0, &mut buf)?;
        stream.write_all(&buf)?;

        // Étape 4 : Sync
        buf.clear();
        frontend::sync(&mut buf);
        stream.write_all(&buf)?;

        // Lire les messages de réponse
        let mut read_buffer = BytesMut::with_capacity(8192);

        let mut columns: Vec<ColumnStorage> = Vec::new();

        loop {
            read_buffer.reserve(8192);
            let n = {
                read_buffer.reserve(8192);
                let dst = read_buffer.chunk_mut();
                let buf: &mut [u8] =
                    unsafe { std::slice::from_raw_parts_mut(dst.as_mut_ptr(), dst.len()) };
                let n = stream.read(buf)?;
                unsafe {
                    read_buffer.advance_mut(n);
                }
                n
            };

            if n == 0 {
                break; // Connexion fermée
            }
            while let Some(message) = backend::Message::parse(&mut read_buffer)? {
                match message {
                    backend::Message::RowDescription(desc) => {
                        let fields = desc.fields().iterator();
                        for field in fields {
                            let f = field?;

                            columns.push(column_from_field(&f))
                        }
                    }
                    backend::Message::DataRow(row) => {
                        let buf = row.buffer();
                        let mut ranges: DataRowRanges<'_> = row.ranges();

                        for col in columns.iter_mut() {
                            let range = ranges.next().unwrap();
                            match range {
                                Some(Some(range)) => {
                                    let bytes = &buf[range];

                                    push_column_value(col, Some(bytes));
                                }
                                Some(None) => push_column_value(col, None),
                                None => panic!("no data"),
                            }
                        }
                    }

                    backend::Message::ReadyForQuery(_) => {
                        break;
                    }

                    backend::Message::ErrorResponse(error) => {
                        print_error(error);
                    }
                    _ => {}
                }
            }

            if read_buffer.is_empty() {
                break;
            }
        }

        Ok(DataFrame::from_iter(
            columns.into_iter().map(|col| column_to_series(col)),
        ))
    }
}
