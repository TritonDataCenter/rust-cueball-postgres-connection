/*
 * Copyright 2019 Joyent, Inc.
 */

use std::error::Error as StdError;
use std::ops::{Deref, DerefMut};

use postgres::Connection as PGConnection;
use postgres::tls::TlsHandshake;

use cueball::backend::Backend;
use cueball::connection::Connection;
use cueball::error::Error;

/// Attribution: This enum was taken from [r2d2-postgres](https://docs.rs/r2d2_postgres) since they had already
/// solved the problem with `TlsHandshake` instance.
/// Like `postgres::TlsMode` except that it owns its `TlsHandshake` instance.
#[derive(Debug)]
pub enum TlsMode {
    /// Like `postgres::TlsMode::None`.
    None,
    /// Like `postgres::TlsMode::Prefer`.
    Prefer(Box<TlsHandshake + Sync + Send>),
    /// Like `postgres::TlsMode::Require`.
    Require(Box<TlsHandshake + Sync + Send>),
}

#[derive(Debug)]
pub struct PostgresConnection {
    pub connection: Option<PGConnection>,
    url: String,
    connected: bool,
    tls_mode: TlsMode
}

impl PostgresConnection {
    pub fn connection_creator<'a>(mut config: PostgresConnectionConfig) -> impl FnMut(&Backend) -> PostgresConnection + 'a {
        move |b| {
            config.host = Some(b.address.to_string());
            config.port = Some(b.port);

            let url: String = config.to_owned().into();

            println!("PG URL: {}", url);
            PostgresConnection {
                connection: None,
                url,
                connected: false,
                tls_mode: TlsMode::None
            }
        }
    }
}

impl Connection for PostgresConnection {
    fn connect(&mut self) -> Result<(), Error> {
        let tls_mode = match self.tls_mode {
            TlsMode::None => postgres::TlsMode::None,
            TlsMode::Prefer(ref n) => postgres::TlsMode::Prefer(&**n),
            TlsMode::Require(ref n) => postgres::TlsMode::Require(&**n),
        };

        match PGConnection::connect(self.url.clone(), tls_mode) {
            Ok(connection) => {
                self.connection = Some(connection);
                self.connected = true;
                Ok(())
            },
            Err(err) => {
                // TODO: Better error handling
                Err(Error::CueballError(err.description().to_string()))
            }
        }
    }

    fn close(&mut self) -> Result<(), Error> {
        self.connection = None;
        self.connected = false;
        Ok(())
    }
}

impl Deref for PostgresConnection
{
    type Target = PGConnection;

    fn deref(&self) -> &PGConnection {
        &self.connection.as_ref().unwrap()
    }
}

impl DerefMut for PostgresConnection
{
    fn deref_mut(&mut self) -> &mut PGConnection {
        self.connection.as_mut().unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct PostgresConnectionConfig {
    pub user: Option<String>,
    pub password: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub application_name: Option<String>
}

impl From<PostgresConnectionConfig> for String {
    fn from(config: PostgresConnectionConfig) -> Self {
        let scheme = "postgresql://";
        let user = config.user.unwrap_or("".into());

        let at = if user.is_empty() {
            ""
        } else {
            "@"
        };

        let host = config.host.unwrap_or_else(|| String::from("localhost"));
        let port = config.port
            .and_then(|p| Some(p.to_string()))
            .unwrap_or_else(|| "".to_string());

        let colon = if port.is_empty() {
            ""
        } else {
            ":"
        };

        let database = config.database.unwrap_or("".into());

        let slash = if database.is_empty() {
            ""
        } else {
            "/"
        };

        let application_name = config.application_name.unwrap_or("".into());
        let question_mark = if application_name.is_empty() {
            ""
        } else {
            "?"
        };

        let app_name_param = if application_name.is_empty() {
            ""
        } else {
            "application_name="
        };

        [scheme,
         user.as_str(),
         at,
         host.as_str(),
         colon,
         port.as_str(),
         slash,
         database.as_str(),
         question_mark,
         app_name_param,
         application_name.as_str()].concat()
    }
}
