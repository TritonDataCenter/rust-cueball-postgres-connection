/*
 * Copyright 2019 Joyent, Inc.
 */

use std::ops::{Deref, DerefMut};

use postgres;
use postgres::{Client, NoTls};

use cueball::backend::Backend;
use cueball::connection::Connection;

pub struct PostgresConnection {
    pub connection: Option<Client>,
    url: String,
    connected: bool,
}

impl PostgresConnection {
    pub fn connection_creator<'a>(
        mut config: PostgresConnectionConfig,
    ) -> impl FnMut(&Backend) -> PostgresConnection + 'a {
        move |b| {
            config.host = Some(b.address.to_string());
            config.port = Some(b.port);

            let url: String = config.to_owned().into();

            PostgresConnection {
                connection: None,
                url,
                connected: false,
            }
        }
    }
}

impl Connection for PostgresConnection {
    type Error = postgres::Error;

    fn connect(&mut self) -> Result<(), Self::Error> {
        let connection =  Client::connect(&self.url, NoTls)?;
        self.connection = Some(connection);
        self.connected = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        self.connection = None;
        self.connected = false;
        Ok(())
    }
}

impl Deref for PostgresConnection {
    type Target = Client;

    fn deref(&self) -> &Client {
        &self.connection.as_ref().unwrap()
    }
}

impl DerefMut for PostgresConnection {
    fn deref_mut(&mut self) -> &mut Client {
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
    pub application_name: Option<String>,
}

impl From<PostgresConnectionConfig> for String {
    fn from(config: PostgresConnectionConfig) -> Self {
        let scheme = "postgresql://";
        let user = config.user.unwrap_or_else(|| "".into());


        let at = if user.is_empty() { "" } else { "@" };

        let host = config.host.unwrap_or_else(|| String::from("localhost"));
        let port = config
            .port
            .and_then(|p| Some(p.to_string()))
            .unwrap_or_else(|| "".to_string());

        let colon = if port.is_empty() { "" } else { ":" };

        let database = config.database.unwrap_or_else(|| "".into());

        let slash = if database.is_empty() { "" } else { "/" };

        let application_name = config.application_name.unwrap_or_else(|| "".into());
        let question_mark = if application_name.is_empty() { "" } else { "?" };

        let app_name_param = if application_name.is_empty() {
            ""
        } else {
            "application_name="
        };

        [
            scheme,
            user.as_str(),
            at,
            host.as_str(),
            colon,
            port.as_str(),
            slash,
            database.as_str(),
            question_mark,
            app_name_param,
            application_name.as_str(),
        ]
        .concat()
    }
}
