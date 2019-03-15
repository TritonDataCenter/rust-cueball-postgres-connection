/*
 * Copyright 2019 Joyent, Inc.
 */

use std::net::{SocketAddr, TcpStream};

use cueball::backend::Backend;
use cueball::connection::Connection;
use cueball::error::Error;


#[derive(Debug)]
pub struct TcpStreamWrapper {
    stream: Option<TcpStream>,
    addr: SocketAddr,
    connected: bool
}

impl Connection for TcpStreamWrapper {
    fn new(b: &Backend) -> Self {
        let addr = SocketAddr::from((b.address, b.port));

        TcpStreamWrapper {
            stream: None,
            addr: addr,
            connected: false
        }
    }

    fn connect(&mut self) -> Result<(), Error> {
        match TcpStream::connect(&self.addr) {
            Ok(stream) => {
                self.stream = Some(stream);
                self.connected = true;
                Ok(())
            },
            Err(err) => {
                Err(Error::IOError(err))
            }
        }
    }

    fn close(&mut self) -> Result<(), Error> {
        self.stream = None;
        self.connected = false;
        Ok(())
    }

    fn set_unwanted(&self) -> () {
        std::unimplemented!()
    }
}
