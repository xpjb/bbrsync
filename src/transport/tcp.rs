use super::{Connection, Incoming, Transport};
use anyhow::{Context, Result};
use std::net::{TcpListener, TcpStream};

pub struct TcpTransport;

pub static TCP: TcpTransport = TcpTransport;

impl Transport for TcpTransport {
    fn connect(&self, address: &str) -> Result<Connection> {
        let stream = TcpStream::connect(address)
            .with_context(|| format!("connecting to {address}"))?;
        stream.set_nodelay(true).ok();
        Ok(Connection {
            reader: Box::new(stream.try_clone()?),
            writer: Box::new(stream),
        })
    }

    fn incoming(&self, address: &str) -> Result<Incoming> {
        let listener = TcpListener::bind(address)
            .with_context(|| format!("binding TCP {address}"))?;
        Ok(Box::new(std::iter::from_fn(move || {
            Some(listener.accept().map_err(Into::into).and_then(|(stream, _)| {
                stream.set_nodelay(true).ok();
                Ok(Connection {
                    reader: Box::new(stream.try_clone()?),
                    writer: Box::new(stream),
                })
            }))
        })))
    }
}
