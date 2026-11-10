use super::{Connection, Incoming, Transport};
use anyhow::{Context, Result};
use std::io::{self, ErrorKind};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

pub struct TcpTransport;

pub static TCP: TcpTransport = TcpTransport;

impl Transport for TcpTransport {
    fn connect(&self, address: &str) -> Result<Connection> {
        let addresses = address
            .to_socket_addrs()
            .with_context(|| format!("resolving {address}"))?;
        let started = Instant::now();
        let mut last_error = io::Error::new(
            ErrorKind::AddrNotAvailable,
            "address resolved to no endpoints",
        );
        for remote in addresses {
            let remaining = CONNECT_TIMEOUT.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                last_error = io::Error::new(
                    ErrorKind::TimedOut,
                    format!("connection attempt exceeded {} seconds", CONNECT_TIMEOUT.as_secs()),
                );
                break;
            }
            match TcpStream::connect_timeout(&remote, remaining) {
                Ok(stream) => {
                    stream.set_nodelay(true).ok();
                    return Ok(Connection {
                        reader: Box::new(stream.try_clone()?),
                        writer: Box::new(stream),
                    });
                }
                Err(error) => last_error = error,
            }
        }
        Err(last_error).with_context(|| {
            format!(
                "connecting to {address} with a {} second timeout",
                CONNECT_TIMEOUT.as_secs()
            )
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
