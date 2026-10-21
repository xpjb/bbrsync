use anyhow::Result;
use std::io::{Read, Write};

pub mod kcp;
pub mod tcp;

pub struct Connection {
    pub(crate) reader: Box<dyn Read + Send>,
    pub(crate) writer: Box<dyn Write + Send>,
}

pub type Incoming = Box<dyn Iterator<Item = Result<Connection>> + Send>;

pub trait Transport: Send + Sync {
    fn connect(&self, address: &str) -> Result<Connection>;
    fn incoming(&self, address: &str) -> Result<Incoming>;
}
