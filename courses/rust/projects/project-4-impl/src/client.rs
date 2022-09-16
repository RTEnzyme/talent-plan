use std::{
    io::{BufReader, BufWriter, Write},
    net::TcpStream,
};

use crate::{GetResp, KvsError, RemoveResp, Request, Result, SetResp};
use serde::Deserialize;
use serde_json::{de::IoRead, Deserializer};
pub struct Client {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl Client {
    pub fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        let reader = Deserializer::from_reader(BufReader::new(stream.try_clone()?));
        let writer = BufWriter::new(stream);
        Ok(Self { reader, writer })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &Request::Get { key })?;
        self.writer.flush()?;
        let resp = GetResp::deserialize(&mut self.reader)?;
        match resp {
            GetResp::Ok(v) => Ok(v),
            GetResp::Err(e) => Err(KvsError::StringErr(e)),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;
        let resp = SetResp::deserialize(&mut self.reader)?;
        match resp {
            SetResp::Ok(_) => Ok(()),
            SetResp::Err(e) => Err(KvsError::StringErr(e)),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Remove { key })?;
        self.writer.flush()?;
        let resp = RemoveResp::deserialize(&mut self.reader)?;
        match resp {
            RemoveResp::Ok(_) => Ok(()),
            RemoveResp::Err(e) => Err(KvsError::StringErr(e)),
        }
    }
}
