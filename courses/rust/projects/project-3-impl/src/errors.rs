use std::{net, string::FromUtf8Error};

use failure::Fail;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "key is not found in KvStore")]
    KeyNotFound,
    #[fail(display = "command is not supported")]
    CommandNotSupported,
    #[fail(display = "{}", _0)]
    IoErr(#[cause] std::io::Error),
    #[fail(display = "{}", _0)]
    SerdeErr(#[cause] serde_json::Error),
    #[fail(display = "{}", _0)]
    IpParseErr(#[cause] net::AddrParseError),
    #[fail(display = "{}", _0)]
    StringErr(String),
    #[fail(display = "{}", _0)]
    SledErr(#[cause] sled::Error),
    #[fail(display = "{}", _0)]
    FromUtf8Error(#[cause] FromUtf8Error),
}

impl From<std::io::Error> for KvsError {
    fn from(e: std::io::Error) -> Self {
        Self::IoErr(e)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(e: serde_json::Error) -> Self {
        Self::SerdeErr(e)
    }
}

impl From<net::AddrParseError> for KvsError {
    fn from(e: net::AddrParseError) -> Self {
        Self::IpParseErr(e)
    }
}

impl From<sled::Error> for KvsError {
    fn from(e: sled::Error) -> Self {
        Self::SledErr(e)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(e: FromUtf8Error) -> Self {
        Self::FromUtf8Error(e)
    }
}

pub type Result<T> = ::std::result::Result<T, KvsError>;
