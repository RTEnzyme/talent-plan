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

pub type Result<T> = ::std::result::Result<T, KvsError>;