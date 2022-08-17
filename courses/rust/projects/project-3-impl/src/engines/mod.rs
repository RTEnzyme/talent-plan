mod sled_engine;
mod kvs_engine;

use std::path::PathBuf;

pub use sled_engine::SledKvsEngine;
pub use kvs_engine::KvsEngine;

use crate::Result;

pub trait Engine {

    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn get(&mut self, key: String) -> Result<Option<String>>;

    fn remove(&mut self, key: String) -> Result<()>;

}