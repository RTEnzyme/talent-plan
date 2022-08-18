mod kvs_engine;
mod sled_engine;
pub use kvs_engine::KvsEngine;
pub use sled_engine::SledKvsEngine;

use crate::Result;

pub trait Engine {
    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn get(&mut self, key: String) -> Result<Option<String>>;

    fn remove(&mut self, key: String) -> Result<()>;
}
