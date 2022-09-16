mod kvs_engine;
mod sled_engine;

// mod sled_engine;
pub use kvs_engine::KvsEngine;
pub use sled_engine::SledKvsEngine;

use crate::Result;

pub trait Engine: Clone + Send + 'static {
    fn set(&self, key: String, value: String) -> Result<()>;

    fn get(&self, key: String) -> Result<Option<String>>;

    fn remove(&self, key: String) -> Result<()>;
}
