use sled::Db;

use crate::Engine;
use crate::Result;

#[derive(Debug)]
pub struct SledKvsEngine {
    db: Db
}

impl SledKvsEngine {
    pub fn new(db: Db) -> Self {
        SledKvsEngine { db }
    }
}

impl Engine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        unimplemented!();
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        unimplemented!();
    }

    fn remove(&mut self, key: String) -> Result<()> {
        unimplemented!();
    }

}

impl SledKvsEngine {
    pub fn open(path: impl Into<std::path::PathBuf>) -> Self {
        todo!()
    }
}