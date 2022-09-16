use sled::Db;

use crate::Engine;
use crate::KvsError;
use crate::Result;

#[derive(Debug, Clone)]
pub struct SledKvsEngine {
    db: Db,
}

impl Engine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.into_bytes()).map(|_| ())?;
        // self.db.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self
            .db
            .get(key.as_bytes())?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        self.db.flush()?;
        Ok(())
    }
}

impl SledKvsEngine {
    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<Self> {
        let db = sled::open(path.into())?;
        Ok(Self { db })
    }

    pub fn new(db: Db) -> Self {
        Self { db }
    }
}
