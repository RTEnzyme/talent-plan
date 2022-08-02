use std::collections::HashMap;

///
/// KvStore is a in-memory key-value store,
/// just implemented by built-in HashMap Data Struct
/// 
/// # Example
/// 
/// ```
/// use kvs::KvStore;
/// 
/// let kv = KvStore::new();
/// println!("KvStore instance has been created;");
/// ```
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore { map: HashMap::new() }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        // convert Option<&T> to Option<T>
        self.map.get(&key).map(|t| t.clone())
    }

    pub fn remove(&mut self, key: String) -> Option<String>{
        self.map.remove(&key)
    }
}