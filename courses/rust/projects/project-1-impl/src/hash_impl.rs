#![deny(missing_docs)]
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
/// let mut store = KvStore::new();
/// store.set("key1".to_owned(), "value1".to_owned());
/// store.set("key2".to_owned(), "value2".to_owned());
/// assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
/// assert_eq!(store.get("key2".to_owned()), Some("value2".to_owned()));
/// store.remove("key1".to_owned());
/// assert_eq!(store.get("key1".to_owned()), None);
/// ```
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// new a KvStore instance
    ///
    /// # Example
    ///
    /// ```rust
    /// use kvs::KvStore;
    ///
    /// let kv = KvStore::new();
    /// println!("KvStore instance has been created;");
    /// ```
    pub fn new() -> Self {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// insert a key-value pair if key is not in store else overwrite the key-value
    ///
    /// # Example
    /// ```rust
    /// use kvs::KvStore;
    ///
    /// let mut kv = KvStore::new();
    /// assert_eq!(kv.get("test".to_owned()), None);
    /// kv.set("test".to_owned(), "test1".to_owned());
    /// assert_eq!(kv.get("test".to_owned()), Some("test1".to_owned()));
    /// kv.set("test".to_owned(), "test2".to_owned());
    /// assert_eq!(kv.get("test".to_owned()), Some("test2".to_owned()));
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// get a value by key
    ///
    /// # Example
    /// ```rust
    /// use kvs::KvStore;
    ///
    /// let mut kv = KvStore::new();
    /// kv.set("test".to_owned(), "test1".to_owned());
    /// let v = kv.get("test".to_owned());
    /// assert_eq!(v, Some("test1".to_owned()));
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        // convert Option<&T> to Option<T>
        self.map.get(&key).cloned()
    }

    /// remove a key-value by key
    ///
    /// # Example
    /// ```rust
    /// use kvs::KvStore;
    ///
    /// let mut kv = KvStore::new();
    /// assert_eq!(kv.get("test".to_owned()), None);
    /// kv.set("test".to_owned(), "test1".to_owned());
    /// assert_eq!(kv.get("test".to_owned()), Some("test1".to_owned()));
    /// kv.remove("test".to_owned());
    /// assert_eq!(kv.get("test".to_owned()), None);
    /// ```
    pub fn remove(&mut self, key: String) -> Option<String> {
        self.map.remove(&key)
    }
}
