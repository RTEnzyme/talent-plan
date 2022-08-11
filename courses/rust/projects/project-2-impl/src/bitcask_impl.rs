#![deny(missing_docs)]
//! # bitcask_impl
//! this KvStore implement bitcask model,  which is a 
//! log-structured key-value database.
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{create_dir_all, File, read_dir, OpenOptions, self};
use std::io::{Read, Seek, BufReader, SeekFrom, self, Write, BufWriter};
use std::ops::Range;
use std::{collections::HashMap, path::PathBuf};
use serde_json::Deserializer;

use crate::{Result, KvsError, Cmd};

const COMPACT_THREADHOLD: u64 = 1024 * 1024;
///
/// KvStore is a log-structured key-value store,
/// inspired by bitcask model.
///
/// # Example
///
/// ```rust
/// use kvs::{KvStore, Result};
/// use tempfile::TempDir;
/// # fn test() -> Result<()> {
/// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
/// let mut store = KvStore::open(temp_dir.path())?;
/// store.set("key1".to_owned(), "value1".to_owned());
/// store.set("key2".to_owned(), "value2".to_owned());
/// assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
/// assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));
/// store.remove("key1".to_owned())?;
/// assert_eq!(store.get("key1".to_owned())?, None);
/// # Ok(())
/// }
/// # fn main() {
/// # test();
/// # }
/// ```
pub struct KvStore {
    key_dir: BTreeMap<String, CmdPos>,
    readers: HashMap<u64, BufReaderWithPos<File>>,

    path: PathBuf,
    writer: BufWriterWithPos<File>,
    current_file_id: u64,

    uncompact: u64,
}

impl KvStore {

    /// open a KvStore by a temp path. Only if open a KvStore instance
    /// , other operation can be used.
    /// 
    /// # Example
    /// ```rust
    /// use kvs::{KvStore, Result};
    /// use tempfile::TempDir;
    /// 
    /// let temp_file = TempDir::new().expect("unable to create temporary working directory");
    /// let mut store = KvStore::open(temp_file.path());
    /// ```
    pub fn open(path: impl Into<PathBuf>) -> Result<Self>{
        // create store path
        let path = path.into();
        let mut uncompact: u64 = 0;
        create_dir_all(&path)?;
        let mut key_dir = BTreeMap::new();
        let mut readers = HashMap::new();

        // load history file
        let file_list = sorted_file_list(&path)?;
        for file_id in &file_list {
            let mut reader = BufReaderWithPos::new(File::open(to_log_file(*file_id, &path))?)?;
            uncompact += load_log(*file_id, &mut reader, &mut key_dir)?;
            readers.insert(*file_id, reader);
        };

        // create current log file 
        let current_file_id = file_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(current_file_id, &path, &mut readers)?;

        // return 
        Ok(KvStore {
            key_dir,
            readers,
            path,
            writer,
            current_file_id,
            uncompact,
        })
    }

    /// insert a key-value pair if key is not in store else overwrite the key-value
    ///
    /// # Example
    /// ```rust
    /// use kvs::{KvStore, Result};
    /// use tempfile::TempDir;
    /// 
    /// let temp_file = TempDir::new().expect("unable to create temporary working directory");
    /// let mut kv = KvStore::open(temp_file.path()).unwrap();
    /// assert_eq!(kv.get("test".to_owned()).unwrap(), None);
    /// kv.set("test".to_owned(), "test1".to_owned()).unwrap();
    /// assert_eq!(kv.get("test".to_owned()).unwrap(), Some("test1".to_owned()));
    /// kv.set("test".to_owned(), "test2".to_owned()).unwrap();
    /// assert_eq!(kv.get("test".to_owned()).unwrap(), Some("test2".to_owned()));
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Cmd::Set { key, value };
        let posi = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Cmd::Set { key, .. } = cmd {
            if let Some(old_cmd) = self.key_dir.insert(key, (self.current_file_id, posi..self.writer.pos).into()) {
                self.uncompact += old_cmd.len;
            }
        }
        if self.uncompact >= COMPACT_THREADHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// get a value by key
    ///
    /// # Example
    /// ```rust
    /// use kvs::{KvStore, Result};
    /// use tempfile::TempDir;
    ///
    /// let temp_file = TempDir::new().expect("unable to create temporary working directory");
    /// let mut kv = KvStore::open(temp_file.path()).unwrap();
    /// kv.set("test".to_owned(), "test1".to_owned()).unwrap();
    /// let v = kv.get("test".to_owned()).unwrap();
    /// assert_eq!(v, Some("test1".to_owned()));
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // convert Option<&T> to Option<T>
        if self.key_dir.contains_key(&key) {
            let cmd_pos = self.key_dir.get(&key).expect("key not found");
            let reader = self.readers
                .get_mut(&cmd_pos.file_id)
                .expect("inconsistancy! Can't find this log file");
            reader.seek(SeekFrom::Start(cmd_pos.kv_pos))?;
            let reader = reader.take(cmd_pos.len);
            if let Cmd::Set{ value, ..} = serde_json::from_reader(reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::CommandNotSupported)
            }
        } else {
            Ok(None)
        }
    }

    /// remove a key-value by key
    ///
    /// # Example
    /// ```rust
    /// use kvs::{KvStore, Result};
    /// use tempfile::TempDir;
    ///
    /// let temp_file = TempDir::new().expect("unable to create temporary working directory");
    /// let mut kv = KvStore::open(temp_file.path()).unwrap();
    /// assert_eq!(kv.get("test".to_owned()).unwrap(), None);
    /// kv.set("test".to_owned(), "test1".to_owned()).unwrap();
    /// assert_eq!(kv.get("test".to_owned()).unwrap(), Some("test1".to_owned()));
    /// kv.remove("test".to_owned()).unwrap();
    /// assert_eq!(kv.get("test".to_owned()).unwrap(), None);
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.key_dir.contains_key(&key) {
            let cmd = Cmd::Remove { key };
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Cmd::Remove { key } = cmd  {
                let old_cmd = self.key_dir.remove(&key).expect("key not found");
                self.uncompact += old_cmd.len;
                if self.uncompact >= COMPACT_THREADHOLD {
                    self.compact()?;
                }
            };
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    fn compact(&mut self) -> Result<()> {
        let compact_file_id = self.current_file_id + 1;
        self.current_file_id += 2;
        self.writer = new_log_file(self.current_file_id, &self.path, &mut self.readers)?;

        let mut compact_writer = new_log_file(compact_file_id, &self.path, &mut self.readers)?;
        let mut compact_pos = 0;
        for (_, cmd_pos) in &mut self.key_dir {
            let CmdPos{file_id, kv_pos, len} = cmd_pos;
            let reader = self.readers.get_mut(file_id).expect("can't find log file;");
            if reader.pos != *kv_pos {
                reader.seek(SeekFrom::Start(*kv_pos))?;
            }
            let mut rdr = reader.take(*len);
            io::copy(&mut rdr, &mut compact_writer)?;

            *file_id = compact_file_id;
            *kv_pos = compact_pos;
            compact_pos += *len;
        };
        compact_writer.flush()?;

        let remove_files: Vec<_> = self.readers
            .keys()
            .filter(|&&k| k < compact_file_id)
            .cloned()
            .collect();
        for file in remove_files {
            self.readers.remove(&file);
            fs::remove_file(to_log_file(file, &self.path))?;
        };
        self.uncompact = 0;
        Ok(())
    }
}

struct CmdPos {
    file_id: u64,
    kv_pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CmdPos {
    fn from((file_id, range): (u64, Range<u64>)) -> Self {
        CmdPos {
            file_id,
            kv_pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

fn sorted_file_list(path: &PathBuf) -> Result<Vec<u64>> {
    let mut file_list: Vec<u64> = read_dir(path)?
        .flat_map(|f| -> Result<_> { Ok(f?.path())})
        .filter(|f| f.is_file() && (f.extension() == Some("log".as_ref())))
        .flat_map(|f| {
            f.file_name()
            .and_then(OsStr::to_str)
            .map(|f| f.trim_end_matches(".log"))
            .map(|s| s.parse::<u64>())
        })
        .flatten()
        .collect();
    file_list.sort_unstable();
    Ok(file_list)
}

fn new_log_file(file_id: u64, dir: &PathBuf, readers: &mut HashMap<u64, BufReaderWithPos<File>>) -> Result<BufWriterWithPos<File>> {
    let path = to_log_file(file_id, dir);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?
    )?;
    readers.insert(file_id, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}

fn to_log_file(file_id: u64, dir: &PathBuf) -> PathBuf {
    dir.join(format!("{}.log", file_id))
}

fn load_log(
    file_id: u64, 
    reader: &mut BufReaderWithPos<File>, 
    key_dir: &mut BTreeMap<String, CmdPos>) -> Result<u64> {
    let mut posi = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Cmd>();
    let mut uncompacted = 0; // number of bytes that can be saved after a compaction.
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Cmd::Remove {key} => {
                if let Some(old_cmd) = key_dir.remove(&key) {
                    // old command can be compacted
                    uncompacted += old_cmd.len;
                }
                // this remove command alse can be compacted
                uncompacted += new_pos - posi;
            },
            Cmd::Set { key, .. } => {
                if let Some(old_cmd) = key_dir.insert(key, (file_id, posi..new_pos).into()) {
                    // old command will be overwritten, so can be compacted
                    uncompacted += old_cmd.len;
                }
            }
        }
        posi = new_pos;
    };
    Ok(uncompacted)
}