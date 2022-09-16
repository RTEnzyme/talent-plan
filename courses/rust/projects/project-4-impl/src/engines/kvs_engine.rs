//! # bitcask_impl
//! this KvStore implement bitcask model,  which is a
//! log-structured key-value database.
//!
use crate::Engine;

use serde_json::Deserializer;
use std::ffi::OsStr;
use std::fs::{create_dir_all, read_dir, File, OpenOptions, remove_file};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use dashmap::DashMap;

use crate::{Cmd, KvsError, Result};

const COMPACT_THRESHOLD: u64 = 1024 * 1024;
///
/// KvStore is a log-structured key-value store,
/// inspired by bitcask model.
///
/// # Example
///
/// ```rust
/// use crate::kvs::{KvStore, Result};
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
/// # test().expect("");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct KvsEngine {
    key_dir: Arc<DashMap<String, CmdPos>>,
    path: Arc<PathBuf>,

    reader: KvsReader,
    writer: Arc<Mutex<KvsWriter>>,
}

#[derive(Debug)]
struct KvsReader {
    path: Arc<PathBuf>,
    readers: Arc<DashMap<u64, BufReaderWithPos<File>>>,
    check_point: Arc<AtomicU64>,
}

#[derive(Debug)]
struct KvsWriter {
    reader: KvsReader,
    key_dir: Arc<DashMap<String, CmdPos>>,
    writer: BufWriterWithPos<File>,
    path: Arc<PathBuf>,

    current_file_id: u64,
    uncompact: u64,
}

impl Engine for KvsEngine {
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
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
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
    fn get(&self, key: String) -> Result<Option<String>> {
        // convert Option<&T> to Option<T>
        if let Some(cmd_pos) = self.key_dir.get(&key) {
            self.reader.read(cmd_pos.value())
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
    fn remove(&self, key: String) -> Result<()> {
        if self.key_dir.contains_key(&key) {
            self.writer.lock().unwrap().remove(key)
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

impl KvsEngine {
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
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        // create store path
        let path = path.into();
        let mut uncompact: u64 = 0;
        create_dir_all(&path)?;
        let mut key_dir = DashMap::new();
        let mut readers = DashMap::new();

        // load history file
        let file_list = sorted_file_list(&path)?;
        for file_id in &file_list {
            let mut reader = BufReaderWithPos::new(File::open(to_log_file(*file_id, &path))?)?;
            uncompact += load_log(*file_id, &mut reader, &mut key_dir)?;
            readers.insert(*file_id, reader);
        }

        // create current log file
        let current_file_id = file_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(current_file_id, &path)?;
        readers.insert(current_file_id, BufReaderWithPos::new(
            File::open(
               to_log_file(current_file_id, &path))?
        )?);
        let path = Arc::new(path);
        let reader = KvsReader {
            path: path.clone(),
            readers: Arc::new(readers),
            check_point: Arc::new(AtomicU64::new(0)),
        };
        let key_dir = Arc::new(key_dir);
        // return
        Ok(KvsEngine{
            key_dir: key_dir.clone(),
            path: path.clone(),
            reader: reader.clone(),
            writer: Arc::new(Mutex::new(KvsWriter {
                reader: reader.clone(),
                key_dir: key_dir.clone(),
                writer,
                current_file_id,
                uncompact,
                path,
            }))
        })
    }


}

impl KvsWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Cmd::Set { key, value };
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Cmd::Set { key, .. } = cmd {
            if let Some(old_cmd) = self
                .key_dir
                .insert(key, (self.current_file_id.into(), pos..self.writer.pos).into())
            {
                self.uncompact += old_cmd.len;
            }
        }
        if self.uncompact >= COMPACT_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Cmd::Remove { key };
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Cmd::Remove { key } = cmd {
            let old_cmd = self.key_dir.remove(&key).expect("key not found").1;
            self.uncompact += old_cmd.len;
            if self.uncompact >= COMPACT_THRESHOLD {
                self.compact()?;
            }
        };
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let compact_file_id = self.current_file_id + 1;
        self.current_file_id += 2;
        self.writer = new_log_file(self.current_file_id, &self.path)?;

        let mut compact_writer = new_log_file(compact_file_id, &self.path)?;
        let mut compact_pos = 0;
        for mut cmd_pos in self.key_dir.iter_mut() {
            let CmdPos {
                file_id,
                kv_pos,
                len,
            } = cmd_pos.value_mut();
            let mut reader = self.reader.readers.get_mut(&file_id).expect("can't find log file;");
            if reader.value_mut().pos != *kv_pos {
                reader.seek(SeekFrom::Start(*kv_pos))?;
            }
            let mut rdr = reader.value_mut().take(len.clone());
            io::copy(&mut rdr, &mut compact_writer)?;

            *file_id = compact_file_id;
            *kv_pos = compact_pos;
            compact_pos += *len;
        }
        compact_writer.flush()?;

        let remove_files: Vec<_> = self
            .reader
            .readers
            .iter()
            .map(|e| e.key().to_owned())
            .filter(|&k| k < compact_file_id)
            .collect();
        for file in remove_files {
            self.reader.readers.remove(&file);
            remove_file(to_log_file(file, &self.path))?;
        }
        // self.reader.
        self.uncompact = 0;
        Ok(())
    }
}

impl KvsReader {
    fn check_point(&self) {
        while !self.readers.is_empty() {
            let file_id = self.readers.iter().next().unwrap();
            if self.check_point.load(Ordering::SeqCst) <= *file_id.key() {
                break;
            }
            self.readers.remove(file_id.key());
        }
    }

    fn read(&self, cmd_pos: &CmdPos) -> Result<Option<String>> {
        self.check_point();
        // if it doesn't contain the key, should we update it
        if !self.readers.contains_key(&cmd_pos.file_id) {

        }
        let mut reader = self
            .readers
            .get_mut(&cmd_pos.file_id)
            .expect("inconsistency! Can't find this log file");
        reader.value_mut().seek(SeekFrom::Start(cmd_pos.kv_pos))?;
        let reader = reader.value_mut().take(cmd_pos.len);
        if let Cmd::Set { value, .. } = serde_json::from_reader(reader)? {
            Ok(Some(value))
        } else {
            Err(KvsError::CommandNotSupported)
        }
    }
}

impl Clone for KvsReader {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            readers: self.readers.clone(),
            check_point: self.check_point.clone()
        }
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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
        .flat_map(|f| -> Result<_> { Ok(f?.path()) })
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

fn new_log_file(
    file_id: u64,
    dir: &Path,
) -> Result<BufWriterWithPos<File>> {
    let path = to_log_file(file_id, dir);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?,
    )?;
    Ok(writer)
}

fn to_log_file(file_id: u64, dir: &Path) -> PathBuf {
    dir.join(format!("{}.log", file_id))
}

fn load_log(
    file_id: u64,
    reader: &mut BufReaderWithPos<File>,
    key_dir: &mut DashMap<String, CmdPos>,
) -> Result<u64> {
    let mut posi = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Cmd>();
    let mut uncompacted = 0; // number of bytes that can be saved after a compaction.
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Cmd::Remove { key } => {
                if let Some(old_cmd) = key_dir.remove(&key) {
                    // old command can be compacted
                    uncompacted += old_cmd.1.len;
                }
                // this remove command alse can be compacted
                uncompacted += new_pos - posi;
            }
            Cmd::Set { key, .. } => {
                if let Some(old_cmd) = key_dir.insert(key, (file_id, posi..new_pos).into()) {
                    // old command will be overwritten, so can be compacted
                    uncompacted += old_cmd.len;
                }
            }
        }
        posi = new_pos;
    }
    Ok(uncompacted)
}
