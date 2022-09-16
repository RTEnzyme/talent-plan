use std::thread;

use super::ThreadPool;


pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_: u32) -> crate::Result<Self>
    where
        Self: Sized {
        Ok(Self)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static {
        thread::spawn(job);
    }
}