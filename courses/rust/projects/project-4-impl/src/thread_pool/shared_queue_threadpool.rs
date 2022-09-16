use std::sync::mpsc;
use super::ThreadPool;

pub struct SharedQueueThreadPool {
    threads: Vec<Worker>,
    sender: 
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> crate::Result<Self>
    where
        Self: Sized {
        todo!()
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static {
        todo!()
    }
}