//! This module provides various thread pools. All thread pools should implement
//! the `ThreadPool` trait.

use super::{Result, KvsError};
use std::thread;
use std::sync::{Arc, mpsc, Mutex};
use crossbeam::channel::{self, Receiver, Sender};

/// the basic ThreadPool
pub trait ThreadPool {
    /// Creates a new thread pool, immediately spawning the specified number of threads.
    /// Returns an error if any thread fails to spawn. All previously-spawned threads are terminated.
    fn new(threads: u32) -> Result<Self> where Self:Sized;

    /// Spawn a function into the threadpool.
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;

    
}

/// Naive Implementation for Thread Pool
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self> {
        Ok(NaiveThreadPool)
    }
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        thread::spawn(job);
    }
}


/// Shared Queue Implementation for Thread Pool
pub struct SharedQueueThreadPool{
    tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

#[derive(Clone)]
struct TaskReceiver(Receiver<Box<dyn FnOnce() + Send + 'static>>);

impl Drop for TaskReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = self.clone();
            if let Err(e) = thread::Builder::new().spawn(move || run_tasks(rx)) {
                println!("Failed to spawn a thread: {}", e);
            }
        }
    }
}

fn run_tasks(rx: TaskReceiver) {
    loop {
        match rx.0.recv() {
            Ok(task) => {
                task();
            }
            Err(_) => println!("Thread exits because the thread pool is destroyed."),
        }
    }
}


impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (tx, rx) = channel::unbounded::<Box<dyn FnOnce() + Send + 'static>>();

        for _ in 0..threads {
            let rx = TaskReceiver(rx.clone());
            thread::Builder::new().spawn(move || run_tasks(rx))?;
        }

        Ok(SharedQueueThreadPool{tx})
    }
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.tx.send(Box::new(job)).unwrap();
    }
}

/// Rayon Implementation for Thread Pool
pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .map_err(|_| KvsError::OtherError)?;
        Ok(RayonThreadPool(pool))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.spawn(job)
    }
}