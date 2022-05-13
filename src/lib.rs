#![deny(missing_docs)]

//! The `KvStore` stores <key,value>(string,string) pairs.
//!
//! <Key,value> pairs are stored in a `HashMap` in memory and not persisted to disk.

/// test
mod error;
mod kvs_engine;
pub mod thread_pool;
pub use error::{Result, KvsError};
pub use kvs_engine::{KvsEngine};
// pub use thread_pool::{ThreadPool, NaiveThreadPool, SharedQueueThreadPool, RayonThreadPool};

use std::collections::HashMap;
use std::clone::Clone;
// use std::io;
use std::io::{Write,Read};
use std::path::PathBuf;

use std::fs;
use std::fs::{File, OpenOptions};

use serde::{Serialize, Deserialize};  

use std::io::SeekFrom;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};
use std::ops::{Deref,DerefMut};
use std::convert::AsRef;
// use crate::{KvsError, Result};

/// a Map based on HashMap to store <key, value> in memory
#[derive(Debug)]
pub struct KvStore {
    dir_path : Arc<PathBuf>,
    file :Arc<Mutex<File>>,
    index_map:Arc<Mutex<HashMap<String, Index>>>,
    offset_begin: Arc<Mutex<usize>>,
    log_file_path : Arc<PathBuf>,
    item_count :Arc<Mutex<u64>>, // 用来统计有多少条命令了，是不是要切了
    sstable_path_vec:Arc<Mutex<Vec<String>>>, // 这个存放的是压缩后的文件，按照sstable_x.txt命名，从_1开始
}

#[derive(Default, Debug, Serialize, Deserialize)]
struct Command {
    action :String,
    key :String,
    value :String,
}


#[derive(Debug)]
struct Index {
    offset_begin :u64,
    offset_end  :u64,
}


impl KvsEngine for KvStore {
    /// set the <key, value> in the KvStore, if key is existed, then override with the new value.
    fn set(&self, key: String, value: String) -> Result<()>{
        let command = serde_json::to_string(&Command{
            action: String::from("set"),
            key:key,
            value:value,
        })?;      

        let mut guard = self.file.lock().unwrap();
        guard.write_all(command.as_bytes())?;
        self.load_index(&mut guard)?;
        drop(guard);

        Ok(())
    }

    /// try to get the value from KvStore with corresponding key, if it doesn't exist, then return None
    fn get(&self, key: String) -> Result<Option<String>> {
        if self.index_map.lock().unwrap().contains_key(&key) {
            let index = &self.index_map.lock().unwrap()[&key];
            let length = index.offset_end - index.offset_begin;

            use std::io::BufReader;

            let f = &*self.file.lock().unwrap();
            let mut reader = BufReader::new(f); 

            reader.seek(SeekFrom::Start(index.offset_begin))?;
            let cmd_reader = reader.take(length);

            let command: Command = serde_json::from_reader(cmd_reader)?;
            
            Ok(Some(command.value))
        } else {
            //开始倒序寻找
            for file_name in self.sstable_path_vec.lock().unwrap().deref() {
                let mut path = PathBuf::new();
                path.push(self.dir_path.as_ref());
                path.push(file_name);

                let mut file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .append(true)
                        .create(true)
                        .open(path)?;

                //先把他们直接读进来处理
                let mut buffer = String::new();
                file.read_to_string(&mut buffer)?;
                
                let mut indices = serde_json::Deserializer::from_str(&buffer).into_iter::<Command>();

                while let Some(command) = indices.next() {
                    let command = command?;
                    if command.key == key {
                        if command.action == "set" {
                            return Ok(Some(command.value))
                        } else if command.action == "rm" {
                            return Ok(None)
                        }
                    }
                    
                }
            }
            Ok(None)
        }
    }

    /// try to remove the <key,value> from KvStore with the given Key, if doesn't exist this key, then do nothing.
    fn remove(&self, key: String) -> Result<()> {
        if self.index_map.lock().unwrap().contains_key(&key) {
            let command = serde_json::to_string(&Command{
                action: String::from("rm"),
                key:key,
                value:String::from(""),
            })?;      
    
            let mut guard = self.file.lock().unwrap();
            guard.write_all(command.as_bytes())?;
            self.load_index(&mut guard)?;
            drop(guard);

            Ok(())
        } else {
            for file_name in self.sstable_path_vec.lock().unwrap().deref() {
                let mut path = PathBuf::new();
                path.push(self.dir_path.as_ref());
                path.push(file_name);

                let mut file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .append(true)
                        .create(true)
                        .open(path)?;

                //先把他们直接读进来处理
                let mut buffer = String::new();
                file.read_to_string(&mut buffer)?;
                
                let mut indices = serde_json::Deserializer::from_str(&buffer).into_iter::<Command>();

                while let Some(command) = indices.next() {
                    let command = command?;
                    if command.key == key {
                        if command.action == "set" {
                            let command = serde_json::to_string(&Command{
                                action: String::from("rm"),
                                key:key,
                                value:String::from(""),
                            })?;      
                    
                            let mut guard = self.file.lock().unwrap();
                            guard.write_all(command.as_bytes())?;
                            self.load_index(&mut guard)?;
                            drop(guard);
                            
                            return Ok(())
                        } else if command.action == "rm" {
                            return Err(KvsError::KeyNotFound)
                        }
                    }
                    
                }
            }
            Err(KvsError::KeyNotFound)
        }
    }
}

impl Clone for Index {
    fn clone(&self) -> Self {
        Index{
            offset_begin:self.offset_begin,
            offset_end: self.offset_end
        }
    }
}

impl Clone for KvStore {
    /// Clone the kv store
    fn clone(&self) -> Self {
        KvStore{
            dir_path:self.dir_path.clone(),
            file:self.file.clone(),
            index_map: self.index_map.clone(),
            offset_begin: self.offset_begin.clone(),
            log_file_path : self.log_file_path.clone(),
            item_count: self.item_count.clone(),
            sstable_path_vec : self.sstable_path_vec.clone(),
        }
    }
}
impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut path = path.into();
        let dir_path = path.clone();
        fs::create_dir_all(&path)?;

        path.push("log.txt"); //这个文件是固定的
        let index_map:HashMap<String, Index> = HashMap::new();
        let offset_begin = 0;
        
        let mut sstable_path_vec : Vec<String> = Vec::new();
        for entry in fs::read_dir(&dir_path)? {
            let file_name = entry?.path().display().to_string();
            let vec: Vec<&str> = file_name.split("/").collect();
            if vec[vec.len() - 1].starts_with("sstable") {
                sstable_path_vec.push(vec[vec.len() - 1].to_string());
            }
        }

        sstable_path_vec.sort();

        //直接创建一个file
        let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .append(true)
                        .create(true)
                        .open(&path)?;
        

        let mut kv_store = KvStore{
            dir_path:Arc::new(dir_path),
            file: Arc::new(Mutex::new(file)),
            index_map: Arc::new(Mutex::new(index_map)),
            offset_begin: Arc::new(Mutex::new(offset_begin)),
            log_file_path : Arc::new(path),
            item_count : Arc::new(Mutex::new(0)),
            sstable_path_vec : Arc::new(Mutex::new(sstable_path_vec)),
        };

        let mut guard = kv_store.file.lock().unwrap();
        kv_store.load_index(&mut guard)?;
        drop(guard);

        Ok(kv_store)

    }

    
    fn load_index(&self, guard:&mut std::sync::MutexGuard<File>) -> Result<()> {
        guard.seek(SeekFrom::Start(*self.offset_begin.lock().unwrap().deref() as u64))?;
        
        let mut buffer = String::new();

        // 读取整个文件
        guard.read_to_string(&mut buffer)?;

        //反序列化
        let mut indices = serde_json::Deserializer::from_str(&buffer).into_iter::<Command>();

        let mut offset_begin = 0;

        while let Some(command) = indices.next() {
            let offset_end = indices.byte_offset();
            let command = command?;
            if command.action == "set" {
                let command_key = command.key;

                let new_offset_begin = (offset_begin + self.offset_begin.lock().unwrap().deref()) as u64;
                let new_offset_end = (offset_end + self.offset_begin.lock().unwrap().deref()) as u64;
                self.index_map.lock().unwrap().insert(command_key, 
                    Index{
                        offset_begin: new_offset_begin,
                        offset_end:new_offset_end,
                    });
            } else if command.action == "rm" {
                let command_key = command.key;
                
                if self.index_map.lock().unwrap().contains_key(&command_key) {
                    self.index_map.lock().unwrap().remove_entry(&command_key);
                }
            }

            *self.item_count.lock().unwrap() += 1;
            offset_begin = offset_end;
        }

        *self.offset_begin.lock().unwrap() += offset_begin;

        // 设定条目超过 2000 就触发压缩
        if *self.item_count.lock().unwrap() > 2000 {
            self.compact(guard)?
        }
        Ok(())
    }

    ///先对文件中切分后剩下的内容重新写入文件，并且更新index情况
    fn restore_rest_file(&self, offset: u64, guard:&mut std::sync::MutexGuard<File>) -> Result<()> {
        guard.seek(SeekFrom::Start(offset))?;
        let mut buffer = String::new();
        guard.read_to_string(&mut buffer)?;
        fs::remove_file(self.log_file_path.deref())?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(self.log_file_path.deref())?;
        
        **(guard) = file;

        *self.offset_begin.lock().unwrap() = 0;
        *self.item_count.lock().unwrap() = 0;

        if buffer.len() != 0 {
            guard.write_all(buffer.as_bytes())?;
            self.load_index(guard)?;
        }
        Ok(())
    }

    /*
    先查看目前文件夹内顺序编号到了多少
    然后创建文件，按序写入
    */
    fn write_into_sstable(&self, key_item_map : &HashMap<String, Command>) -> Result<()> {
        let length = self.sstable_path_vec.lock().unwrap().len();
        let new_file = String::from("sstable_") + &length.to_string() + ".txt";

        let mut new_path = PathBuf::new();
        new_path.push(self.dir_path.as_ref());
        new_path.push(&new_file);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(new_path)?;
        
        for key in key_item_map.keys() {
            let command = serde_json::to_string(&key_item_map[key])?;
            file.write_all(command.as_bytes())?;
        }

        file.seek(SeekFrom::Start(0))?;
        let mut buffer = String::new();
        // 读取整个文件
        file.read_to_string(&mut buffer)?;

        self.sstable_path_vec.lock().unwrap().push(new_file);
        Ok(())
    }

    // 这边做的是一个很暴力的压缩，也就是条数超了就把前面 count 条筛选一下重复的扔掉
    fn compact(&self, guard:&mut std::sync::MutexGuard<File>) -> Result<()> {

        guard.seek(SeekFrom::Start(0))?;
        let mut buffer = String::new();
        // 读取整个文件
        guard.read_to_string(&mut buffer)?;
        let mut indices= serde_json::Deserializer::from_str(&buffer).into_iter::<Command>();

        let mut key_item_map :HashMap<String, Command> = HashMap::new();

        let mut count = 0;
        let mut offset = 0;
        while let Some(command) = indices.next() {
            let command = command?;
            if count > 2000 { //设定大于2000就做压缩
                
                self.restore_rest_file(offset, guard)?;

                self.write_into_sstable(&key_item_map)?;

                return Ok(())
            } else {
                if &command.action == "set" {
                    key_item_map.insert(command.key.clone(), command);
                } else if command.action == "rm" {
                    let command_key = command.key;
                    if key_item_map.contains_key(&command_key) {
                        key_item_map.remove_entry(&command_key);
                    }
                }
            }
            count += 1;
            offset = indices.byte_offset() as u64;
        }
        Ok(())
    }
    
}
