
use super::{Result, KvsError};
use std::path::PathBuf;
use std::fs;
use std::fs::{File, OpenOptions};
use sled;



/// an Engine to store <key, value>
pub trait KvsEngine : Clone + Send + 'static {
    /// try to remove the <key,value> from kvsEngine with the given Key, if doesn't exist this key, then do nothing.
    fn remove(&self, key: String) -> Result<()>; 

    /// try to get the value from kvsEngine with corresponding key, if it doesn't exist, then return None
    fn get(&self, key: String) -> Result<Option<String>>;

    /// set the <key, value> in the kvsEngine, if key is existed, then override with the new value.
    fn set(&self, key: String, value: String) -> Result<()>;
}

/*
lab 4 不需要这个部分了，所以注释掉了
/// Engine with Sled lib
pub struct SledKvsEngine {
    database : sled::Db
}

impl KvsEngine for SledKvsEngine {
    /// set the <key, value> in the SledKvsEngine, if key is existed, then override with the new value.
    fn set(&mut self, key: String, value: String) -> Result<()>{
        self.database.insert(key, value.as_bytes()).map(|_|());
        Ok(())
    }

    /// try to get the value from SledKvsEngine with corresponding key, if it doesn't exist, then return None
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let value = self.database.get(&key).unwrap();
        if let Some(val) = value {
            let vec = AsRef::<[u8]>::as_ref(&val).to_vec();
            let value = String::from_utf8(vec).unwrap();
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// try to remove the <key,value> from SledKvsEngine with the given Key, if doesn't exist this key, then do nothing.
    fn remove(&mut self, key: String) -> Result<()> {
        if let Some(_) = self.database.remove(key).unwrap(){
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

impl SledKvsEngine {
    /// Open the corresponding file as the base data
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let mut path = path.into();
        fs::create_dir_all(&path)?;
        path.push("sled_database");
        let database = sled::open(path).expect("open");

        Ok(SledKvsEngine{
            database
        })


    }
}

*/