use failure::Fail;
use std::io;

/// Error type for kvs.
#[derive(Fail, Debug)]
pub enum KvsError {
    /// IO error.
    #[fail(display = "{}", _0)]
    IoError(#[cause] io::Error),
    /// Serialization or deserialization error.
    #[fail(display = "{}", _0)]
    SerdeError(#[cause] serde_json::Error),
    /// Not found the Key
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// Other Error
    #[fail(display = "Other Error")]
    OtherError,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::IoError(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::SerdeError(err)
    }
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;