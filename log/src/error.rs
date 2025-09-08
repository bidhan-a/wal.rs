use thiserror::Error;

#[derive(Debug, Error)]
pub enum LogError {
    #[error("The index is empty")]
    IndexEmptyError,
    #[error("The index is full")]
    IndexFullError,
    #[error("The index entry is out of bounds")]
    IndexEntryOutOfBoundsError,
    #[error("The offset is invalid")]
    InvalidOffsetError,
    #[error("The record is invalid")]
    InvalidRecordError,
    #[error("Writer error: {0}")]
    WriterError(String),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
}

pub type LogResult<T> = Result<T, LogError>;
