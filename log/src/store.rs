use byteorder::{BigEndian, WriteBytesExt};
use std::{
    fs::File,
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
};

use crate::error::LogResult;

const LEN_SIZE: usize = 8; // Size of the length prefix in bytes

pub struct Store {
    inner: Arc<Mutex<StoreInner>>,
}

struct StoreInner {
    file: File,
    buf: BufWriter<File>,
    size: u64,
}

impl Store {
    pub fn new(file: File) -> LogResult<Self> {
        let metadata = file.metadata()?;
        let size = metadata.len();

        let file_for_writer = file.try_clone()?;
        let buf = BufWriter::new(file_for_writer);

        Ok(Self {
            inner: Arc::new(Mutex::new(StoreInner { file, buf, size })),
        })
    }

    pub fn append(&self, data: &[u8]) -> LogResult<(u64, u64)> {
        let mut inner = self.inner.lock().unwrap();

        // The position where we'll begin writing from.
        let pos = inner.size;

        // Write the length of the data as the prefix.
        inner.buf.write_u64::<BigEndian>(data.len() as u64)?;

        // Write the actual data.
        let bytes_written = inner.buf.write(data)?;

        let total_bytes_written = bytes_written + LEN_SIZE;
        inner.size += total_bytes_written as u64;

        Ok((total_bytes_written as u64, pos))
    }

    pub fn read(&self, pos: u64) -> LogResult<Vec<u8>> {
        let mut inner = self.inner.lock().unwrap();

        // First, flush the buffer to make sure all data is written to the file.
        inner.buf.flush()?;

        // Read the length prefix.
        let mut len_bytes = [0u8; LEN_SIZE];
        inner.file.seek(SeekFrom::Start(pos))?;
        inner.file.read_exact(&mut len_bytes)?;

        // Convert BigEndian bytes to u64.
        let len = u64::from_be_bytes(len_bytes);

        // Read the actual data.
        let mut data = vec![0u8; len as usize];
        inner.file.read_exact(&mut data)?;

        Ok(data)
    }

    pub fn size(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.size
    }

    pub fn close(&self) -> LogResult<()> {
        let mut inner = self.inner.lock().unwrap();

        // Flush any remaining data in the buffer.
        inner.buf.flush()?;
        // Sync the file to ensure all data is written to disk.
        inner.file.sync_all()?;
        // File is automatically closed when dropped.
        Ok(())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use tempfile::NamedTempFile;

    const TEST_DATA: &[u8] = b"I am the wal.rs";

    #[test]
    fn test_store_append_and_read() -> LogResult<()> {
        let temp_file = NamedTempFile::new()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(temp_file.path())?;

        let store = Store::new(file)?;

        // Append data to the store.
        let (bytes_written, pos) = store.append(TEST_DATA)?;
        assert_eq!(bytes_written, TEST_DATA.len() as u64 + LEN_SIZE as u64);
        assert_eq!(pos, 0);

        // Read the data back.
        let data = store.read(pos)?;
        assert_eq!(data, TEST_DATA);

        Ok(())
    }
}
