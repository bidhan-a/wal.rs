use byteorder::{BigEndian, WriteBytesExt};
use cfg::DurabilityPolicy;
use parking_lot::Mutex;
use std::{
    fs::File,
    os::unix::fs::FileExt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::error::LogResult;

const LEN_SIZE: usize = 8; // Size of the length prefix in bytes

pub struct Store {
    file: Arc<File>,
    size: AtomicU64,
    write_lock: Mutex<()>, // Minimal lock for write coordination
    durability: DurabilityPolicy,
    last_sync: Mutex<Instant>,
}

impl Store {
    pub fn new(file: File, durability: DurabilityPolicy) -> LogResult<Self> {
        let metadata = file.metadata()?;
        let size = metadata.len();

        Ok(Self {
            file: Arc::new(file),
            size: AtomicU64::new(size),
            write_lock: Mutex::new(()),
            durability,
            last_sync: Mutex::new(Instant::now()),
        })
    }

    pub fn append(&self, data: &[u8]) -> LogResult<(u64, u64)> {
        let _write_guard = self.write_lock.lock();

        // Atomically allocate the write position.
        let total_size = data.len() + LEN_SIZE;
        let pos = self.size.fetch_add(total_size as u64, Ordering::AcqRel);

        // Prepare the write buffer with length prefix.
        let mut write_buf = Vec::with_capacity(total_size);
        write_buf.write_u64::<BigEndian>(data.len() as u64)?;
        write_buf.extend_from_slice(data);

        // Write at the allocated position using positional I/O
        self.file.write_all_at(&write_buf, pos)?;

        // Handle durability policy
        self.handle_durability()?;

        Ok((total_size as u64, pos))
    }

    pub fn read(&self, pos: u64) -> LogResult<Vec<u8>> {
        // Read the length prefix using positional I/O
        let mut len_bytes = [0u8; LEN_SIZE];
        self.file.read_exact_at(&mut len_bytes, pos)?;

        // Convert BigEndian bytes to u64
        let len = u64::from_be_bytes(len_bytes);

        // Read the actual data using positional I/O
        let mut data = vec![0u8; len as usize];
        self.file.read_exact_at(&mut data, pos + LEN_SIZE as u64)?;

        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Acquire)
    }

    fn handle_durability(&self) -> LogResult<()> {
        match self.durability {
            DurabilityPolicy::Always => {
                self.file.sync_all()?;
            }
            DurabilityPolicy::Interval(interval_ms) => {
                let mut last_sync = self.last_sync.lock();
                let now = Instant::now();
                if now.duration_since(*last_sync) >= Duration::from_millis(interval_ms) {
                    self.file.sync_all()?;
                    *last_sync = now;
                }
            }
            DurabilityPolicy::OnRotate | DurabilityPolicy::Never => {
                // No sync needed.
            }
        }
        Ok(())
    }

    pub fn sync(&self) -> LogResult<()> {
        self.file.sync_all()?;
        let mut last_sync = self.last_sync.lock();
        *last_sync = Instant::now();
        Ok(())
    }

    pub fn close(&self) -> LogResult<()> {
        // Sync the file to ensure all data is written to disk.
        if matches!(self.durability, DurabilityPolicy::OnRotate) {
            self.file.sync_all()?;
        }
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

        let store = Store::new(file, DurabilityPolicy::Never)?;

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
