use cfg::Config;
use crossbeam_channel::{Receiver, Sender, bounded};
use parking_lot::Mutex;
use prost::Message;
use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
};

use api::Record;

use crate::{
    error::{LogError, LogResult},
    index::Index,
    store::Store,
};

struct WriteRequest {
    record: Record,
    response: crossbeam_channel::Sender<LogResult<u64>>,
}

pub(crate) struct Segment {
    inner: Arc<SegmentInner>,
    sender: Sender<WriteRequest>,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<LogResult<()>>>,
}

pub struct SegmentInner {
    pub store: Store,
    pub index: Mutex<Index>,
    pub base_offset: u64,
    pub next_offset: AtomicU64,
    pub store_path: PathBuf,
    pub index_path: PathBuf,
    pub config: Config,
    pub write_lock: Mutex<()>, // Per-segment write coordination
}

impl Segment {
    pub fn new<P: AsRef<Path>>(dir: P, base_offset: u64, config: Config) -> LogResult<Self> {
        let dir = dir.as_ref();

        // Setup store.
        let store_filename = format!("{}.store", base_offset);
        let store_path = dir.join(&store_filename);
        let store_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&store_path)?;
        let store = Store::new(store_file, config.log.durability)?;

        // Setup index.
        let index_filename = format!("{}.index", base_offset);
        let index_path = dir.join(&index_filename);
        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&index_path)?;
        let index = Index::new(index_file, config.log.segment.max_index_size)?;

        // Determine the next offset.
        let next_offset = match index.read_last() {
            Ok(entry) => base_offset + entry.offset as u64 + 1,
            Err(_) => base_offset,
        };

        let inner = Arc::new(SegmentInner {
            store,
            index: Mutex::new(index),
            base_offset,
            next_offset: AtomicU64::new(next_offset),
            store_path,
            index_path,
            config,
            write_lock: Mutex::new(()),
        });

        // Create background writer.
        let (sender, receiver) = bounded(1000); // Buffer up to 1000 requests
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let inner_clone = inner.clone();

        let handle =
            thread::spawn(move || Self::writer_loop(inner_clone, receiver, shutdown_clone, config));

        Ok(Self {
            inner,
            sender,
            shutdown,
            handle: Some(handle),
        })
    }

    pub fn append(&self, record: &Record) -> LogResult<u64> {
        let (response_tx, response_rx) = crossbeam_channel::bounded(1);

        let request = WriteRequest {
            record: record.clone(),
            response: response_tx,
        };

        // Send request to background writer.
        self.sender
            .send(request)
            .map_err(|_| LogError::WriterError("Writer channel closed".to_string()))?;

        // Wait for response (blocking).
        response_rx
            .recv()
            .map_err(|_| LogError::WriterError("Response channel closed".to_string()))?
    }

    /// Read the record at the provided offset.
    pub fn read(&self, offset: u64) -> LogResult<Record> {
        // Get the relative index offset by subtracting base offset from offset.
        if offset < self.inner.base_offset {
            // Offset can't be less than the base offset.
            return Err(LogError::InvalidOffsetError);
        }
        let relative_offset = offset - self.inner.base_offset;

        // Read the index entry.
        let entry = {
            let index = self.inner.index.lock();
            index.read_at(relative_offset as u32)?
        };

        // Read the record from the store.
        let record_bytes = self.inner.store.read(entry.position)?;

        // Decode the record.
        let record = Record::decode(&record_bytes[..]).map_err(|_| LogError::InvalidRecordError)?;

        Ok(record)
    }

    /// Check if the segment contains the provided offset.
    pub fn contains(&self, offset: u64) -> bool {
        let current_next_offset = self.inner.next_offset.load(Ordering::Acquire);
        self.inner.base_offset <= offset && offset < current_next_offset
    }

    /// Check if the segment is maxed out.
    pub fn is_maxed(&self) -> bool {
        self.inner.store.size() >= self.inner.config.log.segment.max_store_size || {
            let index = self.inner.index.lock();
            index.size() >= self.inner.config.log.segment.max_index_size
        }
    }

    /// Get the current next offset
    pub fn next_offset(&self) -> u64 {
        self.inner.next_offset.load(Ordering::Acquire)
    }

    fn writer_loop(
        segment: Arc<SegmentInner>,
        receiver: Receiver<WriteRequest>,
        shutdown: Arc<AtomicBool>,
        config: Config,
    ) -> LogResult<()> {
        let mut batch = Vec::new();
        let batch_size = if config.log.durability == cfg::DurabilityPolicy::Always {
            1
        } else {
            10
        };
        const BATCH_TIMEOUT_MS: u64 = 5;

        while !shutdown.load(Ordering::Acquire) {
            // Collect a batch of requests.
            match receiver.recv_timeout(std::time::Duration::from_millis(BATCH_TIMEOUT_MS)) {
                Ok(request) => {
                    batch.push(request);

                    // Collect more requests up to batch size
                    while batch.len() < batch_size {
                        match receiver.try_recv() {
                            Ok(request) => batch.push(request),
                            Err(_) => break, // No more requests available
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    // Process any pending requests.
                    if batch.is_empty() {
                        continue;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    break;
                }
            }

            // Process the batch.
            {
                let _write_guard = segment.write_lock.lock();

                for request in batch.drain(..) {
                    let result = Self::append_to_segment(&segment, &request.record);
                    let _ = request.response.send(result);
                }

                // Group commit: sync after processing the batch.
                if batch_size > 1 {
                    let _ = segment.store.sync();
                }
            }
        }

        Ok(())
    }

    fn append_to_segment(segment: &SegmentInner, record: &Record) -> LogResult<u64> {
        let mut buf = Vec::new();
        record
            .encode(&mut buf)
            .map_err(|_| LogError::InvalidRecordError)?;

        // Atomically allocate the next offset
        let current_offset = segment.next_offset.fetch_add(1, Ordering::AcqRel);

        // Append record to the store.
        let (_, position) = segment.store.append(&buf)?;

        // Write the index entry.
        // NOTE: Index offsets are relative to the base offset.
        // For example - if base offset = 110 and current offset = 117
        // then the index offset is 7 (117 - 110).
        let relative_offset = current_offset - segment.base_offset;
        {
            let mut index = segment.index.lock();
            index.write(relative_offset as u32, position)?;
        }

        Ok(current_offset)
    }

    pub fn close(&self) -> LogResult<()> {
        // Shutdown writer thread
        self.shutdown.store(true, Ordering::Release);

        {
            let index = self.inner.index.lock();
            index.close()?;
        }
        self.inner.store.close()?;
        Ok(())
    }

    pub fn remove(self) -> LogResult<()> {
        // Close the segment.
        self.close()?;

        // Remove files.
        std::fs::remove_file(&self.inner.store_path)?;
        std::fs::remove_file(&self.inner.index_path)?;

        Ok(())
    }

    // Delegate properties for compatibility
    pub fn base_offset(&self) -> u64 {
        self.inner.base_offset
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);

        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use api::Record;
    use cfg::{DurabilityPolicy, LogConfig, SegmentConfig};

    #[test]
    fn test_segment_append_and_read() -> LogResult<()> {
        let config: Config = Config {
            log: LogConfig {
                segment: SegmentConfig {
                    max_store_size: 1024,
                    max_index_size: 1024,
                    initial_offset: 0,
                },
                durability: DurabilityPolicy::Never,
            },
        };
        let dir = tempfile::tempdir()?;

        // Create test records.
        let record_1 = Record {
            offset: None,
            value: b"record_1".to_vec(),
        };
        let record_2 = Record {
            offset: None,
            value: b"record_2".to_vec(),
        };

        // Create a new segment.
        let segment = Segment::new(dir.path(), 10, config)?;

        // Append the records.
        let offset = segment.append(&record_1)?;
        assert_eq!(offset, 10);
        let offset = segment.append(&record_2)?;
        assert_eq!(offset, 11);

        // Read the records back.
        let read_record = segment.read(10)?;
        assert_eq!(read_record.value, b"record_1".to_vec());
        let read_record = segment.read(11)?;
        assert_eq!(read_record.value, b"record_2".to_vec());

        // Clean up.
        segment.remove()?;

        Ok(())
    }
}
