use cfg::Config;
use prost::Message;
use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
};

use api::Record;

use crate::{
    error::{LogError, LogResult},
    index::Index,
    store::Store,
};

pub struct Segment {
    store: Store,
    index: Index,
    base_offset: u64,
    next_offset: u64,
    store_path: PathBuf,
    index_path: PathBuf,
    config: Config,
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
        let store = Store::new(store_file)?;

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

        Ok(Self {
            store,
            index,
            base_offset,
            next_offset,
            store_path,
            index_path,
            config,
        })
    }

    pub fn append(&mut self, record: &Record) -> LogResult<u64> {
        let mut buf = Vec::new();
        record
            .encode(&mut buf)
            .map_err(|_| LogError::InvalidRecordError)?;

        // Append record to the store.
        let (_, position) = self.store.append(&buf)?;

        // Write the index entry.
        // NOTE: Index offsets are relative to the base offset.
        // For example - if base offset = 110 and next offset = 117
        // then the index offset is 7 (117 - 110).
        let relative_offset = self.next_offset - self.base_offset;
        self.index.write(relative_offset as u32, position)?;

        // The offset where the record was written.
        let current_offset = self.next_offset;

        // Update the next offset.
        self.next_offset += 1;

        Ok(current_offset)
    }

    pub fn read(&self, offset: u64) -> LogResult<Record> {
        // Get the relative index offset by subtracting base offset from offset.
        if offset < self.base_offset {
            // Offset can't be less than the base offset.
            return Err(LogError::InvalidOffsetError);
        }
        let relative_offset = offset - self.base_offset;

        // Read the index entry.
        let entry = self.index.read_at(relative_offset as u32)?;

        // Read the record from the store.
        let record_bytes = self.store.read(entry.position)?;

        // Decode the record.
        let record = Record::decode(&record_bytes[..]).map_err(|_| LogError::InvalidRecordError)?;

        Ok(record)
    }

    /// Check if the segment contains the provided offset.
    pub fn contains(&self, offset: u64) -> bool {
        if self.base_offset <= offset && offset < self.next_offset {
            return true;
        }
        false
    }

    /// Check if the segment is maxed out.
    pub fn is_maxed(&self) -> bool {
        self.store.size() >= self.config.log.segment.max_store_size
            || self.index.size() >= self.config.log.segment.max_index_size
    }

    pub fn close(&self) -> LogResult<()> {
        self.index.close()?;
        self.store.close()?;
        Ok(())
    }

    pub fn remove(self) -> LogResult<()> {
        // Close the segment.
        self.close()?;

        // Remove files.
        std::fs::remove_file(&self.store_path)?;
        std::fs::remove_file(&self.index_path)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use api::Record;
    use cfg::{LogConfig, SegmentConfig};

    #[test]
    fn test_segment_append_and_read() -> LogResult<()> {
        let config: Config = Config {
            log: LogConfig {
                segment: SegmentConfig {
                    max_store_size: 1024,
                    max_index_size: 1024,
                    initial_offset: 0,
                },
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
        let mut segment = Segment::new(dir.path(), 10, config)?;

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
