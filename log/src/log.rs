use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use api::Record;
use cfg::Config;

use crate::{
    error::{LogError, LogResult},
    segment::Segment,
};

pub struct Log {
    inner: Arc<Mutex<LogInner>>,
}

struct LogInner {
    dir: PathBuf,
    segments: Vec<Segment>,
    current_segment_index: Option<usize>,
    config: Config,
}

impl Log {
    pub fn new<P: AsRef<Path>>(dir: P, config: Config) -> LogResult<Self> {
        let log = Self {
            inner: Arc::new(Mutex::new(LogInner {
                dir: dir.as_ref().to_path_buf(),
                current_segment_index: None,
                segments: Vec::new(),
                config,
            })),
        };
        log.setup()?;
        Ok(log)
    }

    pub fn append(&mut self, record: &Record) -> LogResult<u64> {
        let mut inner = self.inner.lock().unwrap();

        let current_segment_index = inner.current_segment_index.unwrap();
        let current_segment = &mut inner.segments[current_segment_index];
        let offset = current_segment.append(&record)?;

        // If the current segment has reached its max size, create a new one.
        if current_segment.is_maxed() {
            inner.create_segment(offset + 1)?;
        }

        Ok(offset)
    }

    pub fn read(&self, offset: u64) -> LogResult<Record> {
        let inner = self.inner.lock().unwrap();

        // Find the correct segment where the offset is present.
        // Note that the offset will be absolute here, not relative.
        let segment = inner
            .segments
            .iter()
            .find(|s| s.contains(offset))
            .ok_or_else(|| LogError::InvalidOffsetError)?;
        let record = segment.read(offset)?;

        Ok(record)
    }

    pub fn close(&self) -> LogResult<()> {
        let inner = self.inner.lock().unwrap();

        for segment in &inner.segments {
            segment.close()?;
        }

        Ok(())
    }

    pub fn remove(&self) -> LogResult<()> {
        let dir = {
            let inner = self.inner.lock().unwrap();
            inner.dir.clone()
        };

        self.close()?;
        std::fs::remove_dir_all(&dir)?;

        Ok(())
    }

    pub fn reset(&self) -> LogResult<()> {
        self.remove()?;
        self.setup()?;
        Ok(())
    }

    fn setup(&self) -> LogResult<()> {
        let mut inner = self.inner.lock().unwrap();

        let entries = std::fs::read_dir(&inner.dir)?;
        let mut base_offsets = Vec::new();

        for entry in entries {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();

            // Extract base offset from filename.
            if let Some(stem) = Path::new(&*file_name).file_stem() {
                if let Ok(offset) = stem.to_string_lossy().parse::<u64>() {
                    base_offsets.push(offset);
                }
            }
        }

        // Sort base offsets.
        base_offsets.sort_unstable();
        // Each segment has two files with the same base offset
        // (store and index), so we deduplicate.
        base_offsets.dedup();

        // Setup segments.
        for base_offset in base_offsets {
            inner.create_segment(base_offset)?;
        }

        // Create initial segment if none exist.
        if inner.segments.is_empty() {
            let initial_offset = inner.config.log.segment.initial_offset;
            inner.create_segment(initial_offset)?;
        }

        Ok(())
    }
}

impl LogInner {
    fn create_segment(&mut self, offset: u64) -> LogResult<()> {
        let segment = Segment::new(&self.dir, offset, self.config)?;
        self.segments.push(segment);
        self.current_segment_index = Some(self.segments.len() - 1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use api::Record;
    use cfg::{LogConfig, SegmentConfig};

    #[test]
    fn test_log_append_and_read() -> LogResult<()> {
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
        let mut log = Log::new(dir.path(), config)?;

        // Append the records.
        let offset = log.append(&record_1)?;
        assert_eq!(offset, 0);
        let offset = log.append(&record_2)?;
        assert_eq!(offset, 1);

        // Read the records back.
        let read_record = log.read(0)?;
        assert_eq!(read_record.value, b"record_1".to_vec());
        let read_record = log.read(1)?;
        assert_eq!(read_record.value, b"record_2".to_vec());

        // Clean up.
        log.remove()?;

        Ok(())
    }
}
