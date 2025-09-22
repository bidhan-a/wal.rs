use parking_lot::RwLock;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use api::Record;
use cfg::Config;

use crate::{
    error::{LogError, LogResult},
    segment::Segment,
};

#[derive(Clone)]
pub struct Log {
    inner: Arc<RwLock<LogInner>>,
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
            inner: Arc::new(RwLock::new(LogInner {
                dir: dir.as_ref().to_path_buf(),
                current_segment_index: None,
                segments: Vec::new(),
                config,
            })),
        };
        log.setup()?;
        Ok(log)
    }

    pub fn append(&self, record: &Record) -> LogResult<u64> {
        // Use write lock for appends to handle mutable access to segments.
        let mut inner = self.inner.write();

        // Check if we need to create initial segment.
        if inner.current_segment_index.is_none() {
            let initial_offset = inner.config.log.segment.initial_offset;
            inner.create_segment(initial_offset)?;
        }

        let current_segment_index = inner.current_segment_index.unwrap();

        // Check if current segment is maxed and needs rotation.
        if inner.segments[current_segment_index].is_maxed() {
            let next_offset = inner.segments[current_segment_index].next_offset();
            inner.create_segment(next_offset)?;
        }

        // Append to current segment.
        let current_segment_index = inner.current_segment_index.unwrap();
        inner.segments[current_segment_index].append(record)
    }

    pub fn read(&self, offset: u64) -> LogResult<Record> {
        let inner = self.inner.read();

        // Find the correct segment where the offset is present.
        // Note that the offset will be absolute here, not relative.
        let segment = inner
            .segments
            .iter()
            .find(|s| s.contains(offset))
            .ok_or_else(|| LogError::InvalidOffsetError)?;

        segment.read(offset)
    }

    /// Get the lowest offset.
    pub fn get_lowest_offset(&self) -> u64 {
        let inner = self.inner.read();
        if inner.segments.is_empty() {
            return 0;
        }
        inner.segments[0].base_offset()
    }

    /// Get the highest offset.
    pub fn get_highest_offset(&self) -> u64 {
        let inner = self.inner.read();
        if inner.segments.is_empty() {
            return 0;
        }
        let last_segment = &inner.segments[inner.segments.len() - 1];
        let offset = last_segment.next_offset();
        if offset == 0 {
            return 0;
        }
        offset - 1
    }

    /// Clean up all segments whose highest offset is lower than the cutoff offset.
    /// This function will be called periodically to free up disk space by removing
    /// old segments whose data has already been processed.
    pub fn cleanup(&self, cutoff_offset: u64) -> LogResult<()> {
        let mut inner = self.inner.write();

        let segments = std::mem::take(&mut inner.segments);
        let mut segments_to_keep = Vec::with_capacity(segments.len());
        for segment in segments {
            if segment.next_offset() < cutoff_offset {
                segment.remove()?;
            } else {
                segments_to_keep.push(segment);
            }
        }
        inner.segments = segments_to_keep;

        // Update current segment index.
        if inner.segments.is_empty() {
            inner.current_segment_index = None;
        } else {
            inner.current_segment_index = Some(inner.segments.len() - 1);
        }

        Ok(())
    }

    pub fn close(&self) -> LogResult<()> {
        let inner = self.inner.read();

        for segment in &inner.segments {
            segment.close()?;
        }

        Ok(())
    }

    pub fn remove(&self) -> LogResult<()> {
        let dir = {
            let inner = self.inner.read();
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
        let mut inner = self.inner.write();

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
    use cfg::{DurabilityPolicy, LogConfig, SegmentConfig};

    #[test]
    fn test_log_append_and_read() -> LogResult<()> {
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
        let log = Log::new(dir.path(), config)?;

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
