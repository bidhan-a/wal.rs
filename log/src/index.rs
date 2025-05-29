use byteorder::{BigEndian, ByteOrder};
use memmap2::{MmapMut, MmapOptions};
use std::fs::File;

use crate::error::{LogError, LogResult};

// TODO: Store this in a config.
const MAX_INDEX_SIZE: usize = 1024; // Maximum size of the index in bytes.

const OFFSET_SIZE: usize = 4; // Size of the offset in bytes (u32).
const POSITION_SIZE: usize = 8; // Size of the position in bytes (u64).
const ENTRY_SIZE: usize = OFFSET_SIZE + POSITION_SIZE; // Size of each index entry in bytes.

pub struct Index {
    file: File,
    mmap: MmapMut,
    size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub offset: u32,
    pub position: u64,
}

impl Index {
    pub fn new(file: File) -> LogResult<Self> {
        let metadata = file.metadata()?;
        let size = metadata.len();

        // Truncate file to max index size.
        file.set_len(MAX_INDEX_SIZE as u64)?;

        // Create a memory-mapped file.
        let mmap = unsafe { MmapOptions::new().len(MAX_INDEX_SIZE).map_mut(&file)? };

        Ok(Self { file, mmap, size })
    }

    pub fn write(&mut self, offset: u32, position: u64) -> LogResult<()> {
        // Check if the index is full.
        if self.mmap.len() < (self.size as usize + ENTRY_SIZE) {
            return Err(LogError::IndexFullError);
        }

        // Get the start position for the new entry.
        let start = self.size as usize;

        // Write the offset.
        BigEndian::write_u32(&mut self.mmap[start..start + OFFSET_SIZE], offset);

        // Write the position.
        BigEndian::write_u64(
            &mut self.mmap[start + OFFSET_SIZE..start + ENTRY_SIZE],
            position,
        );

        // Update the size.
        self.size += ENTRY_SIZE as u64;

        Ok(())
    }

    pub fn read(&self, offset: Option<u32>) -> LogResult<IndexEntry> {
        if self.size == 0 {
            return Err(LogError::IndexEmptyError);
        }

        let entry_offset = match offset {
            Some(o) => o,
            None => ((self.size / ENTRY_SIZE as u64) - 1) as u32, // Get the offset of the last entry.
        };

        let byte_pos = (entry_offset as u64) * ENTRY_SIZE as u64;
        if self.size < byte_pos + ENTRY_SIZE as u64 {
            return Err(LogError::IndexEntryOutOfBoundsError);
        }

        let start = byte_pos as usize;

        // Read offset and position from the mmap.
        let offset = BigEndian::read_u32(&self.mmap[start..start + OFFSET_SIZE]);
        let position = BigEndian::read_u64(&self.mmap[start + OFFSET_SIZE..start + ENTRY_SIZE]);

        Ok(IndexEntry { offset, position })
    }

    pub fn read_at(&self, offset: u32) -> LogResult<IndexEntry> {
        self.read(Some(offset))
    }

    pub fn read_last(&self) -> LogResult<IndexEntry> {
        self.read(None)
    }

    pub fn close(&self) -> LogResult<()> {
        // Flush the mmap to disk.
        self.mmap.flush()?;

        // Sync the file.
        self.file.sync_all()?;

        // Truncate the file to the used size.
        self.file.set_len(self.size)?;

        Ok(())
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_index_write_and_read() -> LogResult<()> {
        let temp_file = NamedTempFile::new()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(temp_file.path())?;

        let mut index = Index::new(file)?;

        // Write some entries.
        index.write(0, 100)?;
        index.write(1, 200)?;
        index.write(2, 300)?;

        // Read entries back.
        let entry0 = index.read_at(0)?;
        let entry1 = index.read_at(1)?;
        let entry2 = index.read_at(2)?;

        assert_eq!(entry0.offset, 0);
        assert_eq!(entry0.position, 100);

        assert_eq!(entry1.offset, 1);
        assert_eq!(entry1.position, 200);

        assert_eq!(entry2.offset, 2);
        assert_eq!(entry2.position, 300);

        // Test reading the last entry.
        let last = index.read_last()?;
        assert_eq!(last.offset, 2);
        assert_eq!(last.position, 300);

        Ok(())
    }
}
