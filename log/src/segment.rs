use prost::Message;
use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
};

use api::Record;

use crate::{index::Index, store::Store};

pub struct Segment {
    store: Store,
    index: Index,
    base_offset: u64,
    next_offset: u64,
    store_path: PathBuf,
    index_path: PathBuf,
}

impl Segment {
    pub fn new<P: AsRef<Path>>(dir: P, base_offset: u64) -> std::io::Result<Self> {
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
        let index = Index::new(index_file)?;

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
        })
    }

    pub fn append(&mut self, record: &Record) -> std::io::Result<u64> {
        let mut buf = Vec::new();
        record.encode(&mut buf)?;

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

    pub fn read(&self, offset: u64) -> std::io::Result<Record> {
        // Get the relative index offset by subtracting base offset from offset.
        if offset < self.base_offset {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Offset is less than base offset",
            ));
        }
        let relative_offset = offset - self.base_offset;

        // Read the index entry.
        let entry = self.index.read_at(relative_offset as u32)?;

        // Read the record from the store.
        let record_bytes = self.store.read(entry.position)?;

        // Decode the record.
        let record = Record::decode(&record_bytes[..])?;

        Ok(record)
    }

    pub fn close(&self) -> std::io::Result<()> {
        self.index.close()?;
        self.store.close()?;
        Ok(())
    }

    pub fn remove(self) -> std::io::Result<()> {
        // Close the segment.
        self.close()?;

        // Remove files.
        std::fs::remove_file(&self.store_path)?;
        std::fs::remove_file(&self.index_path)?;

        Ok(())
    }
}
