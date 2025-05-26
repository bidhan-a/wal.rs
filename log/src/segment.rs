use std::{fs::OpenOptions, path::Path};

use crate::{index::Index, store::Store};

pub struct Segment {
    store: Store,
    index: Index,
    base_offset: u64,
    next_offset: u64,
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
        })
    }
}
