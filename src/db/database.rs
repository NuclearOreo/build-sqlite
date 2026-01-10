//! Database file abstraction for SQLite.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::prelude::*;

use super::constants::{PAGE1_HEADER_OFFSET, PAGE_SIZE_OFFSET};

/// A SQLite database file handle.
pub struct Database {
    file: File,
    pub page_size: usize,
}

impl Database {
    /// Open a SQLite database file.
    pub fn open(path: &str) -> Result<Self> {
        let mut file = File::open(path).context("Failed to open database file")?;

        // Read page size from header
        let mut header = [0u8; 2];
        file.seek(std::io::SeekFrom::Start(PAGE_SIZE_OFFSET as u64))?;
        file.read_exact(&mut header)?;
        let page_size = u16::from_be_bytes(header) as usize;

        Ok(Self { file, page_size })
    }

    /// Read a page from the database (1-indexed).
    pub fn read_page(&mut self, page_num: u32) -> Result<Vec<u8>> {
        let page_offset = (page_num as u64 - 1) * self.page_size as u64;
        let mut page = vec![0u8; self.page_size];
        self.file
            .seek(std::io::SeekFrom::Start(page_offset))
            .context("Failed to seek to page")?;
        self.file
            .read_exact(&mut page)
            .context("Failed to read page")?;
        Ok(page)
    }

    /// Get the header offset for a given page number.
    /// Page 1 has the database header at offset 0, so the page header starts at 100.
    /// Other pages have the page header at offset 0.
    #[allow(dead_code)]
    pub fn header_offset(page_num: u32) -> usize {
        if page_num == 1 {
            PAGE1_HEADER_OFFSET
        } else {
            0
        }
    }
}
