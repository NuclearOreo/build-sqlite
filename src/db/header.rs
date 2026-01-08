//! Database header parsing for SQLite format.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::prelude::*;

use super::constants::{
    CELL_COUNT_OFFSET, DB_HEADER_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE_OFFSET,
};

/// Read the page size from the database header.
///
/// The page size is stored at byte offset 16-17 in the database header
/// as a 2-byte big-endian integer.
///
/// # Arguments
///
/// * `file` - A mutable reference to the opened database file
///
/// # Returns
///
/// Returns the page size in bytes, or an error if reading fails.
pub fn read_page_size(file: &mut File) -> Result<u16> {
    let mut header = [0; DB_HEADER_SIZE];
    file.read_exact(&mut header)
        .context("Failed to read database header")?;
    let page_size = u16::from_be_bytes([header[PAGE_SIZE_OFFSET], header[PAGE_SIZE_OFFSET + 1]]);
    Ok(page_size)
}

/// Read database information including page size and number of tables.
///
/// Reads the SQLite database header and the first page header to extract
/// basic database metadata.
///
/// # Arguments
///
/// * `path` - Path to the SQLite database file
///
/// # Returns
///
/// Returns a tuple of (page_size, number_of_tables), or an error if reading fails.
pub fn read_db_info(path: &str) -> Result<(u16, u16)> {
    let mut file = File::open(path).context("Failed to open database file")?;
    let page_size = read_page_size(&mut file)?;

    let mut page_header = [0; PAGE_HEADER_SIZE];
    file.read_exact(&mut page_header)
        .context("Failed to read page header")?;
    let number_of_tables =
        u16::from_be_bytes([page_header[CELL_COUNT_OFFSET], page_header[CELL_COUNT_OFFSET + 1]]);
    Ok((page_size, number_of_tables))
}
