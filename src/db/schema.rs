//! SQLite schema table parsing.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::prelude::*;

use super::constants::{
    CELL_COUNT_OFFSET, CELL_POINTER_ARRAY_OFFSET, PAGE1_HEADER_OFFSET, SCHEMA_TBL_NAME_COLUMN,
    SCHEMA_TYPE_COLUMN,
};
use super::header::read_page_size;
use super::record::{extract_text_from_serial_type, get_column_size};
use super::varint::read_varint;

/// Read cell offsets from a page's cell pointer array.
///
/// The cell pointer array contains 2-byte big-endian offsets pointing
/// to each cell in the page.
///
/// # Arguments
///
/// * `page` - The page data containing the cell pointer array
///
/// # Returns
///
/// Returns a vector of cell offsets in bytes.
pub fn read_cell_offsets(page: &[u8]) -> Vec<usize> {
    let num_cells = u16::from_be_bytes([
        page[PAGE1_HEADER_OFFSET + CELL_COUNT_OFFSET],
        page[PAGE1_HEADER_OFFSET + CELL_COUNT_OFFSET + 1],
    ]);

    let cell_pointer_array_offset = PAGE1_HEADER_OFFSET + CELL_POINTER_ARRAY_OFFSET;
    let mut cell_offsets = Vec::new();

    for i in 0..num_cells {
        let offset_pos = cell_pointer_array_offset + (i as usize * 2);
        let cell_offset = u16::from_be_bytes([page[offset_pos], page[offset_pos + 1]]);
        cell_offsets.push(cell_offset as usize);
    }

    cell_offsets
}

/// Parse a single cell from the sqlite_schema table to extract table name and type.
///
/// Each cell in the sqlite_schema table contains a record with columns:
/// type, name, tbl_name, rootpage, sql.
///
/// # Arguments
///
/// * `page` - The page data containing the cell
/// * `cell_offset` - The byte offset of the cell in the page
///
/// # Returns
///
/// Returns `Some((type, table_name))` if the cell contains a valid record,
/// `None` otherwise.
pub fn parse_schema_cell(page: &[u8], cell_offset: usize) -> Option<(String, String)> {
    // Read the record size (varint)
    let (_record_size, bytes_read) = read_varint(page, cell_offset);
    let mut pos = cell_offset + bytes_read;

    // Read the rowid (varint) - we can ignore this
    let (_, bytes_read) = read_varint(page, pos);
    pos += bytes_read;

    // Parse the record header
    let record_start = pos;
    let (header_size, bytes_read) = read_varint(page, pos);
    pos += bytes_read;

    // Read serial type codes
    let mut serial_types = Vec::new();
    let header_end = record_start + header_size as usize;

    while pos < header_end {
        let (serial_type, bytes_read) = read_varint(page, pos);
        serial_types.push(serial_type);
        pos += bytes_read;
    }

    // Now read the actual column values
    // sqlite_schema columns: type, name, tbl_name, rootpage, sql
    // We need to read type (index 0) and tbl_name (index 2)
    let mut column_index = 0;
    let mut record_type = String::new();
    let mut tbl_name = String::new();

    for &serial_type in &serial_types {
        let column_size = get_column_size(serial_type);

        if column_index == SCHEMA_TYPE_COLUMN {
            // This is the type column
            if let Some(text) = extract_text_from_serial_type(serial_type, page, pos) {
                record_type = text;
            }
        } else if column_index == SCHEMA_TBL_NAME_COLUMN {
            // This is the tbl_name column
            if let Some(text) = extract_text_from_serial_type(serial_type, page, pos) {
                tbl_name = text;
            }
            break;
        }

        pos += column_size;
        column_index += 1;
    }

    if !record_type.is_empty() && !tbl_name.is_empty() {
        Some((record_type, tbl_name))
    } else {
        None
    }
}

/// Read table names from the sqlite_schema table.
///
/// Parses the sqlite_schema table in the first page of the database
/// to extract all user-defined table names (excluding internal SQLite tables).
///
/// # Arguments
///
/// * `path` - Path to the SQLite database file
///
/// # Returns
///
/// Returns a vector of table names, or an error if the database cannot
/// be read or parsed.
pub fn read_table_names(path: &str) -> Result<Vec<String>> {
    let mut file = File::open(path).context("Failed to open database file")?;
    let page_size = read_page_size(&mut file)?;

    // Read the first page (sqlite_schema page)
    let mut page = vec![0u8; page_size as usize];
    file.seek(std::io::SeekFrom::Start(0))
        .context("Failed to seek to start of file")?;
    file.read_exact(&mut page)
        .context("Failed to read first page")?;

    // Read cell offsets from the page
    let cell_offsets = read_cell_offsets(&page);

    // Parse each cell to extract table names
    let mut table_names = Vec::new();
    for cell_offset in cell_offsets {
        if let Some((record_type, tbl_name)) = parse_schema_cell(&page, cell_offset) {
            // Only include user tables (type = "table" and not internal tables)
            if record_type == "table" && !tbl_name.starts_with("sqlite_") {
                table_names.push(tbl_name);
            }
        }
    }

    Ok(table_names)
}
