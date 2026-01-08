use anyhow::{Context, Result};
use std::fs::File;
use std::io::prelude::*;

// Database header constants
const DB_HEADER_SIZE: usize = 100;
const PAGE_HEADER_SIZE: usize = 112;
const PAGE_SIZE_OFFSET: usize = 16;

// Page header constants (for page 1, header starts at offset 100)
const PAGE1_HEADER_OFFSET: usize = 100;
const CELL_COUNT_OFFSET: usize = 3;
const CELL_POINTER_ARRAY_OFFSET: usize = 8;

// SQLite schema column indices
const SCHEMA_TYPE_COLUMN: usize = 0;
const SCHEMA_TBL_NAME_COLUMN: usize = 2;

// Varint constants
const VARINT_MAX_BYTES: usize = 9;
const VARINT_CONTINUATION_BIT: u8 = 0x80;
const VARINT_DATA_MASK: u8 = 0x7F;

/// Read the page size from the database header.
fn read_page_size(file: &mut File) -> Result<u16> {
    let mut header = [0; DB_HEADER_SIZE];
    file.read_exact(&mut header)
        .context("Failed to read database header")?;
    let page_size = u16::from_be_bytes([header[PAGE_SIZE_OFFSET], header[PAGE_SIZE_OFFSET + 1]]);
    Ok(page_size)
}

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

/// Read a varint from a byte slice starting at the given position.
/// Returns (value, number of bytes read).
fn read_varint(data: &[u8], pos: usize) -> (u64, usize) {
    let mut value: u64 = 0;
    let mut bytes_read = 0;

    for i in 0..VARINT_MAX_BYTES {
        let byte = data[pos + i];
        bytes_read += 1;

        if i == VARINT_MAX_BYTES - 1 {
            // 9th byte uses all 8 bits
            value = (value << 8) | byte as u64;
            break;
        } else {
            // Use lower 7 bits
            value = (value << 7) | (byte & VARINT_DATA_MASK) as u64;

            // If high bit is 0, we're done
            if byte & VARINT_CONTINUATION_BIT == 0 {
                break;
            }
        }
    }

    (value, bytes_read)
}

/// Get the size in bytes of a column value based on its serial type code
fn get_column_size(serial_type: u64) -> usize {
    match serial_type {
        0 => 0,  // NULL
        1 => 1,  // 8-bit integer
        2 => 2,  // 16-bit integer
        3 => 3,  // 24-bit integer
        4 => 4,  // 32-bit integer
        5 => 6,  // 48-bit integer
        6 => 8,  // 64-bit integer
        7 => 8,  // IEEE 754 float
        8 => 0,  // Integer constant 0
        9 => 0,  // Integer constant 1
        10 | 11 => 0, // Reserved
        n if n >= 12 && n % 2 == 0 => ((n - 12) / 2) as usize, // BLOB
        n if n >= 13 && n % 2 == 1 => ((n - 13) / 2) as usize, // Text string
        _ => 0,
    }
}

/// Extract text string from page data based on serial type.
/// Returns Some(String) if the serial type represents text, None otherwise.
fn extract_text_from_serial_type(serial_type: u64, page: &[u8], pos: usize) -> Option<String> {
    if serial_type >= 13 && serial_type % 2 == 1 {
        let text_size = ((serial_type - 13) / 2) as usize;
        let text_bytes = &page[pos..pos + text_size];
        Some(String::from_utf8_lossy(text_bytes).to_string())
    } else {
        None
    }
}

/// Read cell offsets from a page's cell pointer array.
fn read_cell_offsets(page: &[u8]) -> Vec<usize> {
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
/// Returns (type, table_name) if the cell contains a valid record, None otherwise.
fn parse_schema_cell(page: &[u8], cell_offset: usize) -> Option<(String, String)> {
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

/// Read table names from the sqlite_schema table
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
