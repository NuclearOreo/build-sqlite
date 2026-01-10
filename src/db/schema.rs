//! SQLite schema table parsing.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::prelude::*;

use super::constants::{
    CELL_COUNT_OFFSET, CELL_POINTER_ARRAY_OFFSET, PAGE1_HEADER_OFFSET, SCHEMA_ROOTPAGE_COLUMN,
    SCHEMA_SQL_COLUMN, SCHEMA_TBL_NAME_COLUMN, SCHEMA_TYPE_COLUMN,
};
use super::header::read_page_size;
use super::record::{extract_int_from_serial_type, extract_text_from_serial_type, get_column_size};
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

/// Parse a schema cell to extract table metadata including rootpage.
///
/// # Arguments
///
/// * `page` - The page data containing the cell
/// * `cell_offset` - The byte offset of the cell in the page
///
/// # Returns
///
/// Returns `Some((type, table_name, rootpage))` if valid, `None` otherwise.
fn parse_schema_cell_with_rootpage(page: &[u8], cell_offset: usize) -> Option<(String, String, u32)> {
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
    let mut column_index = 0;
    let mut record_type = String::new();
    let mut tbl_name = String::new();
    let mut rootpage: u32 = 0;

    for &serial_type in &serial_types {
        let column_size = get_column_size(serial_type);

        if column_index == SCHEMA_TYPE_COLUMN {
            if let Some(text) = extract_text_from_serial_type(serial_type, page, pos) {
                record_type = text;
            }
        } else if column_index == SCHEMA_TBL_NAME_COLUMN {
            if let Some(text) = extract_text_from_serial_type(serial_type, page, pos) {
                tbl_name = text;
            }
        } else if column_index == SCHEMA_ROOTPAGE_COLUMN {
            if let Some(value) = extract_int_from_serial_type(serial_type, page, pos) {
                rootpage = value as u32;
            }
            break; // We have all the columns we need
        }

        pos += column_size;
        column_index += 1;
    }

    if !record_type.is_empty() && !tbl_name.is_empty() && rootpage > 0 {
        Some((record_type, tbl_name, rootpage))
    } else {
        None
    }
}

/// Find the rootpage for a given table name.
///
/// # Arguments
///
/// * `path` - Path to the SQLite database file
/// * `table_name` - Name of the table to find
///
/// # Returns
///
/// Returns the rootpage number if found, or an error if not found or read fails.
pub fn find_table_rootpage(path: &str, table_name: &str) -> Result<u32> {
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

    // Search for the table
    for cell_offset in cell_offsets {
        if let Some((record_type, tbl_name, rootpage)) = parse_schema_cell_with_rootpage(&page, cell_offset) {
            if record_type == "table" && tbl_name == table_name {
                return Ok(rootpage);
            }
        }
    }

    anyhow::bail!("Table '{}' not found", table_name)
}

/// Count the number of cells in a page.
///
/// # Arguments
///
/// * `page` - The page data
/// * `page_num` - The page number (1-indexed)
///
/// # Returns
///
/// Returns the number of cells in the page.
fn count_cells_in_page(page: &[u8], page_num: u32) -> usize {
    // For page 1, the header starts at offset 100
    // For other pages, the header starts at offset 0
    let header_offset = if page_num == 1 { PAGE1_HEADER_OFFSET } else { 0 };

    let num_cells = u16::from_be_bytes([
        page[header_offset + CELL_COUNT_OFFSET],
        page[header_offset + CELL_COUNT_OFFSET + 1],
    ]);

    num_cells as usize
}

/// Count the number of rows in a table.
///
/// # Arguments
///
/// * `path` - Path to the SQLite database file
/// * `table_name` - Name of the table to count rows in
///
/// # Returns
///
/// Returns the number of rows in the table, or an error if the table
/// doesn't exist or the database cannot be read.
pub fn count_table_rows(path: &str, table_name: &str) -> Result<usize> {
    // Find the rootpage for the table
    let rootpage = find_table_rootpage(path, table_name)?;

    // Open the database file and get page size
    let mut file = File::open(path).context("Failed to open database file")?;
    let page_size = read_page_size(&mut file)?;

    // Calculate the offset to the rootpage
    // Pages are 1-indexed, so page N starts at offset (N-1) * page_size
    let page_offset = (rootpage as u64 - 1) * page_size as u64;

    // Read the page
    let mut page = vec![0u8; page_size as usize];
    file.seek(std::io::SeekFrom::Start(page_offset))
        .context("Failed to seek to table page")?;
    file.read_exact(&mut page)
        .context("Failed to read table page")?;

    // Count cells in the page
    let row_count = count_cells_in_page(&page, rootpage);

    Ok(row_count)
}

/// Table information including rootpage and CREATE TABLE SQL.
pub struct TableInfo {
    pub rootpage: u32,
    pub sql: String,
}

/// Parse a schema cell to extract full table metadata.
fn parse_schema_cell_full(page: &[u8], cell_offset: usize) -> Option<(String, String, u32, String)> {
    let (_record_size, bytes_read) = read_varint(page, cell_offset);
    let mut pos = cell_offset + bytes_read;

    let (_, bytes_read) = read_varint(page, pos);
    pos += bytes_read;

    let record_start = pos;
    let (header_size, bytes_read) = read_varint(page, pos);
    pos += bytes_read;

    let mut serial_types = Vec::new();
    let header_end = record_start + header_size as usize;

    while pos < header_end {
        let (serial_type, bytes_read) = read_varint(page, pos);
        serial_types.push(serial_type);
        pos += bytes_read;
    }

    let mut record_type = String::new();
    let mut tbl_name = String::new();
    let mut rootpage: u32 = 0;
    let mut sql = String::new();

    for (column_index, &serial_type) in serial_types.iter().enumerate() {
        let column_size = get_column_size(serial_type);

        if column_index == SCHEMA_TYPE_COLUMN {
            if let Some(text) = extract_text_from_serial_type(serial_type, page, pos) {
                record_type = text;
            }
        } else if column_index == SCHEMA_TBL_NAME_COLUMN {
            if let Some(text) = extract_text_from_serial_type(serial_type, page, pos) {
                tbl_name = text;
            }
        } else if column_index == SCHEMA_ROOTPAGE_COLUMN {
            if let Some(value) = extract_int_from_serial_type(serial_type, page, pos) {
                rootpage = value as u32;
            }
        } else if column_index == SCHEMA_SQL_COLUMN {
            if let Some(text) = extract_text_from_serial_type(serial_type, page, pos) {
                sql = text;
            }
            break;
        }

        pos += column_size;
    }

    if !record_type.is_empty() && !tbl_name.is_empty() {
        Some((record_type, tbl_name, rootpage, sql))
    } else {
        None
    }
}

/// Get table info (rootpage and SQL) for a given table name.
pub fn get_table_info(path: &str, table_name: &str) -> Result<TableInfo> {
    let mut file = File::open(path).context("Failed to open database file")?;
    let page_size = read_page_size(&mut file)?;

    let mut page = vec![0u8; page_size as usize];
    file.seek(std::io::SeekFrom::Start(0))?;
    file.read_exact(&mut page)?;

    let cell_offsets = read_cell_offsets(&page);

    for cell_offset in cell_offsets {
        if let Some((record_type, tbl_name, rootpage, sql)) = parse_schema_cell_full(&page, cell_offset) {
            if record_type == "table" && tbl_name == table_name {
                return Ok(TableInfo { rootpage, sql });
            }
        }
    }

    anyhow::bail!("Table '{}' not found", table_name)
}

/// Parse column names from a CREATE TABLE statement.
/// Returns a list of column names in order.
pub fn parse_column_names(create_sql: &str) -> Vec<String> {
    // Find the content between the first ( and last )
    let start = match create_sql.find('(') {
        Some(idx) => idx + 1,
        None => return Vec::new(),
    };
    let end = match create_sql.rfind(')') {
        Some(idx) => idx,
        None => return Vec::new(),
    };

    let columns_part = &create_sql[start..end];

    let mut columns = Vec::new();
    for column_def in columns_part.split(',') {
        let column_def = column_def.trim();
        // The column name is the first word
        if let Some(name) = column_def.split_whitespace().next() {
            columns.push(name.to_string());
        }
    }

    columns
}

/// Read cell offsets from any page (not just page 1).
fn read_cell_offsets_from_page(page: &[u8], page_num: u32) -> Vec<usize> {
    let header_offset = if page_num == 1 { PAGE1_HEADER_OFFSET } else { 0 };

    let num_cells = u16::from_be_bytes([
        page[header_offset + CELL_COUNT_OFFSET],
        page[header_offset + CELL_COUNT_OFFSET + 1],
    ]);

    let cell_pointer_array_offset = header_offset + CELL_POINTER_ARRAY_OFFSET;
    let mut cell_offsets = Vec::new();

    for i in 0..num_cells {
        let offset_pos = cell_pointer_array_offset + (i as usize * 2);
        let cell_offset = u16::from_be_bytes([page[offset_pos], page[offset_pos + 1]]);
        cell_offsets.push(cell_offset as usize);
    }

    cell_offsets
}

/// Extract a column value from a table cell as a string.
fn extract_column_value(page: &[u8], cell_offset: usize, column_index: usize) -> Option<String> {
    let (_record_size, bytes_read) = read_varint(page, cell_offset);
    let mut pos = cell_offset + bytes_read;

    // Read rowid
    let (_, bytes_read) = read_varint(page, pos);
    pos += bytes_read;

    // Parse record header
    let record_start = pos;
    let (header_size, bytes_read) = read_varint(page, pos);
    pos += bytes_read;

    let mut serial_types = Vec::new();
    let header_end = record_start + header_size as usize;

    while pos < header_end {
        let (serial_type, bytes_read) = read_varint(page, pos);
        serial_types.push(serial_type);
        pos += bytes_read;
    }

    // Navigate to the target column
    for (idx, &serial_type) in serial_types.iter().enumerate() {
        if idx == column_index {
            // Try to extract as text
            if let Some(text) = extract_text_from_serial_type(serial_type, page, pos) {
                return Some(text);
            }
            // Try to extract as integer
            if let Some(int_val) = extract_int_from_serial_type(serial_type, page, pos) {
                return Some(int_val.to_string());
            }
            return None;
        }
        pos += get_column_size(serial_type);
    }

    None
}

/// Select a column from a table and return all values.
pub fn select_column(path: &str, table_name: &str, column_name: &str) -> Result<Vec<String>> {
    // Get table info
    let table_info = get_table_info(path, table_name)?;

    // Parse column names from CREATE TABLE
    let columns = parse_column_names(&table_info.sql);

    // Find the column index
    let column_index = columns
        .iter()
        .position(|c| c.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in table '{}'", column_name, table_name))?;

    // Open database and read the table's page
    let mut file = File::open(path).context("Failed to open database file")?;
    let page_size = read_page_size(&mut file)?;

    let page_offset = (table_info.rootpage as u64 - 1) * page_size as u64;
    let mut page = vec![0u8; page_size as usize];
    file.seek(std::io::SeekFrom::Start(page_offset))?;
    file.read_exact(&mut page)?;

    // Read all cells and extract column values
    let cell_offsets = read_cell_offsets_from_page(&page, table_info.rootpage);
    let mut values = Vec::new();

    for cell_offset in cell_offsets {
        if let Some(value) = extract_column_value(&page, cell_offset, column_index) {
            values.push(value);
        }
    }

    Ok(values)
}
