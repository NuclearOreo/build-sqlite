use anyhow::Result;
use std::fs::File;
use std::io::prelude::*;

pub fn read_db_info(path: &str) -> Result<(u16, u16)> {
    let mut file = File::open(path)?;
    let mut header = [0; 100];
    let mut page_header = [0; 112];
    file.read_exact(&mut header)?;
    file.read_exact(&mut page_header)?;
    // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
    let page_size = u16::from_be_bytes([header[16], header[17]]);
    let number_of_tables = u16::from_be_bytes([page_header[3], page_header[4]]);
    Ok((page_size, number_of_tables))
}

/// Read a varint from a byte slice starting at the given position.
/// Returns (value, number of bytes read).
fn read_varint(data: &[u8], pos: usize) -> (u64, usize) {
    let mut value: u64 = 0;
    let mut bytes_read = 0;

    for i in 0..9 {
        let byte = data[pos + i];
        bytes_read += 1;

        if i == 8 {
            // 9th byte uses all 8 bits
            value = (value << 8) | byte as u64;
            break;
        } else {
            // Use lower 7 bits
            value = (value << 7) | (byte & 0x7F) as u64;

            // If high bit is 0, we're done
            if byte & 0x80 == 0 {
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

/// Read table names from the sqlite_schema table
pub fn read_table_names(path: &str) -> Result<Vec<String>> {
    let mut file = File::open(path)?;

    // Read the database header to get the page size
    let mut header = [0; 100];
    file.read_exact(&mut header)?;
    let page_size = u16::from_be_bytes([header[16], header[17]]);

    // Read the first page (sqlite_schema page)
    let mut page = vec![0u8; page_size as usize];
    file.seek(std::io::SeekFrom::Start(0))?;
    file.read_exact(&mut page)?;

    // Parse the page header (starts at offset 100 for page 1)
    let page_header_offset = 100;
    let num_cells = u16::from_be_bytes([
        page[page_header_offset + 3],
        page[page_header_offset + 4],
    ]);

    // Read cell pointers (start after the 12-byte page header)
    let cell_pointer_array_offset = page_header_offset + 8;
    let mut cell_offsets = Vec::new();

    for i in 0..num_cells {
        let offset_pos = cell_pointer_array_offset + (i as usize * 2);
        let cell_offset = u16::from_be_bytes([page[offset_pos], page[offset_pos + 1]]);
        cell_offsets.push(cell_offset as usize);
    }

    // Parse each cell to extract table names
    let mut table_names = Vec::new();

    for cell_offset in cell_offsets {
        // Read the record size (varint)
        let (_record_size, bytes_read) = read_varint(&page, cell_offset);
        let mut pos = cell_offset + bytes_read;

        // Read the rowid (varint) - we can ignore this
        let (_, bytes_read) = read_varint(&page, pos);
        pos += bytes_read;

        // Parse the record header
        let record_start = pos;
        let (header_size, bytes_read) = read_varint(&page, pos);
        pos += bytes_read;

        // Read serial type codes
        let mut serial_types = Vec::new();
        let header_end = record_start + header_size as usize;

        while pos < header_end {
            let (serial_type, bytes_read) = read_varint(&page, pos);
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

            if column_index == 0 {
                // This is the type column
                if serial_type >= 13 && serial_type % 2 == 1 {
                    let text_size = ((serial_type - 13) / 2) as usize;
                    let text_bytes = &page[pos..pos + text_size];
                    record_type = String::from_utf8_lossy(text_bytes).to_string();
                }
            } else if column_index == 2 {
                // This is the tbl_name column
                if serial_type >= 13 && serial_type % 2 == 1 {
                    let text_size = ((serial_type - 13) / 2) as usize;
                    let text_bytes = &page[pos..pos + text_size];
                    tbl_name = String::from_utf8_lossy(text_bytes).to_string();
                }
                break;
            }

            pos += column_size;
            column_index += 1;
        }

        // Only include user tables (type = "table" and not internal tables)
        if record_type == "table" && !tbl_name.starts_with("sqlite_") {
            table_names.push(tbl_name);
        }
    }

    Ok(table_names)
}
