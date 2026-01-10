//! Record parsing utilities for SQLite database format.

use crate::db::varint::read_varint;

/// A parsed SQLite record from a table cell.
pub struct Record {
    serial_types: Vec<u64>,
    column_offsets: Vec<usize>,
    data: Vec<u8>,
    pub rowid: i64,
}

/// An index cell contains the indexed value(s) and rowid
pub struct IndexCell {
    pub values: Vec<String>,
    pub rowid: i64,
}

impl Record {
    /// Parse a record from a cell in a page.
    /// Returns the Record and the number of bytes consumed.
    pub fn parse(page: &[u8], cell_offset: usize) -> (Self, usize) {
        let start = cell_offset;
        let mut pos = cell_offset;

        // Read record size (varint)
        let (_record_size, bytes_read) = read_varint(page, pos);
        pos += bytes_read;

        // Read rowid (varint)
        let (rowid, bytes_read) = read_varint(page, pos);
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

        // Calculate column offsets
        let mut column_offsets = Vec::new();
        let mut offset = pos;
        for &serial_type in &serial_types {
            column_offsets.push(offset);
            offset += get_column_size(serial_type);
        }

        // Copy data portion
        let data = page[pos..offset].to_vec();

        // Adjust offsets to be relative to data start
        let data_start = pos;
        let column_offsets: Vec<usize> = column_offsets.iter().map(|&o| o - data_start).collect();

        (
            Self {
                serial_types,
                column_offsets,
                data,
                rowid: rowid as i64,
            },
            offset - start,
        )
    }

    /// Get the number of columns in this record.
    #[allow(dead_code)]
    pub fn column_count(&self) -> usize {
        self.serial_types.len()
    }

    /// Read a column value as a string.
    /// Special case: column_index of usize::MAX means read the rowid
    pub fn read_string(&self, column_index: usize) -> Option<String> {
        // Special case for rowid
        if column_index == usize::MAX {
            return Some(self.rowid.to_string());
        }

        if column_index >= self.serial_types.len() {
            return None;
        }

        let serial_type = self.serial_types[column_index];
        let offset = self.column_offsets[column_index];

        // Try text first
        if let Some(text) = extract_text_from_serial_type(serial_type, &self.data, offset) {
            return Some(text);
        }

        // Try integer
        if let Some(int_val) = extract_int_from_serial_type(serial_type, &self.data, offset) {
            return Some(int_val.to_string());
        }

        // NULL or unknown
        None
    }

    /// Read a column value as an integer.
    pub fn read_int(&self, column_index: usize) -> Option<i64> {
        if column_index >= self.serial_types.len() {
            return None;
        }

        let serial_type = self.serial_types[column_index];
        let offset = self.column_offsets[column_index];

        extract_int_from_serial_type(serial_type, &self.data, offset)
    }

    /// Read multiple columns as strings.
    pub fn read_strings(&self, column_indices: &[usize]) -> Vec<String> {
        column_indices
            .iter()
            .map(|&idx| self.read_string(idx).unwrap_or_default())
            .collect()
    }
}

/// Get the size in bytes of a column value based on its serial type code.
pub fn get_column_size(serial_type: u64) -> usize {
    match serial_type {
        0 => 0,                                                // NULL
        1 => 1,                                                // 8-bit integer
        2 => 2,                                                // 16-bit integer
        3 => 3,                                                // 24-bit integer
        4 => 4,                                                // 32-bit integer
        5 => 6,                                                // 48-bit integer
        6 => 8,                                                // 64-bit integer
        7 => 8,                                                // IEEE 754 float
        8 => 0,                                                // Integer constant 0
        9 => 0,                                                // Integer constant 1
        10 | 11 => 0,                                          // Reserved
        n if n >= 12 && n % 2 == 0 => ((n - 12) / 2) as usize, // BLOB
        n if n >= 13 && n % 2 == 1 => ((n - 13) / 2) as usize, // Text string
        _ => 0,
    }
}

/// Extract text string from data based on serial type.
fn extract_text_from_serial_type(serial_type: u64, data: &[u8], pos: usize) -> Option<String> {
    if serial_type >= 13 && serial_type % 2 == 1 {
        let text_size = ((serial_type - 13) / 2) as usize;
        if pos + text_size > data.len() {
            return None;
        }
        let text_bytes = &data[pos..pos + text_size];
        Some(String::from_utf8_lossy(text_bytes).to_string())
    } else {
        None
    }
}

/// Parse an index leaf cell.
/// For index B-trees, the cell format is: payload_size(varint) + payload
/// The payload contains: record_header + indexed_columns + rowid
pub fn parse_index_cell(page: &[u8], cell_offset: usize) -> IndexCell {
    let mut pos = cell_offset;

    // Read payload size
    let (_payload_size, bytes_read) = read_varint(page, pos);
    pos += bytes_read;

    // Now parse the record header
    let record_start = pos;
    let (header_size, bytes_read) = read_varint(page, pos);
    pos += bytes_read;

    let header_end = record_start + header_size as usize;
    let mut serial_types = Vec::new();

    // Read all serial types from the header
    while pos < header_end && pos < page.len() {
        let (serial_type, bytes_read) = read_varint(page, pos);
        serial_types.push(serial_type);
        pos += bytes_read;
    }

    // The last serial type is for the rowid, everything else is indexed columns
    let rowid_serial_type = serial_types.pop();

    // Read the indexed column values
    let mut values = Vec::new();
    for &serial_type in &serial_types {
        let size = get_column_size(serial_type);

        // Make sure we have enough data
        if pos + size > page.len() {
            break;
        }

        if let Some(text) = extract_text_from_serial_type(serial_type, page, pos) {
            values.push(text);
        } else if let Some(int) = extract_int_from_serial_type(serial_type, page, pos) {
            values.push(int.to_string());
        } else if serial_type == 0 {
            values.push(String::new()); // NULL
        } else {
            values.push(String::new());
        }
        pos += size;
    }

    // Read the rowid
    let rowid = if let Some(serial_type) = rowid_serial_type {
        let _size = get_column_size(serial_type);
        if let Some(int) = extract_int_from_serial_type(serial_type, page, pos) {
            int
        } else {
            0
        }
    } else {
        0
    };

    IndexCell { values, rowid }
}

/// Extract integer value from data based on serial type.
fn extract_int_from_serial_type(serial_type: u64, data: &[u8], pos: usize) -> Option<i64> {
    match serial_type {
        0 => Some(0), // NULL treated as 0
        1 => {
            if pos >= data.len() {
                return None;
            }
            let value = data[pos] as i8;
            Some(value as i64)
        }
        2 => {
            if pos + 2 > data.len() {
                return None;
            }
            let value = i16::from_be_bytes([data[pos], data[pos + 1]]);
            Some(value as i64)
        }
        3 => {
            if pos + 3 > data.len() {
                return None;
            }
            let value = i32::from_be_bytes([0, data[pos], data[pos + 1], data[pos + 2]]);
            let value = if data[pos] & 0x80 != 0 {
                value | 0xFF000000u32 as i32
            } else {
                value
            };
            Some(value as i64)
        }
        4 => {
            if pos + 4 > data.len() {
                return None;
            }
            let value =
                i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            Some(value as i64)
        }
        5 => {
            if pos + 6 > data.len() {
                return None;
            }
            let bytes = [
                0,
                0,
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
            ];
            let value = i64::from_be_bytes(bytes);
            let value = if data[pos] & 0x80 != 0 {
                value | 0xFFFF000000000000u64 as i64
            } else {
                value
            };
            Some(value)
        }
        6 => {
            if pos + 8 > data.len() {
                return None;
            }
            let value = i64::from_be_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]);
            Some(value)
        }
        8 => Some(0),
        9 => Some(1),
        _ => None,
    }
}
