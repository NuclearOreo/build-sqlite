//! Record parsing utilities for SQLite database format.

/// Get the size in bytes of a column value based on its serial type code.
///
/// SQLite uses serial type codes to indicate the type and size of column values.
/// This function returns the number of bytes a value occupies based on its type.
///
/// # Arguments
///
/// * `serial_type` - The serial type code from the record header
///
/// # Returns
///
/// The size in bytes of the column value.
pub fn get_column_size(serial_type: u64) -> usize {
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
///
/// If the serial type represents a text string, this function extracts
/// the text from the specified position in the page data.
///
/// # Arguments
///
/// * `serial_type` - The serial type code from the record header
/// * `page` - The page data containing the text
/// * `pos` - The starting position of the text in the page
///
/// # Returns
///
/// Returns `Some(String)` if the serial type represents text, `None` otherwise.
pub fn extract_text_from_serial_type(serial_type: u64, page: &[u8], pos: usize) -> Option<String> {
    if serial_type >= 13 && serial_type % 2 == 1 {
        let text_size = ((serial_type - 13) / 2) as usize;
        let text_bytes = &page[pos..pos + text_size];
        Some(String::from_utf8_lossy(text_bytes).to_string())
    } else {
        None
    }
}

/// Extract integer value from page data based on serial type.
///
/// If the serial type represents an integer, this function extracts
/// the integer value from the specified position in the page data.
///
/// # Arguments
///
/// * `serial_type` - The serial type code from the record header
/// * `page` - The page data containing the integer
/// * `pos` - The starting position of the integer in the page
///
/// # Returns
///
/// Returns `Some(i64)` if the serial type represents an integer, `None` otherwise.
pub fn extract_int_from_serial_type(serial_type: u64, page: &[u8], pos: usize) -> Option<i64> {
    match serial_type {
        0 => Some(0), // NULL treated as 0
        1 => {
            // 8-bit signed integer
            let value = page[pos] as i8;
            Some(value as i64)
        }
        2 => {
            // 16-bit signed big-endian integer
            let value = i16::from_be_bytes([page[pos], page[pos + 1]]);
            Some(value as i64)
        }
        3 => {
            // 24-bit signed big-endian integer
            let value = i32::from_be_bytes([0, page[pos], page[pos + 1], page[pos + 2]]);
            // Sign extend if negative
            let value = if page[pos] & 0x80 != 0 {
                value | 0xFF000000u32 as i32
            } else {
                value
            };
            Some(value as i64)
        }
        4 => {
            // 32-bit signed big-endian integer
            let value = i32::from_be_bytes([page[pos], page[pos + 1], page[pos + 2], page[pos + 3]]);
            Some(value as i64)
        }
        5 => {
            // 48-bit signed big-endian integer
            let bytes = [0, 0, page[pos], page[pos + 1], page[pos + 2], page[pos + 3], page[pos + 4], page[pos + 5]];
            let value = i64::from_be_bytes(bytes);
            // Sign extend if negative
            let value = if page[pos] & 0x80 != 0 {
                value | 0xFFFF000000000000u64 as i64
            } else {
                value
            };
            Some(value)
        }
        6 => {
            // 64-bit signed big-endian integer
            let value = i64::from_be_bytes([
                page[pos], page[pos + 1], page[pos + 2], page[pos + 3],
                page[pos + 4], page[pos + 5], page[pos + 6], page[pos + 7]
            ]);
            Some(value)
        }
        8 => Some(0), // Integer constant 0
        9 => Some(1), // Integer constant 1
        _ => None,
    }
}
