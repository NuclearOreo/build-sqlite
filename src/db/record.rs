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
