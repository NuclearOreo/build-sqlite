//! Variable-length integer (varint) operations for SQLite format.

const VARINT_MAX_BYTES: usize = 9;
const VARINT_CONTINUATION_BIT: u8 = 0x80;
const VARINT_DATA_MASK: u8 = 0x7F;

/// Read a varint from a byte slice starting at the given position.
///
/// Varints are a variable-length encoding for integers used by SQLite.
/// They use 1-9 bytes depending on the magnitude of the value.
///
/// # Arguments
///
/// * `data` - The byte slice containing the varint
/// * `pos` - The starting position of the varint in the slice
///
/// # Returns
///
/// Returns a tuple of (value, number of bytes read).
pub fn read_varint(data: &[u8], pos: usize) -> (u64, usize) {
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
