//! Constants used throughout the SQLite database parsing.

// Database header constants
pub const DB_HEADER_SIZE: usize = 100;
pub const PAGE_HEADER_SIZE: usize = 112;
pub const PAGE_SIZE_OFFSET: usize = 16;

// Page header constants (for page 1, header starts at offset 100)
pub const PAGE1_HEADER_OFFSET: usize = 100;
pub const CELL_COUNT_OFFSET: usize = 3;
pub const CELL_POINTER_ARRAY_OFFSET: usize = 8;

// SQLite schema column indices
pub const SCHEMA_TYPE_COLUMN: usize = 0;
pub const SCHEMA_TBL_NAME_COLUMN: usize = 2;

// Varint constants
pub const VARINT_MAX_BYTES: usize = 9;
pub const VARINT_CONTINUATION_BIT: u8 = 0x80;
pub const VARINT_DATA_MASK: u8 = 0x7F;
