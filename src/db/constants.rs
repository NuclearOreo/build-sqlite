//! Constants used throughout the SQLite database parsing.

/// Offset of page size in database header.
pub const PAGE_SIZE_OFFSET: usize = 16;

/// Size of the database header (on page 1).
pub const PAGE1_HEADER_OFFSET: usize = 100;

/// Offset of cell count in page header.
pub const CELL_COUNT_OFFSET: usize = 3;

/// Offset of cell pointer array in page header.
pub const CELL_POINTER_ARRAY_OFFSET: usize = 8;
