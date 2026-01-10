//! Page parsing utilities for SQLite database format.

use crate::db::constants::{CELL_COUNT_OFFSET, CELL_POINTER_ARRAY_OFFSET, PAGE1_HEADER_OFFSET};

/// A SQLite database page.
pub struct Page {
    data: Vec<u8>,
    header_offset: usize,
}

impl Page {
    /// Create a new Page from raw data.
    pub fn new(data: Vec<u8>, page_num: u32) -> Self {
        let header_offset = if page_num == 1 {
            PAGE1_HEADER_OFFSET
        } else {
            0
        };
        Self { data, header_offset }
    }

    /// Get the raw page data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get the number of cells in this page.
    pub fn cell_count(&self) -> usize {
        let offset = self.header_offset + CELL_COUNT_OFFSET;
        u16::from_be_bytes([self.data[offset], self.data[offset + 1]]) as usize
    }

    /// Get cell offsets from the cell pointer array.
    pub fn cell_offsets(&self) -> Vec<usize> {
        let num_cells = self.cell_count();
        let array_offset = self.header_offset + CELL_POINTER_ARRAY_OFFSET;

        (0..num_cells)
            .map(|i| {
                let pos = array_offset + i * 2;
                u16::from_be_bytes([self.data[pos], self.data[pos + 1]]) as usize
            })
            .collect()
    }
}
