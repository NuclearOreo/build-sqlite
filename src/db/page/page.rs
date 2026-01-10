//! Page parsing utilities for SQLite database format.

use crate::db::constants::{CELL_COUNT_OFFSET, PAGE1_HEADER_OFFSET};
use crate::db::varint::read_varint;

/// Page type constants from SQLite documentation
const INTERIOR_INDEX_BTREE_PAGE: u8 = 0x02;
const INTERIOR_TABLE_BTREE_PAGE: u8 = 0x05;
const LEAF_INDEX_BTREE_PAGE: u8 = 0x0a;
const LEAF_TABLE_BTREE_PAGE: u8 = 0x0d;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

/// A SQLite database page.
pub struct Page {
    data: Vec<u8>,
    header_offset: usize,
    page_type: PageType,
}

impl Page {
    /// Create a new Page from raw data.
    pub fn new(data: Vec<u8>, page_num: u32) -> Self {
        let header_offset = if page_num == 1 {
            PAGE1_HEADER_OFFSET
        } else {
            0
        };

        // Read page type from first byte of page header
        let page_type_byte = data[header_offset];
        let page_type = match page_type_byte {
            INTERIOR_INDEX_BTREE_PAGE => PageType::InteriorIndex,
            INTERIOR_TABLE_BTREE_PAGE => PageType::InteriorTable,
            LEAF_INDEX_BTREE_PAGE => PageType::LeafIndex,
            LEAF_TABLE_BTREE_PAGE => PageType::LeafTable,
            _ => panic!("Unknown page type: {}", page_type_byte),
        };

        Self {
            data,
            header_offset,
            page_type,
        }
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
        // Interior pages have a 12-byte header, leaf pages have an 8-byte header
        let header_size = if self.is_interior() { 12 } else { 8 };
        let array_offset = self.header_offset + header_size;

        (0..num_cells)
            .map(|i| {
                let pos = array_offset + i * 2;
                u16::from_be_bytes([self.data[pos], self.data[pos + 1]]) as usize
            })
            .collect()
    }

    /// Check if this is a leaf page.
    pub fn is_leaf(&self) -> bool {
        matches!(self.page_type, PageType::LeafTable | PageType::LeafIndex)
    }

    /// Check if this is an interior page.
    pub fn is_interior(&self) -> bool {
        matches!(
            self.page_type,
            PageType::InteriorTable | PageType::InteriorIndex
        )
    }

    /// Get the rightmost pointer for interior pages.
    /// This points to the page containing all keys greater than any key in this page.
    pub fn rightmost_pointer(&self) -> Option<u32> {
        if self.is_interior() {
            // For interior pages, the rightmost pointer is at offset 8 in the page header
            // This is after the standard 8-byte header (1 byte flags + 2 bytes freeblock + 2 bytes cells + 1 byte fragments + 2 bytes cell content area)
            let offset = self.header_offset + 8;
            Some(u32::from_be_bytes([
                self.data[offset],
                self.data[offset + 1],
                self.data[offset + 2],
                self.data[offset + 3],
            ]))
        } else {
            None
        }
    }

    /// Get the page type.
    #[allow(dead_code)]
    pub fn page_type(&self) -> PageType {
        self.page_type
    }

    /// Parse a cell from an interior table page and return the left child pointer.
    /// Interior cells contain: left_child_pointer (4 bytes) + key (varint)
    pub fn parse_interior_cell(&self, cell_offset: usize) -> (u32, i64) {
        // Read 4-byte page number of left child
        let left_child = u32::from_be_bytes([
            self.data[cell_offset],
            self.data[cell_offset + 1],
            self.data[cell_offset + 2],
            self.data[cell_offset + 3],
        ]);

        // Read the key (rowid) as a varint
        let (key, _) = read_varint(&self.data, cell_offset + 4);

        (left_child, key as i64)
    }

    /// Parse a cell from an interior index page.
    /// Returns (left_child_page, key_value)
    pub fn parse_interior_index_cell(&self, cell_offset: usize) -> Result<(u32, String), String> {
        let mut pos = cell_offset;

        // Read 4-byte page number of left child
        let left_child = u32::from_be_bytes([
            self.data[pos],
            self.data[pos + 1],
            self.data[pos + 2],
            self.data[pos + 3],
        ]);
        pos += 4;

        // Read payload size
        let (_payload_size, bytes_read) = read_varint(&self.data, pos);
        pos += bytes_read;

        // Parse record header
        let record_start = pos;
        let (header_size, bytes_read) = read_varint(&self.data, pos);
        pos += bytes_read;

        // Read first serial type (for the indexed column)
        let (serial_type, _) = read_varint(&self.data, pos);

        // Skip to data section
        pos = record_start + header_size as usize;

        // Extract the key value (first column)
        let _size = crate::db::page::record::get_column_size(serial_type);
        if serial_type >= 13 && serial_type % 2 == 1 {
            // Text
            let text_size = ((serial_type - 13) / 2) as usize;
            if pos + text_size <= self.data.len() {
                let key = String::from_utf8_lossy(&self.data[pos..pos + text_size]).to_string();
                Ok((left_child, key))
            } else {
                Err("Not enough data for key".to_string())
            }
        } else {
            Err("Key is not text".to_string())
        }
    }
}
