//! SQLite database file parsing and manipulation.
//!
//! This module provides functionality for reading and parsing SQLite database files,
//! including header information, schema tables, and database records.

mod constants;
mod header;
mod record;
mod schema;
mod varint;

// Re-export public API
pub use header::read_db_info;
pub use schema::read_table_names;
