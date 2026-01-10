//! SQLite database file parsing and manipulation.

mod constants;
mod database;
mod header;
mod varint;

pub mod page;
pub mod schema;

// Re-export public API
pub use header::read_db_info;
pub use schema::{count_table_rows, read_table_names, select_columns};
