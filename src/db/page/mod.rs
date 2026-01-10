//! Page and record parsing for SQLite database format.

mod page;
mod record;

pub use page::Page;
pub use record::{Record, parse_index_cell};
