//! Database header parsing for SQLite format.

use anyhow::Result;

use super::database::Database;
use super::page::Page;

/// Read database information including page size and number of tables.
pub fn read_db_info(path: &str) -> Result<(u16, u16)> {
    let mut db = Database::open(path)?;
    let page_size = db.page_size as u16;

    let page_data = db.read_page(1)?;
    let page = Page::new(page_data, 1);
    let number_of_tables = page.cell_count() as u16;

    Ok((page_size, number_of_tables))
}
