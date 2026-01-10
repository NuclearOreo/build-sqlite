//! SQLite schema table parsing.

use anyhow::Result;

use crate::db::database::Database;
use crate::db::page::{Page, Record};

/// Column indices in the sqlite_schema table.
const SCHEMA_TYPE_COLUMN: usize = 0;
const SCHEMA_TBL_NAME_COLUMN: usize = 2;
const SCHEMA_ROOTPAGE_COLUMN: usize = 3;
const SCHEMA_SQL_COLUMN: usize = 4;

/// An entry from the sqlite_schema table.
#[derive(Debug, Clone)]
pub struct SchemaEntry {
    pub entry_type: String,
    pub tbl_name: String,
    pub rootpage: u32,
    pub sql: String,
}

impl SchemaEntry {
    /// Parse a schema entry from a record.
    fn from_record(record: &Record) -> Option<Self> {
        let entry_type = record.read_string(SCHEMA_TYPE_COLUMN)?;
        let tbl_name = record.read_string(SCHEMA_TBL_NAME_COLUMN)?;

        Some(Self {
            entry_type,
            tbl_name,
            rootpage: record.read_int(SCHEMA_ROOTPAGE_COLUMN).unwrap_or(0) as u32,
            sql: record.read_string(SCHEMA_SQL_COLUMN).unwrap_or_default(),
        })
    }

    /// Check if this is a user table (not an internal sqlite_ table).
    pub fn is_user_table(&self) -> bool {
        self.entry_type == "table" && !self.tbl_name.starts_with("sqlite_")
    }
}

/// Read all schema entries from the database.
pub fn read_schema(db: &mut Database) -> Result<Vec<SchemaEntry>> {
    let page_data = db.read_page(1)?;
    let page = Page::new(page_data, 1);

    let mut entries = Vec::new();
    for cell_offset in page.cell_offsets() {
        let (record, _) = Record::parse(page.data(), cell_offset);
        if let Some(entry) = SchemaEntry::from_record(&record) {
            entries.push(entry);
        }
    }

    Ok(entries)
}

/// Read user table names from the database.
pub fn read_table_names(path: &str) -> Result<Vec<String>> {
    let mut db = Database::open(path)?;
    let entries = read_schema(&mut db)?;

    Ok(entries
        .into_iter()
        .filter(|e| e.is_user_table())
        .map(|e| e.tbl_name)
        .collect())
}

/// Find a table's schema entry by name.
pub fn find_table(db: &mut Database, table_name: &str) -> Result<SchemaEntry> {
    let entries = read_schema(db)?;

    entries
        .into_iter()
        .find(|e| e.entry_type == "table" && e.tbl_name == table_name)
        .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", table_name))
}

/// Parse column names from a CREATE TABLE statement.
pub fn parse_column_names(create_sql: &str) -> Vec<String> {
    let start = match create_sql.find('(') {
        Some(idx) => idx + 1,
        None => return Vec::new(),
    };
    let end = match create_sql.rfind(')') {
        Some(idx) => idx,
        None => return Vec::new(),
    };

    create_sql[start..end]
        .split(',')
        .filter_map(|col_def| col_def.trim().split_whitespace().next())
        .map(String::from)
        .collect()
}

/// Count the number of rows in a table.
pub fn count_table_rows(path: &str, table_name: &str) -> Result<usize> {
    let mut db = Database::open(path)?;
    let table = find_table(&mut db, table_name)?;

    let page_data = db.read_page(table.rootpage)?;
    let page = Page::new(page_data, table.rootpage);

    Ok(page.cell_count())
}

/// Select multiple columns from a table and return all rows.
pub fn select_columns(path: &str, table_name: &str, column_names: &[&str]) -> Result<Vec<Vec<String>>> {
    let mut db = Database::open(path)?;
    let table = find_table(&mut db, table_name)?;

    // Parse column names from CREATE TABLE
    let columns = parse_column_names(&table.sql);

    // Find column indices
    let column_indices: Vec<usize> = column_names
        .iter()
        .map(|col_name| {
            columns
                .iter()
                .position(|c| c.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in table '{}'", col_name, table_name))
        })
        .collect::<Result<Vec<_>>>()?;

    // Read table page and extract rows
    let page_data = db.read_page(table.rootpage)?;
    let page = Page::new(page_data, table.rootpage);

    let rows: Vec<Vec<String>> = page
        .cell_offsets()
        .iter()
        .map(|&offset| {
            let (record, _) = Record::parse(page.data(), offset);
            record.read_strings(&column_indices)
        })
        .collect();

    Ok(rows)
}
