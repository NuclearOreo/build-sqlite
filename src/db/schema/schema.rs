//! SQLite schema table parsing.

use anyhow::Result;

use crate::db::database::Database;
use crate::db::page::{Page, Record, parse_index_cell};

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

    /// Check if this is an index.
    pub fn is_index(&self) -> bool {
        self.entry_type == "index"
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

/// Find an index that can be used for a column on a table.
pub fn find_index_for_column(
    db: &mut Database,
    table_name: &str,
    column_name: &str,
) -> Result<Option<SchemaEntry>> {
    let entries = read_schema(db)?;

    // Look for an index on this table and column
    // Index SQL looks like: CREATE INDEX idx_name on table_name (column_name)
    for entry in entries {
        if entry.is_index() {
            let sql_lower = entry.sql.to_lowercase();
            let expected_pattern = format!("on {} ({})", table_name, column_name).to_lowercase();
            if sql_lower.contains(&expected_pattern) {
                return Ok(Some(entry));
            }
        }
    }

    Ok(None)
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

    // Collect all records from the B-tree
    let mut records = Vec::new();
    traverse_btree_table(&mut db, table.rootpage, &mut records)?;

    Ok(records.len())
}

/// Select multiple columns from a table and return all rows.
pub fn select_columns(
    path: &str,
    table_name: &str,
    column_names: &[&str],
) -> Result<Vec<Vec<String>>> {
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
                .ok_or_else(|| {
                    anyhow::anyhow!("Column '{}' not found in table '{}'", col_name, table_name)
                })
        })
        .collect::<Result<Vec<_>>>()?;

    // Collect all records from the B-tree
    let mut record_data = Vec::new();
    traverse_btree_table(&mut db, table.rootpage, &mut record_data)?;

    // Parse all records and extract requested columns
    let rows: Vec<Vec<String>> = record_data
        .iter()
        .map(|(page_data, offset)| {
            let (record, _) = Record::parse(page_data, *offset);
            record.read_strings(&column_indices)
        })
        .collect();

    Ok(rows)
}

/// Select multiple columns from a table with a WHERE filter and return matching rows.
pub fn select_columns_with_filter(
    path: &str,
    table_name: &str,
    column_names: &[&str],
    where_clause: &str,
) -> Result<Vec<Vec<String>>> {
    let mut db = Database::open(path)?;
    let table = find_table(&mut db, table_name)?;

    // Parse WHERE clause to get column and value
    let (filter_column, filter_value) = parse_where_clause(where_clause)?;

    // Check if there's an index on the filter column
    let index_opt = find_index_for_column(&mut db, table_name, &filter_column)?;

    // Parse column names from CREATE TABLE
    let columns = parse_column_names(&table.sql);

    // Find column indices for SELECT columns
    // Special handling for "id" column which might be the rowid
    let column_indices: Vec<usize> = column_names
        .iter()
        .map(|col_name| {
            if col_name.eq_ignore_ascii_case("id") {
                // Check if this is an INTEGER PRIMARY KEY column (which aliases rowid)
                if table.sql.to_lowercase().contains("id integer primary key") {
                    return Ok(usize::MAX); // Special marker for rowid
                }
            }
            columns
                .iter()
                .position(|c| c.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| {
                    anyhow::anyhow!("Column '{}' not found in table '{}'", col_name, table_name)
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut rows = Vec::new();

    if let Some(index) = index_opt {
        // Use index to find matching rowids
        eprintln!(
            "Using index {} for column {}",
            index.tbl_name, filter_column
        );
        let matching_rowids = search_index_btree(&mut db, index.rootpage, &filter_value)?;

        // Fetch each record by rowid
        for rowid in matching_rowids {
            if let Some((page_data, offset)) = find_record_by_rowid(&mut db, table.rootpage, rowid)?
            {
                let (record, _) = Record::parse(&page_data, offset);
                rows.push(record.read_strings(&column_indices));
            }
        }
    } else {
        // No index available, do full table scan
        eprintln!(
            "No index found for column {}, doing full table scan",
            filter_column
        );

        // Find the index of the filter column
        let filter_column_index = columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(&filter_column))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Column '{}' not found in table '{}'",
                    filter_column,
                    table_name
                )
            })?;

        // Collect all records from the B-tree
        let mut record_data = Vec::new();
        traverse_btree_table(&mut db, table.rootpage, &mut record_data)?;

        // Filter and extract matching records
        for (page_data, offset) in record_data {
            let (record, _) = Record::parse(&page_data, offset);

            // Check if this row matches the filter
            if let Some(value) = record.read_string(filter_column_index) {
                if value == filter_value {
                    // This row matches - extract the requested columns
                    rows.push(record.read_strings(&column_indices));
                }
            }
        }
    }

    Ok(rows)
}

/// Parse a simple WHERE clause of the form "column = 'value'"
fn parse_where_clause(where_clause: &str) -> Result<(String, String)> {
    // Simple parsing for "column = 'value'" pattern
    let parts: Vec<&str> = where_clause.split('=').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid WHERE clause format. Expected: column = 'value'");
    }

    let column = parts[0].trim();
    let value_part = parts[1].trim();

    // Remove quotes from value if present
    let value = if value_part.starts_with('\'') && value_part.ends_with('\'') {
        &value_part[1..value_part.len() - 1]
    } else {
        value_part
    };

    Ok((column.to_string(), value.to_string()))
}

/// Traverse a B-tree starting from the given page and collect all leaf records.
/// Search an index B-tree for matching values and return rowids.
fn search_index_btree(db: &mut Database, page_num: u32, search_value: &str) -> Result<Vec<i64>> {
    let page_data = db.read_page(page_num)?;
    let page = Page::new(page_data, page_num);

    let mut rowids = Vec::new();

    if page.is_leaf() {
        // This is a leaf page, check each cell
        let cells = page.cell_offsets();
        if !cells.is_empty() {
            // Show first few values for debugging
            let first_cell = parse_index_cell(page.data(), cells[0]);
            let last_cell = parse_index_cell(page.data(), cells[cells.len() - 1]);
            eprintln!(
                "Page {} range: '{}' to '{}'",
                page_num,
                first_cell.values.get(0).unwrap_or(&String::new()),
                last_cell.values.get(0).unwrap_or(&String::new())
            );
        }

        for offset in cells {
            let cell = parse_index_cell(page.data(), offset);
            // For now, assume single-column index
            if !cell.values.is_empty() && cell.values[0] == search_value {
                eprintln!(
                    "Found match: rowid={}, value='{}'",
                    cell.rowid, cell.values[0]
                );
                rowids.push(cell.rowid);
            }
        }
        eprintln!(
            "Searched leaf page {}, found {} matches",
            page_num,
            rowids.len()
        );
    } else {
        // This is an interior page, we need to find all matching pages
        // Since the same value can appear in multiple leaf pages, we need to check
        // multiple children
        eprintln!("Searching interior page {}", page_num);
        let cells = page.cell_offsets();
        let mut children_to_search = Vec::new();

        // Find all children that might contain our search value
        for (i, offset) in cells.iter().enumerate() {
            match page.parse_interior_index_cell(*offset) {
                Ok((left_child, key)) => {
                    // The left child contains all values < key
                    // So if our search_value <= key, we need to search left child
                    if search_value <= &key {
                        children_to_search.push(left_child);
                        // If search_value == key, we also need to search the next child
                        // because the value might span pages
                        if search_value == &key && i + 1 < cells.len() {
                            // The next cell's left child or rightmost if last cell
                            continue; // Will be handled by next iteration
                        }
                    }
                    // If this is the last cell and search_value >= key,
                    // we need to search rightmost
                    if i == cells.len() - 1 && search_value >= &key {
                        if let Some(rightmost) = page.rightmost_pointer() {
                            children_to_search.push(rightmost);
                        }
                    }
                }
                Err(_) => {
                    // If parsing fails, search this child to be safe
                    let (left_child, _) = page.parse_interior_cell(*offset);
                    if left_child != 0 {
                        children_to_search.push(left_child);
                    }
                }
            }
        }

        // If no specific children selected, check rightmost
        if children_to_search.is_empty() {
            if let Some(rightmost) = page.rightmost_pointer() {
                children_to_search.push(rightmost);
            }
        }

        // Search all selected children
        for child_page in children_to_search {
            let mut child_rowids = search_index_btree(db, child_page, search_value)?;
            rowids.append(&mut child_rowids);
        }
    }

    Ok(rowids)
}

/// Find a record in a table B-tree by rowid.
fn find_record_by_rowid(
    db: &mut Database,
    page_num: u32,
    target_rowid: i64,
) -> Result<Option<(Vec<u8>, usize)>> {
    let page_data = db.read_page(page_num)?;
    let page = Page::new(page_data, page_num);

    if page.is_leaf() {
        // Search this leaf page for the rowid
        for offset in page.cell_offsets() {
            let (record, _) = Record::parse(page.data(), offset);
            if record.rowid == target_rowid {
                return Ok(Some((page.data().to_vec(), offset)));
            }
        }
    } else {
        // This is an interior page, determine which child to search
        let mut child_to_search = None;

        // Check each interior cell
        for offset in page.cell_offsets() {
            let (left_child, key) = page.parse_interior_cell(offset);
            if target_rowid <= key {
                child_to_search = Some(left_child);
                break;
            }
        }

        // If not found in any cell, search the rightmost child
        if child_to_search.is_none() {
            if let Some(rightmost) = page.rightmost_pointer() {
                child_to_search = Some(rightmost);
            }
        }

        // Search the appropriate child
        if let Some(child_page) = child_to_search {
            return find_record_by_rowid(db, child_page, target_rowid);
        }
    }

    Ok(None)
}

fn traverse_btree_table(
    db: &mut Database,
    page_num: u32,
    records: &mut Vec<(Vec<u8>, usize)>,
) -> Result<()> {
    let page_data = db.read_page(page_num)?;
    let page = Page::new(page_data, page_num);

    if page.is_leaf() {
        // This is a leaf page, collect all records
        for offset in page.cell_offsets() {
            records.push((page.data().to_vec(), offset));
        }
    } else {
        // This is an interior page, traverse child pages
        let mut child_pages = Vec::new();

        // Process each interior cell to get left child pointers
        for offset in page.cell_offsets() {
            let (left_child, _key) = page.parse_interior_cell(offset);
            if left_child == 0 {
                eprintln!(
                    "Warning: found zero page number in interior cell at page {}",
                    page_num
                );
                continue;
            }
            child_pages.push(left_child);
        }

        // Add the rightmost child
        if let Some(rightmost) = page.rightmost_pointer() {
            if rightmost == 0 {
                eprintln!(
                    "Warning: found zero page number in rightmost pointer at page {}",
                    page_num
                );
            } else {
                child_pages.push(rightmost);
            }
        }

        // Recursively traverse all child pages
        for child_page in child_pages {
            traverse_btree_table(db, child_page, records)?;
        }
    }

    Ok(())
}
