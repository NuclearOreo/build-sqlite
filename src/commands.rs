use crate::db;
use anyhow::{Context, Result};

/// Displays database information including page size and number of tables.
///
/// Reads the SQLite database header and prints the database page size
/// and the total number of tables in the database.
///
/// # Arguments
///
/// * `path` - Path to the SQLite database file
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if the file cannot be opened
/// or the database format is invalid.
///
/// # Examples
///
/// ```no_run
/// dbinfo("sample.db")?;
/// // Output:
/// // database page size: 4096
/// // number of tables: 3
/// ```
pub fn dbinfo(path: &str) -> Result<()> {
    let (page_size, number_of_tables) = db::read_db_info(path)
        .context("Failed to read database info")?;
    println!("database page size: {}", page_size);
    println!("number of tables: {}", number_of_tables);
    Ok(())
}

/// Displays all user-defined table names in the database.
///
/// Reads the sqlite_schema table and prints all user-defined table names
/// (excluding internal SQLite tables like sqlite_sequence) separated by spaces.
///
/// # Arguments
///
/// * `path` - Path to the SQLite database file
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if the file cannot be opened,
/// the database format is invalid, or the schema cannot be parsed.
///
/// # Examples
///
/// ```no_run
/// table("sample.db")?;
/// // Output:
/// // users posts comments
/// ```
pub fn table(path: &str) -> Result<()> {
    let table_names = db::read_table_names(path)
        .context("Failed to read table names")?;
    println!("{}", table_names.join(" "));
    Ok(())
}
