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
    let (page_size, number_of_tables) =
        db::read_db_info(path).context("Failed to read database info")?;
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
    let table_names = db::read_table_names(path).context("Failed to read table names")?;
    println!("{}", table_names.join(" "));
    Ok(())
}

/// Execute a SQL query.
///
/// Supports SELECT COUNT(*) and SELECT column FROM table queries.
///
/// # Arguments
///
/// * `path` - Path to the SQLite database file
/// * `query` - The SQL query to execute
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if the query fails.
///
/// # Examples
///
/// ```no_run
/// sql("sample.db", "SELECT COUNT(*) FROM apples")?;
/// // Output:
/// // 4
///
/// sql("sample.db", "SELECT name FROM apples")?;
/// // Output:
/// // Granny Smith
/// // Fuji
/// // ...
/// ```
pub fn sql(path: &str, query: &str) -> Result<()> {
    let parts: Vec<&str> = query.split_whitespace().collect();

    if parts.is_empty() {
        anyhow::bail!("Empty query");
    }

    let upper_query = query.to_uppercase();

    // Check if this is a COUNT query
    if upper_query.contains("COUNT") {
        let table_name = parts.last().unwrap();
        let count = db::count_table_rows(path, table_name).context("Failed to count table rows")?;
        println!("{}", count);
        return Ok(());
    }

    // Parse SELECT columns FROM table [WHERE condition]
    // Expected format: SELECT <column1>, <column2>, ... FROM <table> [WHERE <column> = <value>]
    if parts.len() >= 4 && parts[0].eq_ignore_ascii_case("SELECT") {
        // Find FROM position in the original query (case-insensitive)
        let upper_query_for_from = query.to_uppercase();
        let from_pos_in_query = upper_query_for_from.find(" FROM ");
        if let Some(from_idx) = from_pos_in_query {
            // Extract columns part (between SELECT and FROM)
            let select_len = "SELECT ".len();
            let columns_part = &query[select_len..from_idx];

            // Parse column names (comma-separated, trim whitespace)
            let column_names: Vec<&str> = columns_part.split(',').map(|s| s.trim()).collect();

            // Extract table name and optional WHERE clause
            let after_from = &query[from_idx + " FROM ".len()..];

            // Check if there's a WHERE clause
            let upper_after_from = after_from.to_uppercase();
            let where_pos = upper_after_from.find(" WHERE ");

            let (table_name, where_clause) = if let Some(where_idx) = where_pos {
                let table_part = &after_from[..where_idx];
                let table_name = table_part
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Missing table name after FROM"))?;
                let where_part = &after_from[where_idx + " WHERE ".len()..];
                (table_name, Some(where_part))
            } else {
                let table_name = after_from
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Missing table name after FROM"))?;
                (table_name, None)
            };

            let rows = if let Some(where_clause) = where_clause {
                db::select_columns_with_filter(path, table_name, &column_names, where_clause)
                    .context("Failed to select columns with filter")?
            } else {
                db::select_columns(path, table_name, &column_names)
                    .context("Failed to select columns")?
            };

            for row in rows {
                println!("{}", row.join("|"));
            }
            return Ok(());
        }
    }

    anyhow::bail!("Unsupported query: {}", query)
}
