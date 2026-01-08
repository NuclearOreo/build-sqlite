use crate::db;
use anyhow::{Context, Result};

pub fn dbinfo(path: &str) -> Result<()> {
    let (page_size, number_of_tables) = db::read_db_info(path)
        .context("Failed to read database info")?;
    println!("database page size: {}", page_size);
    println!("number of tables: {}", number_of_tables);
    Ok(())
}

pub fn table(path: &str) -> Result<()> {
    let table_names = db::read_table_names(path)
        .context("Failed to read table names")?;
    println!("{}", table_names.join(" "));
    Ok(())
}
