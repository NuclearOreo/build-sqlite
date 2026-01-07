use crate::db;
use anyhow::Result;

pub fn dbinfo(path: &str) -> Result<()> {
    let (page_size, number_of_tables) = db::read_db_info(path)?;
    println!("database page size: {}", page_size);
    println!("number of tables: {}", number_of_tables);
    Ok(())
}

pub fn table(path: &str) -> Result<()> {
    let table_names = db::read_table_names(path)?;
    println!("{}", table_names.join(" "));
    Ok(())
}
