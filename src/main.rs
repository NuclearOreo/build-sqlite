use anyhow::{bail, Result};

mod commands;
mod db;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => commands::dbinfo(&args[1])?,
        ".table" => commands::table(&args[1])?,
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
