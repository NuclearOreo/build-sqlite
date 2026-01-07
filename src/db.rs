use anyhow::Result;
use std::fs::File;
use std::io::prelude::*;

pub fn read_db_info(path: &str) -> Result<(u16, u16)> {
    let mut file = File::open(path)?;
    let mut header = [0; 100];
    let mut page_header = [0; 112];
    file.read_exact(&mut header)?;
    file.read_exact(&mut page_header)?;

    // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
    let page_size = u16::from_be_bytes([header[16], header[17]]);
    let number_of_tables = u16::from_be_bytes([page_header[3], page_header[4]]);

    Ok((page_size, number_of_tables))
}
