use error::*;
use std::fs::{File, OpenOptions};
use std::path::Path;

#[cfg(feature = "hole_punching")]
use super::hole_punch::punch_hole;


pub fn ensure_file<P: AsRef<Path>>(path: P, size: usize) -> Result<File> {

    // check if file exists
    let exists = path.as_ref().exists();

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;

    file.set_len(size as u64)?;

    if !exists {
        #[cfg(feature = "hole_punching")] punch_hole(&file, size)?;
    }

    Ok(file)
}
