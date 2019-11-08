use crate::error::*;
use std::fs::{create_dir_all, File, OpenOptions};
use std::path::{Path, PathBuf};

#[cfg(feature = "hole_punching")]
mod hole_punch;
#[cfg(feature = "hole_punching")]
use self::hole_punch::punch_hole;


pub fn ensure_file<P: AsRef<Path>>(
    path: P,
    create_size: usize,
    existing_size: Option<usize>
) -> Result<File> {

    // check if file exists
    let exists = path.as_ref().exists();

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;

    if !exists {
        file.set_len(create_size as u64)?;
        #[cfg(feature = "hole_punching")] punch_hole(&file, create_size)?;
    } else {
        if let Some(size) = existing_size {
            // todo: punch hole after enlarging the file
            file.set_len(size as u64)?;
        }
    }

    Ok(file)
}

pub fn ensure_dir<P: AsRef<Path>>(path: P) -> Result<PathBuf> {
    let path = path.as_ref();

    if !path.exists() {
        info!("Directory {} doesn't exist, creating...", path.display());

        create_dir_all(&path)
            .with_context(|_| "Unable to create directory")?;
    } else if path.is_dir() {
        let meta = path.metadata()
            .with_context(|_| "Failed to retrieve metadata for the path")?;

        if meta.permissions().readonly() {
            bail!("The directory is read only");
        }
    } else {
        bail!("Provided path exists and is not a directory");
    }

    let path = path.canonicalize()
        .with_context(|_| "Unable to acquire canonical path")?;

    Ok(path.to_path_buf())
}
