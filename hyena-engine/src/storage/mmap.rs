use error::*;

use fs::ensure_file;

use memmap;
use memmap::{MmapMut, Protection};

use std::path::{Path, PathBuf};
use std::marker::PhantomData;

use super::Storage;

use super::map_type::{map_type, map_type_mut};


pub fn map_file<P: AsRef<Path>>(path: P, size: usize) -> Result<MmapMut> {
    let file = ensure_file(path, size)?;

    unsafe {
        memmap::file(&file)
            .protection(Protection::ReadWrite)
            .map_mut()
            .chain_err(|| "memmap failed")
    }
}

#[derive(Debug)]
pub struct MemmapStorage {
    mmap: MmapMut,
    path: PathBuf,
}

impl MemmapStorage {
    pub fn new<P: AsRef<Path>>(file: P, size: usize) -> Result<MemmapStorage> {
        let path = file.as_ref().to_path_buf();

        let mmap = map_file(&path, size).chain_err(|| "unable to mmap file")?;

        Ok(Self { mmap, path })
    }

    pub fn file_path(&self) -> &Path {
        &self.path
    }
}

impl<'stor, T: 'stor> Storage<'stor, T> for MemmapStorage {
    fn sync(&mut self) -> Result<()> {
        self.mmap
            .flush_async()
            .chain_err(|| "memmap::flush_asyc failed")
    }
}

impl<T> AsRef<[T]> for MemmapStorage {
    fn as_ref(&self) -> &[T] {
        map_type(&self.mmap, PhantomData)
    }
}

impl<T> AsMut<[T]> for MemmapStorage {
    fn as_mut(&mut self) -> &mut [T] {
        map_type_mut(&mut self.mmap, PhantomData)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEMPDIR_PREFIX: &str = "hyena-test";
    const TEST_BYTES_LEN: usize = 10;
    static TEST_BYTES: [u8; TEST_BYTES_LEN] = *b"hyena test";
    const FILE_SIZE: usize = 1 << 20; // 1 MiB

    #[test]
    fn it_maps_new_file() {
        let (_dir, file) = tempfile!(prefix TEMPDIR_PREFIX);

        let _storage = MemmapStorage::new(&file, FILE_SIZE)
            .chain_err(|| "unable to create MemmapStorage")
            .unwrap();

        // verify that the file was in fact created
        // and with correct size

        assert!(file.exists() && file.is_file());

        assert_file_size!(file, FILE_SIZE);
    }

    #[test]
    fn it_maps_existing_file() {
        let (_dir, file) = tempfile!(prefix TEMPDIR_PREFIX);

        ensure_write!(file, TEST_BYTES, FILE_SIZE);

        let storage = MemmapStorage::new(&file, FILE_SIZE)
            .chain_err(|| "unable to create MemmapStorage")
            .unwrap();

        assert_file_size!(file, FILE_SIZE);
        assert_eq!(
            &TEST_BYTES[..],
            &<MemmapStorage as AsRef<[u8]>>::as_ref(&storage)[..TEST_BYTES_LEN]
        );
    }

    #[test]
    fn it_writes_to_new_file() {
        let (_dir, file) = tempfile!(prefix TEMPDIR_PREFIX);

        {
            let mut storage = MemmapStorage::new(&file, FILE_SIZE)
                .chain_err(|| "unable to create MemmapStorage")
                .unwrap();

            &mut storage.as_mut()[..TEST_BYTES_LEN].copy_from_slice(&TEST_BYTES[..]);
        }

        assert_file_size!(file, FILE_SIZE);

        let buf: [u8; TEST_BYTES_LEN] = ensure_read!(file, [0; TEST_BYTES_LEN], FILE_SIZE);

        assert_eq!(&TEST_BYTES[..], &buf[..]);
    }

    #[test]
    fn it_writes_to_existing_file() {
        let (_dir, file) = tempfile!(prefix TEMPDIR_PREFIX);

        ensure_write!(file, TEST_BYTES, FILE_SIZE);

        {
            let mut storage = MemmapStorage::new(&file, FILE_SIZE)
                .chain_err(|| "unable to create MemmapStorage")
                .unwrap();

            &mut storage.as_mut()[TEST_BYTES_LEN..TEST_BYTES_LEN * 2]
                .copy_from_slice(&TEST_BYTES[..]);
        }

        assert_file_size!(file, FILE_SIZE);

        let buf: [u8; TEST_BYTES_LEN * 2] =
            ensure_read!(file, [0; TEST_BYTES_LEN * 2], FILE_SIZE);

        assert_eq!(&TEST_BYTES[..], &buf[..TEST_BYTES_LEN]);
        assert_eq!(&TEST_BYTES[..], &buf[TEST_BYTES_LEN..TEST_BYTES_LEN * 2]);
    }
}
