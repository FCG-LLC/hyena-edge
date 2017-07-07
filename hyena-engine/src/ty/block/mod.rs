use std::collections::HashMap;

#[macro_use]
mod numeric;


pub type BlockId = usize;

pub type BlockMap<'block> = HashMap<BlockId, Block<'block>>;
pub(crate) type BlockTypeMap = HashMap<BlockId, BlockType>;


#[derive(Debug)]
pub enum Block<'block> {
    Memory(memory::Block<'block>),
    #[cfg(feature = "mmap")]
    Memmap(mmap::Block<'block>),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub(crate) enum BlockType {
    Memory(memory::BlockType),
    #[cfg(feature = "mmap")]
    Memmap(mmap::BlockType),
}

impl<'block, 'a> From<&'a Block<'block>> for BlockType {
    fn from(block: &Block) -> BlockType {
        match *block {
            Block::Memory(ref b) => BlockType::Memory(b.into()),
            #[cfg(feature = "mmap")]
            Block::Memmap(ref b) => BlockType::Memmap(b.into()),
        }
    }
}

pub(crate) mod memory {
    use error::*;
    use storage::Storage;
    use storage::memory::PagedMemoryStorage;
    use block::SparseIndex;
    use std::mem::size_of;


    numeric_block_impl!(PagedMemoryStorage);

    impl<'block> Block<'block> {
        pub(crate) fn prepare_dense_storage(size: usize) -> Result<PagedMemoryStorage> {
            PagedMemoryStorage::new(size)
        }

        pub(crate) fn prepare_sparse_storage<T>(
            size: usize,
        ) -> Result<(PagedMemoryStorage, PagedMemoryStorage)> {

            let index_size = size / size_of::<T>() * size_of::<SparseIndex>();

            let data_stor = PagedMemoryStorage::new(size)
                .chain_err(|| "Failed to create data block for sparse storage")?;

            let index_stor = PagedMemoryStorage::new(index_size)
                .chain_err(|| "Failed to create index block for sparse storage")?;

            Ok((data_stor, index_stor))
        }
    }


    #[cfg(test)]
    mod tests {
        use super::*;
        use block::tests::BLOCK_FILE_SIZE;


        #[test]
        fn prepare_dense() {
            let storage = Block::prepare_dense_storage(BLOCK_FILE_SIZE)
                .chain_err(|| "Failed to prepare dense storage")
                .unwrap();

            assert_eq!(storage.len(), BLOCK_FILE_SIZE);
        }

        fn prepare_sparse<T>() {
            let (data_stor, index_stor) = Block::prepare_sparse_storage::<T>(BLOCK_FILE_SIZE)
                .chain_err(|| {
                    format!("Failed to prepare sparse storage for T={}", size_of::<T>())
                })
                .unwrap();

            assert_eq!(data_stor.len(), BLOCK_FILE_SIZE);
            assert_eq!(
                index_stor.len(),
                BLOCK_FILE_SIZE / size_of::<T>() * size_of::<SparseIndex>()
            );
        }

        #[test]
        fn prepare_sparse_i8() {
            prepare_sparse::<i8>();
        }

        #[test]
        fn prepare_sparse_i16() {
            prepare_sparse::<i16>();
        }

        #[test]
        fn prepare_sparse_i32() {
            prepare_sparse::<i32>();
        }

        #[test]
        fn prepare_sparse_i64() {
            prepare_sparse::<i64>();
        }

        #[cfg(feature = "block_128")]
        #[test]
        fn prepare_sparse_i128() {
            prepare_sparse::<i128>();
        }

        #[test]
        fn prepare_sparse_u8() {
            prepare_sparse::<u8>();
        }

        #[test]
        fn prepare_sparse_u16() {
            prepare_sparse::<u16>();
        }

        #[test]
        fn prepare_sparse_u32() {
            prepare_sparse::<u32>();
        }

        #[test]
        fn prepare_sparse_u64() {
            prepare_sparse::<u64>();
        }

        #[cfg(feature = "block_128")]
        #[test]
        fn prepare_sparse_u128() {
            prepare_sparse::<u128>();
        }
    }
}

#[cfg(feature = "mmap")]
pub(crate) mod mmap {
    use error::*;
    use super::BlockId;
    use storage::Storage;
    use storage::mmap::MemmapStorage;
    use block::SparseIndex;
    use std::path::Path;
    use fs::ensure_file;
    use std::mem::size_of;
    use std::fs::remove_file;


    numeric_block_impl!(MemmapStorage);

    impl<'block> Block<'block> {
        pub(crate) fn prepare_dense_storage<P: AsRef<Path>>(
            root: P,
            id: BlockId,
            size: usize,
        ) -> Result<MemmapStorage> {

            let root = root.as_ref();

            if !root.exists() {
                bail!("Destination data directory doesn't exist");
            }

            let data = root.join(format!("block_{}.data", id));

            MemmapStorage::new(data, size)
        }

        pub(crate) fn prepare_sparse_storage<T, P: AsRef<Path>>(
            root: P,
            id: BlockId,
            size: usize,
        ) -> Result<(MemmapStorage, MemmapStorage)> {

            let root = root.as_ref();

            if !root.exists() {
                bail!("Destination data directory doesn't exist");
            }

            let data = root.join(format!("block_{}.data", id));
            let index = root.join(format!("block_{}.index", id));

            let index_size = size / size_of::<T>() * size_of::<SparseIndex>();

            let data_stor = MemmapStorage::new(data, size)
                .chain_err(|| "Failed to create data block for sparse storage")?;

            let index_stor = MemmapStorage::new(index, index_size)
                .chain_err(|| "Failed to create index block for sparse storage")?;

            Ok((data_stor, index_stor))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use block::tests::BLOCK_FILE_SIZE;


        #[test]
        fn prepare_dense() {
            let root = tempdir!();
            let blockid = 123;

            let path = root.as_ref().join(format!("block_{}.data", blockid));

            let storage = Block::prepare_dense_storage(root, blockid, BLOCK_FILE_SIZE)
                .chain_err(|| "Failed to prepare dense storage")
                .unwrap();

            assert_eq!(storage.file_path(), path);
            assert!(path.exists());
            assert!(path.is_file());
            assert_eq!(
                path.metadata()
                    .chain_err(|| "Unable to get metadata for data file")
                    .unwrap()
                    .len() as usize,
                BLOCK_FILE_SIZE
            );
        }

        fn prepare_sparse<T>(blockid: usize) {
            let root = tempdir!();

            let data_path = root.as_ref().join(format!("block_{}.data", blockid));
            let index_path = root.as_ref().join(format!("block_{}.index", blockid));

            let (data_stor, index_stor) =
                Block::prepare_sparse_storage::<T, _>(root, blockid, BLOCK_FILE_SIZE)
                    .chain_err(|| {
                        format!("Failed to prepare sparse storage for T={}", size_of::<T>())
                    })
                    .unwrap();

            assert_eq!(data_stor.file_path(), data_path);
            assert_eq!(index_stor.file_path(), index_path);
            assert!(data_path.exists());
            assert!(data_path.is_file());
            assert!(index_path.exists());
            assert!(index_path.is_file());

            assert_eq!(
                data_path
                    .metadata()
                    .chain_err(|| "Unable to get metadata for data file")
                    .unwrap()
                    .len() as usize,
                BLOCK_FILE_SIZE
            );
            assert_eq!(
                index_path
                    .metadata()
                    .chain_err(|| "Unable to get metadata for index file")
                    .unwrap()
                    .len() as usize,
                BLOCK_FILE_SIZE / size_of::<T>() * size_of::<SparseIndex>()
            );
        }

        #[test]
        fn prepare_sparse_i8() {
            prepare_sparse::<i8>(8);
        }

        #[test]
        fn prepare_sparse_i16() {
            prepare_sparse::<i16>(16);
        }

        #[test]
        fn prepare_sparse_i32() {
            prepare_sparse::<i32>(32);
        }

        #[test]
        fn prepare_sparse_i64() {
            prepare_sparse::<i64>(64);
        }

        #[cfg(feature = "block_128")]
        #[test]
        fn prepare_sparse_i128() {
            prepare_sparse::<i128>(128);
        }

        #[test]
        fn prepare_sparse_u8() {
            prepare_sparse::<u8>(80);
        }

        #[test]
        fn prepare_sparse_u16() {
            prepare_sparse::<u16>(160);
        }

        #[test]
        fn prepare_sparse_u32() {
            prepare_sparse::<u32>(320);
        }

        #[test]
        fn prepare_sparse_u64() {
            prepare_sparse::<u64>(640);
        }

        #[cfg(feature = "block_128")]
        #[test]
        fn prepare_sparse_u128() {
            prepare_sparse::<u128>(1280);
        }
    }
}
