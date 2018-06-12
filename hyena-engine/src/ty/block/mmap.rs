use error::*;
use super::BlockId;
use storage::mmap::MemmapStorage;
use block::SparseIndex;
use std::path::Path;
//use fs::ensure_file;
use std::mem::size_of;
//use std::fs::remove_file;
use params::{BLOCK_SIZE, STRING_POOL_SIZE};
use extprim::i128::i128;
use extprim::u128::u128;

block_impl!(MemmapStorage);

impl<'block> Block<'block> {
    #[inline]
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

    #[inline]
    pub(crate) fn prepare_dense_pool_storage<P: AsRef<Path>>(
        root: P,
        id: BlockId,
        size: usize,
        pool_size: usize
    ) -> Result<(MemmapStorage, MemmapStorage)> {

        let root = root.as_ref();
        let slice_stor = Block::prepare_dense_storage(&root, id, size)?;

        let pool = root.join(format!("block_{}.pool", id));

        let pool_stor = MemmapStorage::new(pool, pool_size)
            .with_context(|_| "Failed to create pool block for dense string storage")?;

        Ok((slice_stor, pool_stor))
    }

    #[inline]
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
            .with_context(|_| "Failed to create data block for sparse storage")?;

        let index_stor = MemmapStorage::new(index, index_size)
            .with_context(|_| "Failed to create index block for sparse storage")?;

        Ok((data_stor, index_stor))
    }

    #[inline]
    pub(crate) fn create<P: AsRef<Path>>(
        root: P,
        block_type: BlockType,
        block_id: BlockId,
    ) -> Result<Block<'block>> {
        use ty::block::ty_impl::*;

        macro_rules! prepare_mmap_dense {
            ($block: ty) => {{
                let storage = Block::prepare_dense_storage(&root, block_id, BLOCK_SIZE)
                                .with_context(|_| "Failed to create storage")
                                .unwrap();

                <$block>::new(storage)
                    .with_context(|_| "Failed to create block")?
                    .into()
            }};
        }

        macro_rules! prepare_mmap_sparse {
            ($block: ty, $T: ty) => {{
                let (data, index) = Block::prepare_sparse_storage::<$T, _>(&root,
                                                                        block_id,
                                                                        BLOCK_SIZE)
                                .with_context(|_| "Failed to create storage")
                                .unwrap();

                <$block>::new(data, index)
                    .with_context(|_| "Failed to create block")?
                    .into()
            }};
        }


        Ok(match block_type {
            BlockType::I8Dense => prepare_mmap_dense!(I8DenseBlock<'block, _>),
            BlockType::I16Dense => prepare_mmap_dense!(I16DenseBlock<'block, _>),
            BlockType::I32Dense => prepare_mmap_dense!(I32DenseBlock<'block, _>),
            BlockType::I64Dense => prepare_mmap_dense!(I64DenseBlock<'block, _>),
            BlockType::I128Dense => prepare_mmap_dense!(I128DenseBlock<'block, _>),
            BlockType::U8Dense => prepare_mmap_dense!(U8DenseBlock<'block, _>),
            BlockType::U16Dense => prepare_mmap_dense!(U16DenseBlock<'block, _>),
            BlockType::U32Dense => prepare_mmap_dense!(U32DenseBlock<'block, _>),
            BlockType::U64Dense => prepare_mmap_dense!(U64DenseBlock<'block, _>),
            BlockType::U128Dense => prepare_mmap_dense!(U128DenseBlock<'block, _>),

            // String
            BlockType::StringDense => {
                let (slice_storage, pool_storage) = Block::prepare_dense_pool_storage(
                    &root,
                    block_id,
                    BLOCK_SIZE,
                    STRING_POOL_SIZE
                )
                    .with_context(|_| "Failed to create dense string storage")
                    .unwrap();

                StringDenseBlock::<'block, _, _>::new(slice_storage, pool_storage)
                    .with_context(|_| "Failed to create block")?
                    .into()
            }

            // Sparse
            BlockType::I8Sparse => prepare_mmap_sparse!(I8SparseBlock<'block, _, _>, i8),
            BlockType::I16Sparse => prepare_mmap_sparse!(I16SparseBlock<'block, _, _>, i16),
            BlockType::I32Sparse => prepare_mmap_sparse!(I32SparseBlock<'block, _, _>, i32),
            BlockType::I64Sparse => prepare_mmap_sparse!(I64SparseBlock<'block, _, _>, i64),
            BlockType::I128Sparse => prepare_mmap_sparse!(I128SparseBlock<'block, _, _>, i128),
            BlockType::U8Sparse => prepare_mmap_sparse!(U8SparseBlock<'block, _, _>, u8),
            BlockType::U16Sparse => prepare_mmap_sparse!(U16SparseBlock<'block, _, _>, u16),
            BlockType::U32Sparse => prepare_mmap_sparse!(U32SparseBlock<'block, _, _>, u32),
            BlockType::U64Sparse => prepare_mmap_sparse!(U64SparseBlock<'block, _, _>, u64),
            BlockType::U128Sparse => prepare_mmap_sparse!(U128SparseBlock<'block, _, _>, u128),
        })
    }
}

impl<'block> From<Block<'block>> for super::Block<'block> {
    fn from(block: Block<'block>) -> super::Block {
        super::Block::Memmap(block)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use params::{BLOCK_SIZE, STRING_POOL_SIZE};


    #[test]
    fn prepare_dense() {
        let root = tempdir!();
        let blockid = 123;

        let path = root.as_ref().join(format!("block_{}.data", blockid));

        let storage = Block::prepare_dense_storage(&root, blockid, BLOCK_SIZE)
            .with_context(|_| "Failed to prepare dense storage")
            .unwrap();

        assert_eq!(storage.file_path(), path);
        assert!(path.exists());
        assert!(path.is_file());
        assert_eq!(
            path.metadata()
                .with_context(|_| "Unable to get metadata for data file")
                .unwrap()
                .len() as usize,
            BLOCK_SIZE
        );
    }

    fn prepare_sparse<T>(blockid: usize) {
        let root = tempdir!();

        let data_path = root.as_ref().join(format!("block_{}.data", blockid));
        let index_path = root.as_ref().join(format!("block_{}.index", blockid));

        let (data_stor, index_stor) =
            Block::prepare_sparse_storage::<T, _>(&root, blockid, BLOCK_SIZE)
                .with_context(|_| {
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
                .with_context(|_| "Unable to get metadata for data file")
                .unwrap()
                .len() as usize,
            BLOCK_SIZE
        );
        assert_eq!(
            index_path
                .metadata()
                .with_context(|_| "Unable to get metadata for index file")
                .unwrap()
                .len() as usize,
            BLOCK_SIZE / size_of::<T>() * size_of::<SparseIndex>()
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

    #[test]
    fn prepare_sparse_u128() {
        prepare_sparse::<u128>(1280);
    }

    #[test]
    fn prepare_dense_string() {
        let root = tempdir!();
        let blockid = 137659;

        let data_path = root.as_ref().join(format!("block_{}.data", blockid));
        let pool_path = root.as_ref().join(format!("block_{}.pool", blockid));

        let (data_stor, pool_stor) =
            Block::prepare_dense_pool_storage(&root, blockid, BLOCK_SIZE, STRING_POOL_SIZE)
                .with_context(|_| {
                    format!("Failed to prepare dense string storage")
                })
                .unwrap();

        assert_eq!(data_stor.file_path(), data_path);
        assert_eq!(pool_stor.file_path(), pool_path);
        assert!(data_path.exists());
        assert!(data_path.is_file());
        assert!(pool_path.exists());
        assert!(pool_path.is_file());

        assert_eq!(
            data_path
                .metadata()
                .with_context(|_| "Unable to get metadata for data file")
                .unwrap()
                .len() as usize,
            BLOCK_SIZE
        );

        assert_eq!(
            pool_path
                .metadata()
                .with_context(|_| "Unable to get metadata for pool file")
                .unwrap()
                .len() as usize,
            STRING_POOL_SIZE
        );
    }

}
