use crate::error::*;
use crate::storage::memory::PagedMemoryStorage;
use crate::block::SparseIndex;
use std::mem::size_of;
use crate::params::{BLOCK_SIZE, STRING_POOL_SIZE};
use extprim::i128::i128;
use extprim::u128::u128;

block_impl!(PagedMemoryStorage);

impl<'block> Block<'block> {
    #[inline]
    pub(crate) fn prepare_dense_storage(size: usize) -> Result<PagedMemoryStorage> {
        Ok(PagedMemoryStorage::new(size)
            .with_context(|_| "Failed to create data block for dense storage")?)
    }

    #[inline]
    pub(crate) fn prepare_sparse_storage<T>(
        size: usize,
    ) -> Result<(PagedMemoryStorage, PagedMemoryStorage)> {

        let index_size = size / size_of::<T>() * size_of::<SparseIndex>();

        let data_stor = PagedMemoryStorage::new(size)
            .with_context(|_| "Failed to create data block for sparse storage")?;

        let index_stor = PagedMemoryStorage::new(index_size)
            .with_context(|_| "Failed to create index block for sparse storage")?;

        Ok((data_stor, index_stor))
    }

    #[inline]
    pub(crate) fn prepare_dense_pool_storage(
        size: usize,
        pool_size: usize
    ) -> Result<(PagedMemoryStorage, PagedMemoryStorage)> {

        let data_stor = PagedMemoryStorage::new(size)
            .with_context(|_| "Failed to create data block for dense storage")?;

        let pool_stor = PagedMemoryStorage::new(pool_size)
            .with_context(|_| "Failed to create pool block for dense storage")?;

        Ok((data_stor, pool_stor))
    }

    #[inline]
    pub(crate) fn create(block_type: BlockType) -> Result<Block<'block>> {
        use crate::ty::block::ty_impl::*;

        macro_rules! prepare_mem_dense {
            ($block: ty) => {{
                let storage = Block::prepare_dense_storage(BLOCK_SIZE)
                                .with_context(|_| "Failed to create storage")
                                .unwrap();

                <$block>::new(storage)
                    .with_context(|_| "Failed to create block")?
                    .into()
            }};
        }

        macro_rules! prepare_mem_sparse {
            ($block: ty, $T: ty) => {{
                let (data, index) = Block::prepare_sparse_storage::<$T>(BLOCK_SIZE)
                                .with_context(|_| "Failed to create storage")
                                .unwrap();

                <$block>::new(data, index)
                    .with_context(|_| "Failed to create block")?
                    .into()
            }};
        }

        Ok(match block_type {
            BlockType::I8Dense => prepare_mem_dense!(I8DenseBlock<'block, _>),
            BlockType::I16Dense => prepare_mem_dense!(I16DenseBlock<'block, _>),
            BlockType::I32Dense => prepare_mem_dense!(I32DenseBlock<'block, _>),
            BlockType::I64Dense => prepare_mem_dense!(I64DenseBlock<'block, _>),
            BlockType::I128Dense => prepare_mem_dense!(I128DenseBlock<'block, _>),
            BlockType::U8Dense => prepare_mem_dense!(U8DenseBlock<'block, _>),
            BlockType::U16Dense => prepare_mem_dense!(U16DenseBlock<'block, _>),
            BlockType::U32Dense => prepare_mem_dense!(U32DenseBlock<'block, _>),
            BlockType::U64Dense => prepare_mem_dense!(U64DenseBlock<'block, _>),
            BlockType::U128Dense => prepare_mem_dense!(U128DenseBlock<'block, _>),

            // String
            BlockType::StringDense => {
                let (slice_storage, pool_storage) =
                    Block::prepare_dense_pool_storage(BLOCK_SIZE, STRING_POOL_SIZE)
                    .with_context(|_| "Failed to create dense string storage")
                    .unwrap();

                StringDenseBlock::<'block, _, _>::new(slice_storage, pool_storage)
                    .with_context(|_| "Failed to create block")?
                    .into()
            }

            // Sparse
            BlockType::I8Sparse => prepare_mem_sparse!(I8SparseBlock<'block, _, _>, i8),
            BlockType::I16Sparse => prepare_mem_sparse!(I16SparseBlock<'block, _, _>, i16),
            BlockType::I32Sparse => prepare_mem_sparse!(I32SparseBlock<'block, _, _>, i32),
            BlockType::I64Sparse => prepare_mem_sparse!(I64SparseBlock<'block, _, _>, i64),
            BlockType::I128Sparse => prepare_mem_sparse!(I128SparseBlock<'block, _, _>, i128),
            BlockType::U8Sparse => prepare_mem_sparse!(U8SparseBlock<'block, _, _>, u8),
            BlockType::U16Sparse => prepare_mem_sparse!(U16SparseBlock<'block, _, _>, u16),
            BlockType::U32Sparse => prepare_mem_sparse!(U32SparseBlock<'block, _, _>, u32),
            BlockType::U64Sparse => prepare_mem_sparse!(U64SparseBlock<'block, _, _>, u64),
            BlockType::U128Sparse => prepare_mem_sparse!(U128SparseBlock<'block, _, _>, u128),
        })
    }
}

impl<'block> From<Block<'block>> for super::Block<'block> {
    fn from(block: Block<'block>) -> super::Block {
        super::Block::Memory(block)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::{BLOCK_SIZE, STRING_POOL_SIZE};


    #[test]
    fn prepare_dense() {
        let storage = Block::prepare_dense_storage(BLOCK_SIZE)
            .with_context(|_| "Failed to prepare dense storage")
            .unwrap();

        assert_eq!(storage.len(), BLOCK_SIZE);
    }

    fn prepare_sparse<T>() {
        let (data_stor, index_stor) = Block::prepare_sparse_storage::<T>(BLOCK_SIZE)
            .with_context(|_| {
                format!("Failed to prepare sparse storage for T={}", size_of::<T>())
            })
            .unwrap();

        assert_eq!(data_stor.len(), BLOCK_SIZE);
        assert_eq!(
            index_stor.len(),
            BLOCK_SIZE / size_of::<T>() * size_of::<SparseIndex>()
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

    #[test]
    fn prepare_sparse_u128() {
        prepare_sparse::<u128>();
    }

    #[test]
    fn prepare_dense_string() {
        let (data_stor, pool_stor) = Block::prepare_dense_pool_storage(BLOCK_SIZE, STRING_POOL_SIZE)
            .with_context(|_| {
                    format!("Failed to prepare dense string storage")
            })
            .unwrap();

        assert_eq!(data_stor.len(), BLOCK_SIZE);
        assert_eq!(pool_stor.len(), STRING_POOL_SIZE);
    }
}
