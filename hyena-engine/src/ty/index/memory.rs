use super::*;
use crate::storage::memory::PagedMemoryStorage;
use crate::params::BLOCK_SIZE;
use std::mem::size_of;
use crate::block;


column_index_impl!(PagedMemoryStorage);

impl<'idx> ColumnIndex<'idx> {
    #[inline]
    pub(crate) fn prepare_storage(size: usize) -> Result<PagedMemoryStorage> {
        Ok(PagedMemoryStorage::new(size)
            .with_context(|_| "Failed to create data block for index storage")?)
    }

    #[inline]
    pub(crate) fn create(index_type: ColumnIndexType) -> Result<ColumnIndex<'idx>> {
        Ok(match index_type {
            ColumnIndexType::Bloom => {
                // see a comment in ty/index/mmap.rs for an explanation
                let slice_storage =
                    ColumnIndex::prepare_storage(BLOCK_SIZE
                        * size_of::<BloomValue>() / size_of::<block::RelativeSlice>())
                    .with_context(|_| "Failed to create dense index storage")
                    .unwrap();

                BloomIndexBlock::<'idx, _>::new(slice_storage)
                    .with_context(|_| "Failed to create block")?
                    .into()
            }
        })
    }
}

impl<'idx> From<ColumnIndex<'idx>> for super::ColumnIndex<'idx> {
    fn from(index: ColumnIndex<'idx>) -> super::ColumnIndex {
        super::ColumnIndex::Memory(index)
    }
}
