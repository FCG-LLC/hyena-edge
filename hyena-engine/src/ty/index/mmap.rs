use super::*;
use params::BLOCK_SIZE;
use std::mem::size_of;
use std::path::Path;
use storage::mmap::MemmapStorage;
use ty::block::BlockId;
use block;


column_index_impl!(MemmapStorage);

impl<'idx> ColumnIndex<'idx> {
    #[inline]
    pub(crate) fn prepare_storage<P: AsRef<Path>>(
        root: P,
        id: BlockId,
        size: usize,
        extension: &'static str,
    ) -> Result<MemmapStorage> {
        let root = root.as_ref();

        if !root.exists() {
            bail!("Destination data directory doesn't exist");
        }

        let data = root.join(format!("block_{}.{}", id, extension));

        MemmapStorage::new(data, size)
    }

    #[inline]
    pub(crate) fn create<P: AsRef<Path>>(
        root: P,
        index_type: ColumnIndexType,
        block_id: BlockId,
    ) -> Result<ColumnIndex<'idx>> {
        Ok(match index_type {
            ColumnIndexType::Bloom => {
                let slice_storage = Self::prepare_storage(
                    &root,
                    block_id,
                    // size_of::<block::RelativeSlice> is the length
                    // of a single element in a StringDense block
                    //
                    // size_of::<BloomValue> is the lenght of a single element in a BloomIndexBlock
                    //
                    // we're adjusting the size of the bloom index block
                    // to allow it to hold the same number of records
                    // otherwise this would determine the maximum number of records
                    // and that would be BLOCK_SIZE / 16 for a 256 bit bloom filter
                    BLOCK_SIZE * size_of::<BloomValue>() / size_of::<block::RelativeSlice>(),
                    "bloom",
                ).with_context(|_| "Failed to create dense index storage")
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
        super::ColumnIndex::Memmap(index)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use params::{BLOCK_SIZE};

    #[test]
    fn prepare() {
        let root = tempdir!();
        let blockid = 123;

        let path = root.as_ref().join(format!("block_{}.bloom", blockid));

        let storage = ColumnIndex::prepare_storage(&root, blockid, BLOCK_SIZE, "bloom")
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

    #[test]
    fn prepare_bloom() {
        let root = tempdir!();
        let blockid = 123;

        let bloom_path = root.as_ref().join(format!("block_{}.bloom", blockid));

        let _index_block = ColumnIndex::create(&root, ColumnIndexType::Bloom, blockid)
            .with_context(|_| "Failed to prepare column index storage")
            .unwrap();

        assert!(bloom_path.exists());
        assert!(bloom_path.is_file());
        assert_eq!(
            bloom_path.metadata()
                .with_context(|_| "Unable to get metadata for data file")
                .unwrap()
                .len() as usize,
            // use constant to verify the correctness of relative calculations
            // and also make this fail if BloomValue size changes
            BLOCK_SIZE * 2
        );
    }

}
