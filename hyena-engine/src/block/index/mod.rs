use crate::block::BlockData;
use std::fmt::Debug;
use crate::ty::RowId;

mod bloom;

pub(crate) use self::bloom::BloomIndexBlock;

pub(crate) trait ScanIndex<'block, 'si, T>
where
    T: 'block + AsRef<[u8]> + Debug,
{
    type Iter: Iterator<Item = &'si Self::FilterValue> + 'si;
    type LookupIter: Iterator<Item = bool> + 'si;
    type FilterValue: PartialEq + Copy + Clone + Debug + 'si;

    fn index_encode_value(value: T) -> Self::FilterValue;

    fn index_append_encoded(&mut self, value: Self::FilterValue);

    fn index_lookup_encoded(&self, rowid: RowId, value: Self::FilterValue) -> bool;

    fn index_lookup_encoded_iter(&'si self, value: Self::FilterValue) -> Self::LookupIter;

    fn index_iter(&'si self) -> Self::Iter;

    // todo: change to Result
    #[inline]
    fn index_append_value(&mut self, value: T) {
        self.index_append_encoded(Self::index_encode_value(value))
    }

    #[inline]
    fn index_lookup_value(&self, rowid: RowId, value: T) -> bool {
        self.index_lookup_encoded(rowid, Self::index_encode_value(value))
    }

    #[inline]
    fn index_lookup_value_iter(&'si self, value: T) -> Self::LookupIter {
        self.index_lookup_encoded_iter(Self::index_encode_value(value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ColumnIndexType {
    Bloom,
}

impl ColumnIndexType {
    #[inline]
    pub fn size_of(&self) -> usize {
        use crate::storage::memory::MemoryStorage;

        // this is a bit of a hack
        // we have to provide a concrete type for the S generic param in BloomIndexBlock
        // in order to be able to call size_of()
        // but the result of size_of() is independent of the storage
        // so MemoryStorage is as good as any, and it's always available

        type DummyStorage = MemoryStorage<u8>;

        match *self {
            ColumnIndexType::Bloom => BloomIndexBlock::<DummyStorage>::size_of(),
        }
    }
}
