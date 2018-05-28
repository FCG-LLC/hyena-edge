use super::ScanIndex;
use block::{BlockData, BufferHead, IndexMut, IndexRef};
use error::*;
use hyena_bloom_filter::{BloomValue, DefaultBloomFilter};
use std::fmt::Debug;
use std::marker::PhantomData;
use storage::Storage;
use ty::RowId;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmptyIndex;

#[derive(Debug)]
pub struct BloomIndexBlock<'si, S>
where
    S: 'si + Storage<'si, BloomValue>,
{
    storage: S,
    /// the tip of the buffer
    head: usize,
    base: PhantomData<&'si [BloomValue]>,
}

impl<'si, S> BloomIndexBlock<'si, S>
where
    S: 'si + Storage<'si, BloomValue>,
{
    pub(crate) fn new(storage: S) -> Result<BloomIndexBlock<'si, S>> {
        Ok(BloomIndexBlock {
            storage,
            head: 0,
            base: PhantomData,
        })
    }
}

impl<'si, S> BlockData<'si, BloomValue, EmptyIndex> for BloomIndexBlock<'si, S>
where
    S: 'si + Storage<'si, BloomValue>,
{
}

impl<'si, S> AsRef<[BloomValue]> for BloomIndexBlock<'si, S>
where
    S: 'si + Storage<'si, BloomValue>,
{
    fn as_ref(&self) -> &[BloomValue] {
        self.storage.as_ref()
    }
}

impl<'si, S> AsMut<[BloomValue]> for BloomIndexBlock<'si, S>
where
    S: 'si + Storage<'si, BloomValue>,
{
    fn as_mut(&mut self) -> &mut [BloomValue] {
        self.storage.as_mut()
    }
}

impl<'si, S> BufferHead for BloomIndexBlock<'si, S>
where
    S: 'si + Storage<'si, BloomValue>,
{
    fn head(&self) -> usize {
        self.head
    }

    fn mut_head(&mut self) -> &mut usize {
        &mut self.head
    }
}

impl<'si, S> IndexRef<[EmptyIndex]> for BloomIndexBlock<'si, S>
where
    S: 'si + Storage<'si, BloomValue>,
{
    fn as_ref_index(&self) -> &[EmptyIndex] {
        &[][..]
    }
}

impl<'si, S> IndexMut<[EmptyIndex]> for BloomIndexBlock<'si, S>
where
    S: 'si + Storage<'si, BloomValue>,
{
    fn as_mut_index(&mut self) -> &mut [EmptyIndex] {
        &mut [][..]
    }
}

pub(crate) struct BloomLookupIter<'si> {
    it: Box<Iterator<Item = bool> + 'si>,
}

impl<'si> BloomLookupIter<'si> {
    pub(crate) fn new(slice: &'si [BloomValue], value: BloomValue) -> BloomLookupIter<'si> {
        BloomLookupIter {
            it: Box::new(slice.iter().map(move |idx| idx.contains(value))),
        }
    }
}

impl<'si> Iterator for BloomLookupIter<'si> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        self.it.next()
    }
}

pub(crate) struct BloomIter<'si> {
    it: Box<Iterator<Item = &'si BloomValue> + 'si>,
}

impl<'si> BloomIter<'si> {
    pub(crate) fn new(slice: &'si [BloomValue]) -> BloomIter<'si> {
        BloomIter {
            it: Box::new(slice.iter()),
        }
    }
}

impl<'si> Iterator for BloomIter<'si> {
    type Item = &'si BloomValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.it.next()
    }
}

impl<'block, 'si, T, S> ScanIndex<'block, 'si, T> for BloomIndexBlock<'si, S>
where
    T: 'block + AsRef<[u8]> + Debug,
    S: 'si + Storage<'si, BloomValue>,
{
    type Iter = BloomIter<'si>;
    type LookupIter = BloomLookupIter<'si>;
    type FilterValue = BloomValue;

    #[inline]
    fn index_encode_value(value: T) -> Self::FilterValue {
        DefaultBloomFilter::encode(&value)
    }

    #[inline]
    fn index_append_encoded(&mut self, value: Self::FilterValue) {
        debug_assert!(
            self.head < self.storage.len(),
            "bounds check failed for bloom index block"
        );
        {
            let store = self.as_mut_slice_append();
            store[0] = value;
        }

        self.head += 1;
    }

    // todo: split into two separate traits
    // to allow calling methods on encoded value without prividing bogus T

    #[inline]
    fn index_lookup_encoded(&self, rowid: RowId, value: Self::FilterValue) -> bool {
        debug_assert!(
            rowid < self.storage.len(),
            "bounds check failed for bloom index block"
        );

        self.as_slice()[rowid].contains(value)
    }

    #[inline]
    fn index_lookup_encoded_iter(&'si self, value: Self::FilterValue) -> Self::LookupIter {
        BloomLookupIter::new(self.as_slice(), value)
    }

    #[inline]
    fn index_iter(&'si self) -> Self::Iter {
        BloomIter::new(self.as_slice())
    }
}
