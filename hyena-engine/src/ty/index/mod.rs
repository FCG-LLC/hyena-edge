use block::index::{BloomIndexBlock, ColumnIndexType, ScanIndex};
use error::*;
use hyena_bloom_filter::BloomValue;
use hyena_common::collections::HashMap;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::RwLock;
use ty::block::BlockId;

#[macro_use]
mod ty_impl;
pub(crate) mod memory;
pub(crate) mod mmap;

pub type ColumnIndexMap<'idx> = HashMap<BlockId, RwLock<ColumnIndex<'idx>>>;

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ColumnIndexStorageMap(ColumnIndexStorageMapType);
pub(crate) type ColumnIndexStorageMapType = HashMap<BlockId, ColumnIndexStorage>;

impl<'idx> Deref for ColumnIndexStorageMap {
    type Target = ColumnIndexStorageMapType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'idx, 'a> From<&'a ColumnIndexMap<'idx>> for ColumnIndexStorageMap {
    fn from(index_map: &ColumnIndexMap) -> ColumnIndexStorageMap {
        index_map
            .iter()
            .map(|(block_id, index)| (*block_id, index.into()))
            .collect::<ColumnIndexStorageMapType>()
            .into()
    }
}

impl<'idx> DerefMut for ColumnIndexStorageMap {
    fn deref_mut(&mut self) -> &mut <Self as Deref>::Target {
        &mut self.0
    }
}

impl From<ColumnIndexStorageMapType> for ColumnIndexStorageMap {
    fn from(index_hmap: ColumnIndexStorageMapType) -> ColumnIndexStorageMap {
        ColumnIndexStorageMap(index_hmap)
    }
}

impl<'idx, 'a> From<&'a ColumnIndex<'idx>> for ColumnIndexStorage {
    fn from(index: &ColumnIndex) -> ColumnIndexStorage {
        match *index {
            ColumnIndex::Memory(ref b) => ColumnIndexStorage::Memory(b.into()),
            #[cfg(feature = "mmap")]
            ColumnIndex::Memmap(ref b) => ColumnIndexStorage::Memmap(b.into()),
        }
    }
}

impl<'idx, 'a> From<&'a RwLock<ColumnIndex<'idx>>> for ColumnIndexStorage {
    fn from(block: &RwLock<ColumnIndex>) -> ColumnIndexStorage {
        (acquire!(read block)).into()
    }
}

macro_rules! column_index_map_expr {
    (mut $self:expr, $idxref:ident, $body:block) => {{
        use ty::index::ColumnIndex::*;

        match $self {
            Memory(ref mut $idxref) => $body,
            #[cfg(feature = "mmap")]
            Memmap(ref mut $idxref) => $body,
        }
    }};

    ($self:expr, $idxref:ident, $body:block) => {{
        use ty::index::ColumnIndex::*;

        match $self {
            Memory(ref $idxref) => $body,
            #[cfg(feature = "mmap")]
            Memmap(ref $idxref) => $body,
        }
    }};
}

#[derive(Debug)]
pub enum ColumnIndex<'idx> {
    Memory(memory::ColumnIndex<'idx>),
    #[cfg(feature = "mmap")]
    Memmap(mmap::ColumnIndex<'idx>),
}

impl<'idx> ColumnIndex<'idx> {
    #[inline]
    pub(crate) fn append_value<'v, T>(&mut self, value: T)
    where
        T: 'v + AsRef<[u8]> + Debug,
    {
        column_index_map_expr!(mut *self, idx, {
            idx.append_value(value)
        })
    }

    #[inline]
    pub(crate) fn iter<'v>(&'idx self) -> Box<Iterator<Item = &'idx BloomValue> + 'idx> {
        column_index_map_expr!(*self, idx, { idx.iter() })
    }

    #[inline]
    pub(crate) fn set_head(&mut self, head: usize) -> Result<()> {
        column_index_map_expr!(mut *self, idx, {
            idx.set_head(head)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ColumnIndexStorage {
    Memory(ColumnIndexType),
    #[cfg(feature = "mmap")]
    Memmap(ColumnIndexType),
}
