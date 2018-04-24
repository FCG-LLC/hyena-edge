use error::*;
use hyena_common::collections::HashMap;
use std::ops::Deref;
use std::sync::RwLock;
use block;

#[macro_use]
pub(crate) mod ty_impl;

pub(crate) mod memory;
#[cfg(feature = "mmap")]
pub(crate) mod mmap;

pub type BlockId = usize;

pub type BlockMap<'block> = HashMap<BlockId, RwLock<Block<'block>>>;
pub(crate) type BlockHeadMap = HashMap<BlockId, usize>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockStorageMap(BlockStorageMapType);
pub(crate) type BlockStorageMapType = HashMap<BlockId, BlockStorage>;


impl<'block> Deref for BlockStorageMap {
    type Target = BlockStorageMapType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'block, 'a> From<&'a BlockMap<'block>> for BlockStorageMap {
    fn from(block_map: &BlockMap) -> BlockStorageMap {
        block_map
            .iter()
            .map(|(block_id, block)| (*block_id, block.into()))
            .collect::<BlockStorageMapType>()
            .into()
    }
}

impl From<BlockStorageMapType> for BlockStorageMap {
    fn from(block_hmap: BlockStorageMapType) -> BlockStorageMap {
        BlockStorageMap(block_hmap)
    }
}

#[derive(Debug, Serialize)]
pub enum Block<'block> {
    Memory(memory::Block<'block>),
    #[cfg(feature = "mmap")]
    Memmap(mmap::Block<'block>),
}


impl<'block> PartialEq<BlockStorage> for Block<'block> {
    fn eq(&self, rhs: &BlockStorage) -> bool {
        let self_ty: BlockStorage = self.into();

        self_ty == *rhs
    }
}

macro_rules! block_map_expr {
    ($self: expr, $blockref: ident, $body: block) => {
        match $self {
            Memory(ref $blockref) => $body,
            #[cfg(feature = "mmap")]
            Memmap(ref $blockref) => $body,
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum BlockStorageType {
    Memory,
    #[cfg(feature = "mmap")]
    Memmap,
}

impl<'bs> From<&'bs BlockStorage> for BlockStorageType {
    fn from(bs: &BlockStorage) -> BlockStorageType {
        use self::BlockStorage::*;

        match *bs {
            Memory(_) => BlockStorageType::Memory,
            #[cfg(feature = "mmap")]
            Memmap(_) => BlockStorageType::Memmap,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum BlockStorage {
    Memory(block::BlockType),
    #[cfg(feature = "mmap")]
    Memmap(block::BlockType),
}

impl BlockStorage {
    pub fn size_of(&self) -> usize {
        use self::BlockStorage::*;

        block_map_expr!(*self, block, { block.size_of() })
    }

    pub fn is_sparse(&self) -> bool {
        use self::BlockStorage::*;

        block_map_expr!(*self, block, { block.is_sparse() })
    }

    pub fn storage_type(&self) -> BlockStorageType {
        self.into()
    }
}

impl<'block> Block<'block> {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        use self::Block::*;

        block_map_expr!(*self, blk, { blk.len() })
    }

    #[inline]
    pub(crate) fn size(&self) -> usize {
        use self::Block::*;

        block_map_expr!(*self, blk, { blk.size() })
    }

    #[inline]
    #[allow(unused)]
    pub(crate) fn is_empty(&self) -> bool {
        use self::Block::*;

        block_map_expr!(*self, blk, { blk.is_empty() })
    }

    #[inline]
    pub fn is_sparse(&self) -> bool {
        use self::Block::*;

        block_map_expr!(*self, block, { block.is_sparse() })
    }
}

impl<'block, 'a> From<&'a Block<'block>> for BlockStorage {
    fn from(block: &Block) -> BlockStorage {
        match *block {
            Block::Memory(ref b) => BlockStorage::Memory(b.into()),
            #[cfg(feature = "mmap")]
            Block::Memmap(ref b) => BlockStorage::Memmap(b.into()),
        }
    }
}

impl<'block, 'a> From<&'a RwLock<Block<'block>>> for BlockStorage {
    fn from(block: &RwLock<Block>) -> BlockStorage {
        (acquire!(read block)).into()
    }
}

impl Deref for BlockStorage {
    type Target = block::BlockType;

    fn deref(&self) -> &Self::Target {
        use self::BlockStorage::*;

        match *self {
            Memory(ref bt) | Memmap(ref bt) => bt,
        }
    }
}