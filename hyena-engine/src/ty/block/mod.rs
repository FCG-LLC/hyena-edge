use error::*;
use std::collections::HashMap;
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
pub struct BlockTypeMap(BlockTypeMapTy);
pub(crate) type BlockTypeMapTy = HashMap<BlockId, BlockType>;


impl<'block> Deref for BlockTypeMap {
    type Target = BlockTypeMapTy;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'block, 'a> From<&'a BlockMap<'block>> for BlockTypeMap {
    fn from(block_map: &BlockMap) -> BlockTypeMap {
        block_map
            .iter()
            .map(|(block_id, block)| (*block_id, block.into()))
            .collect::<BlockTypeMapTy>()
            .into()
    }
}

impl From<BlockTypeMapTy> for BlockTypeMap {
    fn from(block_hmap: BlockTypeMapTy) -> BlockTypeMap {
        BlockTypeMap(block_hmap)
    }
}

#[derive(Debug, Serialize)]
pub enum Block<'block> {
    Memory(memory::Block<'block>),
    #[cfg(feature = "mmap")]
    Memmap(mmap::Block<'block>),
}


impl<'block> PartialEq<BlockType> for Block<'block> {
    fn eq(&self, rhs: &BlockType) -> bool {
        let self_ty: BlockType = self.into();

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
pub enum BlockType {
    Memory(block::BlockType),
    #[cfg(feature = "mmap")]
    Memmap(block::BlockType),
}

impl BlockType {
    pub fn size_of(&self) -> usize {
        use self::BlockType::*;

        block_map_expr!(*self, block, { block.size_of() })
    }

    pub fn is_sparse(&self) -> bool {
        use self::BlockType::*;

        block_map_expr!(*self, block, { block.is_sparse() })
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

impl<'block, 'a> From<&'a Block<'block>> for BlockType {
    fn from(block: &Block) -> BlockType {
        match *block {
            Block::Memory(ref b) => BlockType::Memory(b.into()),
            #[cfg(feature = "mmap")]
            Block::Memmap(ref b) => BlockType::Memmap(b.into()),
        }
    }
}

impl<'block, 'a> From<&'a RwLock<Block<'block>>> for BlockType {
    fn from(block: &RwLock<Block>) -> BlockType {
        (acquire!(read block)).into()
    }
}
