use block::{DenseNumericBlock, SparseIndexedNumericBlock};
use storage::Storage;


macro_rules! block_impl {
    ($ST: ty, $SI: ty) => {
        use ty::block::ty_impl::*;
        use ty::block::{Block as TyBlock, BlockType as TyBlockType, BlockTypeMapTy,
                        BlockTypeMap as TyBlockTypeMap};
        use std::collections::hash_map::HashMap;
        use std::sync::RwLock;


        pub(crate) type BlockTypeMap = HashMap<BlockId, BlockType>;

        #[derive(Debug)]
        pub enum Block<'block> {
            // Dense, Signed
            I8Dense(I8DenseBlock<'block, $ST>),
            I16Dense(I16DenseBlock<'block, $ST>),
            I32Dense(I32DenseBlock<'block, $ST>),
            I64Dense(I64DenseBlock<'block, $ST>),
            #[cfg(feature = "block_128")]
            I128Dense(I128DenseBlock<'block, $ST>),

            // Dense, Unsigned
            U8Dense(U8DenseBlock<'block, $ST>),
            U16Dense(U16DenseBlock<'block, $ST>),
            U32Dense(U32DenseBlock<'block, $ST>),
            U64Dense(U64DenseBlock<'block, $ST>),
            #[cfg(feature = "block_128")]
            U128Dense(U128DenseBlock<'block, $ST>),

            // Sparse, Signed
            I8Sparse(I8SparseBlock<'block, $ST, $SI>),
            I16Sparse(I16SparseBlock<'block, $ST, $SI>),
            I32Sparse(I32SparseBlock<'block, $ST, $SI>),
            I64Sparse(I64SparseBlock<'block, $ST, $SI>),
            #[cfg(feature = "block_128")]
            I128Sparse(I128SparseBlock<'block, $ST, $SI>),

            // Sparse, Unsigned
            U8Sparse(U8SparseBlock<'block, $ST, $SI>),
            U16Sparse(U16SparseBlock<'block, $ST, $SI>),
            U32Sparse(U32SparseBlock<'block, $ST, $SI>),
            U64Sparse(U64SparseBlock<'block, $ST, $SI>),
            #[cfg(feature = "block_128")]
            U128Sparse(U128SparseBlock<'block, $ST, $SI>),
        }

        impl<'block> From<I8DenseBlock<'block, $ST>> for Block<'block> {
            fn from(block: I8DenseBlock<'block, $ST>) -> Block<'block> {
                Block::I8Dense(block)
            }
        }

        impl<'block> From<I16DenseBlock<'block, $ST>> for Block<'block> {
            fn from(block: I16DenseBlock<'block, $ST>) -> Block<'block> {
                Block::I16Dense(block)
            }
        }

        impl<'block> From<I32DenseBlock<'block, $ST>> for Block<'block> {
            fn from(block: I32DenseBlock<'block, $ST>) -> Block<'block> {
                Block::I32Dense(block)
            }
        }

        impl<'block> From<I64DenseBlock<'block, $ST>> for Block<'block> {
            fn from(block: I64DenseBlock<'block, $ST>) -> Block<'block> {
                Block::I64Dense(block)
            }
        }


        #[cfg(feature = "block_128")]
        impl<'block> From<I128DenseBlock<'block, $ST>> for Block<'block> {
            fn from(block: I128DenseBlock<'block, $ST>) -> Block<'block> {
                Block::I128Dense(block)
            }
        }

        impl<'block> From<U8DenseBlock<'block, $ST>> for Block<'block> {
            fn from(block: U8DenseBlock<'block, $ST>) -> Block<'block> {
                Block::U8Dense(block)
            }
        }

        impl<'block> From<U16DenseBlock<'block, $ST>> for Block<'block> {
            fn from(block: U16DenseBlock<'block, $ST>) -> Block<'block> {
                Block::U16Dense(block)
            }
        }

        impl<'block> From<U32DenseBlock<'block, $ST>> for Block<'block> {
            fn from(block: U32DenseBlock<'block, $ST>) -> Block<'block> {
                Block::U32Dense(block)
            }
        }

        impl<'block> From<U64DenseBlock<'block, $ST>> for Block<'block> {
            fn from(block: U64DenseBlock<'block, $ST>) -> Block<'block> {
                Block::U64Dense(block)
            }
        }


        #[cfg(feature = "block_128")]
        impl<'block> From<U128DenseBlock<'block, $ST>> for Block<'block> {
            fn from(block: U128DenseBlock<'block, $ST>) -> Block<'block> {
                Block::U128Dense(block)
            }
        }

        // Sparse


        impl<'block> From<I8SparseBlock<'block, $ST, $SI>> for Block<'block> {
            fn from(block: I8SparseBlock<'block, $ST, $SI>) -> Block<'block> {
                Block::I8Sparse(block)
            }
        }

        impl<'block> From<I16SparseBlock<'block, $ST, $SI>> for Block<'block> {
            fn from(block: I16SparseBlock<'block, $ST, $SI>) -> Block<'block> {
                Block::I16Sparse(block)
            }
        }

        impl<'block> From<I32SparseBlock<'block, $ST, $SI>> for Block<'block> {
            fn from(block: I32SparseBlock<'block, $ST, $SI>) -> Block<'block> {
                Block::I32Sparse(block)
            }
        }

        impl<'block> From<I64SparseBlock<'block, $ST, $SI>> for Block<'block> {
            fn from(block: I64SparseBlock<'block, $ST, $SI>) -> Block<'block> {
                Block::I64Sparse(block)
            }
        }

        #[cfg(feature = "block_128")]
        impl<'block> From<I128SparseBlock<'block, $ST, $SI>> for Block<'block> {
            fn from(block: I128SparseBlock<'block, $ST, $SI>) -> Block<'block> {
                Block::I128Sparse(block)
            }
        }

        impl<'block> From<U8SparseBlock<'block, $ST, $SI>> for Block<'block> {
            fn from(block: U8SparseBlock<'block, $ST, $SI>) -> Block<'block> {
                Block::U8Sparse(block)
            }
        }

        impl<'block> From<U16SparseBlock<'block, $ST, $SI>> for Block<'block> {
            fn from(block: U16SparseBlock<'block, $ST, $SI>) -> Block<'block> {
                Block::U16Sparse(block)
            }
        }

        impl<'block> From<U32SparseBlock<'block, $ST, $SI>> for Block<'block> {
            fn from(block: U32SparseBlock<'block, $ST, $SI>) -> Block<'block> {
                Block::U32Sparse(block)
            }
        }

        impl<'block> From<U64SparseBlock<'block, $ST, $SI>> for Block<'block> {
            fn from(block: U64SparseBlock<'block, $ST, $SI>) -> Block<'block> {
                Block::U64Sparse(block)
            }
        }

        #[cfg(feature = "block_128")]
        impl<'block> From<U128SparseBlock<'block, $ST, $SI>> for Block<'block> {
            fn from(block: U128SparseBlock<'block, $ST, $SI>) -> Block<'block> {
                Block::U128Sparse(block)
            }
        }

        // only for serialization/deserialization purposes

        #[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
        pub enum BlockType {
            I8Dense,
            I16Dense,
            I32Dense,
            I64Dense,
            #[cfg(feature = "block_128")]
            I128Dense,

            // Dense, Unsigned
            U8Dense,
            U16Dense,
            U32Dense,
            U64Dense,
            #[cfg(feature = "block_128")]
            U128Dense,

            // Sparse, Signed
            I8Sparse,
            I16Sparse,
            I32Sparse,
            I64Sparse,
            #[cfg(feature = "block_128")]
            I128Sparse,

            // Sparse, Unsigned
            U8Sparse,
            U16Sparse,
            U32Sparse,
            U64Sparse,
            #[cfg(feature = "block_128")]
            U128Sparse,
        }

        impl<'block, 'a> From<&'a Block<'block>> for BlockType {
            fn from(block: &Block) -> BlockType {
                use self::Block::*;

                match *block {
                    // Dense, Signed
                    I8Dense(..) => BlockType::I8Dense,
                    I16Dense(..) => BlockType::I16Dense,
                    I32Dense(..) => BlockType::I32Dense,
                    I64Dense(..) => BlockType::I64Dense,
                    #[cfg(feature = "block_128")]
                    I128Dense(..) => BlockType::I128Dense,

                    // Dense, Unsigned
                    U8Dense(..) => BlockType::U8Dense,
                    U16Dense(..) => BlockType::U16Dense,
                    U32Dense(..) => BlockType::U32Dense,
                    U64Dense(..) => BlockType::U64Dense,
                    #[cfg(feature = "block_128")]
                    U128Dense(..) => BlockType::U128Dense,

                    // Sparse, Signed
                    I8Sparse(..) => BlockType::I8Sparse,
                    I16Sparse(..) => BlockType::I16Sparse,
                    I32Sparse(..) => BlockType::I32Sparse,
                    I64Sparse(..) => BlockType::I64Sparse,
                    #[cfg(feature = "block_128")]
                    I128Sparse(..) => BlockType::I128Sparse,

                    // Sparse, Unsigned
                    U8Sparse(..) => BlockType::U8Sparse,
                    U16Sparse(..) => BlockType::U16Sparse,
                    U32Sparse(..) => BlockType::U32Sparse,
                    U64Sparse(..) => BlockType::U64Sparse,
                    #[cfg(feature = "block_128")]
                    U128Sparse(..) => BlockType::U128Sparse,
                }
            }
        }

        impl<'block, 'a> From<&'a RwLock<Block<'block>>> for BlockType {
            fn from(block: &RwLock<Block>) -> BlockType {
                (&*acquire!(read block)).into()
            }
        }

        impl<'block> PartialEq<BlockType> for TyBlock<'block> {
            fn eq(&self, rhs: &BlockType) -> bool {
                let self_ty: TyBlockType = self.into();
                let rhs_ty: TyBlockType = (*rhs).into();

                self_ty == rhs_ty
            }
        }

        impl From<BlockTypeMap> for TyBlockTypeMap {
            fn from(block_hmap: BlockTypeMap) -> TyBlockTypeMap {
                block_hmap.iter()
                .map(|(block_id, block_type)| {
                    (*block_id, (*block_type).into())
                })
                .collect::<BlockTypeMapTy>()
                .into()
            }
        }
    };

    ($ST: ty) => {
        block_impl!($ST, $ST);
    };
}

#[cfg(test)]
macro_rules! map_block_type_variants {
    ($mac: ident $(, $arg: ident),* $(,)*) => {
        (|| {
            #[cfg(not(feature = "block_128"))]
            return $mac!($($arg,)* I8Dense,
                                    I16Dense,
                                    I32Dense,
                                    I64Dense,
                                    U8Dense,
                                    U16Dense,
                                    U32Dense,
                                    U64Dense,
                                    I8Sparse,
                                    I16Sparse,
                                    I32Sparse,
                                    I64Sparse,
                                    U8Sparse,
                                    U16Sparse,
                                    U32Sparse,
                                    U64Sparse);

            #[cfg(feature = "block_128")]
            return $mac!($($arg,)* I8Dense,
                                    I16Dense,
                                    I32Dense,
                                    I64Dense,
                                    I128Dense,
                                    U8Dense,
                                    U16Dense,
                                    U32Dense,
                                    U64Dense,
                                    U128Dense,
                                    I8Sparse,
                                    I16Sparse,
                                    I32Sparse,
                                    I64Sparse,
                                    I128Sparse,
                                    U8Sparse,
                                    U16Sparse,
                                    U32Sparse,
                                    U64Sparse,
                                    U128Sparse);
        })()
    }
}

pub(crate) type I8DenseBlock<'block, S> = DenseNumericBlock<'block, i8, S>;
pub(crate) type I16DenseBlock<'block, S> = DenseNumericBlock<'block, i16, S>;
pub(crate) type I32DenseBlock<'block, S> = DenseNumericBlock<'block, i32, S>;
pub(crate) type I64DenseBlock<'block, S> = DenseNumericBlock<'block, i64, S>;

#[cfg(feature = "block_128")]
pub(crate) type I128DenseBlock<'block, S> = DenseNumericBlock<'block, i128, S>;

pub(crate) type U8DenseBlock<'block, S> = DenseNumericBlock<'block, u8, S>;
pub(crate) type U16DenseBlock<'block, S> = DenseNumericBlock<'block, u16, S>;
pub(crate) type U32DenseBlock<'block, S> = DenseNumericBlock<'block, u32, S>;
pub(crate) type U64DenseBlock<'block, S> = DenseNumericBlock<'block, u64, S>;

#[cfg(feature = "block_128")]
pub(crate) type U128DenseBlock<'block, S> = DenseNumericBlock<'block, u128, S>;


pub(crate) type I8SparseBlock<'block, ST, SI> = SparseIndexedNumericBlock<'block, i8, ST, SI>;
pub(crate) type I16SparseBlock<'block, ST, SI> = SparseIndexedNumericBlock<'block, i16, ST, SI>;
pub(crate) type I32SparseBlock<'block, ST, SI> = SparseIndexedNumericBlock<'block, i32, ST, SI>;
pub(crate) type I64SparseBlock<'block, ST, SI> = SparseIndexedNumericBlock<'block, i64, ST, SI>;

#[cfg(feature = "block_128")]
pub(crate) type I128SparseBlock<'block, ST, SI> = SparseIndexedNumericBlock<'block, i128, ST, SI>;

pub(crate) type U8SparseBlock<'block, ST, SI> = SparseIndexedNumericBlock<'block, u8, ST, SI>;
pub(crate) type U16SparseBlock<'block, ST, SI> = SparseIndexedNumericBlock<'block, u16, ST, SI>;
pub(crate) type U32SparseBlock<'block, ST, SI> = SparseIndexedNumericBlock<'block, u32, ST, SI>;
pub(crate) type U64SparseBlock<'block, ST, SI> = SparseIndexedNumericBlock<'block, u64, ST, SI>;

#[cfg(feature = "block_128")]
pub(crate) type U128SparseBlock<'block, ST, SI> = SparseIndexedNumericBlock<'block, u128, ST, SI>;
