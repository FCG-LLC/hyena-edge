use block::{DenseNumericBlock, SparseIndexedNumericBlock};
use storage::Storage;


macro_rules! numeric_block_impl {
    ($ST: ty, $SI: ty) => {
        use ty::block::numeric::*;

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
    };

    ($ST: ty) => {
        numeric_block_impl!($ST, $ST);
    };
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
