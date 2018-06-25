use error::*;
use std::fmt::Debug;
use extprim::u128::u128;

#[macro_use]
mod macros;
mod numeric;
mod string;
mod relative;
pub(crate) mod index;

pub(crate) use self::numeric::{DenseNumericBlock, SparseIndexedNumericBlock};
pub(crate) use self::string::DenseStringBlock;
pub use self::numeric::SparseIndex;
pub(crate) use self::relative::RelativeSlice;
pub use self::index::ColumnIndexType;


// This will probably get merged into BlockData

pub trait BufferHead {
    #[inline]
    fn head(&self) -> usize;

    #[inline]
    fn mut_head(&mut self) -> &mut usize;

    #[inline]
    fn pool_head(&self) -> Option<usize> {
        None
    }

    #[inline]
    fn set_pool_head(&mut self, _head: usize) {
        unimplemented!()
    }

    #[inline]
    fn is_pooled(&self) -> bool {
        self.pool_head().is_some()
    }
}

pub trait IndexRef<T>
where
    T: ?Sized,
{
    fn as_ref_index(&self) -> &T;
}

pub trait IndexMut<T>
where
    T: ?Sized,
{
    fn as_mut_index(&mut self) -> &mut T;
}

pub(crate) trait SliceOffset {

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn to_slice<'buffer, T>(&self, buffer: &'buffer [T]) -> &'buffer [T];
    fn to_str<'buffer>(&self, buffer: &'buffer [u8]) -> &'buffer str;

}


/// Base trait for all Blocks
///
/// For now we use the safest approach to handling data
/// via AsRef and AsMut for every access
/// This adds some overhead, but will serve as a good basis for refactoring
/// and comparisons in benchmarks

pub trait BlockData<'block, T: 'block, I: 'block>
    : BufferHead + AsRef<[T]> + AsMut<[T]> + IndexRef<[I]> + IndexMut<[I]> + Debug
    {
    // data only

    #[inline]
    fn as_slice(&self) -> &[T] {
        let head = self.head();
        &self.as_ref()[..head]
    }

    #[inline]
    fn as_mut_slice(&mut self) -> &mut [T] {
        let head = self.head();
        &mut self.as_mut()[..head]
    }

    #[inline]
    fn as_mut_slice_append(&mut self) -> &mut [T] {
        let head = self.head();
        &mut self.as_mut()[head..]
    }

    // index only

    #[inline]
    fn as_index_slice(&self) -> &[I] {
        let head = self.head();
        &self.as_ref_index()[..head]
    }

    #[inline]
    fn as_mut_index_slice(&mut self) -> &mut [I] {
        let head = self.head();
        &mut self.as_mut_index()[..head]
    }

    #[inline]
    fn as_mut_index_slice_append(&mut self) -> &mut [I] {
        let head = self.head();
        &mut self.as_mut_index()[head..]
    }

    // both data and index

    #[inline]
    fn as_indexed_slice(&self) -> (&[I], &[T]) {
        let head = self.head();
        (&self.as_ref_index()[..head], &self.as_ref()[..head])
    }

    #[inline]
    fn as_mut_indexed_slice(&mut self) -> (&mut [I], &mut [T]) {
        let head = self.head();

        (&mut [][..], &mut self.as_mut()[..head])
    }

    #[inline]
    fn as_mut_indexed_slice_append(&mut self) -> (&mut [I], &mut [T]) {
        let head = self.head();

        (&mut [][..], &mut self.as_mut()[head..])
    }

    #[inline]
    fn is_indexed() -> bool {
        false
    }

    /// The length of valid data buffer
    ///
    /// This should be interpreted as self.data[0..self.head]
    #[inline]
    fn len(&self) -> usize {
        self.head()
    }

    /// The length of the whole data buffer
    ///
    /// This should be interpreted as self.data[..]

    #[inline]
    fn size(&self) -> usize {
        self.as_ref().len()
    }

    /// The length of the free data buffer
    ///
    /// This should be interpreted as self.data[self.head..]

    #[inline]
    fn free_len(&self) -> usize {
        self.size().saturating_sub(self.len())
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    #[inline]
    fn size_of() -> usize {
        ::std::mem::size_of::<T>()
    }

    #[inline]
    fn set_written(&mut self, count: usize) -> Result<()> {
        let size = self.size();
        let head = self.mut_head();

        if count <= size {
            *head += count;
            Ok(())
        } else {
            // TODO: migrate to proper ErrorKind
            Err(err_msg("by_count exceeds current head"))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, EnumString)]
pub enum BlockType {
    I8Dense,
    I16Dense,
    I32Dense,
    I64Dense,
    I128Dense,

    // Dense, Unsigned
    U8Dense,
    U16Dense,
    U32Dense,
    U64Dense,
    U128Dense,

    // UTF-8 String
    StringDense,

    // Sparse, Signed
    I8Sparse,
    I16Sparse,
    I32Sparse,
    I64Sparse,
    I128Sparse,

    // Sparse, Unsigned
    U8Sparse,
    U16Sparse,
    U32Sparse,
    U64Sparse,
    U128Sparse,
}

impl BlockType {
    #[inline]
    pub fn size_of(&self) -> usize {
        use std::mem::size_of;
        use self::BlockType::*;

        match *self {
            I8Dense | U8Dense | I8Sparse | U8Sparse => size_of::<u8>(),
            I16Dense | U16Dense | I16Sparse | U16Sparse => size_of::<u16>(),
            I32Dense | U32Dense | I32Sparse | U32Sparse => size_of::<u32>(),
            I64Dense | U64Dense | I64Sparse | U64Sparse => size_of::<u64>(),
            I128Dense | U128Dense | I128Sparse | U128Sparse => size_of::<u128>(),
            StringDense => size_of::<RelativeSlice>(),
        }
    }

    #[inline]
    pub fn is_sparse(&self) -> bool {
        use self::BlockType::*;

        match *self {
            I8Dense | U8Dense |
            I16Dense | U16Dense |
            I32Dense | U32Dense |
            I64Dense | U64Dense => false,
            I128Dense | U128Dense => false,

            StringDense => false,

            I8Sparse | U8Sparse |
            I16Sparse | U16Sparse |
            I32Sparse | U32Sparse |
            I64Sparse | U64Sparse  => true,
            I128Sparse | U128Sparse => true,
        }
    }

    #[inline]
    pub fn is_pooled(&self) -> bool {
        use self::BlockType::*;

        match *self {
            I8Dense | U8Dense |
            I16Dense | U16Dense |
            I32Dense | U32Dense |
            I64Dense | U64Dense => false,
            I128Dense | U128Dense => false,

            StringDense => true,

            I8Sparse | U8Sparse |
            I16Sparse | U16Sparse |
            I32Sparse | U32Sparse |
            I64Sparse | U64Sparse  => false,
            I128Sparse | U128Sparse => false,
        }
    }
}
