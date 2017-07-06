use error::*;

use rayon::prelude::*;

use std::path::Path;
use std::marker::PhantomData;

use storage::Storage;

mod numeric;

pub(crate) use self::numeric::{DenseNumericBlock, SparseNumericBlock, SparseIndexedNumericBlock};

// This will probably get merged into BlockData

pub trait BufferHead {
    #[inline]
    fn head(&self) -> usize;

    #[inline]
    fn mut_head(&mut self) -> &mut usize;
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



/// Base trait for all Blocks
///
/// For now we use the safest approach to handling data
/// via AsRef and AsMut for every access
/// This adds some overhead, but will serve as a good basis for refactoring
/// and comparisons in benchmarks

pub trait BlockData<'block, T: 'block, I: 'block>
    : BufferHead + AsRef<[T]> + AsMut<[T]> + IndexRef<[I]> + IndexMut<[I]> {
    // data only

    fn as_slice(&self) -> &[T] {
        let head = self.head();
        &self.as_ref()[..head]
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        let head = self.head();
        &mut self.as_mut()[..head]
    }

    fn as_mut_slice_append(&mut self) -> &mut [T] {
        let head = self.head();
        &mut self.as_mut()[head..]
    }

    // index only

    fn as_index_slice(&self) -> &[I] {
        let head = self.head();
        &self.as_ref_index()[..head]
    }

    fn as_mut_index_slice(&mut self) -> &mut [I] {
        let head = self.head();
        &mut self.as_mut_index()[..head]
    }

    fn as_mut_index_slice_append(&mut self) -> &mut [I] {
        let head = self.head();
        &mut self.as_mut_index()[head..]
    }

    // both data and index

    fn as_indexed_slice(&self) -> (&[I], &[T]) {
        let head = self.head();
        (&self.as_ref_index()[..head], &self.as_ref()[..head])
    }

    fn as_mut_indexed_slice(&mut self) -> (&mut [I], &mut [T]) {
        let head = self.head();

        (&mut [][..], &mut self.as_mut()[..head])
    }

    fn as_mut_indexed_slice_append(&mut self) -> (&mut [I], &mut [T]) {
        let head = self.head();

        (&mut [][..], &mut self.as_mut()[head..])
    }

    fn is_indexed() -> bool {
        false
    }

    /// The length of valid data buffer
    ///
    /// This should be interpreted as self.data[0..self.head]
    fn len(&self) -> usize {
        self.head()
    }

    fn size(&self) -> usize {
        self.as_ref().len()
    }

    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    fn set_written(&mut self, count: usize) -> Result<()> {
        let size = self.size();
        let mut head = self.mut_head();

        if count <= size {
            *head += count;
            Ok(())
        } else {
            // TODO: migrate to proper ErrorKind
            Err("by_count exceeds current head".into())
        }
    }
}




// the minimal requirements for the "schema" are primary, timestamp-based key
// and a secondary, source, u32

// secondary partitioning key is hardcoded for now

pub struct Catalog {
    columns: Vec<Column>,
}

// impl Catalog {
//     pub fn new<P: AsRef<Path>>(location: P) -> Catalog {
//
//
//
//     }
// }

pub type Offset = u32;

// pub enum Block<'block> {
//     Key(DenseNumericBlock<'block, TimestampKey, MemmapStorage>),
//     U64Dense(DenseNumericBlock<'block, u64>),
//     U64Sparse(SparseNumericBlock<'block, u64>),
//     U32Sparse(SparseNumericBlock<'block, u32>),
// }

pub struct Column {
    name: String,
}

pub struct SecondaryKey {}

// pub struct Partition<'part> {
//     min_key: TimestampKey,
//     max_key: TimestampKey,
//     blocks: Vec<Block<'part>>,
// }

#[derive(Debug, Clone, PartialEq)]
pub enum ScanComparison {
    Lt,
    LtEq,
    Eq,
    GtEq,
    Gt,
    NotEq,
}

#[cfg(test)]
mod tests {
    pub(crate) const BLOCK_FILE_SIZE: usize = 1 << 20; // 1 MiB

}
