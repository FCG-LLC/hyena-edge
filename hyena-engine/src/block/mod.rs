use error::*;

use rayon::prelude::*;

use std::path::Path;
use std::marker::PhantomData;
use std::fmt::Debug;

use storage::Storage;

mod numeric;

pub(crate) use self::numeric::{DenseNumericBlock, SparseIndex, SparseIndexedNumericBlock,
                               SparseNumericBlock};

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
    : BufferHead + AsRef<[T]> + AsMut<[T]> + IndexRef<[I]> + IndexMut<[I]> + Debug
    {
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
    #[inline]
    fn len(&self) -> usize {
        self.head()
    }

    #[inline]
    fn size(&self) -> usize {
        self.as_ref().len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    #[inline]
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
