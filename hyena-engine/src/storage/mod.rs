use crate::error::*;
use std::fmt::Debug;


#[cfg(feature = "mmap")]
pub(crate) mod mmap;
pub(crate) mod memory;

pub(crate) mod manager;


pub trait Storage<'stor, T: 'stor>: AsRef<[T]> + AsMut<[T]> + Debug {
    fn sync(&mut self) -> Result<()>;

    fn as_ptr(&self) -> *const T {
        self.as_ref().as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut T {
        self.as_mut().as_mut_ptr()
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }
}

pub trait ByteStorage<'stor>: Storage<'stor, u8> {
    fn size(&self) -> usize {
        self.as_ref().len()
    }
}

pub trait Realloc: Sized {
    /// Realloc (grow/shrink) the storage
    fn realloc(self, size: usize) -> Result<Self>;

    fn realloc_size(&self) -> usize;

    /// Realloc to ensure that `size` will fit
    ///
    /// It's safe to call this function even if `size` already fits

    #[inline]
    fn realloc_for(self, used: usize, needed: usize) -> Result<Self> {
        let len = self.realloc_size();
        let free = len.saturating_sub(used);

        if free < needed {
            // we need to realloc
            // use the simplest grow strategy, i.e. double the space

            let to_alloc = needed - free;
            let to_alloc = to_alloc / len + if to_alloc % len != 0 { 1 } else { 0 };
            let to_alloc = to_alloc * len;
            let to_alloc = to_alloc + len;

            self.realloc(to_alloc)
        } else {
            Ok(self)
        }
    }

    #[inline]
    fn needs_realloc(&self, used: usize, needed: usize) -> bool {
        let len = self.realloc_size();
        let free = len.saturating_sub(used);

        free < needed
    }
}
