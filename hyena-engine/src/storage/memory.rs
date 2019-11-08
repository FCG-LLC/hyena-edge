use crate::error::*;
use super::{Storage, Realloc, ByteStorage};
use hyena_common::map_type::{map_type, map_type_mut};
use std::mem::{size_of, zeroed};
use std::intrinsics::copy_nonoverlapping;
use std::marker::PhantomData;
use std::fmt;
use std::fmt::Debug;


const PAGE_SIZE: usize = 1 << 12;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct MemoryStorage<Align: Zero + Clone> {
    data: Vec<Align>,
}

impl<Align: Zero + Debug + Copy + Clone + PartialEq> MemoryStorage<Align> {
    pub fn new(size: usize) -> Result<MemoryStorage<Align>> {
        assert_eq!(size % size_of::<Align>(), 0);

        Ok(MemoryStorage {
            data: vec![Align::zero(); size / size_of::<Align>()],
        })
    }

    pub fn len(&self) -> usize {
        self.data.len() * size_of::<Align>()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<'stor, T: 'stor, Align: Zero + Debug + Copy + Clone + PartialEq> Storage<'stor, T>
    for MemoryStorage<Align> {
    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<'stor, Align: Zero + Debug + Copy + Clone + PartialEq> ByteStorage<'stor>
    for MemoryStorage<Align> {}

impl<T, Align: Zero + Debug + Copy + Clone + PartialEq> AsRef<[T]> for MemoryStorage<Align> {
    fn as_ref(&self) -> &[T] {
        map_type(&self.data, PhantomData)
    }
}

impl<T, Align: Zero + Debug + Copy + Clone + PartialEq> AsMut<[T]> for MemoryStorage<Align> {
    fn as_mut(&mut self) -> &mut [T] {
        map_type_mut(&mut self.data, PhantomData)
    }
}

impl<Align: Zero + Debug + Copy + Clone + PartialEq> Realloc for MemoryStorage<Align> {
    fn realloc(self, size: usize) -> Result<Self> {

        let mut new_storage = Self::new(size)?;

        let copy_size = ::std::cmp::min(size, self.realloc_size());

        new_storage.as_mut()[..copy_size]
            .copy_from_slice(&<MemoryStorage<Align> as AsRef<[u8]>>::as_ref(&self)[..copy_size]);

        Ok(new_storage)
    }

    fn realloc_size(&self) -> usize {
        <Self as ByteStorage>::size(self)
    }
}

pub trait Zero {
    fn zero() -> Self;
}

macro_rules! auto_impl_zero {
    ($($t: ty),*) => {
        $(impl Zero for $t {
            fn zero() -> $t {
                0
            }
        })*
    };
}

auto_impl_zero!(u8, u16, u32, u64);

impl Zero for Page {
    fn zero() -> Page {
        Page([0; size_of::<Page>()])
    }
}

#[derive(Copy)]
pub struct Page([u8; PAGE_SIZE]);

impl fmt::Debug for Page {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_list().entries(self.0.iter()).finish()
    }
}

impl Clone for Page {
    fn clone(&self) -> Page {
        let mut dst = std::mem::MaybeUninit::<Page>::uninit();
        unsafe {
            copy_nonoverlapping(self, dst.as_mut_ptr(), 1);
            dst.assume_init()
        }
    }
}

impl Default for Page {
    fn default() -> Page {
        unsafe {
            zeroed()
        }
    }
}

impl PartialEq<Page> for Page {
    #[inline]
    fn eq(&self, other: &Page) -> bool {
        self.0[..] == other.0[..]
    }
    #[inline]
    fn ne(&self, other: &Page) -> bool {
        self.0[..] != other.0[..]
    }
}

pub type PagedMemoryStorage = MemoryStorage<Page>;

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_BYTES_LEN: usize = 10;
    static TEST_BYTES: [u8; TEST_BYTES_LEN] = *b"hyena test";
    const BLOCK_SIZE: usize = 1 << 20; // 1 MiB

    fn create_block() -> PagedMemoryStorage {
        PagedMemoryStorage::new(BLOCK_SIZE)
            .with_context(|_| "failed to create PagedMemoryStorage")
            .unwrap()
    }

    mod align {
        use super::*;

        fn deref_aligned<T, M: AsRef<[T]>>(storage: &M) -> &[T] {
            storage.as_ref()
        }

        #[allow(non_snake_case)]
        fn as_T<T>() {
            deref_aligned::<T, _>(&create_block());
        }

        #[test]
        fn as_u8() {
            as_T::<u8>()
        }

        #[test]
        fn as_u16() {
            as_T::<u16>()
        }

        #[test]
        fn as_u32() {
            as_T::<u32>()
        }

        #[test]
        fn as_u64() {
            as_T::<u64>()
        }
    }

    #[test]
    fn it_can_write() {
        let mut storage = create_block();

        &storage.as_mut()[..TEST_BYTES_LEN].copy_from_slice(&TEST_BYTES[..]);

        assert_eq!(
            &<PagedMemoryStorage as AsRef<[u8]>>::as_ref(&storage)[..TEST_BYTES_LEN],
            &TEST_BYTES[..]
        );

        assert_eq!(
            <PagedMemoryStorage as AsRef<[u8]>>::as_ref(&storage)[..].len(),
            BLOCK_SIZE
        );
    }

    #[test]
    fn it_shrinks_block() {
        let storage = create_block();

        let mut storage = storage.realloc(BLOCK_SIZE / 2)
            .with_context(|_| "failed to shrink PagedMemoryStorage")
            .unwrap();

        &storage.as_mut()[..TEST_BYTES_LEN].copy_from_slice(&TEST_BYTES[..]);

        assert_eq!(
            &<PagedMemoryStorage as AsRef<[u8]>>::as_ref(&storage)[..TEST_BYTES_LEN],
            &TEST_BYTES[..]
        );

        assert_eq!(
            <PagedMemoryStorage as AsRef<[u8]>>::as_ref(&storage)[..].len(),
            BLOCK_SIZE / 2
        );
    }

    #[test]
    fn it_grows_block() {
        let storage = create_block();

        let mut storage = storage.realloc(BLOCK_SIZE * 2)
            .with_context(|_| "failed to shrink PagedMemoryStorage")
            .unwrap();

        &storage.as_mut()[..TEST_BYTES_LEN].copy_from_slice(&TEST_BYTES[..]);

        assert_eq!(
            &<PagedMemoryStorage as AsRef<[u8]>>::as_ref(&storage)[..TEST_BYTES_LEN],
            &TEST_BYTES[..]
        );

        assert_eq!(
            <PagedMemoryStorage as AsRef<[u8]>>::as_ref(&storage)[..].len(),
            BLOCK_SIZE * 2
        );
    }
}
