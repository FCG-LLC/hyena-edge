use error::*;
use super::Storage;
use super::map_type::{map_type, map_type_mut};
use std::mem::{size_of, uninitialized};
use std::intrinsics::copy_nonoverlapping;
use std::marker::PhantomData;
use std::fmt;


#[derive(Debug)]
pub struct MemoryStorage<Align: Zero + Clone> {
    data: Vec<Align>,
}

impl<Align: Zero + Clone> MemoryStorage<Align> {
    pub fn new(size: usize) -> Result<MemoryStorage<Align>> {
        assert_eq!(size % size_of::<Align>(), 0);

        Ok(MemoryStorage {
            data: vec![Align::zero(); size / size_of::<Align>()],
        })
    }
}

impl<'stor, T: 'stor, Align: Zero + Clone> Storage<'stor, T> for MemoryStorage<Align> {
    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<T, Align: Zero + Clone> AsRef<[T]> for MemoryStorage<Align> {
    fn as_ref(&self) -> &[T] {
        map_type(&self.data, PhantomData)
    }
}

impl<T, Align: Zero + Clone> AsMut<[T]> for MemoryStorage<Align> {
    fn as_mut(&mut self) -> &mut [T] {
        map_type_mut(&mut self.data, PhantomData)
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
        // size should be replaced with size_of::<Page>()
        // after const fn is stabilized
        Page([0; 1 << 12])
    }
}

#[derive(Copy)]
pub struct Page([u8; 1 << 12]);

impl fmt::Debug for Page {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_list().entries(self.0.iter()).finish()
    }
}

impl Clone for Page {
    fn clone(&self) -> Page {
        unsafe {
            let mut dst = uninitialized::<Page>();
            copy_nonoverlapping(self, &mut dst, 1);
            dst
        }
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
            .chain_err(|| "failed to create PagedMemoryStorage")
            .unwrap()
    }

    mod align {
        use super::*;

        fn deref_aligned<T, M: AsRef<[T]>>(storage: &M) -> &[T] {
            storage.as_ref()
        }

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
}
