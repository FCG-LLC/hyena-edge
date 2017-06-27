use error::*;

use chrono::prelude::*;

use rayon::prelude::*;

use std::path::Path;
use std::marker::PhantomData;

use storage::Storage;
use storage::mmap::MemmapStorage;

pub trait BufferHead {
    #[inline]
    fn head(&self) -> usize;

    #[inline]
    fn mut_head(&mut self) -> &mut usize;
}

pub trait BlockData<'block, T: 'block>: BufferHead + AsRef<[T]> + AsMut<[T]> {
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

        if count < size {
            *head += count;
            Ok(())
        } else {
            // TODO: migrate to proper ErrorKind
            Err("by_count exceeds current head".into())
        }
    }
}


#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimestampKey(u64);

impl From<TimestampKey> for u64 {
    fn from(source: TimestampKey) -> u64 {
        source.0
    }
}

impl From<u64> for TimestampKey {
    fn from(source: u64) -> TimestampKey {
        TimestampKey(source)
    }
}

pub trait ToTimestampMicros {
    fn to_timestamp_micros(&self) -> u64;
}

impl<T: TimeZone> ToTimestampMicros for DateTime<T> {
    fn to_timestamp_micros(&self) -> u64 {
        (self.timestamp() * 1_000_000 + self.timestamp_subsec_micros() as i64) as u64
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

pub enum Block<'block> {
    Key(DenseNumericBlock<'block, TimestampKey, MemmapStorage>),
//     U64Dense(DenseNumericBlock<'block, u64>),
//     U64Sparse(SparseNumericBlock<'block, u64>),
//     U32Sparse(SparseNumericBlock<'block, u32>),
}

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

pub struct DenseNumericBlock<'block, T: 'block, S: 'block + Storage<'block, T>> {
    storage: S,
    //     /// whole mmaped buffer with proper lifetime attached
    //     data: &'block mut [T],
    /// the tip of the buffer
    head: usize,
    base: PhantomData<&'block [T]>,
}

impl<'block, T: 'block, S: 'block + Storage<'block, T>> DenseNumericBlock<'block, T, S> {
    pub fn new(mut storage: S) -> Result<DenseNumericBlock<'block, T, S>> {

        // We explicitly mark this slice as living long enough
        // because we are not doing reallocations anywhere within storage impls
        // and because storage allocates on heap or via mmap
        // and we're not Clone
        // also, we shouldn't touch storage mutably from now on
        // (except sync())

        Ok(DenseNumericBlock {
            storage,
            head: 0,
            base: PhantomData,
        })
    }
}

impl<'block, T: 'block, S: 'block + Storage<'block, T>> BufferHead
    for DenseNumericBlock<'block, T, S> {
    fn head(&self) -> usize {
        self.head
    }

    fn mut_head(&mut self) -> &mut usize {
        &mut self.head
    }
}

impl<'block, T: 'block, S: 'block + Storage<'block, T>> BlockData<'block, T>
    for DenseNumericBlock<'block, T, S> {
}

impl<'block, T: 'block, S: 'block + Storage<'block, T>> AsRef<[T]>
    for DenseNumericBlock<'block, T, S> {
    fn as_ref(&self) -> &[T] {
        self.storage.as_ref()
    }
}

impl<'block, T: 'block, S: 'block + Storage<'block, T>> AsMut<[T]>
    for DenseNumericBlock<'block, T, S> {
    fn as_mut(&mut self) -> &mut [T] {
        self.storage.as_mut()
    }
}


// pub struct SparseNumericBlock<'block, T: 'block> {
//     mmap: MmapMut,
//     data: &'block mut [T],
// }
//
// impl<'block, T> SparseNumericBlock<'block, T> {
//     pub fn new<P: AsRef<Path>>(data: P,
//                                block_size: usize)
//                                -> Result<SparseNumericBlock<'block, T>> {
//         let mut mmap = map_file(data, block_size)
//             .chain_err(|| "unable to mmap block file")?;
//         let data = map_type(&mut mmap, PhantomData);
//
//         Ok(SparseNumericBlock { mmap, data })
//     }
//
//     pub fn len(&self) -> usize {
//         self.data.len()
//     }
//
//     pub fn is_empty(&self) -> bool {
//         self.data.is_empty()
//     }
// }
//
// impl<'block, T> BlockData<'block, T> for SparseNumericBlock<'block, T> {
//     fn as_slice(&'block self) -> &'block [T] {
//         self.data
//     }
//
//     fn as_slice_mut(&'block mut self) -> &'block mut [T] {
//         self.data
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    const BLOCK_FILE_SIZE: usize = 1 << 20; // 1 MiB

    mod dense {
        use super::*;

        mod generic {
            use super::*;

            pub(super) fn block_64<'block, S: 'block + Storage<'block, TimestampKey>>(storage: S) {
                let mut block = DenseNumericBlock::new(storage)
                    .chain_err(|| "failed to create block")
                    .unwrap();

                let d1 = Utc::now().to_timestamp_micros().into();
                let d2 = (Utc::now().to_timestamp_micros() + 5_000_000).into();

                {
                    let mut data = block.as_mut_slice_append();

                    data[0] = d1;
                    data[1] = d2;
                }

                block.set_written(2).unwrap();


                let data = block.as_slice();

                assert_eq!(data[0], d1);
                assert_eq!(data[1], d2);
            }
        }

        mod memory {
            use super::*;
            use storage::memory::PagedMemoryStorage;

            fn make_storage() -> PagedMemoryStorage {
                PagedMemoryStorage::new(BLOCK_FILE_SIZE)
                    .chain_err(|| "failed to create memory storage")
                    .unwrap()
            }

            #[test]
            fn block_64() {
                super::generic::block_64(make_storage());
            }
        }

        #[cfg(feature = "mmap")]
        mod mmap {
            use super::*;
            use storage::mmap::MemmapStorage;

            fn make_storage() -> MemmapStorage {
                let (_dir, file) = tempfile!(persistent);

                MemmapStorage::new(file, BLOCK_FILE_SIZE)
                    .chain_err(|| "failed to create memory storage")
                    .unwrap()
            }

            #[test]
            fn block_64() {
                super::generic::block_64(make_storage());
            }
        }
    }

    mod sparse {
        use super::*;

    }

}
