use error::*;

use rayon::prelude::*;

use std::path::Path;
use std::marker::PhantomData;
use std::fmt::Debug;

use storage::Storage;
use ty::{ToTimestampMicros, Timestamp};

use block::{BlockData, IndexRef, IndexMut, BufferHead};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DenseIndex;

#[derive(Debug)]
pub struct DenseNumericBlock<'block, T: 'block + Debug, S: 'block +
Storage<'block, T>> {
    storage: S,
    /// the tip of the buffer
    head: usize,
    base: PhantomData<&'block [T]>,
}

impl<'block, T: 'block + Debug, S: 'block + Storage<'block, T>>
DenseNumericBlock<'block, T, S> {
    pub fn new(mut storage: S) -> Result<DenseNumericBlock<'block, T, S>> {

        Ok(DenseNumericBlock {
            storage,
            head: 0,
            base: PhantomData,
        })
    }
}

impl<'block, T: 'block + Debug, S: 'block + Storage<'block, T>> BufferHead
    for DenseNumericBlock<'block, T, S> {
    fn head(&self) -> usize {
        self.head
    }

    fn mut_head(&mut self) -> &mut usize {
        &mut self.head
    }
}

impl<'block, T: 'block + Debug, S: 'block + Storage<'block, T>>
BlockData<'block, T, DenseIndex>
    for DenseNumericBlock<'block, T, S> {
}

impl<'block, T: 'block + Debug, S: 'block + Storage<'block, T>> AsRef<[T]>
    for DenseNumericBlock<'block, T, S> {
    fn as_ref(&self) -> &[T] {
        self.storage.as_ref()
    }
}

impl<'block, T: 'block + Debug, S: 'block + Storage<'block, T>> AsMut<[T]>
    for DenseNumericBlock<'block, T, S> {
    fn as_mut(&mut self) -> &mut [T] {
        self.storage.as_mut()
    }
}

impl<'block, T, ST> IndexRef<[DenseIndex]> for DenseNumericBlock<'block, T, ST>
where
    T: 'block + Debug,
    ST: 'block + Storage<'block, T>,
{
    fn as_ref_index(&self) -> &[DenseIndex] {
        &[][..]
    }
}

impl<'block, T, ST> IndexMut<[DenseIndex]> for DenseNumericBlock<'block, T, ST>
where
    T: 'block + Debug,
    ST: 'block + Storage<'block, T>,
{
    fn as_mut_index(&mut self) -> &mut [DenseIndex] {
        &mut [][..]
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    use block::tests::BLOCK_FILE_SIZE;


    #[cfg(feature = "block_128")]
    #[macro_use]
    mod block_128 {
        macro_rules! dense_block_128_impl {
            ($T: tt, $data: expr) => {{
                let data = $data;

                let mut block = DenseNumericBlock::<$T, _>::new(data)
                    .chain_err(|| "failed to create block")
                    .unwrap();

                let s = [!0, 0];
                let source = s.into_iter().cycle();

                let len = {
                    let mut data = block.as_mut_slice_append();
                    let len = data.len();
                    let source = source.clone().take(len).map(|v| *v).collect::<Vec<_>>();

                    data.copy_from_slice(&source);
                    len
                };

                assert_eq!(len * size_of::<$T>(), BLOCK_FILE_SIZE);

                block.set_written(len).unwrap();

                let data = block.as_slice();

                let source = source.take(len).map(|v| *v).collect::<Vec<_>>();

                assert_eq!(data, &source[..]);
            }};
        }
    }

    mod generic {
        use super::*;
        use num::{Integer, Float, Zero};
        use std::mem::size_of;
        use std::fmt::Debug;
        use std::ops::Not;
        use chrono::prelude::*;


        pub(super) fn block_ts<'block, S: 'block + Storage<'block, Timestamp>>(storage: S) {
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


        pub(super) fn block_t<'block, T, S>(storage: S)
        where
            T: 'block + Not<Output = T> + Integer + Zero + PartialEq + Copy + Debug,
            S: 'block + Storage<'block, T>,
        {
            let mut block = DenseNumericBlock::new(storage)
                .chain_err(|| "failed to create block")
                .unwrap();

            let s = [!T::zero(), T::zero()];
            let source = s.into_iter().cycle();

            let len = {
                let mut data = block.as_mut_slice_append();
                let len = data.len();
                let source = source.clone().take(len).map(|v| *v).collect::<Vec<_>>();

                data.copy_from_slice(&source);
                len
            };

            assert_eq!(len * size_of::<T>(), BLOCK_FILE_SIZE);

            block.set_written(len).unwrap();

            let data = block.as_slice();

            let source = source.take(len).map(|v| *v).collect::<Vec<_>>();

            assert_eq!(data, &source[..]);
        }

        pub(super) fn block_tf<'block, T, S>(storage: S)
        where
            T: 'block + Float + Zero + PartialEq + Copy + Debug,
            S: 'block + Storage<'block, T>,
        {
            let mut block = DenseNumericBlock::new(storage)
                .chain_err(|| "failed to create block")
                .unwrap();

            let s = [T::max_value(), T::zero()];
            let source = s.into_iter().cycle();

            let len = {
                let mut data = block.as_mut_slice_append();
                let len = data.len();
                let source = source.clone().take(len).map(|v| *v).collect::<Vec<_>>();

                data.copy_from_slice(&source);
                len
            };

            assert_eq!(len * size_of::<T>(), BLOCK_FILE_SIZE);

            block.set_written(len).unwrap();

            let data = block.as_slice();

            let source = source.take(len).map(|v| *v).collect::<Vec<_>>();

            assert_eq!(data, &source[..]);
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
        fn block_ts() {
            super::generic::block_ts(make_storage());
        }

        #[cfg(feature = "block_128")]
        #[test]
        fn block_u128() {
            dense_block_128_impl!(u128, make_storage());
        }

        #[test]
        fn block_u64() {
            super::generic::block_t::<u64, _>(make_storage());
        }

        #[test]
        fn block_u32() {
            super::generic::block_t::<u32, _>(make_storage());
        }

        #[test]
        fn block_u16() {
            super::generic::block_t::<u16, _>(make_storage());
        }

        #[test]
        fn block_u8() {
            super::generic::block_t::<u8, _>(make_storage());
        }

        #[cfg(feature = "block_128")]
        #[test]
        fn block_i128() {
            dense_block_128_impl!(i128, make_storage());
        }

        #[test]
        fn block_i64() {
            super::generic::block_t::<i64, _>(make_storage());
        }

        #[test]
        fn block_i32() {
            super::generic::block_t::<i32, _>(make_storage());
        }

        #[test]
        fn block_i16() {
            super::generic::block_t::<i16, _>(make_storage());
        }

        #[test]
        fn block_i8() {
            super::generic::block_t::<i8, _>(make_storage());
        }

        #[test]
        fn block_f32() {
            super::generic::block_tf::<f32, _>(make_storage());
        }

        #[test]
        fn block_f64() {
            super::generic::block_tf::<f64, _>(make_storage());
        }
    }

    #[cfg(feature = "mmap")]
    mod mmap {
        use super::*;
        use storage::mmap::MemmapStorage;

        fn make_storage(name: &str) -> MemmapStorage {
            let (_dir, file) = tempfile!(name);

            MemmapStorage::new(file, BLOCK_FILE_SIZE)
                .chain_err(|| "failed to create memory storage")
                .unwrap()
        }

        #[test]
        fn block_ts() {
            super::generic::block_ts(make_storage("dense_ts"));
        }

        #[cfg(feature = "block_128")]
        #[test]
        fn block_u128() {
            dense_block_128_impl!(u128, make_storage("dense_u128"));
        }

        #[test]
        fn block_u64() {
            super::generic::block_t::<u64, _>(make_storage("dense_u64"));
        }

        #[test]
        fn block_u32() {
            super::generic::block_t::<u32, _>(make_storage("dense_u32"));
        }

        #[test]
        fn block_u16() {
            super::generic::block_t::<u16, _>(make_storage("dense_u16"));
        }

        #[test]
        fn block_u8() {
            super::generic::block_t::<u8, _>(make_storage("dense_u8"));
        }

        #[cfg(feature = "block_128")]
        #[test]
        fn block_i128() {
            dense_block_128_impl!(i128, make_storage("dense_i128"));
        }

        #[test]
        fn block_i64() {
            super::generic::block_t::<i64, _>(make_storage("dense_i64"));
        }

        #[test]
        fn block_i32() {
            super::generic::block_t::<i32, _>(make_storage("dense_i32"));
        }

        #[test]
        fn block_i16() {
            super::generic::block_t::<i16, _>(make_storage("dense_i16"));
        }

        #[test]
        fn block_i8() {
            super::generic::block_t::<i8, _>(make_storage("dense_i8"));
        }

        #[test]
        fn block_f32() {
            super::generic::block_tf::<f32, _>(make_storage("dense_f32"));
        }

        #[test]
        fn block_f64() {
            super::generic::block_tf::<f64, _>(make_storage("dense_f64"));
        }
    }
}
