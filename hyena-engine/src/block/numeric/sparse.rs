use error::*;
use std::marker::PhantomData;
use std::fmt::Debug;
use storage::Storage;
use block::{BlockData, BufferHead, IndexMut, IndexRef};

pub type SparseIndex = u32;

pub type SparseIndexedNumericBlock<'block, T, ST, SI> = SparseNumericBlock<
    'block,
    T,
    SparseIndex,
    ST,
    SI,
>;

#[derive(Debug)]
pub struct SparseNumericBlock<'block, T, I, ST, SI>
where
    T: 'block + Debug,
    I: 'block + Debug,
    ST: 'block + Storage<'block, T>,
    SI: 'block + Storage<'block, I>,
{
    storage: ST,
    index: SI,
    /// the tip of the buffer
    head: usize,
    _storage_marker: PhantomData<&'block [T]>,
    _index_marker: PhantomData<&'block [I]>,
}

impl<'block, T, I, ST, SI> SparseNumericBlock<'block, T, I, ST, SI>
where
    T: 'block + Debug,
    I: 'block + Debug,
    ST: 'block + Storage<'block, T>,
    SI: 'block + Storage<'block, I>,
{
    pub fn new(storage: ST, index: SI) -> Result<SparseNumericBlock<'block, T, I, ST, SI>> {

        Ok(SparseNumericBlock {
            storage,
            index,
            head: 0,
            _storage_marker: PhantomData,
            _index_marker: PhantomData,
        })
    }
}

impl<'block, T, I, ST, SI> BufferHead for SparseNumericBlock<'block, T, I, ST, SI>
where
    T: 'block + Debug,
    I: 'block + Debug,
    ST: 'block + Storage<'block, T>,
    SI: 'block + Storage<'block, I>,
{
    fn head(&self) -> usize {
        self.head
    }

    fn mut_head(&mut self) -> &mut usize {
        &mut self.head
    }
}

impl<'block, T, I, ST, SI> BlockData<'block, T, I> for SparseNumericBlock<'block, T, I, ST, SI>
where
    T: 'block + Debug,
    I: 'block + Debug,
    ST: 'block + Storage<'block, T>,
    SI: 'block + Storage<'block, I>,
{
    fn is_indexed() -> bool {
        true
    }

    fn as_mut_indexed_slice(&mut self) -> (&mut [I], &mut [T]) {
        let head = self.head();

        let SparseNumericBlock {
            ref mut index,
            ref mut storage,
            ..
        } = *self;

        (&mut index.as_mut()[..head], &mut storage.as_mut()[..head])
    }

    fn as_mut_indexed_slice_append(&mut self) -> (&mut [I], &mut [T]) {
        let head = self.head();

        let SparseNumericBlock {
            ref mut index,
            ref mut storage,
            ..
        } = *self;

        (&mut index.as_mut()[head..], &mut storage.as_mut()[head..])
    }
}

impl<'block, T, I, ST, SI> AsRef<[T]> for SparseNumericBlock<'block, T, I, ST, SI>
where
    T: 'block + Debug,
    I: 'block + Debug,
    ST: 'block + Storage<'block, T>,
    SI: 'block + Storage<'block, I>,
{
    fn as_ref(&self) -> &[T] {
        self.storage.as_ref()
    }
}

impl<'block, T, I, ST, SI> AsMut<[T]> for SparseNumericBlock<'block, T, I, ST, SI>
where
    T: 'block + Debug,
    I: 'block + Debug,
    ST: 'block + Storage<'block, T>,
    SI: 'block + Storage<'block, I>,
{
    fn as_mut(&mut self) -> &mut [T] {
        self.storage.as_mut()
    }
}

impl<'block, T, I, ST, SI> IndexRef<[I]> for SparseNumericBlock<'block, T, I, ST, SI>
where
    T: 'block + Debug,
    I: 'block + Debug,
    ST: 'block + Storage<'block, T>,
    SI: 'block + Storage<'block, I>,
{
    fn as_ref_index(&self) -> &[I] {
        self.index.as_ref()
    }
}

impl<'block, T, I, ST, SI> IndexMut<[I]> for SparseNumericBlock<'block, T, I, ST, SI>
where
    T: 'block + Debug,
    I: 'block + Debug,
    ST: 'block + Storage<'block, T>,
    SI: 'block + Storage<'block, I>,
{
    fn as_mut_index(&mut self) -> &mut [I] {
        self.index.as_mut()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use params::tests::BLOCK_SIZE;
    use extprim::i128::i128;
    use extprim::u128::u128;

    #[macro_use]
    mod block_128 {
        macro_rules! sparse_block_128_impl {
            ($T: tt, $data: expr, $index: expr) => {{
                let data = $data;
                let index = $index;

                let mut block = SparseIndexedNumericBlock::<$T, _, _>::new(data, index)
                    .with_context(|_| "failed to create block")
                    .unwrap();

                let s = [!$T::zero(), $T::zero(), !$T::zero()];
                let source = s.into_iter().cycle();

                let len = {
                    // write every 3rd index

                    let (index, data) = block.as_mut_indexed_slice_append();

                    let len = data.len();

                    assert_eq!(len, index.len());

                    let idx_source = (0..len)
                        .filter_map(|idx| if idx % 3 == 0 {
                            Some(idx as SparseIndex)
                        } else {
                            None
                        })
                        .collect::<Vec<_>>();
                    let idx_len = idx_source.len();

                    assert!(idx_len <= len);

                    let source = source.clone().take(idx_len).map(|v| *v).collect::<Vec<_>>();

                    data[..idx_len].copy_from_slice(&source);
                    index[..idx_len].copy_from_slice(&idx_source);
                    idx_len
                };

                assert!(len * size_of::<$T>() <= BLOCK_SIZE);

                block.set_written(len).unwrap();

                let data = block.as_slice();
                let index = block.as_index_slice();

                let source = source.take(len).map(|v| *v).collect::<Vec<_>>();
                let idx_source = (0..index.len())
                    .map(|idx| idx as SparseIndex * 3)
                    .collect::<Vec<_>>();

                assert_eq!(data, &source[..]);
                assert_eq!(index, &idx_source[..]);
            }};
        }
    }

    mod generic {
        use super::*;
        use num::{Float, Integer, Zero};
        use std::mem::size_of;
        use std::fmt::Debug;
        use std::ops::Not;


        pub(super) fn block_t<'block, T, ST, SI>(data: ST, index: SI)
        where
            T: 'block + Not<Output = T> + Integer + Zero + PartialEq + Copy + Debug,
            ST: 'block + Storage<'block, T>,
            SI: 'block + Storage<'block, SparseIndex>,
        {

            let mut block = SparseIndexedNumericBlock::new(data, index)
                .with_context(|_| "failed to create block")
                .unwrap();

            let s = [!T::zero(), T::zero(), !T::zero()];
            let source = s.into_iter().cycle();

            let len = {
                // write every 3rd index

                let (index, data) = block.as_mut_indexed_slice_append();

                let len = data.len();

                assert_eq!(len, index.len());

                let idx_source = (0..len)
                    .filter_map(|idx| if idx % 3 == 0 {
                        Some(idx as SparseIndex)
                    } else {
                        None
                    })
                    .collect::<Vec<_>>();
                let idx_len = idx_source.len();

                assert!(idx_len <= len);

                let source = source.clone().take(idx_len).map(|v| *v).collect::<Vec<_>>();

                data[..idx_len].copy_from_slice(&source);
                index[..idx_len].copy_from_slice(&idx_source);
                idx_len
            };

            assert!(len * size_of::<T>() <= BLOCK_SIZE);

            block.set_written(len).unwrap();

            let data = block.as_slice();
            let index = block.as_index_slice();

            let source = source.take(len).map(|v| *v).collect::<Vec<_>>();
            let idx_source = (0..index.len())
                .map(|idx| idx as SparseIndex * 3)
                .collect::<Vec<_>>();

            assert_eq!(data, &source[..]);
            assert_eq!(index, &idx_source[..]);
        }

        pub(super) fn block_tf<'block, T, ST, SI>(data: ST, index: SI)
        where
            T: 'block + Float + Zero + PartialEq + Copy + Debug,
            ST: 'block + Storage<'block, T>,
            SI: 'block + Storage<'block, SparseIndex>,
        {

            let mut block = SparseIndexedNumericBlock::new(data, index)
                .with_context(|_| "failed to create block")
                .unwrap();

            let s = [T::max_value(), T::zero(), T::max_value()];
            let source = s.into_iter().cycle();

            let len = {
                // write every 3rd index

                let (index, data) = block.as_mut_indexed_slice_append();

                let len = data.len();

                assert_eq!(len, index.len());

                let idx_source = (0..len)
                    .filter_map(|idx| if idx % 3 == 0 {
                        Some(idx as SparseIndex)
                    } else {
                        None
                    })
                    .collect::<Vec<_>>();
                let idx_len = idx_source.len();

                assert!(idx_len <= len);

                let source = source.clone().take(idx_len).map(|v| *v).collect::<Vec<_>>();

                data[..idx_len].copy_from_slice(&source);
                index[..idx_len].copy_from_slice(&idx_source);
                idx_len
            };

            assert!(len * size_of::<T>() <= BLOCK_SIZE);

            block.set_written(len).unwrap();

            let data = block.as_slice();
            let index = block.as_index_slice();

            let source = source.take(len).map(|v| *v).collect::<Vec<_>>();
            let idx_source = (0..index.len())
                .map(|idx| idx as SparseIndex * 3)
                .collect::<Vec<_>>();

            assert_eq!(data, &source[..]);
            assert_eq!(index, &idx_source[..]);
        }
    }

    mod memory {
        use super::*;
        use num::{Float, Integer, Zero};
        use std::mem::size_of;
        use std::fmt::Debug;
        use std::ops::Not;

        use storage::memory::PagedMemoryStorage;

        fn make_storage<T>() -> (PagedMemoryStorage, PagedMemoryStorage) {
            (
                PagedMemoryStorage::new(BLOCK_SIZE)
                    .with_context(|_| "failed to create memory storage")
                    .unwrap(),
                PagedMemoryStorage::new(BLOCK_SIZE / size_of::<T>() * size_of::<SparseIndex>())
                    .with_context(|_| "failed to create memory storage")
                    .unwrap(),
            )
        }

        fn block_t<T>()
        where
            T: Not<Output = T> + Integer + Zero + PartialEq + Copy + Debug,
        {
            let (storage, index) = make_storage::<T>();
            super::generic::block_t::<T, _, _>(storage, index);
        }

        fn block_tf<T>()
        where
            T: Float + Zero + PartialEq + Copy + Debug,
        {
            let (storage, index) = make_storage::<T>();
            super::generic::block_tf::<T, _, _>(storage, index);
        }

        #[test]
        fn block_u128() {
            let (data, index) = make_storage::<u128>();

            sparse_block_128_impl!(u128, data, index);
        }

        #[test]
        fn block_u64() {
            block_t::<u64>()
        }

        #[test]
        fn block_u32() {
            block_t::<u32>()
        }

        #[test]
        fn block_u16() {
            block_t::<u16>()
        }

        #[test]
        fn block_u8() {
            block_t::<u8>()
        }

        #[test]
        fn block_i128() {
            let (data, index) = make_storage::<i128>();

            sparse_block_128_impl!(i128, data, index);
        }

        #[test]
        fn block_i64() {
            block_t::<i64>()
        }

        #[test]
        fn block_i32() {
            block_t::<i32>()
        }

        #[test]
        fn block_i16() {
            block_t::<i16>()
        }

        #[test]
        fn block_i8() {
            block_t::<i8>()
        }

        #[test]
        fn block_f32() {
            block_tf::<f32>()
        }

        #[test]
        fn block_f64() {
            block_tf::<f64>()
        }
    }

    #[cfg(feature = "mmap")]
    mod mmap {
        use super::*;
        use storage::mmap::MemmapStorage;
        use num::{Float, Integer, Zero};
        use std::mem::size_of;
        use std::fmt::Debug;
        use std::ops::Not;

        fn make_storage<T>(name: &str) -> (MemmapStorage, MemmapStorage) {
            let data = name.to_string() + ".bin";
            let index = name.to_string() + ".idx";

            let (_dir, index, data) = tempfile!(index, data);

            (
                MemmapStorage::new(data, BLOCK_SIZE)
                    .with_context(|_| "failed to create memory storage")
                    .unwrap(),
                MemmapStorage::new(
                    index,
                    BLOCK_SIZE / size_of::<T>() * size_of::<SparseIndex>(),
                ).with_context(|_| "failed to create memory storage")
                    .unwrap(),
            )
        }

        fn block_t<T>(name: &str)
        where
            T: Not<Output = T> + Integer + Zero + PartialEq + Copy + Debug,
        {
            let (storage, index) = make_storage::<T>(name);
            super::generic::block_t::<T, _, _>(storage, index);
        }

        fn block_tf<T>(name: &str)
        where
            T: Float + Zero + PartialEq + Copy + Debug,
        {
            let (storage, index) = make_storage::<T>(name);
            super::generic::block_tf::<T, _, _>(storage, index);
        }
        
        #[test]
        fn block_u128() {
            let (data, index) = make_storage::<u128>("sparse_u128");

            sparse_block_128_impl!(u128, data, index);
        }

        #[test]
        fn block_u64() {
            block_t::<u64>("sparse_u64");
        }

        #[test]
        fn block_u32() {
            block_t::<u32>("sparse_u32")
        }

        #[test]
        fn block_u16() {
            block_t::<u16>("sparse_u16")
        }

        #[test]
        fn block_u8() {
            block_t::<u8>("sparse_u8")
        }

        #[test]
        fn block_i128() {
            let (data, index) = make_storage::<i128>("sparse_i128");

            sparse_block_128_impl!(i128, data, index);
        }

        #[test]
        fn block_i64() {
            block_t::<i64>("sparse_i64")
        }

        #[test]
        fn block_i32() {
            block_t::<i32>("sparse_i32")
        }

        #[test]
        fn block_i16() {
            block_t::<i16>("sparse_i16")
        }

        #[test]
        fn block_i8() {
            block_t::<i8>("sparse_i8")
        }

        #[test]
        fn block_f32() {
            block_tf::<f32>("sparse_f32")
        }

        #[test]
        fn block_f64() {
            block_tf::<f64>("sparse_f64")
        }
    }
}
