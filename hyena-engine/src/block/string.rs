use block::{BlockData, BufferHead, IndexMut, IndexRef, SliceOffset, RelativeSlice};
use error::*;
use std::marker::PhantomData;
use storage::{Realloc, Storage};
use ty::RowId;


#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DenseIndex;



#[derive(Debug)]
pub struct DenseStringBlock<'block, S, P>
where
    S: 'block + Storage<'block, RelativeSlice>,
    P: 'block + Storage<'block, u8> + Realloc + Default,
{
    storage: S,
    pool: P,
    /// the tip of the buffer
    head: usize,
    pool_head: usize,
    base: PhantomData<&'block [RelativeSlice]>,
}

impl<'block, S, P> DenseStringBlock<'block, S, P>
where
    S: 'block + Storage<'block, RelativeSlice>,
    P: 'block + Storage<'block, u8> + Realloc + Default,
{
    pub fn new(storage: S, pool: P) -> Result<DenseStringBlock<'block, S, P>> {
        Ok(DenseStringBlock {
            storage,
            pool,
            head: 0,
            pool_head: 0,
            base: PhantomData,
        })
    }

    /// Append new string to this string block
    ///
    /// This function assumes that `string` points to a valid UTF-8 byte sequence.
    /// Providing invalid UTF-8 can lead to triggering undefined behavior at some later point

    pub fn append_string(&mut self, string: impl AsRef<str>) -> Result<usize> {
        // check if there's still space available in this block
        if self.head + 1 > self.storage.len() {
            bail!("string block is full");
        }

        // check if the string would fit within the pool
        let sstr = string.as_ref();
        let sbytes = sstr.as_bytes();
        let slen = sbytes.len();

        if self.pool.needs_realloc(self.pool_head, slen) {
            // we need to realloc

            // default pool shouldn't allocate
            let pool = ::std::mem::replace(&mut self.pool, Default::default());

            let pool = pool.realloc_for(self.pool_head, slen)?;
            ::std::mem::replace(&mut self.pool, pool);
        }

        // append new string
        let pool_head = self.pool_head;
        let slice_head = self.head;
        {
            let dest = &mut self.pool.as_mut()[pool_head..];
            let dest = &mut dest[..slen];
            &mut dest[..].copy_from_slice(&sbytes[..]);

            self.storage.as_mut()[slice_head] = RelativeSlice::new(pool_head, slen);
            self.head += 1;
            self.pool_head += slen;
        }

        Ok(slen)
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        let base = self.pool.as_ptr();

        self.as_slice()
            .iter()
            .map(move |v| unsafe { v.to_str_ptr(base) })
    }
}

impl<'block, S, P> BufferHead for DenseStringBlock<'block, S, P>
where
    S: 'block + Storage<'block, RelativeSlice>,
    P: 'block + Storage<'block, u8> + Realloc + Default,
{
    fn head(&self) -> usize {
        self.head
    }

    fn mut_head(&mut self) -> &mut usize {
        &mut self.head
    }
}

impl<'block, S, P> BlockData<'block, RelativeSlice, DenseIndex> for DenseStringBlock<'block, S, P>
where
    S: 'block + Storage<'block, RelativeSlice>,
    P: 'block + Storage<'block, u8> + Realloc + Default,
{
}

impl<'block, S, P> AsRef<[RelativeSlice]> for DenseStringBlock<'block, S, P>
where
    S: 'block + Storage<'block, RelativeSlice>,
    P: 'block + Storage<'block, u8> + Realloc + Default,
{
    fn as_ref(&self) -> &[RelativeSlice] {
        self.storage.as_ref()
    }
}

impl<'block, S, P> AsMut<[RelativeSlice]> for DenseStringBlock<'block, S, P>
where
    S: 'block + Storage<'block, RelativeSlice>,
    P: 'block + Storage<'block, u8> + Realloc + Default,
{
    fn as_mut(&mut self) -> &mut [RelativeSlice] {
        self.storage.as_mut()
    }
}

impl<'block, S, P> ::std::ops::Index<RowId> for DenseStringBlock<'block, S, P>
where
    S: 'block + Storage<'block, RelativeSlice>,
    P: 'block + Storage<'block, u8> + Realloc + Default,
{
    type Output = str;

    fn index(&self, rowid: RowId) -> &Self::Output {
        let slice = self.as_slice()[rowid];

        let base = self.pool.as_ptr();

        unsafe { slice.to_str_ptr(base) }
    }
}

impl<'block, S, P> IndexRef<[DenseIndex]> for DenseStringBlock<'block, S, P>
where
    S: 'block + Storage<'block, RelativeSlice>,
    P: 'block + Storage<'block, u8> + Realloc + Default,
{
    fn as_ref_index(&self) -> &[DenseIndex] {
        &[][..]
    }
}

impl<'block, S, P> IndexMut<[DenseIndex]> for DenseStringBlock<'block, S, P>
where
    S: 'block + Storage<'block, RelativeSlice>,
    P: 'block + Storage<'block, u8> + Realloc + Default,
{
    fn as_mut_index(&mut self) -> &mut [DenseIndex] {
        &mut [][..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use params::BLOCK_SIZE;

    mod generic {
        use super::*;
        use std::fmt::Debug;
        use std::mem::{size_of, size_of_val};
        use storage::ByteStorage;

        pub(super) fn block_string<'block, S, P>(
            storage: S,
            pool: P,
            payload_len: usize,
            value_count: usize,
        ) where
            S: 'block + Storage<'block, RelativeSlice>,
            P: 'block + Storage<'block, u8> + Realloc + Default,
        {
            let mut block = DenseStringBlock::new(storage, pool)
                .with_context(|_| "failed to create string block")
                .unwrap();

            let s = [b'X', b'x', b'Y'];
            let source_str = s
                .into_iter()
                .cycle()
                .take(payload_len)
                .cloned()
                .collect::<Vec<_>>();

            assert_eq!(source_str.len(), payload_len);

            let source_str = String::from_utf8(source_str).unwrap();

            let written = ::std::iter::repeat(())
                .take(value_count)
                .map(|_| block.append_string(&source_str))
                .sum::<Result<usize>>()
                .with_context(|_| "failed to append strings")
                .unwrap();

            assert_eq!(value_count, block.len());

            for value in block.iter() {
                assert_eq!(value, source_str);
            }
        }

        const PAGE_4K: usize = 1 << 12;
        const BLOCK_1M: usize = PAGE_4K * 256;
        const RELATIVE_SLICE_SIZE: usize = size_of::<RelativeSlice>();

        pub(super) fn single<'block, F, P, S>(slice_storage: F, pool_storage: P)
        where
            F: Fn(usize) -> S,
            P: Fn(usize) -> S,
            S: 'block + Storage<'block, RelativeSlice> + ByteStorage<'block> + Realloc + Default,
        {
            super::generic::block_string(slice_storage(BLOCK_SIZE), pool_storage(PAGE_4K), 10, 1);
        }

        pub(super) fn full_no_realloc<'block, F, P, S>(slice_storage: F, pool_storage: P)
        where
            F: Fn(usize) -> S,
            P: Fn(usize) -> S,
            S: 'block + Storage<'block, RelativeSlice> + ByteStorage<'block> + Realloc + Default,
        {
            let value_count = BLOCK_1M / RELATIVE_SLICE_SIZE;
            super::generic::block_string(
                slice_storage(BLOCK_SIZE),
                pool_storage(BLOCK_1M),
                10,
                value_count,
            );
        }

        pub(super) fn no_full_no_realloc<'block, F, P, S>(slice_storage: F, pool_storage: P)
        where
            F: Fn(usize) -> S,
            P: Fn(usize) -> S,
            S: 'block + Storage<'block, RelativeSlice> + ByteStorage<'block> + Realloc + Default,
        {
            let value_count = 4096;
            super::generic::block_string(
                slice_storage(BLOCK_SIZE),
                pool_storage(BLOCK_1M),
                10,
                value_count,
            );
        }

        pub(super) fn full_realloc<'block, F, P, S>(slice_storage: F, pool_storage: P)
        where
            F: Fn(usize) -> S,
            P: Fn(usize) -> S,
            S: 'block + Storage<'block, RelativeSlice> + ByteStorage<'block> + Realloc + Default,
        {
            let value_count = BLOCK_1M / RELATIVE_SLICE_SIZE;
            super::generic::block_string(
                slice_storage(BLOCK_1M),
                pool_storage(PAGE_4K),
                10,
                value_count,
            );
        }

        pub(super) fn no_full_realloc<'block, F, P, S>(slice_storage: F, pool_storage: P)
        where
            F: Fn(usize) -> S,
            P: Fn(usize) -> S,
            S: 'block + Storage<'block, RelativeSlice> + ByteStorage<'block> + Realloc + Default,
        {
            let value_count = 4096;
            super::generic::block_string(
                slice_storage(BLOCK_1M),
                pool_storage(PAGE_4K),
                10,
                value_count,
            );
        }

        pub(super) fn overflow<'block, F, P, S>(slice_storage: F, pool_storage: P)
        where
            F: Fn(usize) -> S,
            P: Fn(usize) -> S,
            S: 'block + Storage<'block, RelativeSlice> + ByteStorage<'block> + Realloc + Default,
        {
            let value_count = BLOCK_1M / RELATIVE_SLICE_SIZE;
            super::generic::block_string(
                slice_storage(PAGE_4K),
                pool_storage(PAGE_4K),
                10,
                value_count,
            );
        }
    }

    mod memory {
        use super::*;
        use storage::memory::PagedMemoryStorage;

        fn make_storage(size: usize) -> PagedMemoryStorage {
            PagedMemoryStorage::new(size)
                .with_context(|_| "failed to create memory storage")
                .unwrap()
        }

        #[test]
        fn single() {
            super::generic::single(make_storage, make_storage);
        }

        #[test]
        fn full_no_realloc() {
            super::generic::full_no_realloc(make_storage, make_storage);
        }

        #[test]
        fn no_full_no_realloc() {
            super::generic::no_full_no_realloc(make_storage, make_storage);
        }

        #[test]
        fn full_realloc() {
            super::generic::full_realloc(make_storage, make_storage);
        }

        #[test]
        fn no_full_realloc() {
            super::generic::no_full_realloc(make_storage, make_storage);
        }

        #[test]
        #[should_panic(expected = "string block is full")]
        fn overflow() {
            super::generic::overflow(make_storage, make_storage);
        }
    }

    #[cfg(feature = "mmap")]
    mod mmap {
        use super::*;
        use std::path::Path;
        use storage::mmap::MemmapStorage;

        fn make_storage(dir: impl AsRef<Path>, name: &str, size: usize) -> MemmapStorage {
            let mut file = dir.as_ref().to_path_buf();
            file.push(name);

            MemmapStorage::new(file, size)
                .with_context(|_| "failed to create memmap storage")
                .unwrap()
        }

        #[test]
        fn single() {
            let dir = tempdir!();
            super::generic::single(
                |size| make_storage(&dir, "str_single_slice", size),
                |size| make_storage(&dir, "str_single_pool", size),
            );
        }

        #[test]
        fn full_no_realloc() {
            let dir = tempdir!();
            super::generic::full_no_realloc(
                |size| make_storage(&dir, "str_full_no_realloc_slice", size),
                |size| make_storage(&dir, "str_full_no_realloc_pool", size),
            );
        }

        #[test]
        fn no_full_no_realloc() {
            let dir = tempdir!();
            super::generic::no_full_no_realloc(
                |size| make_storage(&dir, "str_no_full_no_realloc_slice", size),
                |size| make_storage(&dir, "str_no_full_no_realloc_pool", size),
            );
        }

        #[test]
        fn full_realloc() {
            let dir = tempdir!();
            super::generic::full_realloc(
                |size| make_storage(&dir, "str_full_realloc_slice", size),
                |size| make_storage(&dir, "str_full_realloc_pool", size),
            );
        }

        #[test]
        fn no_full_realloc() {
            let dir = tempdir!();
            super::generic::no_full_realloc(
                |size| make_storage(&dir, "str_no_full_realloc_slice", size),
                |size| make_storage(&dir, "str_no_full_realloc_pool", size),
            );
        }

        #[test]
        #[should_panic(expected = "string block is full")]
        fn overflow() {
            let dir = tempdir!();
            super::generic::overflow(
                |size| make_storage(&dir, "str_overflow_slice", size),
                |size| make_storage(&dir, "str_overflow_pool", size),
            );
        }
    }
}
