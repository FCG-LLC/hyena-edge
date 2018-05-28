use std::slice::from_raw_parts;
use std::str::from_utf8_unchecked;
use block::SliceOffset;

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelativeSlice {
    base: usize,
    length: usize,
}

impl RelativeSlice {
    pub(crate) fn new(base: usize, length: usize) -> RelativeSlice {
        // from  https://doc.rust-lang.org/std/primitive.pointer.html#method.offset
        //
        // The compiler and standard library generally tries to ensure allocations never reach a
        // size where an offset is a concern. For instance, Vec and Box ensure they never allocate
        // more than isize::MAX bytes, so vec.as_ptr().offset(vec.len() as isize) is always safe.

        debug_assert!(base as isize <= ::std::isize::MAX);
        debug_assert!(length as isize <= ::std::isize::MAX);

        RelativeSlice { base, length }
    }

    pub unsafe fn to_slice_ptr<'buffer, T>(&self, buffer: *const T) -> &'buffer [T] {
        let base = buffer.add(self.base);

        from_raw_parts(base, self.length)
    }

    pub unsafe fn to_str_ptr<'buffer>(&self, buffer: *const u8) -> &'buffer str {
        let slice = self.to_slice_ptr(buffer);

        from_utf8_unchecked(slice)
    }
}

impl SliceOffset for RelativeSlice {

    fn is_empty(&self) -> bool {
        self.length == 0
    }

    fn len(&self) -> usize {
        self.length
    }

    fn to_slice<'buffer, T>(&self, buffer: &'buffer [T]) -> &'buffer [T] {
        debug_assert!(buffer.len() >= self.base + self.length);

        unsafe { self.to_slice_ptr(buffer.as_ptr()) }
    }

    fn to_str<'buffer>(&self, buffer: &'buffer [u8]) -> &'buffer str {
        debug_assert!(buffer.len() >= self.base + self.length);

        unsafe { self.to_str_ptr(buffer.as_ptr()) }
    }

}
