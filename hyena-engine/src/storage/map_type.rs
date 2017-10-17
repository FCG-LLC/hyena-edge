use std::ops::{Deref, DerefMut};
use std::mem::{align_of, size_of};
use std::marker::PhantomData;


#[inline]
pub fn map_type<'map, 'owner, D, M: Deref<Target = [D]>, T>(
    map: &'map M,
    _block: PhantomData<&'owner T>,
) -> &'owner [T] {
    let length = map_len::<D, M, T>(map);

    unsafe { ::std::slice::from_raw_parts(map.as_ptr() as *const T, length) }
}

#[inline]
pub fn map_type_mut<'map, 'owner, D, M: DerefMut<Target = [D]>, T>(
    map: &'map mut M,
    _block: PhantomData<&'owner T>,
) -> &'owner mut [T] {
    let length = map_len::<D, M, T>(map);

    unsafe { ::std::slice::from_raw_parts_mut(map.as_mut_ptr() as *mut T, length) }
}

#[inline]
fn map_len<'map, D, M: Deref<Target = [D]>, T>(map: &'map M) -> usize {
    let bytes_len = map.len() * size_of::<D>();

    // check pointer alignment
    debug_assert_eq!(
        (map.as_ptr() as usize) % align_of::<T>(),
        0,
        "map_len failed: bad memory alignment"
    );
    debug_assert_eq!(
        bytes_len % size_of::<T>(),
        0,
        "map_len failed: cannot map type that is not a multiple of T ({})",
        size_of::<T>()
    );

    bytes_len / size_of::<T>()
}
