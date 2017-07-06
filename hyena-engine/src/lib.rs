#![cfg_attr(feature = "block_128", feature(i128_type))]

#[macro_use]
extern crate log;
#[macro_use]
extern crate error_chain;
extern crate rayon;
extern crate chrono;
#[cfg(feature = "mmap")]
extern crate memmap;
#[cfg(feature = "hole_punching")]
extern crate libc;
extern crate uuid;

#[cfg(test)]
extern crate tempdir;
#[cfg(test)]
extern crate num;

mod error;
#[macro_use]
pub(crate) mod helpers;
mod fs;
mod storage;
#[cfg(feature = "hole_punching")]
mod libc_utils;
mod block;

mod ty;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
