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
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate bincode;

#[cfg(test)]
extern crate tempdir;
#[cfg(test)]
extern crate num;
#[cfg(test)]
extern crate rand;

pub(crate) mod params;

mod error;
#[macro_use]
pub(crate) mod helpers;
#[macro_use]
pub(crate) mod serde_utils;

mod fs;
mod storage;
#[cfg(feature = "hole_punching")]
mod libc_utils;
mod block;

#[macro_use]
mod ty;
mod partition;
// mod catalog;
// mod mutator;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
