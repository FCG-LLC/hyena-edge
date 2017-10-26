#![cfg_attr(feature = "block_128", feature(i128_type))]
#![cfg_attr(feature = "nightly", feature(test))]

#[cfg(all(feature = "nightly", test))]
extern crate test;

#[macro_use]
extern crate log;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate cfg_if;
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
#[cfg(feature = "perf")]
extern crate flame;
#[cfg(test)]
#[macro_use]
extern crate prettytable;
#[cfg(test)]
extern crate term;

#[cfg(test)]
#[macro_use]
extern crate static_assertions;

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
pub mod catalog;
mod mutator;
mod scanner;
pub mod api;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
