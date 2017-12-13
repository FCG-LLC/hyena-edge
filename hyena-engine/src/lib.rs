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
extern crate byteorder;

extern crate tempdir;
#[cfg(test)]
extern crate num;
#[cfg(test)]
extern crate rand;
#[cfg(feature = "perf")]
extern crate flame;
#[cfg(test)]
extern crate prettytable;
#[cfg(test)]
extern crate term;

#[cfg(test)]
#[macro_use]
extern crate static_assertions;
extern crate extprim;

pub(crate) mod params;

pub mod error;
#[macro_use]
pub mod helpers;
#[macro_use]
pub(crate) mod serde_utils;

mod fs;
mod storage;
#[cfg(feature = "hole_punching")]
mod libc_utils;
pub mod block;

#[macro_use]
pub mod ty;
pub mod partition;
pub mod catalog;
pub mod mutator;
mod scanner;
pub mod huuid;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
