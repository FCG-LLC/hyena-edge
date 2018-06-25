#![cfg_attr(feature = "nightly", feature(test))]
#[cfg(all(feature = "nightly", test))]
extern crate test;

extern crate bincode;
extern crate chrono;
extern crate extprim;
extern crate failure;
extern crate fxhash;
extern crate num;
extern crate rand;
extern crate serde;
extern crate rayon;
extern crate regex;
#[macro_use]
extern crate serde_derive;
extern crate uuid;

pub(crate) mod error;
pub mod collections;
pub mod lock;
pub mod libc;
pub mod map_type;
pub mod serde_utils;
pub mod ty;

mod macros;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
