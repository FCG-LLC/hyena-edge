#![cfg_attr(feature = "nightly", feature(test))]
#[cfg(all(feature = "nightly", test))]
extern crate test;

#[macro_use]
extern crate serde_derive;

pub(crate) mod error;
pub mod collections;
pub mod iter;
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
