extern crate bincode;
extern crate byteorder;
#[macro_use]
extern crate cfg_if;
extern crate chrono;
extern crate extprim;
extern crate failure;
extern crate num;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate uuid;

pub(crate) mod error;
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
