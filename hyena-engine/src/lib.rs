#![cfg_attr(feature = "nightly", feature(test))]

#[cfg(all(feature = "nightly", test))]
extern crate test;

#[macro_use]
extern crate log;
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
#[macro_use]
extern crate hyena_common;

#[cfg(test)]
#[macro_use]
extern crate hyena_test;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate failure_derive;

#[cfg(test)]
extern crate num;
#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate prettytable;
#[cfg(test)]
extern crate term;

#[cfg(test)]
#[macro_use]
extern crate static_assertions;
extern crate extprim;

pub(crate) mod params;

mod error;
#[macro_use]
pub(crate) mod helpers;

mod fs;
mod storage;
mod block;

#[macro_use]
mod ty;
mod partition;
pub mod catalog;
mod mutator;
mod scanner;

pub use self::error::{Error, Result};
pub use self::scanner::{ScanFilters, Scan, ScanTsRange, ScanFilterOp, ScanResult, ScanFilterApply,
    ScanFilter};
pub use self::catalog::{Catalog, Column, ColumnMap};
pub use self::ty::{RowId, ColumnId, BlockType as BlockStorageType};
pub use self::block::{BlockType, SparseIndex};
pub use self::ty::fragment::{Fragment, FragmentIter, TimestampFragment};
pub use hyena_common::ty::Value;
pub use hyena_common::ty::{Timestamp, MAX_TIMESTAMP_VALUE, MIN_TIMESTAMP_VALUE};

pub use self::mutator::{Append, BlockData};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
