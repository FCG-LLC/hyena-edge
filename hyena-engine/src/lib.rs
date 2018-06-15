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
#[macro_use]
extern crate hyena_common;
extern crate hyena_bloom_filter;

#[cfg(test)]
#[macro_use]
extern crate hyena_test;
#[macro_use]
extern crate failure;

#[cfg(test)]
extern crate num;
#[cfg(test)]
extern crate rand;

#[cfg(test)]
#[macro_use]
extern crate static_assertions;
extern crate extprim;

extern crate strum;
#[macro_use]
extern crate strum_macros;

#[cfg(feature = "debug")]
extern crate prettytable;
#[cfg(feature = "debug")]
extern crate term;

#[cfg(feature = "debug")]
pub mod debug;

pub(crate) mod params;

mod error;

mod fs;
mod storage;
#[macro_use]
mod block;

#[macro_use]
mod ty;
pub mod datastore;
mod mutator;
mod scanner;

pub use self::error::{Error, Result};
pub use self::scanner::{ScanFilters, Scan, ScanTsRange, ScanFilterOp, ScanResult, ScanFilterApply,
    ScanFilter, ScanData, Regex};
pub use self::datastore::{Catalog, Column, ColumnMap};
pub use self::ty::{RowId, ColumnId, BlockStorage, ColumnIndexStorage};
pub use self::block::{BlockType, SparseIndex, ColumnIndexType};
pub use self::ty::fragment::{Fragment, FragmentIter, TimestampFragment};
pub use self::ty::block::memory::Block as MemoryBlock;
pub use self::ty::block::mmap::Block as MemmapBlock;
pub use self::params::SourceId;
pub use hyena_common::ty::Value;
pub use hyena_common::ty::{Timestamp, MAX_TIMESTAMP_VALUE, MIN_TIMESTAMP_VALUE};

pub use self::mutator::{Append, BlockData};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
