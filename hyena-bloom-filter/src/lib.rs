#![cfg_attr(feature = "nightly", feature(test))]

#[cfg(all(feature = "nightly", test))]
extern crate test;

#[macro_use]
extern crate serde_derive;

mod bloom_value;
mod bloom_filter;
mod ngram;

pub use self::bloom_value::BloomValue;
pub use self::bloom_filter::BloomFilter;
pub use self::ngram::{Ngram, Trigram};

use fnv::FnvBuildHasher;
use fxhash::FxBuildHasher;


pub type DefaultBloomFilter = BloomFilter<FnvBuildHasher, FxBuildHasher>;
