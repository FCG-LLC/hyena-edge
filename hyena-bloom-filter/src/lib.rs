extern crate fnv;
extern crate fxhash;

mod bloom;
mod bloom_filter;
mod ngram;

pub use self::bloom::Bloom;
pub use self::bloom_filter::BloomFilter;
pub use self::ngram::{Ngram, Trigram};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
