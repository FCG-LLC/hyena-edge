//! # Scan API
//!
//! This module contains tools for preparing scan requests.
//!
//! ## Filtering
//!
//! Hyena currently supports filtering with expressions in form
//!
//! ```python
//! (A && B) || (C && D)
//! ```
//! where A, B, C, D are column filter expressions.
//!
//! ### Example
//!
//! ```rust
//! # #[macro_use]
//! # extern crate hyena_common;
//! # extern crate hyena_engine;
//! # use hyena_engine::{ScanFilters, ScanFilter, ScanFilterOp};
//! // with schema:
//!
//! // 1 -> source_id
//! // 2 -> col1
//! // 3 -> col2
//!
//! // (source_id > 1 && source_id < 3 && col1 > 1 && col1 < 3) || (col1 < 7 && col2 == 12)
//!
//! # fn main() {
//!
//! let filters: ScanFilters = hashmap! {
//!     1 => vec![  // OR
//!         vec![   // AND #0, source_id > 1 && source_id > 3
//!                  ScanFilter::from(ScanFilterOp::Gt(1)),
//!                  ScanFilter::from(ScanFilterOp::Lt(3)),
//!         ],
//!         // AND #1, implied `true`
//!     ],
//!     2 => vec![  // OR
//!         vec![   // AND #0, col1 > 1 && col1 < 3
//!             ScanFilter::from(ScanFilterOp::Lt(3)),
//!             ScanFilter::from(ScanFilterOp::Gt(1)),
//!         ],
//!         vec![   // AND #1, col1 < 7
//!             ScanFilter::from(ScanFilterOp::Lt(7)),
//!         ],
//!     ],
//!     3 => vec![  // OR
//!         vec![   // AND #0, implied `true`
//!             ],
//!         vec![   // AND #1, col2 == 12
//!             ScanFilter::from(ScanFilterOp::Eq(12)),
//!         ],
//!     ],
//! };
//!
//! # }
//! ```
//!
//! ```text
//! AND:
//!
//! #0: (source_id > 1 && source_id < 3 && col1 > 1 && col1 < 3)
//!     filters[1][0] && filters[2][0] && filters[3][0]
//!     Because filters[3][0] is empty, we assume it's `true`
//!
//! #1: (col1 < 7 && col2 == 12)
//!     filters[1][1] && filters[2][1] && filters[3][1]
//!     Because filters[1][1] is not present, we assume it's `true`
//!
//! OR:
//!
//! #0 || #1
//!
//! +---------------+-----------+-------------+
//! | column        |  and #0 ↓ |   and #1 ↓  |
//! +---------------+-----------+-------------+
//! | source_id (1) | > 1, < 3  |             |
//! +---------------+-----------+-------------+
//! | col1 (2)      | > 1, < 3  |     < 7     |
//! +--------+------+-----------+-------------+
//! | col2 (3)      |           |     = 12    |
//! +===============+===========+=============+
//! |      OR →     |   AND #0  |    AND #1   |
//! +---------------+-----------+-------------+
//! ```

use error::*;
use ty::ColumnId;
use hyena_common::collections::{HashMap, HashSet};
use std::fmt::{Display, Debug};
use std::hash::Hash;
use std::ops::Deref;
use datastore::PartitionId;
use params::SourceId;
use ty::fragment::Fragment;
use hyena_common::ty::Timestamp;
use extprim::i128::i128;
use extprim::u128::u128;
use rayon::prelude::*;

pub use hyena_common::ty::Regex;
use hyena_bloom_filter::{DefaultBloomFilter, BloomValue};

pub type ScanFilters = OrScanFilters;
pub type ScanData = HashMap<ColumnId, Option<Fragment>>;

pub type OrScanFilters = HashMap<ColumnId, Vec<AndScanFilters>>;
pub type AndScanFilters = Vec<ScanFilter>;

pub(crate) type BloomFilterValues = HashMap<ColumnId, Vec<BloomValue>>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scan {
    pub(crate) ts_range: Option<ScanTsRange>,
    pub(crate) partitions: Option<HashSet<PartitionId>>,
    pub(crate) groups: Option<Vec<SourceId>>,
    pub(crate) projection: Option<Vec<ColumnId>>,
    pub(crate) filters: Option<ScanFilters>,
    pub(crate) bloom_filters: Option<BloomFilterValues>,    // OR/ANY
    pub(crate) or_clauses_count: usize,
}

impl Scan {
    pub fn new(
        filters: Option<ScanFilters>,
        projection: Option<Vec<ColumnId>>,
        groups: Option<Vec<SourceId>>,
        partitions: Option<HashSet<PartitionId>>,
        ts_range: Option<ScanTsRange>,
    ) -> Scan {
        let or_clauses_count = filters
            .as_ref()
            .map(|filters| Self::count_or_clauses(filters))
            .unwrap_or_default();

        let bloom_filters = if let Some(ref f) = filters {
            Self::compute_bloom_values(f)
        } else {
            None
        };

        Scan {
            filters,
            projection,
            groups,
            partitions,
            ts_range,
            or_clauses_count,
            bloom_filters,
        }
    }

    pub(crate) fn count_or_clauses(filters: &ScanFilters) -> usize {
        filters.iter().map(|(_, and_filters)| and_filters.len()).max().unwrap_or_default()
    }

    fn compute_bloom_values(filters: &ScanFilters) -> Option<BloomFilterValues> {
        filters
            .iter()
            .map(|(colid, and_filters)| {
                (*colid, and_filters
                    .iter()
                    .flat_map(|v| v.iter())
                    .map(|v| v.bloom_value().ok_or(()))
                    .collect::<::std::result::Result<Vec<BloomValue>, ()>>())
            })
            .map(|(colid, v)| v.map(|v| (colid, v)))
            .collect::<::std::result::Result<_, ()>>()
            .ok()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScanTsRange {
    Full,
    Bounded { start: Timestamp, end: Timestamp }, // inclusive, exclusive
    From { start: Timestamp },              // inclusive
    To { end: Timestamp },                  // exclusive
}

impl ScanTsRange {
    /// Check if this range overlaps with another range defined by the params
    ///
    /// Passed values should satisfy `min <= max` condition

    #[inline]
    pub fn overlaps_with<TS>(&self, min: TS, max: TS) -> bool
    where Timestamp: From<TS> {
        let min = Timestamp::from(min);
        let max = Timestamp::from(max);

        debug_assert!(min <= max);

        match *self {
            ScanTsRange::Full => true,
            ScanTsRange::Bounded { start, end }
                if start <= max && min < end => true,
            ScanTsRange::From { start } if start <= max => true,
            ScanTsRange::To { end } if end > min => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScanFilterOp<T: Display + Debug + Clone + PartialEq + PartialOrd + Hash + Eq> {
    Lt(T),
    LtEq(T),
    Eq(T),
    GtEq(T),
    Gt(T),
    NotEq(T),
    In(HashSet<T>),

    // String ops

    /// For String column, test if the haystack starts with the needle
    /// For other types, cast to String and then do the test, so potentially much slower
    /// as it needs to alloc
    StartsWith(T),
    /// For String column, test if the haystack ends with the needle
    /// For other types, cast to String and then do the test, so potentially much slower
    /// as it needs to alloc
    EndsWith(T),
    /// For String column, test if the haystack starts with the needle
    /// For other types, cast to String and then do the test, so potentially much slower
    /// as it needs to alloc
    Contains(T),
    Matches(Regex),
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ScanResult {
    pub(crate) data: ScanData,
}

impl ScanResult {

    /// Return merge identity for `ScanResult`
    ///
    /// According to Wikipedia:
    /// "An identity element is a special type of element of a set with respect to a binary
    /// operation on that set, which leaves other elements unchanged when combined with them."
    pub(crate) fn merge_identity() -> ScanResult {
        Default::default()
    }

    /// Merge two `ScanResult`s
    ///
    /// We are assuming that the contained columns are the same
    /// because that should always be the case in our "supertable" model
    pub(crate) fn merge(&mut self, mut other: ScanResult) -> Result<()> {
        let offset = self.dense_len();

        for (k, v) in self.data.iter_mut() {
            // default for Option is None
            let o = other.data.remove(k).unwrap_or_default();

            if v.is_none() {
                *v = o;
            } else if o.is_some() {
                let self_data = v.as_mut().unwrap();
                let mut other_data = o.unwrap();

                self_data
                    .merge(&mut other_data, offset)
                    .with_context(|_| "unable to merge scan results")?;
            }
        }

        // add columns that are in other but not in self
        self.data.extend(other.data.drain());

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.try_dense_len()
            .or_else(|| {
            // all sparse columns, and here if differs from dense_len
            // as dense len finds the expected output fragment length
            // and len() should give us a number of distinct rows
            // this is very heavy operation, because we have to track
            // all rows from all of the columns

            Some(self.data
                .par_iter()
                .map(|(_, col)| {
                    col.as_ref()
                        // TODO: verify that FragmentIter's Value wrapping
                        // gets optimized out by the compiler
                        .map_or(hashset! { 0 },
                            |col| col.iter().map(|(idx, _)| idx).collect::<HashSet<_>>())
                })
                .reduce(|| HashSet::new(), |a, b| a.union(&b).into_iter().cloned().collect())
                .len())
        })
        .unwrap_or_default()
    }

    /// Get the length of a dense result `Fragment`
    ///
    /// This is needed for proper sparse offset calculation.

    fn dense_len(&self) -> usize {
        // try to find the first dense column
        // in most cases it should be a `ts`, which has index = 0

        self.try_dense_len()
            .or_else(|| {
            // all sparse columns, so we have to find the maximum index value
            // which means iterating over all columns

            use std::cmp::max;

            Some(self.data.iter().fold(0, |max_idx, (_, col)| {
                // all columns have to be sparse here
                // hence why only debug-time assert
                debug_assert!(col.as_ref().map_or(true, |col| col.is_sparse()));

                // this is not a logic error, as this is used to determine
                // the base offset for sparse index
                // so zero in case of all empty Fragments is perfectly valid
                max(max_idx,
                    col.as_ref()
                        .map_or(0, |col| col.max_index().unwrap_or(0)))
            }))
        })
        .unwrap_or_default()
    }

    #[inline]
    fn try_dense_len(&self) -> Option<usize> {
        self.data.iter()
            .find(|&(colidx, col)| {
                // this is a very ugly hack
                // that needs to be here until we get rid of source_id column
                // treat source_id column (index == 1) as sparse
                // as it's always empty anyway
                *colidx != 1 &&

                // this is the proper part
                // that gets to stay
                col.as_ref().map_or(false, |col| !col.is_sparse())
            })
            .map(|(_, dense)| {
                dense.as_ref().map_or(0, |col| col.len())
            })
    }

    }
}

impl From<ScanData> for ScanResult {
    fn from(data: ScanData) -> ScanResult {
        ScanResult { data }
    }
}

pub trait ScanFilterApply<T> {
    #[inline]
    fn apply(&self, tested: &T) -> bool;
}

pub(crate) trait ScanFilterBloom {
    #[inline]
    fn bloom_value(&self) -> Option<BloomValue> {
        None
    }
}

impl Deref for ScanResult {
    type Target = ScanData;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl IntoIterator for ScanResult {
    type Item = <ScanData as IntoIterator>::Item;
    type IntoIter = <ScanData as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

macro_rules! scan_filter_impl {

    // separate ScanFilterOp impl, as for some types (String)
    // a custom impl is required
    (ops $( $ty: ty, $variant: ident, $varname: expr ),+ $(,)*) => {
        $(

        impl ScanFilterOp<$ty> {
            #[inline]
            fn apply(&self, tested: &$ty) -> bool {
                use self::ScanFilterOp::*;

                match *self {
                    Lt(ref v) => tested < v,
                    LtEq(ref v) => tested <= v,
                    Eq(ref v) => tested == v,
                    GtEq(ref v) => tested >= v,
                    Gt(ref v) => tested > v,
                    NotEq(ref v) => tested != v,
                    In(ref v) => v.contains(&tested),
                    StartsWith(ref v) => {
                        let tested = tested.to_string();
                        let v = v.to_string();

                        tested.starts_with(&v)
                    }
                    EndsWith(ref v) => {
                        let tested = tested.to_string();
                        let v = v.to_string();

                        tested.ends_with(&v)
                    }
                    Contains(ref v) => {
                        let tested = tested.to_string();
                        let v = v.to_string();

                        tested.contains(&v)
                    }
                    Matches(ref reg) => {
                        let tested = tested.to_string();

                        reg.is_match(&tested)
                    }
                }
            }

            #[inline]
            fn bloom_value(&self) -> Option<BloomValue> {
                use self::ScanFilterOp::*;

                match *self {
                    Lt(_)
                    | LtEq(_)
                    | Eq(_)
                    | GtEq(_)
                    | Gt(_)
                    | NotEq(_) => None,
                    In(_) => None,
                    StartsWith(ref v) => {
                        let v = v.to_string();

                        Some(DefaultBloomFilter::encode(&v))
                    }
                    EndsWith(ref v) => {
                        let v = v.to_string();

                        Some(DefaultBloomFilter::encode(&v))
                    }
                    Contains(ref v) => {
                        let v = v.to_string();

                        Some(DefaultBloomFilter::encode(&v))
                    }
                    Matches(_) => None,
                }
            }

        }
        )+

        $(

        impl From<ScanFilterOp<$ty>> for ScanFilter {
            fn from(source: ScanFilterOp<$ty>) -> ScanFilter {
                ScanFilter::$variant(source)
            }
        }

        )+
    };

    ($( $ty: ty, $variant: ident, $varname: expr ),+ $(,)*) => {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        pub enum ScanFilter {
            $(

            $variant(ScanFilterOp<$ty>),

            )+
        }

        $(

        impl ScanFilterApply<$ty> for ScanFilter {
            #[inline]
            fn apply(&self, tested: &$ty) -> bool {
                if let ScanFilter::$variant(ref op) = *self {
                    op.apply(tested)
                } else {
                    // fail hard in debug builds
                    debug_assert!(false,
                        "Wrong scan filter variant for the column, expected {:?} and got {:?}",
                        self.variant_name(), $varname);

                    error!("unable to apply filter op");
                    false
                }
            }
        }

        )+

        impl ScanFilter {
            #[inline]
            fn variant_name(&self) -> &'static str {
                match *self {
                    $(
                        ScanFilter::$variant(_) => $varname,
                    )+
                }
            }
        }

        impl ScanFilterBloom for ScanFilter {
            #[inline]
            fn bloom_value(&self) -> Option<BloomValue> {
                match *self {
                $(
                    ScanFilter::$variant(ref op) => op.bloom_value(),
                )+
                }
            }
        }

    };
}

scan_filter_impl! {
    u8, U8, "U8",
    u16, U16, "U16",
    u32, U32, "U32",
    u64, U64, "U64",
    u128, U128, "U128",
    String, String, "String",
    i8, I8, "I8",
    i16, I16, "I16",
    i32, I32, "I32",
    i64, I64, "I64",
    i128, I128, "I128",
}

scan_filter_impl! { ops
    u8, U8, "U8",
    u16, U16, "U16",
    u32, U32, "U32",
    u64, U64, "U64",
    u128, U128, "U128",
    i8, I8, "I8",
    i16, I16, "I16",
    i32, I32, "I32",
    i64, I64, "I64",
    i128, I128, "I128",
}


// String

impl ScanFilterOp<String> {
    #[inline]
    fn apply(&self, tested: &str) -> bool {
        use self::ScanFilterOp::*;

        match *self {
            Lt(ref v) => tested < <String as AsRef<str>>::as_ref(v),
            LtEq(ref v) => tested <= <String as AsRef<str>>::as_ref(v),
            Eq(ref v) => tested == <String as AsRef<str>>::as_ref(v),
            GtEq(ref v) => tested >= <String as AsRef<str>>::as_ref(v),
            Gt(ref v) => tested > <String as AsRef<str>>::as_ref(v),
            NotEq(ref v) => tested != <String as AsRef<str>>::as_ref(v),
            In(ref v) => v.contains(tested),
            StartsWith(ref v) => tested.starts_with(<String as AsRef<str>>::as_ref(v)),
            EndsWith(ref v) => tested.ends_with(<String as AsRef<str>>::as_ref(v)),
            Contains(ref v) => tested.contains(<String as AsRef<str>>::as_ref(v)),
            Matches(ref v) => v.is_match(tested),
        }
    }

    #[inline]
    fn bloom_value(&self) -> Option<BloomValue> {
        use self::ScanFilterOp::*;

        match *self {
            Lt(_) => None,
            LtEq(_) => None,
            Eq(ref v) => Some(DefaultBloomFilter::encode(v)),
            GtEq(_) => None,
            Gt(_) => None,
            NotEq(_) => None,
            In(_) => None,
            StartsWith(ref v) => Some(DefaultBloomFilter::encode(v)),
            EndsWith(ref v) => Some(DefaultBloomFilter::encode(v)),
            Contains(ref v) => Some(DefaultBloomFilter::encode(v)),
            Matches(ref v) => {
                let regex_str = v.as_str();

                if Regex::escape(regex_str) == regex_str {
                    Some(DefaultBloomFilter::encode(regex_str))
                } else {
                    None
                }
            }
        }
    }
}

impl From<ScanFilterOp<String>> for ScanFilter {
    fn from(source: ScanFilterOp<String>) -> ScanFilter {
        ScanFilter::String(source)
    }
}

impl<'a> ScanFilterApply<&'a str> for ScanFilter {
    #[inline]
    fn apply(&self, tested: &&'a str) -> bool {
        if let ScanFilter::String(ref op) = *self {
            op.apply(*tested)
        } else {
            // fail hard in debug builds
            debug_assert!(false,
                "Wrong scan filter variant for the column, expected {:?}, got String",
                self.variant_name());

            error!("unable to apply filter op");
            false
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    mod filter {
        use super::*;

        #[test]
        fn type_is_right() {
            let op = ScanFilter::U8(ScanFilterOp::Eq(10));

            assert!(op.apply(&10_u8));
        }

        #[test]
        #[should_panic(
            expected = "Wrong scan filter variant for the column, expected \"U8\" and got \"U16\"")]
        fn type_is_wrong() {
            let op = ScanFilter::U8(ScanFilterOp::Eq(10));

            assert!(op.apply(&10_u16));
        }
    }

    mod merge {
        use super::*;

        fn prepare() -> (ScanResult, ScanResult, ScanResult) {
            let a = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(seqfill!(vec u64, 5))),
                1 => Some(Fragment::from(seqfill!(vec u32))),
                3 => Some(Fragment::from(seqfill!(vec u16, 5))),
                5 => Some(Fragment::from(seqfill!(vec u64, 5))),
            });

            let b = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(seqfill!(vec u64, 10, 5))),
                1 => Some(Fragment::from(seqfill!(vec u32))),
                3 => Some(Fragment::from(seqfill!(vec u16, 5, 15))),
                5 => Some(Fragment::from(seqfill!(vec u64, 10, 20))),
            });

            let expected = ScanResult::from(hashmap! {
                0 => Some({
                    let mut frag = seqfill!(vec u64, 5);
                    frag.extend(seqfill!(vec u64, 10, 5));
                    Fragment::from(frag)
                }),
                1 => Some(Fragment::from(seqfill!(vec u32))),
                3 => Some({
                    let mut frag = seqfill!(vec u16, 5);
                    frag.extend(seqfill!(vec u16, 5, 15));
                    Fragment::from(frag)
                }),
                5 => Some({
                    let mut frag = seqfill!(vec u64, 5);
                    frag.extend(seqfill!(vec u64, 10, 20));
                    Fragment::from(frag)
                }),
            });

            (a, b, expected)
        }

        #[test]
        fn some_a_some_b() {

            let (mut merged, b, expected) = prepare();

            merged.merge(b).with_context(|_| "merge failed").unwrap();

            assert_eq!(merged, expected);
        }

        #[test]
        fn some_a_none_b() {
            let (mut merged, mut b, mut expected) = prepare();

            // remove one fragment
            b.data.remove(&3);

            // replace expected
            expected.data.remove(&3);
            expected.data.insert(3, merged.data[&3].clone());

            let (b, expected) = (b, expected);

            merged.merge(b).with_context(|_| "merge failed").unwrap();

            assert_eq!(merged, expected);
        }

        #[test]
        fn some_a_some_b_sparse() {
            let mut a = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(seqfill!(vec u64, 10))),
                3 => Some(Fragment::from(seqfill!(vec u16, 10))),
                5 => Some(Fragment::from((seqfill!(vec u64, 5), seqfill!(vec u32, 5, 5)))),
            });

            let b = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(seqfill!(vec u64, 10, 20))),
                3 => Some(Fragment::from(seqfill!(vec u16, 10, 15))),
                5 => Some(Fragment::from((seqfill!(vec u64, 5), seqfill!(vec u32, 5)))),
            });

            let expected = ScanResult::from(hashmap! {
                0 => Some({
                    let mut frag = seqfill!(vec u64, 10);
                    frag.extend(seqfill!(vec u64, 10, 20));
                    Fragment::from(frag)
                }),
                3 => Some({
                    let mut frag = seqfill!(vec u16, 10);
                    frag.extend(seqfill!(vec u16, 10, 15));
                    Fragment::from(frag)
                }),
                5 => Some({
                    let mut data = seqfill!(vec u64, 5);
                    data.extend(seqfill!(vec u64, 5));
                    let mut idx = seqfill!(vec u32, 5, 5);
                    idx.extend(seqfill!(vec u32, 5, 10));

                    Fragment::from((data, idx))
                }),
            });

            a.merge(b).with_context(|_| "merge failed").unwrap();

            assert_eq!(expected, a);
        }
    }

    mod dense_len {
        use super::*;

        #[test]
        fn dense_ts() {
            let expected = 5;

            let result = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(seqfill!(vec u64, expected))),
                1 => Some(Fragment::from(seqfill!(vec u32, expected))),
            });

            assert_eq!(result.dense_len(), expected);
        }

        #[test]
        fn dense_no_ts() {
            let expected = 20;

            let result = ScanResult::from(hashmap! {
                4 => Some(Fragment::from((seqfill!(vec u64, 14), seqfill!(vec u32, 4, 11)))),
                6 => Some(Fragment::from(seqfill!(vec u32, expected))),
            });

            assert_eq!(result.dense_len(), expected);
        }

        #[test]
        fn empty() {
            let expected = 0;

            let result = ScanResult::from(hashmap! {});

            assert_eq!(result.dense_len(), expected);
        }

        #[test]
        fn empty_columns() {
            let expected = 0;

            let result = ScanResult::from(hashmap! {
                1 => None,
            });

            assert_eq!(result.dense_len(), expected);
        }

        #[test]
        fn sparse() {
            let expected = 14;

            let result = ScanResult::from(hashmap! {
                3 => Some(Fragment::from((seqfill!(vec u64, 4), seqfill!(vec u32, 3, 8, 2)))),
                4 => Some(Fragment::from((seqfill!(vec u64, 14), seqfill!(vec u32, 4, 11)))),
            });

            assert_eq!(result.dense_len(), expected);
        }
    }

    mod ts_range {
        use super::*;

        // Default range:
        //
        //       |-------------|
        //       10           17

        #[inline]
        fn overlaps_test(range: &ScanTsRange) -> bool {
            let (min, max) = (10, 17);

            range.overlaps_with(min, max)
        }

        #[test]
        fn full() {
            assert_eq!(overlaps_test(&ScanTsRange::Full), true);
        }

        mod bounded {
            use super::*;

            #[inline]
            fn bounds_check<T>(start: T, end: T) -> bool
            where Timestamp: From<T> {
                let start = Timestamp::from(start);
                let end = Timestamp::from(end);

                overlaps_test(&ScanTsRange::Bounded { start, end })
            }


            #[inline]
            fn assert_bounds<T>(start: T, end: T, checks: bool)
            where Timestamp: From<T> {
                assert_eq!(bounds_check(start, end), checks);
            }

            //      |-------------|
            //      10           17
            //          [-----)
            //          12   15

            #[test]
            fn contained() {
                assert_bounds(12, 15, true)
            }

            //      |-------------|
            //      10           17
            //  [---------------------)
            //  8                    19

            #[test]
            fn contains() {
                assert_bounds(8, 19, true)
            }

            //      |-------------|
            //      10           17
            //  [-------)
            //  8       12

            #[test]
            fn overlaps_left() {
                assert_bounds(8, 12, true)
            }

            //      |-------------|
            //      10           17
            //                [-------)
            //                15     19

            #[test]
            fn overlaps_right() {
                assert_bounds(15, 19, true)
            }

            //      |-------------|
            //      10           17
            //                    [---)
            //                    17 19

            #[test]
            fn overlaps_right_edge() {
                assert_bounds(17, 19, true)
            }

            //      |-------------|
            //      10           17
            //                  [---)
            //                  16 18

            #[test]
            fn overlaps_right_near_edge() {
                assert_bounds(16, 18, true)
            }

            //      |-------------|
            //      10           17
            //  [---)
            //  8  10

            #[test]
            fn overlaps_left_edge() {
                assert_bounds(8, 10, false)
            }

            //      |-------------|
            //      10           17
            //  [-----)
            //  8    11

            #[test]
            fn overlaps_left_near_edge() {
                assert_bounds(8, 11, true)
            }

            //      |-------------|
            //      10           17
            //                      [---)
            //                      18 20

            #[test]
            fn adjacent_right() {
                assert_bounds(18, 20, false)
            }

            //      |-------------|
            //      10           17
            //  [-)
            //  8 9

            #[test]
            fn adjacent_left() {
                assert_bounds(8, 9, false)
            }
        }

        mod from {
            use super::*;

            //      |-------------|
            //      10           17
            //  [=>
            //  8

            #[test]
            fn from_left() {
                assert_eq!(overlaps_test(&ScanTsRange::From { start: Timestamp::from(8) }), true);
            }

            //      |-------------|
            //      10           17
            //      [=>
            //      10

            #[test]
            fn from_left_edge() {
                assert_eq!(overlaps_test(&ScanTsRange::From { start: Timestamp::from(10) }), true);
            }

            //      |-------------|
            //      10           17
            //            [=>
            //            13

            #[test]
            fn from_within() {
                assert_eq!(overlaps_test(&ScanTsRange::From { start: Timestamp::from(13) }), true);
            }

            //      |-------------|
            //      10           17
            //                      [=>
            //                      18

            #[test]
            fn from_right() {
                assert_eq!(overlaps_test(&ScanTsRange::From { start: Timestamp::from(18) }), false);
            }

            //      |-------------|
            //      10           17
            //                    [=>
            //                    17

            #[test]
            fn from_right_edge() {
                assert_eq!(overlaps_test(&ScanTsRange::From { start: Timestamp::from(17) }), true);
            }
        }

        mod to {
            use super::*;

            //      |-------------|
            //      10           17
            //  =>)
            //    9

            #[test]
            fn to_left() {
                assert_eq!(overlaps_test(&ScanTsRange::To { end: Timestamp::from(9) }), false);
            }

            //      |-------------|
            //      10           17
            //    =>)
            //      10

            #[test]
            fn to_left_edge() {
                assert_eq!(overlaps_test(&ScanTsRange::To { end: Timestamp::from(10) }), false);
            }

            //      |-------------|
            //      10           17
            //          =>)
            //            13

            #[test]
            fn to_within() {
                assert_eq!(overlaps_test(&ScanTsRange::To { end: Timestamp::from(13) }), true);
            }

            //      |-------------|
            //      10           17
            //                    =>)
            //                      18

            #[test]
            fn to_right() {
                assert_eq!(overlaps_test(&ScanTsRange::To { end: Timestamp::from(18) }), true);
            }

            //      |-------------|
            //      10           17
            //                  =>)
            //                   17

            #[test]
            fn to_right_edge() {
                assert_eq!(overlaps_test(&ScanTsRange::To { end: Timestamp::from(17) }), true);
            }
        }
    }
}
