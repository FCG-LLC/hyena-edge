use error::*;
use ty::ColumnId;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use partition::PartitionId;
use params::SourceId;
use ty::fragment::Fragment;
use extprim::i128::i128;
use extprim::u128::u128;

pub type ScanFilters = HashMap<ColumnId, Vec<ScanFilter>>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scan {
    pub(crate) ts_range: Option<ScanTsRange>,
    pub(crate) partitions: Option<HashSet<PartitionId>>,
    pub(crate) groups: Option<Vec<SourceId>>,
    pub(crate) projection: Option<Vec<ColumnId>>,
    pub(crate) filters: HashMap<ColumnId, Vec<ScanFilter>>,
}

impl Scan {
    pub fn new(
        filters: HashMap<ColumnId, Vec<ScanFilter>>,
        projection: Option<Vec<ColumnId>>,
        groups: Option<Vec<SourceId>>,
        partitions: Option<HashSet<PartitionId>>,
        ts_range: Option<ScanTsRange>,
    ) -> Scan {
        Scan {
            filters,
            projection,
            groups,
            partitions,
            ts_range,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScanTsRange {
    Full,
    Bounded { start: u64, end: u64 }, // inclusive, exclusive
    From { start: u64 },              // inclusive
    To { end: u64 },                  // exclusive
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScanFilterOp<T: Debug + Clone + PartialEq + PartialOrd + Hash + Eq> {
    Lt(T),
    LtEq(T),
    Eq(T),
    GtEq(T),
    Gt(T),
    NotEq(T),
    In(HashSet<T>),
}

impl<T: Debug + Clone + PartialEq + PartialOrd + Hash + Eq> ScanFilterOp<T> {
    #[inline]
    fn apply(&self, tested: &T) -> bool {
        use self::ScanFilterOp::*;

        match *self {
            Lt(ref v) => tested < v,
            LtEq(ref v) => tested <= v,
            Eq(ref v) => tested == v,
            GtEq(ref v) => tested >= v,
            Gt(ref v) => tested > v,
            NotEq(ref v) => tested != v,
            In(ref v) => v.contains(&tested),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ScanResult {
    pub(crate) data: HashMap<ColumnId, Option<Fragment>>,
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
                    .chain_err(|| "unable to merge scan results")?;
            }
        }

        // add columns that are in other but not in self
        self.data.extend(other.data.drain());

        Ok(())
    }

    /// Get the length of a dense result `Fragment`
    ///
    /// This is needed for proper sparse offset calculation.

    fn dense_len(&self) -> usize {
        // try to find the first dense column
        // in most cases it should be a `ts`, which has index = 0

        if let Some((_, dense)) = self.data.iter()
            .find(|&(colidx, col)| {
                // this is a very ugly hack
                // that needs to be here until we get rid of source_id column
                // treat source_id column (index == 1) as sparse
                // as it's always empty anyway
                *colidx != 1 &&

                // this is the proper part
                // that gets to stay
                col.as_ref().map_or(false, |col| !col.is_sparse())
            }) {

            dense.as_ref().map_or(0, |col| col.len())
        } else {
            // all sparse columns, so we have to find the maximum index value
            // which means iterating over all columns

            use std::cmp::max;

            self.data.iter().fold(0, |max_idx, (_, col)| {
                // all columns have to be sparse here
                // hence why only debug-time assert
                debug_assert!(col.as_ref().map_or(true, |col| col.is_sparse()));

                // this is not a logic error, as this is used to determine
                // the base offset for sparse index
                // so zero in case of all empty Fragments is perfectly valid
                max(max_idx,
                    col.as_ref()
                        .map_or(0, |col| col.max_index().unwrap_or(0)))
            })
        }

    }
}

impl From<HashMap<ColumnId, Option<Fragment>>> for ScanResult {
    fn from(data: HashMap<ColumnId, Option<Fragment>>) -> ScanResult {
        ScanResult { data }
    }
}

pub trait ScanFilterApply<T> {
    #[inline]
    fn apply(&self, tested: &T) -> bool;
}

macro_rules! scan_filter_impl {

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

        $(

        impl From<ScanFilterOp<$ty>> for ScanFilter {
            fn from(source: ScanFilterOp<$ty>) -> ScanFilter {
                ScanFilter::$variant(source)
            }
        }

        )+
    };
}

scan_filter_impl! {
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

            merged.merge(b).chain_err(|| "merge failed").unwrap();

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

            merged.merge(b).chain_err(|| "merge failed").unwrap();

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

            a.merge(b).chain_err(|| "merge failed").unwrap();

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
}
