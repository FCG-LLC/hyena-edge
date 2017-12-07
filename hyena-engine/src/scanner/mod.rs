use error::*;
use ty::ColumnId;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use partition::PartitionId;
use params::SourceId;
use ty::fragment::Fragment;
use extprim::i128::i128;
use extprim::u128::u128;

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

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScanFilterOp<T: Debug + Clone + PartialEq + PartialOrd> {
    Lt(T),
    LtEq(T),
    Eq(T),
    GtEq(T),
    Gt(T),
    NotEq(T),
}

impl<T: Debug + Clone + PartialEq + PartialOrd> ScanFilterOp<T> {
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ScanResult {
    pub(crate) data: HashMap<ColumnId, Option<Fragment>>,
}

impl ScanResult {

    /// Merge two `ScanResult`s
    ///
    /// We are assuming that the contained columns are the same
    /// because that should always be the case in our "supertable" model
    ///
    pub(crate) fn merge(&mut self, mut other: ScanResult) -> Result<()> {
        for (k, v) in self.data.iter_mut() {
            let o = if let Some(o) = other.data.remove(k) {
                o
            } else {
                None
            };

            if v.is_none() {
                *v = o;
            } else if o.is_some() {
                let self_data = v.as_mut().unwrap();
                let mut other_data = o.unwrap();

                self_data
                    .merge(&mut other_data)
                    .chain_err(|| "unable to merge scan results")?;
            }
        }

        Ok(())
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

    ($( $ty: ty, $variant: ident ),+ $(,)*) => {
        #[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
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
                    error!("unable to apply filter op");
                    false
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
}

scan_filter_impl! {
    u8, U8,
    u16, U16,
    u32, U32,
    u64, U64,
    u128, U128,
    i8, I8,
    i16, I16,
    i32, I32,
    i64, I64,
    i128, I128,
}

#[cfg(test)]
mod tests {
    use super::*;

    mod merge {
        use super::*;

        fn prepare() -> (ScanResult, ScanResult, ScanResult) {
            let a = ScanResult::from(hashmap_mut! {
                0 => Some(Fragment::from(seqfill!(vec u64, 5))),
                1 => Some(Fragment::from(seqfill!(vec u32))),
                3 => Some(Fragment::from(seqfill!(vec u16, 5))),
                5 => Some(Fragment::from(seqfill!(vec u64, 5))),
            });

            let b = ScanResult::from(hashmap_mut! {
                0 => Some(Fragment::from(seqfill!(vec u64, 10, 5))),
                1 => Some(Fragment::from(seqfill!(vec u32))),
                3 => Some(Fragment::from(seqfill!(vec u16, 5, 15))),
                5 => Some(Fragment::from(seqfill!(vec u64, 10, 20))),
            });

            let expected = ScanResult::from(hashmap_mut! {
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
    }
}
