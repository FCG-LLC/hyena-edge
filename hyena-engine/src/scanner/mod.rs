use error::*;
use ty::ColumnId;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use serde::{Deserialize, Serialize};
use partition::PartitionId;
use params::SourceId;
use ty::fragment::Fragment;

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
                let mut self_data = v.as_mut().unwrap();
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
    i8, I8,
    i16, I16,
    i32, I32,
    i64, I64,
}
