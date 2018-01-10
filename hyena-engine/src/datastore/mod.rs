use error::*;
use ty::{BlockType as TyBlockType, ColumnId};
use hyena_common::ty::{Timestamp, MIN_TIMESTAMP};
use block::{BlockType, SparseIndex};
use storage::manager::{PartitionGroupManager, PartitionManager};
use std::collections::hash_map::HashMap;
use std::collections::vec_deque::VecDeque;
use std::path::{Path, PathBuf};
use std::iter::FromIterator;
use std::fmt::{Display, Error as FmtError, Formatter};
use std::default::Default;
use std::ops::Deref;
use std::result::Result as StdResult;
use std::sync::RwLock;
use params::{SourceId, CATALOG_METADATA, PARTITION_GROUP_METADATA};
use mutator::append::Append;
use scanner::{Scan, ScanResult};
use ty::block::{BlockTypeMap, BlockTypeMapTy};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

mod catalog;
mod column;
mod partition;
mod partition_group;
mod partition_meta;

pub use self::catalog::Catalog;
pub use self::column::Column;
pub(crate) use self::partition::{Partition, PartitionId};
pub(crate) use self::partition_meta::PartitionMeta;
pub(crate) use self::partition_group::PartitionGroup;

pub(crate) type PartitionMap<'part> = HashMap<PartitionMeta, Partition<'part>>;
pub(crate) type PartitionGroupMap<'pg> = HashMap<SourceId, PartitionGroup<'pg>>;
pub type ColumnMap = HashMap<ColumnId, Column>;

#[cfg(test)]
mod tests;
