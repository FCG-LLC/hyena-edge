use ty::ColumnId;
use hyena_common::collections::HashMap;
use params::SourceId;

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
