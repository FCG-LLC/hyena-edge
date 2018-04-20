/// The size of a single block file
#[cfg(not(test))]
pub(crate) const BLOCK_SIZE: usize = 1 << 20; // 1 MiB

/// The size of a single block file for test code
#[cfg(test)]
pub(crate) const BLOCK_SIZE: usize = 1 << 20; // 1 MiB

/// The name of partition metadata file
pub(crate) const PARTITION_METADATA: &str = "meta.data";

/// The name of partition group metadata file
pub(crate) const PARTITION_GROUP_METADATA: &str = "pgmeta.data";

/// The name of catalog metadata file
pub(crate) const CATALOG_METADATA: &str = "catalog.data";

/// The type of source_id column
pub type SourceId = u32;

// The index of timestamp column
pub const TIMESTAMP_COLUMN: super::ty::ColumnId = 0;