#[macro_use]
pub(super) mod block;

#[macro_use]
pub mod fragment;

pub(crate) use self::block::{BlockHeadMap, BlockMap, BlockStorageMap, BlockStorageMapType};
pub use self::block::{BlockStorage, BlockStorageType};
pub use self::fragment::{Fragment, FragmentRef, TimestampFragment};

pub type ColumnId = usize;
pub type RowId = usize;
pub use params::SourceId;
