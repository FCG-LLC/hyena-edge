#[macro_use]
pub(super) mod block;

#[macro_use]
pub mod fragment;

pub(crate) use self::block::{BlockHeadMap, BlockId, BlockMap, BlockStorageTypeMap};
pub use self::block::BlockStorageType;
pub use self::fragment::{Fragment, FragmentRef, TimestampFragment};

pub type ColumnId = usize;
pub type RowId = usize;
pub use params::SourceId;
