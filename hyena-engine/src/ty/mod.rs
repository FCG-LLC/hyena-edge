#[macro_use]
pub(super) mod block;

#[macro_use]
pub mod fragment;

pub(crate) use self::block::{BlockHeadMap, BlockId, BlockMap, BlockTypeMap};
pub use self::block::BlockType;
pub use self::fragment::{Fragment, FragmentRef, TimestampFragment};

pub type ColumnId = usize;
pub type RowId = usize;
pub use params::SourceId;
