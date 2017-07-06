use std::collections::HashMap;

pub(super) mod timestamp;
pub(super) mod block;

pub(crate) use self::timestamp::{Timestamp, ToTimestampMicros};
pub(crate) use self::block::Block;


pub type ColumnId = usize;
pub type BlockId = usize;

pub type BlockMap<'block> = HashMap<BlockId, Block<'block>>;
