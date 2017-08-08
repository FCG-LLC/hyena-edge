use error::*;
use std::collections::HashMap;
use ty::{ColumnId, Fragment, FragmentRef};

pub mod append;

pub type BlockData = HashMap<ColumnId, Fragment>;
pub type BlockRefData<'frag> = HashMap<ColumnId, FragmentRef<'frag>>;
