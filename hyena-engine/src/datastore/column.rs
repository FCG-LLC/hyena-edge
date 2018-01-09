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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub(crate) ty: TyBlockType,
    pub(crate) name: String,
}

impl Column {
    pub fn new(ty: TyBlockType, name: &str) -> Column {
        Column {
            ty,
            name: name.to_owned(),
        }
    }
}

impl Deref for Column {
    type Target = TyBlockType;

    fn deref(&self) -> &Self::Target {
        &self.ty
    }
}

impl Display for Column {
    fn fmt(&self, fmt: &mut Formatter) -> StdResult<(), FmtError> {
        write!(fmt, "{}", self.name)
    }
}
