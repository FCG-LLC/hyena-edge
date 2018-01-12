use ty::{BlockStorage, BlockStorageType};
use block::BlockType;
use std::fmt::{Display, Error as FmtError, Formatter};
use std::ops::Deref;
use std::result::Result as StdResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub(crate) ty: BlockStorage,
    pub(crate) name: String,
}

impl Column {
    pub fn new(ty: BlockStorage, name: &str) -> Column {
        Column {
            ty,
            name: name.to_owned(),
        }
    }

    pub fn block_type(&self) -> BlockType {
        *self.ty
    }

    pub fn storage_type(&self) -> BlockStorageType {
        self.ty.storage_type()
    }
}

impl Deref for Column {
    type Target = BlockStorage;

    fn deref(&self) -> &Self::Target {
        &self.ty
    }
}

impl Display for Column {
    fn fmt(&self, fmt: &mut Formatter) -> StdResult<(), FmtError> {
        write!(fmt, "{}", self.name)
    }
}
