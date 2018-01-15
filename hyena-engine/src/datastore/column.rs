use ty::BlockType as TyBlockType;
use std::fmt::{Display, Error as FmtError, Formatter};
use std::ops::Deref;
use std::result::Result as StdResult;

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
