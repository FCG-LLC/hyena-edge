pub(super) mod dense;
pub(super) mod sparse;

pub(crate) use self::dense::DenseNumericBlock;
pub(crate) use self::sparse::{SparseNumericBlock, SparseIndexedNumericBlock, SparseIndex};
