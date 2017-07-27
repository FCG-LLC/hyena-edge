use error::*;
use block::SparseIndex;
use ty::Timestamp;
use std::slice::from_raw_parts;
use std::mem::transmute;


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Fragment {
    I8Dense(Vec<i8>),
    I16Dense(Vec<i16>),
    I32Dense(Vec<i32>),
    I64Dense(Vec<i64>),
    #[cfg(feature = "block_128")]
    I128Dense(Vec<i128>),

    // Dense, Unsigned
    U8Dense(Vec<u8>),
    U16Dense(Vec<u16>),
    U32Dense(Vec<u32>),
    U64Dense(Vec<u64>),
    #[cfg(feature = "block_128")]
    U128Dense(Vec<u128>),

    // Sparse, Signed
    I8Sparse(Vec<i8>, Vec<SparseIndex>),
    I16Sparse(Vec<i16>, Vec<SparseIndex>),
    I32Sparse(Vec<i32>, Vec<SparseIndex>),
    I64Sparse(Vec<i64>, Vec<SparseIndex>),
    #[cfg(feature = "block_128")]
    I128Sparse(Vec<i128>, Vec<SparseIndex>),

    // Sparse, Unsigned
    U8Sparse(Vec<u8>, Vec<SparseIndex>),
    U16Sparse(Vec<u16>, Vec<SparseIndex>),
    U32Sparse(Vec<u32>, Vec<SparseIndex>),
    U64Sparse(Vec<u64>, Vec<SparseIndex>),
    #[cfg(feature = "block_128")]
    U128Sparse(Vec<u128>, Vec<SparseIndex>),
}

macro_rules! fragment_variant_impl {

    (dense $($V: ident, $T: ty);* $(;)*) => {
        $(
            impl From<Vec<$T>> for Fragment {
                fn from(source: Vec<$T>) -> Fragment {
                    Fragment::$V(source)
                }
            }
        )*
    };

    (sparse $($V: ident, $T: ty);* $(;)*) => {
        $(
            impl From<(Vec<$T>, Vec<SparseIndex>)> for Fragment {
                fn from(source: (Vec<$T>, Vec<SparseIndex>)) -> Fragment {
                    Fragment::$V(source.0, source.1)
                }
            }
        )*
    };
}

fragment_variant_impl!(dense
                       I8Dense, i8;
                       I16Dense, i16;
                       I32Dense, i32;
                       I64Dense, i64;
                       U8Dense, u8;
                       U16Dense, u16;
                       U32Dense, u32;
                       U64Dense, u64;);

fragment_variant_impl!(sparse
                       I8Sparse, i8;
                       I16Sparse, i16;
                       I32Sparse, i32;
                       I64Sparse, i64;
                       U8Sparse, u8;
                       U16Sparse, u16;
                       U32Sparse, u32;
                       U64Sparse, u64;);


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimestampFragment(Vec<Timestamp>);

impl From<Vec<u64>> for TimestampFragment {
    fn from(source: Vec<u64>) -> TimestampFragment {
        TimestampFragment(unsafe { transmute(source) })
    }
}

impl From<Vec<Timestamp>> for TimestampFragment {
    fn from(source: Vec<Timestamp>) -> TimestampFragment {
        TimestampFragment(source)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    mod dense {
        use super::*;

        macro_rules! dense_fragment_test_impl {

            ($($mod: ident, $T: ident, $B: ty);* $(;)*) => {
                $(
                    mod $mod {
                        use super::*;

                        #[test]
                        fn is_eq() {
                            let buf = (1..100).into_iter().collect::<Vec<$B>>();

                            let frag = Fragment::from(buf.clone());

                            assert_variant!(frag, Fragment::$T(val), &val[..] == &buf[..])
                        }
                    }
                )*
            }
        }

        dense_fragment_test_impl!(i8_dense, I8Dense, i8;
                                i16_dense, I16Dense, i16;
                                i32_dense, I32Dense, i32;
                                i64_dense, I64Dense, i64;
                                u8_dense, U8Dense, u8;
                                u16_dense, U16Dense, u16;
                                u32_dense, U32Dense, u32;
                                u64_dense, U64Dense, u64;);
    }

    mod sparse {
        use super::*;

        macro_rules! sparse_fragment_test_impl {

            ($($mod: ident, $T: ident, $B: ty);* $(;)*) => {
                $(
                    mod $mod {
                        use super::*;

                        #[test]
                        fn is_eq() {
                            let buf = (1..100).into_iter().collect::<Vec<$B>>();
                            let idx = (1..100).into_iter().map(|v| v * 3).collect::<Vec<_>>();

                            let frag = Fragment::from((buf.clone(), idx.clone()));

                            assert_variant!(frag, Fragment::$T(val, vidx),
                                            &val[..] == &buf[..] && &vidx[..] == &idx[..])
                        }
                    }
                )*
            }
        }

        sparse_fragment_test_impl!(i8_sparse, I8Sparse, i8;
                                i16_sparse, I16Sparse, i16;
                                i32_sparse, I32Sparse, i32;
                                i64_sparse, I64Sparse, i64;
                                u8_sparse, U8Sparse, u8;
                                u16_sparse, U16Sparse, u16;
                                u32_sparse, U32Sparse, u32;
                                u64_sparse, U64Sparse, u64;);
    }

    mod timestamp {
        use super::*;
        use helpers::random::timestamp::{RandomTimestamp, RandomTimestampGen};

        #[test]
        fn is_eq() {
            let buf = RandomTimestampGen::iter()
                .take(100)
                .collect::<Vec<Timestamp>>();

            let frag = TimestampFragment::from(buf.clone());

            assert_eq!(frag.0, &buf[..]);
        }
    }
}
