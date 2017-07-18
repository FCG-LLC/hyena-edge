use error::*;
use block::SparseIndex;
use ty::Timestamp;
use ty::basic::*;
use std::slice::from_raw_parts;


#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum Fragment<'a> {
    I8Dense(&'a [I8]),
    I16Dense(&'a [I16]),
    I32Dense(&'a [I32]),
    I64Dense(&'a [I64]),
    #[cfg(feature = "block_128")]
    I128Dense(&'a [I128]),

    // Dense, Unsigned
    U8Dense(&'a [U8]),
    U16Dense(&'a [U16]),
    U32Dense(&'a [U32]),
    U64Dense(&'a [U64]),
    #[cfg(feature = "block_128")]
    U128Dense(&'a [U128]),

    // Sparse, Signed
    I8Sparse(&'a [I8], &'a [SparseIndex]),
    I16Sparse(&'a [I16], &'a [SparseIndex]),
    I32Sparse(&'a [I32], &'a [SparseIndex]),
    I64Sparse(&'a [I64], &'a [SparseIndex]),
    #[cfg(feature = "block_128")]
    I128Sparse(&'a [I128], &'a [SparseIndex]),

    // Sparse, Unsigned
    U8Sparse(&'a [U8], &'a [SparseIndex]),
    U16Sparse(&'a [U16], &'a [SparseIndex]),
    U32Sparse(&'a [U32], &'a [SparseIndex]),
    U64Sparse(&'a [U64], &'a [SparseIndex]),
    #[cfg(feature = "block_128")]
    U128Sparse(&'a [U128], &'a [SparseIndex]),
}

macro_rules! fragment_variant_impl {

    (dense $($V: ident, $BT: ty, $T: ty);* $(;)*) => {
        $(
            impl<'a> From<&'a [$BT]> for Fragment<'a> {
                fn from(source: &'a [$BT]) -> Fragment<'a> {
                    Fragment::$V(source)
                }
            }

            impl<'a> From<&'a [$T]> for Fragment<'a> {
                fn from(source: &'a [$T]) -> Fragment<'a> {
                    Fragment::$V(unsafe {
                        from_raw_parts(source.as_ptr() as *const $BT, source.len())
                    })
                }
            }
        )*
    };

    (sparse $($V: ident, $BT: ty, $T: ty);* $(;)*) => {
        $(
            impl<'a> From<(&'a [$BT], &'a [SparseIndex])> for Fragment<'a> {
                fn from(source: (&'a [$BT], &'a [SparseIndex])) -> Fragment<'a> {
                    Fragment::$V(source.0, source.1)
                }
            }

            impl<'a> From<(&'a [$T], &'a [SparseIndex])> for Fragment<'a> {
                fn from(source: (&'a [$T], &'a [SparseIndex])) -> Fragment<'a> {
                    Fragment::$V(unsafe {
                        from_raw_parts(source.0.as_ptr() as *const $BT, source.0.len())
                    }, source.1)
                }
            }
        )*
    };
}

fragment_variant_impl!(dense
                       I8Dense, I8, i8;
                       I16Dense, I16, i16;
                       I32Dense, I32, i32;
                       I64Dense, I64, i64;
                       U8Dense, U8, u8;
                       U16Dense, U16, u16;
                       U32Dense, U32, u32;
                       U64Dense, U64, u64;);

fragment_variant_impl!(sparse
                       I8Sparse, I8, i8;
                       I16Sparse, I16, i16;
                       I32Sparse, I32, i32;
                       I64Sparse, I64, i64;
                       U8Sparse, U8, u8;
                       U16Sparse, U16, u16;
                       U32Sparse, U32, u32;
                       U64Sparse, U64, u64;);


pub struct TimestampFragment<'a>(&'a [Timestamp]);

impl<'a> From<&'a [u64]> for TimestampFragment<'a> {
    fn from(source: &'a [u64]) -> TimestampFragment<'a> {
        TimestampFragment(unsafe {
            from_raw_parts(source.as_ptr() as *const Timestamp, source.len())
        })
    }
}

impl<'a> From<&'a [Timestamp]> for TimestampFragment<'a> {
    fn from(source: &'a [Timestamp]) -> TimestampFragment<'a> {
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

                            let frag = Fragment::from(&buf[..]);

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

                            let frag = Fragment::from((&buf[..], &idx[..]));

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

            let frag = TimestampFragment::from(&buf[..]);

            assert_eq!(frag.0, &buf[..]);
        }
    }
}
