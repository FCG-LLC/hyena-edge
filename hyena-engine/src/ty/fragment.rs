use error::*;
use block::SparseIndex;
use ty::Timestamp;
use std::mem::transmute;
use extprim::i128::i128;
use extprim::u128::u128;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Fragment {
    I8Dense(Vec<i8>),
    I16Dense(Vec<i16>),
    I32Dense(Vec<i32>),
    I64Dense(Vec<i64>),
    I128Dense(Vec<i128>),

    // Dense, Unsigned
    U8Dense(Vec<u8>),
    U16Dense(Vec<u16>),
    U32Dense(Vec<u32>),
    U64Dense(Vec<u64>),
    U128Dense(Vec<u128>),

    // Sparse, Signed
    I8Sparse(Vec<i8>, Vec<SparseIndex>),
    I16Sparse(Vec<i16>, Vec<SparseIndex>),
    I32Sparse(Vec<i32>, Vec<SparseIndex>),
    I64Sparse(Vec<i64>, Vec<SparseIndex>),
    I128Sparse(Vec<i128>, Vec<SparseIndex>),

    // Sparse, Unsigned
    U8Sparse(Vec<u8>, Vec<SparseIndex>),
    U16Sparse(Vec<u16>, Vec<SparseIndex>),
    U32Sparse(Vec<u32>, Vec<SparseIndex>),
    U64Sparse(Vec<u64>, Vec<SparseIndex>),
    U128Sparse(Vec<u128>, Vec<SparseIndex>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum FragmentRef<'frag> {
    I8Dense(&'frag [i8]),
    I16Dense(&'frag [i16]),
    I32Dense(&'frag [i32]),
    I64Dense(&'frag [i64]),
    I128Dense(&'frag [i128]),

    // Dense, Unsigned
    U8Dense(&'frag [u8]),
    U16Dense(&'frag [u16]),
    U32Dense(&'frag [u32]),
    U64Dense(&'frag [u64]),
    U128Dense(&'frag [u128]),

    // Sparse, Signed
    I8Sparse(&'frag [i8], &'frag [SparseIndex]),
    I16Sparse(&'frag [i16], &'frag [SparseIndex]),
    I32Sparse(&'frag [i32], &'frag [SparseIndex]),
    I64Sparse(&'frag [i64], &'frag [SparseIndex]),
    I128Sparse(&'frag [i128], &'frag [SparseIndex]),

    // Sparse, Unsigned
    U8Sparse(&'frag [u8], &'frag [SparseIndex]),
    U16Sparse(&'frag [u16], &'frag [SparseIndex]),
    U32Sparse(&'frag [u32], &'frag [SparseIndex]),
    U64Sparse(&'frag [u64], &'frag [SparseIndex]),
    U128Sparse(&'frag [u128], &'frag [SparseIndex]),
}

macro_rules! frag_apply {
    (@ $self: expr, $block: ident, $idx: ident
        dense [ $dense: block, $( $dense_variants: ident ),+ $(,)* ]
        sparse [ $sparse: block, $( $sparse_variants: ident ),+ $(,)* ]) => {{

        match $self {
            $(
                $dense_variants(ref $block) => $dense,
            )+

            $(
                $sparse_variants(ref $block, ref $idx) => $sparse,
            )+
        }
    }};

    ($self: expr, $block: ident, $idx: ident, $dense: block, $sparse: block) => {
        frag_apply!(@ $self, $block, $idx
            dense [ $dense, I8Dense, I16Dense, I32Dense, I64Dense, I128Dense,
                            U8Dense, U16Dense, U32Dense, U64Dense, U128Dense ]
            sparse [ $sparse, I8Sparse, I16Sparse, I32Sparse, I64Sparse, I128Sparse,
                              U8Sparse, U16Sparse, U32Sparse, U64Sparse, U128Sparse ]
        )
    };
}

impl Fragment {
    #[allow(unused)]
    pub fn split_at<'frag: 'fragref, 'fragref>(
        &'frag self,
        mid: usize,
    ) -> (FragmentRef<'fragref>, FragmentRef<'fragref>) {
        use self::Fragment::*;

        frag_apply!(
            *self,
            blk,
            idx,
            {
                let fragments = &blk[..].split_at(mid);
                (fragments.0.into(), fragments.1.into())
            },
            {
                let fragments = &blk[..].split_at(mid);
                let indices = &idx[..].split_at(mid);
                (
                    (fragments.0, indices.0).into(),
                    (fragments.1, indices.1).into(),
                )
            }
        )
    }

    #[allow(unused)]
    pub fn is_sparse(&self) -> bool {
        use self::Fragment::*;

        frag_apply!(*self, _blk, _idx, { false }, { true })
    }

    #[allow(unused)]
    pub fn split_at_idx<'frag: 'fragref, 'fragref>(
        &'frag self,
        idx: SparseIndex,
    ) -> Result<(FragmentRef<'fragref>, FragmentRef<'fragref>)> {
        use self::Fragment::*;

        if self.is_sparse() {
            Ok(frag_apply!(*self, _blk, blk_idx, { unreachable!() }, {
                let mid = if let Some(idx) = blk_idx.iter().position(|val| *val >= idx) {
                    idx
                } else {
                    blk_idx.len()
                };

                let fragments = &_blk[..].split_at(mid);
                let indices = &blk_idx[..].split_at(mid);
                (
                    (fragments.0, indices.0).into(),
                    (fragments.1, indices.1).into(),
                )
            }))
        } else {
            Err("split_at_idx called on a dense block".into())
        }
    }

    #[allow(unused)]
    pub fn len(&self) -> usize {
        use self::Fragment::*;

        frag_apply!(*self, blk, _idx, { blk.len() }, { blk.len() })
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        use self::Fragment::*;

        frag_apply!(*self, blk, _idx, { blk.is_empty() }, { blk.is_empty() })
    }
}

impl<'fragref> FragmentRef<'fragref> {
    pub fn split_at<'frag>(
        &'frag self,
        mid: usize,
    ) -> (FragmentRef<'fragref>, FragmentRef<'fragref>) {
        use self::FragmentRef::*;

        frag_apply!(
            *self,
            blk,
            idx,
            {
                let fragments = &blk[..].split_at(mid);
                (fragments.0.into(), fragments.1.into())
            },
            {
                let fragments = &blk[..].split_at(mid);
                let indices = &idx[..].split_at(mid);
                (
                    (fragments.0, indices.0).into(),
                    (fragments.1, indices.1).into(),
                )
            }
        )
    }

    pub fn is_sparse(&self) -> bool {
        use self::FragmentRef::*;

        frag_apply!(*self, _blk, _idx, { false }, { true })
    }

    pub fn split_at_idx<'frag>(
        &'frag self,
        idx: SparseIndex,
    ) -> Result<(FragmentRef<'fragref>, FragmentRef<'fragref>)> {
        use self::FragmentRef::*;

        if self.is_sparse() {
            Ok(frag_apply!(*self, _blk, blk_idx, { unreachable!() }, {
                let mid = if let Some(idx) = blk_idx.iter().position(|val| *val >= idx) {
                    idx
                } else {
                    blk_idx.len()
                };

                let fragments = &_blk[..].split_at(mid);
                let indices = &blk_idx[..].split_at(mid);
                (
                    (fragments.0, indices.0).into(),
                    (fragments.1, indices.1).into(),
                )
            }))
        } else {
            Err("split_at_idx called on a dense block".into())
        }
    }

    #[allow(unused)]
    pub fn len(&self) -> usize {
        use self::FragmentRef::*;

        frag_apply!(*self, blk, _idx, { blk.len() }, { blk.len() })
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        use self::FragmentRef::*;

        frag_apply!(*self, blk, _idx, { blk.is_empty() }, { blk.is_empty() })
    }
}


impl<'frag> From<&'frag Fragment> for FragmentRef<'frag> {
    fn from(source: &'frag Fragment) -> FragmentRef<'frag> {
        use self::Fragment::*;

        frag_apply!(*source, blk, idx, { FragmentRef::from(blk.as_slice()) }, {
            FragmentRef::from((blk.as_slice(), idx.as_slice()))
        })
    }
}

impl<'frag> From<&'frag TimestampFragment> for FragmentRef<'frag> {
    fn from(source: &'frag TimestampFragment) -> FragmentRef<'frag> {
        FragmentRef::from(unsafe {
            transmute::<&'frag [Timestamp], &'frag [u64]>(&source.0)
        })
    }
}

impl<'frag> From<&'frag [Timestamp]> for FragmentRef<'frag> {
    fn from(source: &'frag [Timestamp]) -> FragmentRef<'frag> {
        FragmentRef::from(unsafe {
            transmute::<&'frag [Timestamp], &'frag [u64]>(source)
        })
    }
}

macro_rules! fragment_variant_impl {

    (dense $($V: ident, $T: ty);* $(;)*) => {
        $(
            impl From<Vec<$T>> for Fragment {
                fn from(source: Vec<$T>) -> Fragment {
                    Fragment::$V(source)
                }
            }

            impl<'frag> From<&'frag [$T]> for FragmentRef<'frag> {
                fn from(source: &'frag [$T]) -> FragmentRef<'frag> {
                    FragmentRef::$V(source)
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

            impl<'frag> From<(&'frag [$T], &'frag [SparseIndex])> for FragmentRef<'frag> {
                fn from(source: (&'frag [$T], &'frag [SparseIndex])) -> FragmentRef<'frag> {
                    FragmentRef::$V(source.0, source.1)
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
                       I128Dense, i128;
                       U8Dense, u8;
                       U16Dense, u16;
                       U32Dense, u32;
                       U64Dense, u64;
                       U128Dense, u128;);

fragment_variant_impl!(sparse
                       I8Sparse, i8;
                       I16Sparse, i16;
                       I32Sparse, i32;
                       I64Sparse, i64;
                       I128Sparse, i128;
                       U8Sparse, u8;
                       U16Sparse, u16;
                       U32Sparse, u32;
                       U64Sparse, u64;
                       U128Sparse, u128;);


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimestampFragment(Vec<Timestamp>);

impl TimestampFragment {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_slice(&self) -> &[Timestamp] {
        self.0.as_slice()
    }
}

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

impl From<TimestampFragment> for Fragment {
    fn from(source: TimestampFragment) -> Fragment {
        Fragment::from(unsafe { transmute::<Vec<Timestamp>, Vec<u64>>(source.0) })
    }
}

impl From<Vec<Timestamp>> for Fragment {
    fn from(source: Vec<Timestamp>) -> Fragment {
        Fragment::from(unsafe { transmute::<Vec<Timestamp>, Vec<u64>>(source) })
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

                        #[test]
                        fn is_sparse() {
                            let buf = (1..100).into_iter().collect::<Vec<$B>>();

                            let frag = Fragment::from(buf.clone());

                            assert!(!frag.is_sparse());
                        }

                        #[test]
                        #[should_panic(expected = "split_at_idx called on a dense block")]
                        fn split_at_idx() {
                            let buf = (1..100).into_iter().collect::<Vec<$B>>();

                            let frag = Fragment::from(buf.clone());

                            frag.split_at_idx(100).unwrap();
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

                        #[test]
                        fn is_sparse() {
                            let buf = (1..100).into_iter().collect::<Vec<$B>>();
                            let idx = (1..100).into_iter().map(|v| v * 3).collect::<Vec<_>>();

                            let frag = Fragment::from((buf.clone(), idx.clone()));

                            assert!(frag.is_sparse());
                        }

                        #[test]
                        fn split_at_idx() {
                            // 300 dense records

                            let buf = (1..100).into_iter().collect::<Vec<$B>>();
                            let idx = (1..100).into_iter().map(|v| v * 3).collect::<Vec<_>>();

                            let frag = Fragment::from((buf.clone(), idx.clone()));

                            let (left, right) = frag.split_at_idx(100)
                                .chain_err(|| "failed to split sparse fragment")
                                .unwrap();

                            assert_variant!(left, FragmentRef::$T(_, vidx),
                                            *vidx.last().unwrap() < 100);

                            assert_variant!(right, FragmentRef::$T(_, vidx),
                                            *vidx.first().unwrap() >= 100);
                        }

                        #[test]
                        fn split_at_idx_2() {
                            // 40 dense records
                            let dense_count = 50;
                            let sparse_count = 10;
                            let sparse_step = 4;

                            let buf = seqfill!(vec $B, sparse_count);
                            let idx = seqfill!(vec u32, sparse_count, 0, sparse_step);

                            let frag = Fragment::from((buf, idx));

                            let (left, right) = frag.split_at_idx(dense_count)
                                .chain_err(|| "failed to split sparse fragment")
                                .unwrap();

                            assert_eq!(left.len(), 10);
                            assert!(right.is_empty());

                            assert_variant!(left, FragmentRef::$T(_, vidx),
                                            *vidx.last().unwrap() < dense_count);
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
        use helpers::random::timestamp::RandomTimestampGen;

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
