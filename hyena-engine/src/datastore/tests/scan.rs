use super::*;
use scanner::ScanFilterOp;

/// The tests that use full blocks and generated data
///
/// The 'heavy' suite

#[macro_use]
mod full {
    use super::*;

    macro_rules! scan_test_impl {
        (init) => {
            scan_test_impl!(init count MAX_RECORDS - 1)
        };

        (init count $count: expr) => {{
            use block::BlockType;
            use ty::block::BlockStorage::Memmap;
            use ty::fragment::Fragment;

            let now = <Timestamp as Default>::default();

            let record_count = $count;

            let mut v = vec![Timestamp::from(0); record_count];
            seqfill!(Timestamp, &mut v[..], now);

            let data = hashmap! {
                2 => Fragment::from(seqfill!(vec u8 as u8, record_count)),
                3 => Fragment::from(seqfill!(vec u16 as u8, record_count)),
                4 => Fragment::from(seqfill!(vec u32 as u8, record_count)),
                5 => Fragment::from(seqfill!(vec u64 as u8, record_count)),
            };

            let td = append_test_impl!(
                hashmap! {
                    0 => Column::new(Memmap(BlockType::U64Dense), "ts"),
                    1 => Column::new(Memmap(BlockType::U32Dense), "source"),
                    2 => Column::new(Memmap(BlockType::U8Dense), "col1"),
                    3 => Column::new(Memmap(BlockType::U16Dense), "col2"),
                    4 => Column::new(Memmap(BlockType::U32Dense), "col3"),
                    5 => Column::new(Memmap(BlockType::U64Dense), "col4"),
                },
                now,
                [
                    v.into(),
                    data
                ]
            );

            let cat = Catalog::with_data(&td)
                .with_context(|_| "unable to open catalog")
                .unwrap();

            (td, cat, now)
        }};

        (init sparse) => {
            scan_test_impl!(init sparse count MAX_RECORDS - 4, 4);
        };

        (init sparse count $count: expr, $sparse_ratio: expr) => {{
            use block::BlockType;
            use ty::block::BlockStorage::Memmap;
            use ty::fragment::Fragment;

            let now = <Timestamp as Default>::default();

            let dense_count = $count;
            let sparse_ratio = $sparse_ratio;
            let record_count = dense_count / sparse_ratio;

            let mut v = vec![Timestamp::from(0); dense_count];
            seqfill!(Timestamp, &mut v[..], now);

            let data = hashmap! {
                2 => Fragment::from((
                    seqfill!(vec u8 as u8, record_count),
                    seqfill!(vec u32, record_count, 0, sparse_ratio)
                )),
                3 => Fragment::from((
                    seqfill!(vec u16 as u8, record_count),
                    seqfill!(vec u32, record_count, 0, sparse_ratio)
                )),
                4 => Fragment::from((
                    seqfill!(vec u32 as u8, record_count),
                    seqfill!(vec u32, record_count, 0, sparse_ratio)
                )),
                5 => Fragment::from((
                    seqfill!(vec u64 as u8, record_count),
                    seqfill!(vec u32, record_count, 0, sparse_ratio)
                )),
            };

            let td = append_test_impl!(
                hashmap! {
                    0 => Column::new(Memmap(BlockType::U64Dense), "ts"),
                    1 => Column::new(Memmap(BlockType::U32Dense), "source"),
                    2 => Column::new(Memmap(BlockType::U8Sparse), "col1"),
                    3 => Column::new(Memmap(BlockType::U16Sparse), "col2"),
                    4 => Column::new(Memmap(BlockType::U32Sparse), "col3"),
                    5 => Column::new(Memmap(BlockType::U64Sparse), "col4"),
                },
                now,
                [
                    v.into(),
                    data
                ]
            );

            let cat = Catalog::with_data(&td)
                .with_context(|_| "unable to open catalog")
                .unwrap();

            (td, cat, now)
        }};

        (dense simple $( $name: ident, $idx: expr, $value: expr),+ $(,)*) => {
            scan_test_impl!(dense long MAX_RECORDS - 1, $( $name, $idx, $value, )+ );
        };

        (dense long $count: expr, $( $name: ident, $idx: expr, $value: expr ),+ $(,)*) => {
            scan_test_impl!(dense multi $count,
                $( $name, |v| v < $value as u8, [
                    $idx, [ ScanFilterOp::Lt($value).into(), ],
                ],)+ );
        };

        (dense multi $count: expr, $( $name: ident, $exp_filter: expr,
            [ $( $idx: expr,
                [ $( $value: expr, )+ $(,*)* ]
                ),+ $(,)*
            ] ),+ $(,)*) =>
        {
            $(
            #[test]
            fn $name() {
                use ty::fragment::Fragment;
                use scanner::ScanResult;

                let (_td, catalog, now) = scan_test_impl!(init count $count);

                let record_count = $count;

                let mut filters = HashMap::new();

                $(
                    filters.insert($idx, vec![vec![$( $value )+]]);
                )+

                let scan = Scan::new(
                    Some(filters),
                    None,
                    None,
                    None,
                    None,
                );

                let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

                let mut v = vec![Timestamp::from(0); record_count];
                seqfill!(Timestamp, &mut v[..], now);
                let v = u8_filter(v, $exp_filter);

                let expected = ScanResult::from(hashmap! {
                    0 => Some(Fragment::from(v)),
                    1 => Some(Fragment::from(Vec::<SparseIndex>::new())),
                    2 => Some(Fragment::from(u8_filter(
                        seqfill!(vec u8, record_count), $exp_filter))),
                    3 => Some(Fragment::from(u8_filter(
                        seqfill!(vec u16 as u8, record_count), $exp_filter))),
                    4 => Some(Fragment::from(u8_filter(
                        seqfill!(vec u32 as u8, record_count), $exp_filter))),
                    5 => Some(Fragment::from(u8_filter(
                        seqfill!(vec u64 as u8, record_count), $exp_filter))),
                });

                assert_eq!(result, expected);
            }
            )+
        };

        (sparse simple $( $name: ident, $idx: expr, $value: expr ),+ $(,)*) => {
            scan_test_impl!(sparse long MAX_RECORDS - 4, 4, $( $name, $idx, $value, )+ );
        };

        (sparse long $count: expr, $sparse_ratio: expr,
            $( $name: ident, $idx: expr, $value: expr ),+ $(,)*) => {

            scan_test_impl!(sparse multi $count, $sparse_ratio,
                $( $name, |v| (v as u64) < ($value as u64), [
                    $idx, [ ScanFilterOp::Lt($value).into(), ],
                ],)+ );
        };

        (sparse multi $count: expr, $sparse_ratio: expr,
            $( $name: ident, $exp_filter: expr,
                [ $( $idx: expr,
                    [ $( $value: expr, )+ $(,*)* ]
                    ),+ $(,)*
                ] ),+ $(,)*) => {
            $(
            #[test]
            fn $name() {
                use ty::fragment::Fragment;
                use scanner::ScanResult;

                let (_td, catalog, now) =
                    scan_test_impl!(init sparse count $count, $sparse_ratio);

                let dense_count = $count;
                let sparse_ratio = $sparse_ratio;
                let record_count = dense_count / sparse_ratio;

                let mut filters = HashMap::new();

                $(
                    filters.insert($idx, vec![vec![$( $value )+]]);
                )+

                let scan = Scan::new(
                    Some(filters),
                    None,
                    None,
                    None,
                    None,
                );

                let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

                let mut v = vec![Timestamp::from(0); dense_count];
                seqfill!(Timestamp, &mut v[..], now);
                let v = u8_dense_filter(v, $exp_filter, sparse_ratio);

                let expected = ScanResult::from(hashmap! {
                    0 => Some(Fragment::from(v)),
                    1 => Some(Fragment::from(Vec::<SparseIndex>::new())),
                    2 => Some(Fragment::from(u8_sparse_filter(
                        seqfill!(vec u8 as u8, record_count),
                        seqfill!(vec u32, record_count, 0, sparse_ratio),
                        $exp_filter,
                    ))),
                    3 => Some(Fragment::from(u8_sparse_filter(
                        seqfill!(vec u16 as u8, record_count),
                        seqfill!(vec u32, record_count, 0, sparse_ratio),
                        $exp_filter,
                    ))),
                    4 => Some(Fragment::from(u8_sparse_filter(
                        seqfill!(vec u32 as u8, record_count),
                        seqfill!(vec u32, record_count, 0, sparse_ratio),
                        $exp_filter,
                    ))),
                    5 => Some(Fragment::from(u8_sparse_filter(
                        seqfill!(vec u64 as u8, record_count),
                        seqfill!(vec u32, record_count, 0, sparse_ratio),
                        $exp_filter,
                    ))),
                });

                assert_eq!(result, expected);
            }
            )+
        }
    }

    mod pruning {
        use super::*;
        use block::BlockType;
        use self::BlockStorage::Memmap;
        use ty::fragment::Fragment;
        use scanner::{ScanResult, ScanFilter};


        #[test]
        fn partition() {
            let now = 1;

            let record_count = MAX_RECORDS * 4 - 1;

            let mut v = vec![Timestamp::from(0); record_count];
            seqfill!(Timestamp, &mut v[..], now);

            let td = append_test_impl!(
                [1],
                hashmap! {
                    0 => Column::new(Memmap(BlockType::U64Dense), "ts"),
                    1 => Column::new(Memmap(BlockType::U32Dense), "source_id"),
                },
                now,
                [
                    v.into(),
                    hashmap! {}
                ]
            );

            let cat = Catalog::with_data(&td)
                .with_context(|_| "unable to open catalog")
                .unwrap();

            let pids = cat.groups
                .iter()
                .flat_map(|(_, pg)| {
                    let parts = acquire!(read pg.mutable_partitions);
                    parts
                        .iter()
                        // is_empty filtering is required because there's an empty
                        // prtition created for each new partition group
                        .filter_map(|p| if !p.is_empty() { Some(p.get_id()) } else { None })
                        .collect::<Vec<_>>()
                        .into_iter()
                })
                .collect::<Vec<_>>();

            assert_eq!(pids.len(), 4);

            let parts = hashset! {pids[0], pids[2]};

            let v = seqfill!(iter u64, MAX_RECORDS, 1)
                .chain(seqfill!(iter u64, MAX_RECORDS, MAX_RECORDS * 2 + 1))
                .collect::<Vec<_>>();

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(v)),
                1 => Some(Fragment::from(Vec::<u32>::new())),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    0 => vec![vec![ScanFilter::U64(ScanFilterOp::Gt(0))]] // a.k.a. full scan
                }),
                None,
                None,
                Some(parts),
                None,
            );

            let result = cat.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        mod ts_range {
            use super::*;
            use scanner::ScanTsRange;

            fn ts_range_test_impl(ts_range: Option<ScanTsRange>, expected_partitions: Vec<usize>) {
                let now = 1;

                let record_count = MAX_RECORDS * 10;

                let mut v = vec![Timestamp::from(0); record_count];
                seqfill!(Timestamp, &mut v[..], now);

                let expected = v.chunks(MAX_RECORDS)
                    .enumerate()
                    .filter(|&(idx, _)| expected_partitions.contains(&idx))
                    .flat_map(|(_, data)| data)
                    .map(|t| u64::from(*t))
                    .collect::<Vec<_>>();

                let td = append_test_impl!(
                    [1],
                    hashmap! {
                        0 => Column::new(Memmap(BlockType::U64Dense), "ts"),
                        1 => Column::new(Memmap(BlockType::U32Dense), "source_id"),
                    },
                    now,
                    [
                        v.into(),
                        hashmap! {}
                    ]
                );

                let cat = Catalog::with_data(&td)
                    .with_context(|_| "unable to open catalog")
                    .unwrap();

                let expected = ScanResult::from(hashmap! {
                    0 => Some(Fragment::from(expected)),
                    1 => Some(Fragment::from(Vec::<u32>::default())),
                });

                let scan = Scan::new(
                    None,   // full scan
                    None,
                    None,
                    None,
                    ts_range,
                );

                let result = cat.scan(&scan).with_context(|_| "scan failed").unwrap();

                assert_eq!(expected, result);
            }

            #[test]
            fn default() {
                ts_range_test_impl(None, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
            }

            #[test]
            fn full() {
                ts_range_test_impl(Some(ScanTsRange::Full), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
            }

            #[test]
            fn bound() {
                let start = Timestamp::from((MAX_RECORDS * 2 + MAX_RECORDS / 2) as u64); // 2,5 =>
                let end = Timestamp::from((MAX_RECORDS * 6 + MAX_RECORDS / 2) as u64); // => 6,5
                ts_range_test_impl(Some(ScanTsRange::Bounded { start, end }), vec![2, 3, 4, 5, 6]);
            }

            #[test]
            fn left_bound() {
                let start = Timestamp::from((MAX_RECORDS * 6 + MAX_RECORDS / 2) as u64); // 6,5 =>
                ts_range_test_impl(Some(ScanTsRange::From { start }), vec![6, 7, 8, 9]);
            }

            #[test]
            fn right_bound() {
                let end = Timestamp::from((MAX_RECORDS * 3 + MAX_RECORDS / 2) as u64); // => 3,5
                ts_range_test_impl(Some(ScanTsRange::To { end }), vec![0, 1, 2, 3]);
            }
        }
    }

    mod dense {
        use super::*;

        fn u8_filter<T, F>(data: Vec<T>, val: F) -> Vec<T>
            where
                T: Ord,
                F: Fn(u8) -> bool,
        {
            data.into_iter()
                .enumerate()
                .filter_map(|(idx, v)| if val(idx as u8) {
                    Some(v)
                }  else {
                    None
                })
                .collect::<Vec<_>>()
        }

        scan_test_impl!(dense simple
            simple_u8, 2, 100_u8,
            simple_u16, 3, 100_u16,
            simple_u32, 4, 100_u32,
            simple_u64, 5, 100_u64,);

        scan_test_impl!(dense long MAX_RECORDS * 2 - 1,
            long_u8, 2, 100_u8,
            long_u16, 3, 100_u16,
            long_u32, 4, 100_u32,
            long_u64, 5, 100_u64,);

        scan_test_impl!(dense multi MAX_RECORDS * 2 - 1,
            multi_u8, move |v| v < 100 && v > 30, [
                2, [ ScanFilterOp::Lt(100_u8).into(), ],
                3, [ ScanFilterOp::Gt(30_u16).into(), ],
            ],
            multi_u16, move |v| v < 100 && v > 30, [
                3, [ ScanFilterOp::Lt(100_u16).into(), ],
                4, [ ScanFilterOp::Gt(30_u32).into(), ],
            ],
            multi_u32, move |v| v < 100 && v > 30, [
                4, [ ScanFilterOp::Lt(100_u32).into(), ],
                5, [ ScanFilterOp::Gt(30_u64).into(), ],
            ],
            multi_u64, move |v| v < 100 && v > 30, [
                5, [ ScanFilterOp::Lt(100_u64).into(), ],
                2, [ ScanFilterOp::Gt(30_u8).into(), ],
            ],
        );
    }

    mod sparse {
        use super::*;
        use std::ops::{Add, AddAssign, Sub};
        use num::{FromPrimitive, Zero};

        // Help generate ts data for the test expectation
        //
        // Sparse test filter for dense columns.
        // This helper filters out every 4th record and additionally checks the
        // accopanying value of the record
        fn u8_dense_filter<T, R, F>(data: Vec<T>, val: F, sparse_ratio: R) -> Vec<T>
            where
                T: Ord,
                R: Ord +
                    Copy +
                    FromPrimitive +
                    Sub<Output = R> +
                    Add<Output = R> +
                    AddAssign +
                    Zero,
                F: Fn(u8) -> bool,
        {
            let one = R::from_u8(1).expect("Unable to convert value");
            let ratio = sparse_ratio - one;

            data.into_iter()
                .scan((ratio, 0_u8),
                    |&mut (ref mut pos, ref mut idx), v| {
                    *pos += one;
                    if *pos > ratio {
                        *pos = R::zero();

                        if {
                            let t = val(*idx);
                            *idx = idx.wrapping_add(1_u8);
                            t
                        } {
                            Some(Some(v))
                        } else {
                            Some(None)
                        }
                    }  else {
                        Some(None)
                    }
                })
                .filter_map(|v| v)
                .collect::<Vec<_>>()
        }

        // Help generate sparse data for the test expectation
        //
        // Sparse test filter for sparse columns.
        // This helper filters out generated values from a sparse data
        // while translating resulting rowidx values
        // so the resulting sparse column looks like a dense one (no nulls)
        fn u8_sparse_filter<T, F>(data: Vec<T>, index: Vec<SparseIndex>, val: F)
            -> (Vec<T>, Vec<SparseIndex>)
            where T: Ord + PartialEq + From<u8> + Copy,
                F: Fn(T) -> bool,
        {
            data.into_iter()
                .zip(index.into_iter())
                .scan(0_usize, |projected_idx, (v, _)| {
                    if {
                        val(v)
                    } {
                        Some(Some((v, {
                            let t = *projected_idx;
                            *projected_idx += 1;
                            t as SparseIndex
                        })))
                    } else {
                        Some(None)
                    }
                })
                .filter_map(|element| element)
                .collect::<Vec<_>>()
                .into_iter()
                .unzip()
        }

        scan_test_impl!(sparse simple
            simple_u8, 2, 100_u8,
            simple_u16, 3, 100_u16,
            simple_u32, 4, 100_u32,
            simple_u64, 5, 100_u64,);

        scan_test_impl!(sparse long MAX_RECORDS * 2 - 4, 4,
            long_u8, 2, 100_u8,
            long_u16, 3, 100_u16,
            long_u32, 4, 100_u32,
            long_u64, 5, 100_u64,);

        scan_test_impl!(sparse multi MAX_RECORDS * 2 - 1, 4,
            multi_u8, move |v| v < 100 && v > 30, [
                2, [ ScanFilterOp::Lt(100_u8).into(), ],
                3, [ ScanFilterOp::Gt(30_u16).into(), ],
            ],
            multi_u16, move |v| v < 100 && v > 30, [
                3, [ ScanFilterOp::Lt(100_u16).into(), ],
                4, [ ScanFilterOp::Gt(30_u32).into(), ],
            ],
            multi_u32, move |v| v < 100 && v > 30, [
                4, [ ScanFilterOp::Lt(100_u32).into(), ],
                5, [ ScanFilterOp::Gt(30_u64).into(), ],
            ],
            multi_u64, move |v| v < 100 && v > 30, [
                5, [ ScanFilterOp::Lt(100_u64).into(), ],
                2, [ ScanFilterOp::Gt(30_u8).into(), ],
            ],
        );
    }
}

/// The tests that use manually crafted blocks
///
/// The 'light' suite
mod minimal {
    use super::*;
    use ty::fragment::Fragment;
    use scanner::ScanFilter;
    use self::BlockStorage::Memmap;

    /// Helper for 'minimal' scan tests
    ///
    /// Creates a temp directory with fresh catalog
    /// and initializes it with provided schema / data
    /// by leveraging append_test_impl!
    ///
    /// ## First variant: init shourtcut for single-pg tests
    ///
    /// $ts: base timestamp
    /// $length: records count
    /// dense []: dense data/schema info
    /// $dense_idx: a column index for the dense value (of type ColumnId)
    /// $dense_ty: type of the dense column (e.g. U64Dense)
    /// $dense_name: a name of the dense column (e.g. "dense1")
    /// vec![$dense_data]: data values (e.g. vec![1_u64, 2, 3] for U64Dense block)
    ///
    /// Remember that this is not a true vec! macro,
    /// but rather an emulation for better readability.
    /// Also remember to provide correct value types, so no '1' but '1_u32'
    /// for U32Dense block.
    ///
    /// sparse []: sparse data/schema info
    /// $sparse_idx: a column index for the dense value (of type ColumnId)
    /// $sparse_ty: type of the sparse column (e.g. U64Sparse)
    /// $sparse_name: a name of the column (e.g. "sparse1")
    /// vec![$sparse_data]: data values for the sparse column
    /// vec![$sparse_data_idx]: index values for the sparse column
    /// (always of type `SparseIndex`)
    ///
    /// Also remember to double-check column indexes to properly match dense/sparse
    /// columns.
    ///
    /// ## Second variant: init for multi-pg tests
    ///
    /// $ts: base timestamp
    /// $length: records count
    ///
    /// schema
    ///
    /// dense []: dense schema info
    /// $dense_schema_idx: a column index for the dense value (of type ColumnId)
    /// $dense_ty: type of the dense column (e.g. U64Dense)
    /// $dense_name: a name of the dense column (e.g. "dense1")
    ///
    /// sparse []: sparse schema info
    /// $sparse_schema_idx: a column index for the sparse value (of type ColumnId)
    /// $sparse_ty: type of the sparse column (e.g. U64Sparse)
    /// $sparse_name: a name of the column (e.g. "sparse1")
    ///
    /// data []:
    /// $source_id: partition group id (a.k.a. source_id)
    ///
    /// dense []:
    /// $dense_idx: a column index for the dense value (of type ColumnId)
    /// vec![$dense_data]: data values (e.g. vec![1_u64, 2, 3] for U64Dense block)
    ///
    /// sparse []:
    /// $sparse_idx: a column index for the sparse value (of type ColumnId)
    /// vec![$sparse_data]: data values for the sparse column
    /// vec![$sparse_data_idx]: index values for the sparse column
    /// (always of type `SparseIndex`)

    macro_rules! scan_minimal_init {
        ($ts:expr, $length:expr,
            dense [ $( $dense_idx:expr => (
                $dense_ty:ident,
                $dense_name:expr,
                vec![$($dense_data:tt)*]) ),* $(,)*],
            sparse [ $( $sparse_idx:expr => (
                $sparse_ty:ident,
                $sparse_name:expr,
                vec![$($sparse_data:tt)*],
                vec![$($sparse_data_idx:tt)*]) ),* $(,)*]
            $(,)*
        ) => {
            scan_minimal_init!($ts, $length,
                    dense [$(
                        $dense_idx => (
                            $dense_ty,
                            $dense_name,
                            vec![$( $dense_data )*]
                        ),
                    )*],
                    sparse [$(
                        $sparse_idx => (
                            $sparse_ty,
                            $sparse_name,
                            vec![$($sparse_data)*],
                            vec![$($sparse_data_idx)*]
                        ),
                    )*],
                    indexes []
            )
        };

        ($ts:expr, $length:expr,
            dense [ $( $dense_idx:expr => (
                $dense_ty:ident,
                $dense_name:expr,
                vec![$($dense_data:tt)*]) ),* $(,)*],
            sparse [ $( $sparse_idx:expr => (
                $sparse_ty:ident,
                $sparse_name:expr,
                vec![$($sparse_data:tt)*],
                vec![$($sparse_data_idx:tt)*]) ),* $(,)*],
            indexes [ $( $column_idx:expr => $column_idx_ty:expr ),* $(,)* ]
            $(,)*
        ) => {
            scan_minimal_init!($ts, $length,
                schema
                    dense [$(
                        $dense_idx => (
                            $dense_ty,
                            $dense_name
                        ),
                    )*],
                    sparse [$(
                        $sparse_idx => (
                            $sparse_ty,
                            $sparse_name
                        ),
                    )*],
                    indexes [ $( $column_idx => $column_idx_ty )* ],
                data [
                    1 => [
                        dense [$(
                            $dense_idx => (
                                vec![$( $dense_data )*]
                            ),
                        )*],
                        sparse [$(
                            $sparse_idx => (
                                vec![$( $sparse_data )*],
                                vec![$( $sparse_data_idx )*]
                            ),
                        )*]
                    ],
                ]
            )
        };

        ($ts:expr, $length:expr,
            schema
                dense [ $( $dense_schema_idx:expr => (
                    $dense_ty:ident,
                    $dense_name:expr
                )),* $(,)*],
                sparse [ $( $sparse_schema_idx:expr => (
                    $sparse_ty:ident,
                    $sparse_name:expr
                )),* $(,)*],

            data [ $(
                $source_id: expr => [
                    dense [ $( $dense_idx:expr => (
                            $dense_data: expr
                    )),* $(,)*],
                    sparse [ $( $sparse_idx:expr => (
                        vec![$( $sparse_data:tt )*],
                        vec![$( $sparse_data_idx:tt )*]
                    )),* $(,)*]
                ]
            ),* $(,)* ]
        ) => {
            scan_minimal_init!($ts, $length,
                schema
                    dense [$(
                        $dense_schema_idx => (
                            $dense_ty,
                            $dense_name
                        ),
                    )*],
                    sparse [$(
                        $sparse_schema_idx => (
                            $sparse_ty,
                            $sparse_name
                        ),
                    )*],
                    indexes [],
                data [$(
                    $source_id => [
                        dense [$(
                            $dense_idx => (
                                $dense_data
                            ),
                        )*],
                        sparse [$(
                            $sparse_idx => (
                                vec![$( $sparse_data )*],
                                vec![$( $sparse_data_idx )*]
                            ),
                        )*]
                    ],
                )*]
            )
        };

        ($ts:expr, $length:expr,
            schema
                dense [ $( $dense_schema_idx:expr => (
                    $dense_ty:ident,
                    $dense_name:expr
                )),* $(,)*],
                sparse [ $( $sparse_schema_idx:expr => (
                    $sparse_ty:ident,
                    $sparse_name:expr
                )),* $(,)*],
                indexes [ $( $column_idx:expr => $column_idx_ty:expr ),* $(,)* ],

            data [ $(
                $source_id: expr => [
                    dense [ $( $dense_idx:expr => (
                            $dense_data: expr
                    )),* $(,)*],
                    sparse [ $( $sparse_idx:expr => (
                        vec![$( $sparse_data:tt )*],
                        vec![$( $sparse_data_idx:tt )*]
                    )),* $(,)*]
                ]
            ),* $(,)* ]
        ) => {{
            use block::BlockType;
            use ty::ColumnIndexStorageMap;


            let now = $ts;

            let mut v = vec![Timestamp::from(0); $length];
            seqfill!(Timestamp, &mut v[..], now);

            // ts column is awlays present
            let mut schema = hashmap! {
                0 => Column::new(Memmap(BlockType::U64Dense), "ts"),
            };

            // adjust schema first

            $(
                schema.insert($dense_schema_idx,
                    Column::new(Memmap(BlockType::$dense_ty), $dense_name));
            )*

            $(
                schema.insert($sparse_schema_idx,
                    Column::new(Memmap(BlockType::$sparse_ty), $sparse_name));

            )*

            let indexes = ColumnIndexStorageMap::from(hashmap! {
                $(
                    $column_idx => ::ty::ColumnIndexStorage::Memmap($column_idx_ty),
                )*
            });

            let td = append_test_impl!(default pg,
                schema,
                indexes,
                now,
                $(
                [
                    v.clone().into(),
                    {
                        let mut data = hashmap! {};
                    $(
                        data.insert($dense_idx,
                            Fragment::from($dense_data));
                    )*

                    $(

                        data.insert($sparse_idx,
                            Fragment::from((
                                vec![$($sparse_data)*],
                                vec![$($sparse_data_idx)*])));
                    )*

                        data
                    },
                    $source_id
                ],
                )*
            );

            let cat = Catalog::with_data(&td)
                .with_context(|_| "unable to open catalog")
                .unwrap();

            (td, cat, now)
        }};

//             +--------+----+---------+---------+
//             | rowidx | ts | sparse1 | sparse2 |
//             +--------+----+---------+---------+
//             |   0    | 1  |         | 10      |
//             +--------+----+---------+---------+
//             |   1    | 2  | 1       | 20      |
//             +--------+----+---------+---------+
//             |   2    | 3  |         |         |
//             +--------+----+---------+---------+
//             |   3    | 4  | 2       |         |
//             +--------+----+---------+---------+
//             |   4    | 5  |         |         |
//             +--------+----+---------+---------+
//             |   5    | 6  | 3       | 30      |
//             +--------+----+---------+---------+
//             |   6    | 7  |         |         |
//             +--------+----+---------+---------+
//             |   7    | 8  |         |         |
//             +--------+----+---------+---------+
//             |   8    | 9  | 4       | 7       |
//             +--------+----+---------+---------+
//             |   9    | 10 |         | 8       |
//             +--------+----+---------+---------+
        () => {
            scan_minimal_init!(1, 10,
                dense [],
                sparse [
                    1 => (U8Sparse, "sparse1",
                        vec![1_u8, 2, 3, 4,], vec![1_u32, 3, 5, 8,]),
                    2 => (U16Sparse, "sparse2",
                        vec![ 10_u16, 20, 30, 7, 8, ], vec![ 0_u32, 1, 5, 8, 9, ]),
                ])
        };

//             +--------+----+--------+---------+--------+---------+
//             | rowidx | ts | dense1 | sparse1 | dense2 | sparse2 |
//             +--------+----+--------+---------+--------+---------+
//             |   0    | 1  | 5      |         | 10     | 10      |
//             +--------+----+--------+---------+--------+---------+
//             |   1    | 2  | 6      | 1       | 20     | 20      |
//             +--------+----+--------+---------+--------+---------+
//             |   2    | 3  | 7      |         | 30     |         |
//             +--------+----+--------+---------+--------+---------+
//             |   3    | 4  | 2      | 2       | 40     |         |
//             +--------+----+--------+---------+--------+---------+
//             |   4    | 5  | 3      |         | 50     |         |
//             +--------+----+--------+---------+--------+---------+
//             |   5    | 6  | 4      | 3       | 50     | 30      |
//             +--------+----+--------+---------+--------+---------+
//             |   6    | 7  | 1      |         | 40     |         |
//             +--------+----+--------+---------+--------+---------+
//             |   7    | 8  | 2      |         | 30     |         |
//             +--------+----+--------+---------+--------+---------+
//             |   8    | 9  | 3      | 4       | 20     | 7       |
//             +--------+----+--------+---------+--------+---------+
//             |   9    | 10 | 4      |         | 10     | 8       |
//             +--------+----+--------+---------+--------+---------+
        (ops) => {
            scan_minimal_init!(1, 10,
                dense [
                    1 => (U32Dense, "dense1",
                        vec![5_u32, 6, 7, 2, 3, 4, 1, 2, 3, 4]),
                    3 => (U16Dense, "dense2",
                        vec![10_u16, 20, 30, 40, 50, 50, 40, 30, 20, 10]),
                ],
                sparse [
                    2 => (U8Sparse, "sparse1",
                        vec![1_u8, 2, 3, 4,], vec![1_u32, 3, 5, 8,]),
                    4 => (U16Sparse, "sparse2",
                        vec![ 10_u16, 20, 30, 7, 8, ], vec![ 0_u32, 1, 5, 8, 9, ]),
                ])
        };

//             +--------+--------+----+--------+--------+---------+---------+
//             | rowidx | source | ts | dense1 | dense2 | sparse1 | sparse2 |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   0    |   1    | 1  | 1      | 11     | 10      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   1    |   1    | 2  | 2      | 12     |         | 5       |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   2    |   1    | 3  | 3      | 13     | 20      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   3    |   1    | 4  | 4      | 14     |         | 4       |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   4    |   1    | 5  | 5      | 15     | 30      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   5    |   1    | 6  | 6      | 16     |         | 3       |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   6    |   1    | 7  | 7      | 17     | 40      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   7    |   1    | 8  | 8      | 18     |         | 2       |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   8    |   1    | 9  | 9      | 19     | 50      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   9    |   1    | 10 | 10     | 20     |         | 1       |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   10   |   5    | 1  | 21     | 211    | 12      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   11   |   5    | 2  | 22     | 212    |         | 25      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   12   |   5    | 3  | 23     | 213    | 22      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   13   |   5    | 4  | 24     | 214    |         | 24      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   14   |   5    | 5  | 25     | 215    | 32      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   15   |   5    | 6  | 26     | 216    |         | 23      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   16   |   5    | 7  | 27     | 217    | 42      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   17   |   5    | 8  | 28     | 218    |         | 22      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   18   |   5    | 9  | 29     | 219    | 52      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   19   |   5    | 10 | 30     | 220    |         | 21      |
//             +--------+--------+----+--------+--------+---------+---------+

        (multi) => {
            scan_minimal_init!(
                1, 10,
                schema
                    dense [
                        1 => (U16Dense, "dense1"),
                        2 => (U32Dense, "dense2")
                    ],
                    sparse [
                        3 => (U8Sparse, "sparse1"),
                        4 => (U32Sparse, "sparse2")
                    ],
                data [
                    1 => [
                        dense [
                            1 => (vec![1_u16, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
                            2 => (vec![11_u32, 12, 13, 14, 15, 16, 17, 18, 19, 20]),
                        ],
                        sparse [
                            3 => (
                                vec![10_u8, 20, 30, 40, 50],
                                vec![0_u32, 2, 4, 6, 8]
                            ),
                            4 => (
                                vec![5_u32, 4, 3, 2, 1],
                                vec![1_u32, 3, 5, 7, 9]
                            ),
                        ]
                    ],
                    5 => [
                        dense [
                            1 => (vec![21_u16, 22, 23, 24, 25, 26, 27, 28, 29, 30]),
                            2 => (vec![211_u32, 212, 213, 214,
                                        215, 216, 217, 218, 219, 220]),
                        ],
                        sparse [
                            3 => (
                                vec![12_u8, 22, 32, 42, 52],
                                vec![0_u32, 2, 4, 6, 8]
                            ),
                            4 => (
                                vec![25_u32, 24, 23, 22, 21],
                                vec![1_u32, 3, 5, 7, 9]
                            ),
                        ]
                    ]
                ]
            )
        };

//             +--------+--------+----+--------+--------+---------+---------+
//             | rowidx | source | ts | dense1 | dense2 | sparse1 | sparse2 |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   0    |   1    | 1  | 1      | 11     | 10      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   1    |   1    | 2  | 2      | 12     |         | 5       |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   2    |   1    | 3  | 3      | 13     | 20      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   3    |   1    | 4  | 4      | 14     |         | 4       |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   4    |   1    | 5  | 5      | 15     | 30      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   5    |   1    | 6  | 6      | 16     |         | 3       |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   6    |   1    | 7  | 7      | 17     | 40      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   7    |   1    | 8  | 8      | 18     |         | 2       |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   8    |   1    | 9  | 9      | 19     | 50      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   9    |   1    | 10 | 10     | 20     |         | 1       |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   10   |   5    | 1  | 21     | 211    | 12      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   11   |   5    | 2  | 22     | 212    |         | 25      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   12   |   5    | 3  | 23     | 213    | 22      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   13   |   5    | 4  | 24     | 214    |         | 24      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   14   |   5    | 5  | 25     | 215    | 32      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   15   |   5    | 6  | 26     | 216    |         | 23      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   16   |   5    | 7  | 27     | 217    | 42      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   17   |   5    | 8  | 28     | 218    |         | 22      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   18   |   5    | 9  | 29     | 219    | 52      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   19   |   5    | 10 | 30     | 220    |         | 21      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   20   |   7    | 1  | 31     | 311    | 13      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   21   |   7    | 2  | 32     | 312    |         | 35      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   22   |   7    | 3  | 33     | 313    | 23      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   23   |   7    | 4  | 34     | 314    |         | 34      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   24   |   7    | 5  | 35     | 315    | 33      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   25   |   7    | 6  | 36     | 316    |         | 33      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   26   |   7    | 7  | 37     | 317    | 43      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   27   |   7    | 8  | 38     | 318    |         | 32      |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   28   |   7    | 9  | 39     | 319    | 53      |         |
//             +--------+--------+----+--------+--------+---------+---------+
//             |   29   |   7    | 10 | 40     | 320    |         | 31      |
//             +--------+--------+----+--------+--------+---------+---------+

        (multi full) => {
            scan_minimal_init!(
                1, 10,
                schema
                    dense [
                        1 => (U16Dense, "dense1"),
                        2 => (U32Dense, "dense2")
                    ],
                    sparse [
                        3 => (U8Sparse, "sparse1"),
                        4 => (U32Sparse, "sparse2")
                    ],
                data [
                    1 => [
                        dense [
                            1 => (vec![1_u16, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
                            2 => (vec![11_u32, 12, 13, 14, 15, 16, 17, 18, 19, 20]),
                        ],
                        sparse [
                            3 => (
                                vec![10_u8, 20, 30, 40, 50],
                                vec![0_u32, 2, 4, 6, 8]
                            ),
                            4 => (
                                vec![5_u32, 4, 3, 2, 1],
                                vec![1_u32, 3, 5, 7, 9]
                            ),
                        ]
                    ],
                    5 => [
                        dense [
                            1 => (vec![21_u16, 22, 23, 24, 25, 26, 27, 28, 29, 30]),
                            2 => (vec![211_u32, 212, 213, 214,
                                        215, 216, 217, 218, 219, 220]),
                        ],
                        sparse [
                            3 => (
                                vec![12_u8, 22, 32, 42, 52],
                                vec![0_u32, 2, 4, 6, 8]
                            ),
                            4 => (
                                vec![25_u32, 24, 23, 22, 21],
                                vec![1_u32, 3, 5, 7, 9]
                            ),
                        ]
                    ],
                    7 => [
                        dense [
                            1 => (vec![31_u16, 32, 33, 34, 35, 36, 37, 38, 39, 40]),
                            2 => (vec![311_u32, 312, 313, 314,
                                        315, 316, 317, 318, 319, 320]),
                        ],
                        sparse [
                            3 => (
                                vec![13_u8, 23, 33, 43, 53],
                                vec![0_u32, 2, 4, 6, 8]
                            ),
                            4 => (
                                vec![35_u32, 34, 33, 32, 31],
                                vec![1_u32, 3, 5, 7, 9]
                            ),
                        ]
                    ]
                ]
            )
        };

//             +--------+-----------+-------------+--------+
//             | rowidx | timestamp |   string1   | dense2 |
//             +--------+-----------+-------------+--------+
//             |   0    | 1         | Lorem       | 11     |
//             +--------+-----------+-------------+--------+
//             |   1    | 2         | ipsum       | 12     |
//             +--------+-----------+-------------+--------+
//             |   2    | 3         | dolor       | 13     |
//             +--------+-----------+-------------+--------+
//             |   3    | 4         | sit         | 14     |
//             +--------+-----------+-------------+--------+
//             |   4    | 5         | amet,       | 15     |
//             +--------+-----------+-------------+--------+
//             |   5    | 6         | consectetur | 16     |
//             +--------+-----------+-------------+--------+
//             |   6    | 7         | adipiscing  | 17     |
//             +--------+-----------+-------------+--------+
//             |   7    | 8         | elit,       | 18     |
//             +--------+-----------+-------------+--------+
//             |   8    | 9         | sed         | 19     |
//             +--------+-----------+-------------+--------+
//             |   9    | 10        | do          | 20     |
//             +--------+-----------+-------------+--------+
        (string $( $column_idx:expr => $column_idx_ty:ident ),*) => {
            scan_minimal_init!(
                1, 10,
                schema
                    dense [
                        1 => (StringDense, "string1"),
                        2 => (U8Dense, "dense2")
                    ],
                    sparse [],
                    indexes [$( $column_idx => $column_idx_ty )*],
                data [
                    1 => [
                        dense [
                            1 => (text!(gen 10)),
                            2 => (vec![11_u8, 12, 13, 14, 15, 16, 17, 18, 19, 20]),
                        ],
                        sparse []
                    ]
                ]
            )
        };

        (string indexed) => {{
            use block::ColumnIndexType::Bloom;

            scan_minimal_init!(string 1 => Bloom)
        }};

//             +--------+-----------+--------------+
//             | rowidx | timestamp |   string1    |
//             +--------+-----------+--------------+
//             |   0    | 1         | ❤❤❤          |
//             +--------+-----------+--------------+
//             |   1    | 2         | 🚲🚑⛄🚑     |
//             +--------+-----------+--------------+
//             |   2    | 3         | ⌚❤❤         |
//             +--------+-----------+--------------+
//             |   3    | 4         | tes❤t        |
//             +--------+-----------+--------------+
//             |   4    | 5         | hyena⌚❤hyena|
//             +--------+-----------+--------------+
        (string utf8 $( $column_idx:expr => $column_idx_ty:ident ),*) => {
            scan_minimal_init!(
                1, 5,
                schema
                    dense [
                        1 => (StringDense, "string1")
                    ],
                    sparse [],
                    indexes [$( $column_idx => $column_idx_ty )*],
                data [
                    1 => [
                        dense [
                            1 => (vec![
                                "❤❤❤".to_owned(),
                                "🚲🚑⛄🚑".to_owned(),
                                "⌚❤❤".to_owned(),
                                "tes❤t".to_owned(),
                                "hyena⌚❤hyena".to_owned()
                            ]),
                        ],
                        sparse []
                    ]
                ]
            )
        };

        (string utf8 indexed) => {{
            use block::ColumnIndexType::Bloom;

            scan_minimal_init!(string utf8 1 => Bloom)
        }};
    }

    #[test]
    fn scan_dense() {
        let (_td, catalog, _) = scan_minimal_init!();

        let expected = ScanResult::from(hashmap! {
            0 => Some(Fragment::from(vec![1_u64, 2, 3])),
            1 => Some(Fragment::from((vec![1_u8], vec![1_u32]))),
            2 => Some(Fragment::from((vec![10_u16, 20], vec![0_u32, 1_u32]))),
        });

        let scan = Scan::new(
            Some(hashmap! {
                0 => vec![vec![ScanFilter::U64(ScanFilterOp::Lt(4))]]
            }),
            None,
            None,
            None,
            None,
        );

        let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

        assert_eq!(expected, result);
    }

    #[test]
    fn scan_sparse1() {
        let (_td, catalog, _) = scan_minimal_init!();

        let expected = ScanResult::from(hashmap! {
            0 => Some(Fragment::from(vec![9_u64])),
            1 => Some(Fragment::from((vec![4_u8], vec![0_u32]))),
            2 => Some(Fragment::from((vec![7_u16], vec![0_u32]))),
        });

        let scan = Scan::new(
            Some(hashmap! {
                1 => vec![vec![ScanFilter::U8(ScanFilterOp::GtEq(4))]]
            }),
            None,
            None,
            None,
            None,
        );

        let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

        assert_eq!(expected, result);
    }

    #[test]
    fn scan_sparse2() {
        let (_td, catalog, _) = scan_minimal_init!();

        let expected = ScanResult::from(hashmap! {
            0 => Some(Fragment::from(vec![2_u64, 5, 10])),
            1 => Some(Fragment::from((vec![1_u8], vec![0_u32]))),
            2 => Some(Fragment::from((vec![20_u16, 8], vec![0_u32, 2]))),
        });

        let scan = Scan::new(
            Some(hashmap! {
                0 => vec![vec![ScanFilter::U64(ScanFilterOp::In(hashset![2, 5, 10]))]]
            }),
            None,
            None,
            None,
            None,
        );

        let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

        assert_eq!(expected, result);
    }

    #[test]
    fn scan_full() {
        let (_td, catalog, _) = scan_minimal_init!(ops);

        let expected = ScanResult::from(hashmap! {
            0 => Some(Fragment::from(vec![1_u64, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            1 => Some(Fragment::from(vec![5_u32, 6, 7, 2, 3, 4, 1, 2, 3, 4])),
            2 => Some(Fragment::from((vec![1_u8, 2, 3, 4], vec![1_u32, 3, 5, 8]))),
            3 => Some(Fragment::from(vec![10_u16, 20, 30, 40, 50, 50, 40, 30, 20, 10])),
            4 => Some(Fragment::from((vec![10_u16, 20, 30, 7, 8], vec![0_u32, 1, 5, 8, 9]))),
        });

        let scan = Scan::new(
            None,
            None,
            None,
            None,
            None,
        );

        let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

        assert_eq!(expected, result);
    }

    mod string {
        use super::*;

        macro_rules! strvec {
            ($($s: expr),+ $(,)*) => {
                vec![
                $(
                    $s.to_owned(),
                )+
                ]
            };
        }

        fn scan_lt_impl(indexed: bool) {
            let (_td, catalog, _) = if indexed {
                scan_minimal_init!(string indexed)
            } else {
                scan_minimal_init!(string)
            };

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![1_u64, 3, 5, 6, 7, 8, 10])),
                1 => Some(Fragment::from(strvec!["Lorem", "dolor", "amet,", "consectetur",
                        "adipiscing", "elit,", "do"])),
                2 => Some(Fragment::from(vec![11_u8, 13, 15, 16, 17, 18, 20])),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::String(ScanFilterOp::Lt("ipsum".to_owned()))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        fn scan_lteq_impl(indexed: bool) {
            let (_td, catalog, _) = if indexed {
                scan_minimal_init!(string indexed)
            } else {
                scan_minimal_init!(string)
            };

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![1_u64, 2, 3, 5, 6, 7, 8, 10])),
                1 => Some(Fragment::from(strvec!["Lorem", "ipsum", "dolor", "amet,", "consectetur",
                        "adipiscing", "elit,", "do"])),
                2 => Some(Fragment::from(vec![11_u8, 12, 13, 15, 16, 17, 18, 20])),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::String(ScanFilterOp::LtEq("ipsum".to_owned()))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        fn scan_gt_impl(indexed: bool) {
            let (_td, catalog, _) = if indexed {
                scan_minimal_init!(string indexed)
            } else {
                scan_minimal_init!(string)
            };

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![4_u64, 9])),
                1 => Some(Fragment::from(strvec!["sit", "sed"])),
                2 => Some(Fragment::from(vec![14_u8, 19])),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::String(ScanFilterOp::Gt("ipsum".to_owned()))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        fn scan_gteq_impl(indexed: bool) {
            let (_td, catalog, _) = if indexed {
                scan_minimal_init!(string indexed)
            } else {
                scan_minimal_init!(string)
            };

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![2_u64, 4, 9])),
                1 => Some(Fragment::from(strvec!["ipsum", "sit", "sed"])),
                2 => Some(Fragment::from(vec![12_u8, 14, 19])),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::String(ScanFilterOp::GtEq("ipsum".to_owned()))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        fn scan_eq_impl(indexed: bool) {
            let (_td, catalog, _) = if indexed {
                scan_minimal_init!(string indexed)
            } else {
                scan_minimal_init!(string)
            };

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![2_u64])),
                1 => Some(Fragment::from(strvec!["ipsum"])),
                2 => Some(Fragment::from(vec![12_u8])),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::String(ScanFilterOp::Eq("ipsum".to_owned()))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        fn scan_noteq_impl(indexed: bool) {
            let (_td, catalog, _) = if indexed {
                scan_minimal_init!(string indexed)
            } else {
                scan_minimal_init!(string)
            };

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![1_u64, 3, 4, 5, 6, 7, 8, 9, 10])),
                1 => Some(Fragment::from(strvec!["Lorem", "dolor", "sit", "amet,", "consectetur",
                    "adipiscing", "elit,", "sed", "do"])),
                2 => Some(Fragment::from(vec![11_u8, 13, 14, 15, 16, 17, 18, 19, 20])),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::String(ScanFilterOp::NotEq("ipsum".to_owned()))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        fn scan_in_impl(indexed: bool) {
            let (_td, catalog, _) = if indexed {
                scan_minimal_init!(string indexed)
            } else {
                scan_minimal_init!(string)
            };

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![2_u64, 8, 9])),
                1 => Some(Fragment::from(strvec!["ipsum", "elit,", "sed"])),
                2 => Some(Fragment::from(vec![12_u8, 18, 19])),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::String(ScanFilterOp::In(hashset! {
                        "ipsum".to_owned(),
                        "elit,".to_owned(),
                        "sed".to_owned(),
                        "not_present".to_owned(),
                    }))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        fn scan_startswith_impl(indexed: bool) {
            let (_td, catalog, _) = if indexed {
                scan_minimal_init!(string indexed)
            } else {
                scan_minimal_init!(string)
            };

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![4_u64, 9])),
                1 => Some(Fragment::from(strvec!["sit", "sed"])),
                2 => Some(Fragment::from(vec![14_u8, 19])),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::String(ScanFilterOp::StartsWith("s".to_owned()))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        fn scan_endswith_impl(indexed: bool) {
            let (_td, catalog, _) = if indexed {
                scan_minimal_init!(string indexed)
            } else {
                scan_minimal_init!(string)
            };

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![4_u64])),
                1 => Some(Fragment::from(strvec!["sit"])),
                2 => Some(Fragment::from(vec![14_u8])),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::String(ScanFilterOp::EndsWith("it".to_owned()))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        fn scan_contains_impl(indexed: bool) {
            let (_td, catalog, _) = if indexed {
                scan_minimal_init!(string indexed)
            } else {
                scan_minimal_init!(string)
            };

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![6_u64, 9])),
                1 => Some(Fragment::from(strvec!["consectetur", "sed"])),
                2 => Some(Fragment::from(vec![16_u8, 19])),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::String(ScanFilterOp::Contains("se".to_owned()))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        fn scan_matches_impl(indexed: bool) {
            use hyena_common::ty::Regex;

            let (_td, catalog, _) = if indexed {
                scan_minimal_init!(string indexed)
            } else {
                scan_minimal_init!(string)
            };

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![4_u64, 5, 6, 8])),
                1 => Some(Fragment::from(strvec!["sit", "amet,", "consectetur", "elit,"])),
                2 => Some(Fragment::from(vec![14_u8, 15, 16, 18])),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::String(ScanFilterOp::Matches(
                        Regex::new("[ie]t").unwrap()))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        fn scan_matches_utf8_impl(indexed: bool) {
            use hyena_common::ty::Regex;

            let (_td, catalog, _) = if indexed {
                scan_minimal_init!(string utf8 indexed)
            } else {
                scan_minimal_init!(string utf8)
            };

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![1_u64, 5])),
                1 => Some(Fragment::from(strvec![
                    "❤❤❤",
                    "hyena⌚❤hyena"
                ])),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::String(ScanFilterOp::Matches(
                        Regex::new("(❤{3,}|⌚❤[^❤])").unwrap()))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

        mod indexed {
            use super::*;

            #[test]
            fn scan_lt() {
                scan_lt_impl(true)
            }

            #[test]
            fn scan_lteq() {
                scan_lteq_impl(true)
            }

            #[test]
            fn scan_gt() {
                scan_gt_impl(true)
            }

            #[test]
            fn scan_gteq() {
                scan_gteq_impl(true)
            }

            #[test]
            fn scan_eq() {
                scan_eq_impl(true)
            }

            #[test]
            fn scan_noteq() {
                scan_noteq_impl(true)
            }

            #[test]
            fn scan_in() {
                scan_in_impl(true)
            }

            #[test]
            fn scan_startswith() {
                scan_startswith_impl(true)
            }

            #[test]
            fn scan_endswith() {
                scan_endswith_impl(true)
            }

            #[test]
            fn scan_contains() {
                scan_contains_impl(true)
            }

            #[test]
            fn scan_matches() {
                scan_matches_impl(true)
            }

            #[test]
            fn scan_matches_utf8() {
                scan_matches_utf8_impl(true)
            }

        }

        #[test]
        fn scan_lt() {
            scan_lt_impl(false)
        }

        #[test]
        fn scan_lteq() {
            scan_lteq_impl(false)
        }

        #[test]
        fn scan_gt() {
            scan_gt_impl(false)
        }

        #[test]
        fn scan_gteq() {
            scan_gteq_impl(false)
        }

        #[test]
        fn scan_eq() {
            scan_eq_impl(false)
        }

        #[test]
        fn scan_noteq() {
            scan_noteq_impl(false)
        }

        #[test]
        fn scan_in() {
            scan_in_impl(false)
        }

        #[test]
        fn scan_startswith() {
            scan_startswith_impl(false)
        }

        #[test]
        fn scan_endswith() {
            scan_endswith_impl(false)
        }

        #[test]
        fn scan_contains() {
            scan_contains_impl(false)
        }

        #[test]
        fn scan_matches() {
            scan_matches_impl(false)
        }

        #[test]
        fn scan_matches_utf8() {
            scan_matches_utf8_impl(false)
        }
    }

    mod and_op {
        use super::*;

//             dense1 < 3 && dense2 > 30
//             +--------+----+--------+---------+--------+---------+
//             | rowidx | ts | dense1 | sparse1 | dense2 | sparse2 |
//             +--------+----+--------+---------+--------+---------+
//             |   0    | 4  | 2      | 2       | 40     |         |
//             +--------+----+--------+---------+--------+---------+
//             |   1    | 7  | 1      |         | 40     |         |
//             +--------+----+--------+---------+--------+---------+
        #[test]
        fn two_dense_one_block() {
            let (_td, catalog, _) = scan_minimal_init!(ops);

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![4_u64, 7])),
                1 => Some(Fragment::from(vec![2_u32, 1])),
                2 => Some(Fragment::from((vec![2_u8], vec![0_u32]))),
                3 => Some(Fragment::from(vec![40_u16, 40])),
                4 => Some(Fragment::from((Vec::<u16>::new(), Vec::<u32>::new()))),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::U32(ScanFilterOp::Lt(3))]],
                    3 => vec![vec![ScanFilter::U16(ScanFilterOp::Gt(30))]],
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

//             sparse1 < 4 && sparse2 > 15
//             +--------+----+--------+---------+--------+---------+
//             | rowidx | ts | dense1 | sparse1 | dense2 | sparse2 |
//             +--------+----+--------+---------+--------+---------+
//             |   0    | 2  | 6      | 1       | 20     | 20      |
//             +--------+----+--------+---------+--------+---------+
//             |   1    | 6  | 4      | 3       | 50     | 30      |
//             +--------+----+--------+---------+--------+---------+
        #[test]
        fn two_sparse_one_block() {
            let (_td, catalog, _) = scan_minimal_init!(ops);

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![2_u64, 6])),
                1 => Some(Fragment::from(vec![6_u32, 4])),
                2 => Some(Fragment::from((vec![1_u8, 3], vec![0_u32, 1]))),
                3 => Some(Fragment::from(vec![20_u16, 50])),
                4 => Some(Fragment::from((vec![20_u16, 30], vec![0_u32, 1]))),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    2 => vec![vec![ScanFilter::U8(ScanFilterOp::Lt(4))]],
                    4 => vec![vec![ScanFilter::U16(ScanFilterOp::Gt(15))]],
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }

//             dense1 > 4 && sparse2 < 15
//             +--------+----+--------+---------+--------+---------+
//             | rowidx | ts | dense1 | sparse1 | dense2 | sparse2 |
//             +--------+----+--------+---------+--------+---------+
//             |   0    | 1  | 5      |         | 10     | 10      |
//             +--------+----+--------+---------+--------+---------+
        #[test]
        fn sparse_dense_one_block() {
            let (_td, catalog, _) = scan_minimal_init!(ops);

            let expected = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![1_u64])),
                1 => Some(Fragment::from(vec![5_u32])),
                2 => Some(Fragment::from((Vec::<u8>::new(), Vec::<u32>::new()))),
                3 => Some(Fragment::from(vec![10_u16])),
                4 => Some(Fragment::from((vec![10_u16], vec![0_u32]))),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    1 => vec![vec![ScanFilter::U32(ScanFilterOp::Gt(4))]],
                    4 => vec![vec![ScanFilter::U16(ScanFilterOp::Lt(15))]],
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            assert_eq!(expected, result);
        }
    }

    mod multi_pg {
        use super::*;

        /// Merge two ScanResults (expects) and compare to the actual result
        ///
        /// This function is needed because the ordering of the ScanResult merge
        /// is not guaranteed by Catalog::scan()
        ///
        /// To implement the assertion we just try both ways to merge
        /// and panic! only if both produce ScanResult != result.
        fn scan_assert(result: ScanResult, a: ScanResult, b: ScanResult) {
            fn merge(a: &ScanResult, b: &ScanResult) -> ScanResult {
                let mut a = a.clone();
                let b = b.clone();

                a.merge(b).expect("ScanResult merge failed");
                a
            }

            if merge(&a, &b) != result && merge(&b, &a) != result {
                panic!("ScanResult assertion failed: {:?} != {:?} + {:?}",
                    result, a, b);
            }
        }

        #[test]
        fn scan() {
            let (_td, catalog, _) = scan_minimal_init!(multi);

            let expected_1 = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![1_u64, 2, 3])),
                1 => Some(Fragment::from(vec![1_u16, 2, 3])),
                2 => Some(Fragment::from(vec![11_u32, 12, 13])),
                3 => Some(Fragment::from((vec![10_u8, 20], vec![0_u32, 2_u32]))),
                4 => Some(Fragment::from((vec![5_u32], vec![1_u32]))),
            });

            let expected_2 = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![1_u64, 2, 3])),
                1 => Some(Fragment::from(vec![21_u16, 22, 23])),
                2 => Some(Fragment::from(vec![211_u32, 212, 213])),
                3 => Some(Fragment::from((vec![12_u8, 22], vec![0_u32, 2_u32]))),
                4 => Some(Fragment::from((vec![25_u32], vec![1_u32]))),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    0 => vec![vec![ScanFilter::U64(ScanFilterOp::Lt(4))]]
                }),
                None,
                None,
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            scan_assert(result, expected_1, expected_2);
        }

        #[test]
        fn pg_filter() {
            let (_td, catalog, _) = scan_minimal_init!(multi full);

            let expected_1 = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![1_u64, 2, 3])),
                1 => Some(Fragment::from(vec![1_u16, 2, 3])),
                2 => Some(Fragment::from(vec![11_u32, 12, 13])),
                3 => Some(Fragment::from((vec![10_u8, 20], vec![0_u32, 2_u32]))),
                4 => Some(Fragment::from((vec![5_u32], vec![1_u32]))),
            });

            let expected_2 = ScanResult::from(hashmap! {
                0 => Some(Fragment::from(vec![1_u64, 2, 3])),
                1 => Some(Fragment::from(vec![31_u16, 32, 33])),
                2 => Some(Fragment::from(vec![311_u32, 312, 313])),
                3 => Some(Fragment::from((vec![13_u8, 23], vec![0_u32, 2_u32]))),
                4 => Some(Fragment::from((vec![35_u32], vec![1_u32]))),
            });

            let scan = Scan::new(
                Some(hashmap! {
                    0 => vec![vec![ScanFilter::U64(ScanFilterOp::Lt(4))]]
                }),
                None,
                Some(vec![1, 7]),
                None,
                None,
            );

            let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

            scan_assert(result, expected_1, expected_2);
        }

        mod projection {
            use super::*;

            #[test]
            fn selected() {
                let (_td, catalog, _) = scan_minimal_init!(multi);

                let expected_1 = ScanResult::from(hashmap! {
                    0 => Some(Fragment::from(vec![1_u64, 2, 3])),
                    2 => Some(Fragment::from(vec![11_u32, 12, 13])),
                    4 => Some(Fragment::from((vec![5_u32], vec![1_u32]))),
                });

                let expected_2 = ScanResult::from(hashmap! {
                    0 => Some(Fragment::from(vec![1_u64, 2, 3])),
                    2 => Some(Fragment::from(vec![211_u32, 212, 213])),
                    4 => Some(Fragment::from((vec![25_u32], vec![1_u32]))),
                });

                let scan = Scan::new(
                    Some(hashmap! {
                        0 => vec![vec![ScanFilter::U64(ScanFilterOp::Lt(4))]]
                    }),
                    Some(vec![0, 2, 4]),
                    None,
                    None,
                    None,
                );

                let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

                scan_assert(result, expected_1, expected_2);
            }

            #[test]
            fn no_queried() {
                let (_td, catalog, _) = scan_minimal_init!(multi);

                let expected_1 = ScanResult::from(hashmap! {
                    2 => Some(Fragment::from(vec![11_u32, 12, 13])),
                    4 => Some(Fragment::from((vec![5_u32], vec![1_u32]))),
                });

                let expected_2 = ScanResult::from(hashmap! {
                    2 => Some(Fragment::from(vec![211_u32, 212, 213])),
                    4 => Some(Fragment::from((vec![25_u32], vec![1_u32]))),
                });

                let scan = Scan::new(
                    Some(hashmap! {
                        0 => vec![vec![ScanFilter::U64(ScanFilterOp::Lt(4))]]
                    }),
                    Some(vec![2, 4]),
                    None,
                    None,
                    None,
                );

                let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

                scan_assert(result, expected_1, expected_2);
            }

            #[test]
            fn non_existent() {
                let (_td, catalog, _) = scan_minimal_init!(multi);

                let expected_1 = ScanResult::from(hashmap! {
                    2 => Some(Fragment::from(vec![11_u32, 12, 13])),
                    4 => Some(Fragment::from((vec![5_u32], vec![1_u32]))),
                    7 => None,
                });

                let expected_2 = ScanResult::from(hashmap! {
                    2 => Some(Fragment::from(vec![211_u32, 212, 213])),
                    4 => Some(Fragment::from((vec![25_u32], vec![1_u32]))),
                });

                let scan = Scan::new(
                    Some(hashmap! {
                        0 => vec![vec![ScanFilter::U64(ScanFilterOp::Lt(4))]]
                    }),
                    Some(vec![2, 4, 7]),
                    None,
                    None,
                    None,
                );

                let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

                scan_assert(result, expected_1, expected_2);
            }
        }
    }
}

#[cfg(all(feature = "nightly", test))]
mod benches {
    use test::Bencher;
    use super::*;

    macro_rules! scan_bench_impl {
        (dense simple $( $name: ident, $idx: expr, $value: expr ),+ $(,)*) => {
            $(
            #[bench]
            fn $name(b: &mut Bencher) {
                let (_td, catalog, _) = scan_test_impl!(init);

                let scan = Scan::new(
                    Some(hashmap! {
                        $idx => vec![vec![ScanFilterOp::Lt($value).into()]]
                    }),
                    None,
                    None,
                    None,
                    None,
                );

                b.iter(|| catalog.scan(&scan).with_context(|_| "scan failed").unwrap());
            }
            )+
        };

        (sparse simple $( $name: ident, $idx: expr, $value: expr ),+ $(,)*) => {
            $(
            #[bench]
            fn $name(b: &mut Bencher) {
                let (_td, catalog, _) = scan_test_impl!(init sparse);

                let scan = Scan::new(
                    Some(hashmap! {
                        $idx => vec![vec![ScanFilterOp::Lt($value).into()]]
                    }),
                    None,
                    None,
                    None,
                    None,
                );

                b.iter(|| catalog.scan(&scan).with_context(|_| "scan failed").unwrap());
            }
            )+
        };
    }

    scan_bench_impl!(dense simple
        simple_u8, 2, 100_u8,
        simple_u16, 3, 100_u16,
        simple_u32, 4, 100_u32,
        simple_u64, 5, 100_u64,);

    scan_bench_impl!(sparse simple
        simple_sparse_u8, 2, 100_u8,
        simple_sparse_u16, 3, 100_u16,
        simple_sparse_u32, 4, 100_u32,
        simple_sparse_u64, 5, 100_u64,);
}
