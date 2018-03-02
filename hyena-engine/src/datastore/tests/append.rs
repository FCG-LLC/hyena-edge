use super::*;
use ty::fragment::Fragment;

pub(crate) const DEFAULT_PARTITION_GROUPS: [SourceId; 3] = [1, 5, 7];

macro_rules! append_test_impl {
    (init $columns: expr) => {
        append_test_impl!(init $columns, <Timestamp as Default>::default())

    };

    (init $columns: expr, $ts_min: expr) => {{
        // assume 1, 5, 7 as default partition group ids for tests
        use datastore::tests::append::DEFAULT_PARTITION_GROUPS;

        append_test_impl!(init DEFAULT_PARTITION_GROUPS, $columns, $ts_min)
    }};

    (init $partition_groups: expr, $columns: expr, $ts_min: expr) => {{
        let ts_min = $ts_min;

        let source_ids = $partition_groups;

        let root = tempdir!();

        let mut cat = Catalog::new(&root)
            .with_context(|_| "Unable to create catalog")
            .unwrap();

        let columns = $columns;

        cat.ensure_columns(
            columns.into(),
        ).unwrap();

        for source_id in &source_ids {

            let pg = cat.ensure_group(*source_id)
                .with_context(|_| "Unable to retrieve partition group")
                .unwrap();

            let part = pg.create_partition(ts_min)
                .with_context(|_| "Unable to create partition")
                .unwrap();

            let mut vp = VecDeque::new();
            vp.push_front(part);

            pg.mutable_partitions = locked!(rw vp);
        }

        (root, cat, ts_min)
    }};

    ($schema: expr,
        $now: expr,
        $([
            $ts: expr,
            $data: expr    // HashMap
        ]),+ $(,)*) => {

        append_test_impl!($schema, $now, $([
            $ts,
            $data,
            1  // default source_id used in tests
        ])+)
    };

    ($partition_groups: expr,
        $schema: expr,
        $now: expr,
        $([
            $ts: expr,
            $data: expr    // HashMap
        ]),+ $(,)*) => {

        append_test_impl!($partition_groups, $schema, $now, $([
            $ts,
            $data,
            1  // default source_id used in tests
        ])+)
    };

    ($schema: expr,
        $now: expr,
        $([
            $ts: expr,
            $data: expr,    // HashMap
            $source_id: expr
        ]),+ $(,)*) => {{
        use datastore::tests::append::DEFAULT_PARTITION_GROUPS;

        append_test_impl!(DEFAULT_PARTITION_GROUPS, $schema, $now, $([
            $ts,
            $data,
            $source_id
        ],)+)
    }};

    ($partition_groups: expr,
        $schema: expr,
        $now: expr,
        $([
            $ts: expr,
            $data: expr,    // HashMap
            $source_id: expr
        ]),+ $(,)*) => {{

        let columns = $schema;

        let now = $now;

        let init = append_test_impl!(init $partition_groups, columns.clone(), now);
        let cat = init.1;

        $(

        let data = $data;

        let append = Append {
            ts: $ts,
            source_id: $source_id,
            data,
        };


        cat.append(&append).expect("unable to append fragment");

        )+

        init.0
    }};

    ($schema: expr,
        $now: expr,
        $expected_partitions: expr, // Vec<>
        $([
            $ts: expr,
            $ts_count: expr,
            $block_counts: expr,
            $data: expr    // HashMap
        ]),+ $(,)*) => {{

        #[allow(unused)]
        use block::BlockType as BlockTy;
        use ty::block::BlockId;
        use hyena_test::tempfile::TempDirExt;
        use params::PARTITION_METADATA;
        use ty::fragment::Fragment::*;
        use std::mem::transmute;
        use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator,
            ParallelIterator};

        let columns = $schema;
        let expected_partitions = $expected_partitions;

        let now = $now;

        let (td, part_ids) = {

            let init = append_test_impl!(init columns.clone(), now);
            let cat = init.1;

            $(

            let block_counts: HashMap<BlockId, (usize, usize, usize)> = $block_counts;
            let ts_count = $ts_count;
            let data = $data;

            // assert block counts
            for (id, &(count, step, start)) in &block_counts {
                assert!(
                    ts_count >= start + count * step,
                    "too many records for block {}",
                    id
                );
            }


            let append = Append {
                ts: $ts,
                source_id: 1,
                data,
            };


            cat.append(&append).expect("unable to append fragment");

            )+

            let parts = acquire!(read cat.groups[&1].mutable_partitions);
            let pids = parts.iter().map(|p| p.get_id()).collect::<Vec<_>>();

            (init.0, pids)
        };

        let root = RootManager::new(&td)
            .with_context(|_| "unable to instantiate RootManager")
            .unwrap();

        let pg_root = PartitionGroupManager::new(&root, 1)
            .with_context(|_| "unable to instantiate PartitionGroupManager")
            .unwrap();

            // assert catalog meta data
            assert!(td.exists_file(CATALOG_METADATA), "catalog metadata not found");

            // assert partition group meta data
            assert!(td.exists_file(
                    pg_root.as_ref().join(PARTITION_GROUP_METADATA)
                ),
                "partition group metadata not found"
            );

        part_ids.par_iter().enumerate().for_each(|(pidx, part_id)| {

            let block_data = expected_partitions.get(pidx)
                .ok_or_else(|| "expected a partition assertion data")
                .unwrap();

            let part_root = PartitionManager::new(&pg_root, part_id, now)
                .with_context(|_| "unable to instantiate PartitionManager")
                .unwrap();

            // assert partition meta data
            assert!(
                td.exists_file(part_root.as_ref().join(PARTITION_METADATA)),
                "partition {} metadata not found",
                part_id
            );

            // assert blocks
            columns.par_iter().for_each(|(id, col)| {
                if block_data.contains_key(&id) {
                    assert!(td.exists_file(
                            part_root.as_ref().join(format!("block_{}.data", id))
                        ),
                        "couldn't find block {}",
                        id);

                    if (*col).is_sparse() {
                        assert!(td.exists_file(
                                part_root.as_ref().join(format!("block_{}.index", id))
                            ),
                            "couldn't find block {} index file",
                            id);
                    }
                }
            });

            // assert data
            block_data.par_iter().for_each(|(id, frag)| {
                frag_apply!(
                    *frag,
                    blk,
                    idx,
                    {
                        let block = format!("block_{}.data", id);

                        let bdata = td.read_vec(part_root.as_ref().join(block))
                            .with_context(|_| "unable to read block data")
                            .unwrap();

                        let mapped = unsafe { transmute::<_, &[u8]>(blk.as_slice()) };

                        assert_eq!(
                            &mapped[..],
                            &bdata[..mapped.len()],
                            "dense block {} of partition {} ({}) data verification failed",
                            id,
                            part_id,
                            pidx);
                    },
                    {
                        let block = format!("block_{}.data", id);
                        let index = format!("block_{}.index", id);

                        let bdata = td.read_vec(part_root.as_ref().join(block))
                            .with_context(|_| "unable to read block data")
                            .unwrap();

                        let mapped = unsafe { transmute::<_, &[u8]>(blk.as_slice()) };

                        assert_eq!(
                            &mapped[..],
                            &bdata[..mapped.len()],
                            "sparse block {} of partition {} ({}) data verification failed",
                            id,
                            part_id,
                            pidx
                        );

                        let bidx = td.read_vec(part_root.as_ref().join(index))
                            .with_context(|_| "unable to read index data")
                            .unwrap();

                        let mapped = unsafe { transmute::<_, &[u8]>(idx.as_slice()) };

                        assert_eq!(
                            &mapped[..],
                            &bidx[..mapped.len()],
                            "sparse block {} of partition {} ({}) index verification \
                            failed",
                            id,
                            part_id,
                            pidx
                        );
                    }
                );
            });
        });

        td
    }};
}

#[cfg(all(feature = "nightly", test))]
mod benches {
    use test::Bencher;
    use super::*;

    #[bench]
    fn tiny(b: &mut Bencher) {
        use block::BlockType as BlockTy;
        use ty::block::BlockStorage::Memmap;

        let record_count = 1;

        let columns = hashmap! {
            0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
            1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
            2 => Column::new(Memmap(BlockTy::U8Dense), "col1"),
            3 => Column::new(Memmap(BlockTy::U32Dense), "col2"),
        };

        let data = hashmap! {
            2 => random!(gen u8, record_count).into(),
            3 => random!(gen u32, record_count).into(),
        };


        let init = append_test_impl!(init columns);

        let ts = RandomTimestampGen::iter_range_from(init.2)
            .take(record_count)
            .collect::<Vec<Timestamp>>()
            .into();

        let append = Append {
            ts,
            source_id: 1,
            data,
        };

        let cat = init.1;

        b.iter(|| cat.append(&append).expect("unable to append fragment"));
    }

    #[bench]
    fn small(b: &mut Bencher) {
        use block::BlockType as BlockTy;
        use ty::block::BlockStorage::Memmap;

        let record_count = 100;

        let columns = hashmap! {
            0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
            1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
            2 => Column::new(Memmap(BlockTy::U8Dense), "col1"),
            3 => Column::new(Memmap(BlockTy::U32Dense), "col2"),
        };

        let data = hashmap! {
            2 => random!(gen u8, record_count).into(),
            3 => random!(gen u32, record_count).into(),
        };

        let init = append_test_impl!(init columns);

        let ts = RandomTimestampGen::iter_range_from(init.2)
            .take(record_count)
            .collect::<Vec<Timestamp>>()
            .into();

        let append = Append {
            ts,
            source_id: 1,
            data,
        };

        let cat = init.1;

        b.iter(|| cat.append(&append).expect("unable to append fragment"));
    }

    #[bench]
    fn lots_columns(b: &mut Bencher) {
        use block::BlockType as BlockTy;
        use ty::block::BlockStorage::Memmap;

        let record_count = 100;
        let column_count = 10000;

        let mut columns = hashmap! {
            0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
            1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
        };

        let mut data = hashmap!{};

        for idx in 2..column_count {
            columns.insert(
                idx,
                Column::new(Memmap(BlockTy::U32Dense), &format!("col{}", idx)),
            );
            data.insert(idx, random!(gen u32, record_count).into());
        }

        let init = append_test_impl!(init columns);

        let ts = RandomTimestampGen::iter_range_from(init.2)
            .take(record_count)
            .collect::<Vec<Timestamp>>()
            .into();

        let append = Append {
            ts,
            source_id: 1,
            data,
        };

        let cat = init.1;

        b.iter(|| cat.append(&append).expect("unable to append fragment"));
    }

    #[bench]
    fn big_data(b: &mut Bencher) {
        use block::BlockType as BlockTy;
        use ty::block::BlockStorage::Memmap;

        let record_count = MAX_RECORDS;

        let columns = hashmap! {
            0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
            1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
            2 => Column::new(Memmap(BlockTy::U8Dense), "col1"),
            3 => Column::new(Memmap(BlockTy::U32Dense), "col2"),
        };

        let data = hashmap! {
            2 => random!(gen u8, record_count).into(),
            3 => random!(gen u32, record_count).into(),
        };


        let init = append_test_impl!(init columns);

        let ts = RandomTimestampGen::iter_range_from(init.2)
            .take(record_count)
            .collect::<Vec<Timestamp>>()
            .into();

        let append = Append {
            ts,
            source_id: 1,
            data,
        };

        let cat = init.1;

        b.iter(|| cat.append(&append).expect("unable to append fragment"));
    }
}

mod dense {
    use super::*;
    use ty::block::BlockStorage::Memmap;

    #[test]
    fn ts_only() {
        use std::mem::transmute;

        let now = <Timestamp as Default>::default();

        let record_count = 100;

        let data = seqfill!(vec u64, record_count);

        let v = unsafe { transmute::<_, Vec<Timestamp>>(data.clone()) };

        let expected = hashmap! {
            0 => Fragment::from(data)
        };

        append_test_impl!(
            hashmap! {
                0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
            },
            now,
            vec![expected],
            [v.into(), record_count, hashmap!{}, hashmap!{}]
        );
    }

    #[test]
    fn current_only() {
        let now = <Timestamp as Default>::default();

        let record_count = 100;

        let mut v = vec![Timestamp::from(0); record_count];
        seqfill!(Timestamp, &mut v[..], now);

        let data = hashmap! {
            2 => Fragment::from(seqfill!(vec u8, record_count)),
            3 => Fragment::from(seqfill!(vec u32, record_count)),
        };

        let mut expected = data.clone();

        expected.insert(0, Fragment::from(v.clone()));

        append_test_impl!(
            hashmap! {
                0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                2 => Column::new(Memmap(BlockTy::U8Dense), "col1"),
                3 => Column::new(Memmap(BlockTy::U32Dense), "col2"),
            },
            now,
            vec![expected],
            [
                v.into(),
                record_count,
                hashmap! {
                    2 => (record_count, 0, 0),
                    3 => (record_count, 0, 0),
                },
                data
            ]
        );
    }

    #[test]
    fn current_full() {
        let now = <Timestamp as Default>::default();

        let record_count = MAX_RECORDS - 1;

        let mut v = vec![Timestamp::from(0); record_count];
        seqfill!(Timestamp, &mut v[..], now);

        let data = hashmap! {
            2 => Fragment::from(seqfill!(vec u8, record_count)),
            3 => Fragment::from(seqfill!(vec u32, record_count)),
        };

        let mut expected = data.clone();

        expected.insert(0, Fragment::from(v.clone()));

        append_test_impl!(
            hashmap! {
                0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                2 => Column::new(Memmap(BlockTy::U8Dense), "col1"),
                3 => Column::new(Memmap(BlockTy::U32Dense), "col2"),
            },
            now,
            vec![expected],
            [
                v.into(),
                record_count,
                hashmap! {
                    2 => (record_count, 0, 0),
                    3 => (record_count, 0, 0),
                },
                data
            ]
        );
    }

    #[test]
    fn two() {
        let now = <Timestamp as Default>::default();

        let record_count = MAX_RECORDS + 100;

        let mut v = vec![Timestamp::from(0); record_count];
        seqfill!(Timestamp, &mut v[..], now);

        let data = hashmap! {
            2 => Fragment::from(seqfill!(vec u8, record_count)),
            3 => Fragment::from(seqfill!(vec u32, record_count)),
        };

        let expected = vec![
            hashmap! {
                0 => Fragment::from(Vec::from(&v[..MAX_RECORDS])),
                2 => Fragment::from(seqfill!(vec u8, MAX_RECORDS)),
                3 => Fragment::from(seqfill!(vec u32, MAX_RECORDS)),
            },
            hashmap! {
                0 => Fragment::from(Vec::from(&v[MAX_RECORDS..])),
                2 => Fragment::from(seqfill!(vec u8, 100, MAX_RECORDS)),
                3 => Fragment::from(seqfill!(vec u32, 100, MAX_RECORDS)),
            },
        ];

        append_test_impl!(
            hashmap! {
                0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                2 => Column::new(Memmap(BlockTy::U8Dense), "col1"),
                3 => Column::new(Memmap(BlockTy::U32Dense), "col2"),
            },
            now,
            expected,
            [
                v.into(),
                record_count,
                hashmap! {
                    2 => (record_count, 0, 0),
                    3 => (record_count, 0, 0),
                },
                data
            ]
        );
    }

    #[test]
    fn two_full() {
        let now = <Timestamp as Default>::default();

        let record_count = MAX_RECORDS * 2;

        let mut v = vec![Timestamp::from(0); record_count];
        seqfill!(Timestamp, &mut v[..], now);

        let data = hashmap! {
            2 => Fragment::from(seqfill!(vec u8, record_count)),
            3 => Fragment::from(seqfill!(vec u32, record_count)),
        };

        let expected = vec![
            hashmap!{},
            hashmap! {
                0 => Fragment::from(Vec::from(&v[..MAX_RECORDS])),
                2 => Fragment::from(seqfill!(vec u8, MAX_RECORDS)),
                3 => Fragment::from(seqfill!(vec u32, MAX_RECORDS)),
            },
            hashmap! {
                0 => Fragment::from(Vec::from(&v[MAX_RECORDS..])),
                2 => Fragment::from(seqfill!(vec u8, MAX_RECORDS, MAX_RECORDS)),
                3 => Fragment::from(seqfill!(vec u32, MAX_RECORDS, MAX_RECORDS)),
            },
        ];

        append_test_impl!(
            hashmap! {
                0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                2 => Column::new(Memmap(BlockTy::U8Dense), "col1"),
                3 => Column::new(Memmap(BlockTy::U32Dense), "col2"),
            },
            now,
            expected,
            [
                v.into(),
                record_count,
                hashmap! {
                    2 => (record_count, 0, 0),
                    3 => (record_count, 0, 0),
                },
                data
            ]
        );
    }

    #[test]
    fn consecutive_small() {
        let now = <Timestamp as Default>::default();

        let record_count = 100;

        let mut v_1 = vec![Timestamp::from(0); record_count];
        let ts_base = seqfill!(Timestamp, &mut v_1[..], now);

        let mut v_2 = vec![Timestamp::from(0); record_count];
        seqfill!(Timestamp, &mut v_2[..], ts_base);

        let data = hashmap! {
            2 => Fragment::from(seqfill!(vec u8, record_count)),
            3 => Fragment::from(seqfill!(vec u32, record_count)),
        };

        let expected = vec![
            hashmap! {
                0 => <Fragment as From<Vec<Timestamp>>>::from(
                    merge_iter!(v_1.clone().into_iter(), v_2.clone().into_iter())
                ),
                2 => Fragment::from(multiply_vec!(seqfill!(vec u8, record_count), 2)),
                3 => Fragment::from(multiply_vec!(seqfill!(vec u32, record_count), 2)),
            },
        ];

        append_test_impl!(
            hashmap! {
                0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                2 => Column::new(Memmap(BlockTy::U8Dense), "col1"),
                3 => Column::new(Memmap(BlockTy::U32Dense), "col2"),
            },
            now,
            expected,
            [
                v_1.into(),
                record_count,
                hashmap! {
                    2 => (record_count, 0, 0),
                    3 => (record_count, 0, 0),
                },
                data.clone()
            ],
            [
                v_2.into(),
                record_count,
                hashmap! {
                    2 => (record_count, 0, 0),
                    3 => (record_count, 0, 0),
                },
                data
            ]
        );
    }

    #[test]
    fn consecutive_two() {
        let now = <Timestamp as Default>::default();

        let record_count_1 = MAX_RECORDS + 100;
        let record_count_2 = 100;

        let mut v_1 = vec![Timestamp::from(0); record_count_1];
        let ts_base = seqfill!(Timestamp, &mut v_1[..], now);

        let mut v_2 = vec![Timestamp::from(0); record_count_2];
        seqfill!(Timestamp, &mut v_2[..], ts_base);

        let b1_c2 = seqfill!(vec u8, record_count_1);
        let b1_c3 = seqfill!(vec u32, record_count_1);

        let data_1 = hashmap! {
            2 => Fragment::from(b1_c2.clone()),
            3 => Fragment::from(b1_c3.clone()),
        };

        let b2_c2 = seqfill!(vec u8, record_count_2);
        let b2_c3 = seqfill!(vec u32, record_count_2);

        let data_2 = hashmap! {
            2 => Fragment::from(b2_c2.clone()),
            3 => Fragment::from(b2_c3.clone()),
        };

        let expected = vec![
            hashmap! {
                0 => Fragment::from(v_1[..MAX_RECORDS].to_vec()),
                2 => Fragment::from(b1_c2[..MAX_RECORDS].to_vec()),
                3 => Fragment::from(b1_c3[..MAX_RECORDS].to_vec()),
            },
            hashmap! {
                0 => Fragment::from(merge_iter!(
                        into Vec<Timestamp>,
                        v_1[MAX_RECORDS..].iter().cloned(),
                        v_2.clone().into_iter()
                )),
                2 => Fragment::from(merge_iter!(
                        into Vec<u8>,
                        b1_c2[MAX_RECORDS..].iter().cloned(),
                        b2_c2.into_iter()
                )),
                3 => Fragment::from(merge_iter!(
                        into Vec<u32>,
                        b1_c3[MAX_RECORDS..].iter().cloned(),
                        b2_c3.into_iter()
                )),
            },
        ];

        append_test_impl!(
            hashmap! {
                0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                2 => Column::new(Memmap(BlockTy::U8Dense), "col1"),
                3 => Column::new(Memmap(BlockTy::U32Dense), "col2"),
            },
            now,
            expected,
            [
                v_1.into(),
                record_count_1,
                hashmap! {
                    2 => (record_count_1, 0, 0),
                    3 => (record_count_1, 0, 0),
                },
                data_1
            ],
            [
                v_2.into(),
                record_count_2,
                hashmap! {
                    2 => (record_count_2, 0, 0),
                    3 => (record_count_2, 0, 0),
                },
                data_2
            ]
        );
    }

    #[test]
    fn consecutive_two_full() {
        let now = <Timestamp as Default>::default();

        let record_count_1 = MAX_RECORDS;
        let record_count_2 = MAX_RECORDS;

        let mut v_1 = vec![Timestamp::from(0); record_count_1];
        let ts_base = seqfill!(Timestamp, &mut v_1[..], now);

        let mut v_2 = vec![Timestamp::from(0); record_count_2];
        seqfill!(Timestamp, &mut v_2[..], ts_base);

        let b1_c2 = seqfill!(vec u8, record_count_1);
        let b1_c3 = seqfill!(vec u32, record_count_1);

        let data_1 = hashmap! {
            2 => Fragment::from(b1_c2.clone()),
            3 => Fragment::from(b1_c3.clone()),
        };

        let b2_c2 = seqfill!(vec u8, record_count_2);
        let b2_c3 = seqfill!(vec u32, record_count_2);

        let data_2 = hashmap! {
            2 => Fragment::from(b2_c2.clone()),
            3 => Fragment::from(b2_c3.clone()),
        };

        let expected = vec![
            hashmap! {},
            hashmap! {
                0 => Fragment::from(v_1.clone()),
                2 => Fragment::from(b1_c2),
                3 => Fragment::from(b1_c3),
            },
            hashmap! {
                0 => Fragment::from(v_2.clone()),
                2 => Fragment::from(b2_c2),
                3 => Fragment::from(b2_c3),
            },
        ];

        append_test_impl!(
            hashmap! {
                0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                2 => Column::new(Memmap(BlockTy::U8Dense), "col1"),
                3 => Column::new(Memmap(BlockTy::U32Dense), "col2"),
            },
            now,
            expected,
            [
                v_1.into(),
                record_count_1,
                hashmap! {
                    2 => (record_count_1, 0, 0),
                    3 => (record_count_1, 0, 0),
                },
                data_1
            ],
            [
                v_2.into(),
                record_count_2,
                hashmap! {
                    2 => (record_count_2, 0, 0),
                    3 => (record_count_2, 0, 0),
                },
                data_2
            ]
        );
    }

    #[test]
    fn consecutive_two_overflow() {
        let now = <Timestamp as Default>::default();

        let record_count_1 = MAX_RECORDS + 100;
        let record_count_2 = MAX_RECORDS + 100;

        let mut v_1 = vec![Timestamp::from(0); record_count_1];
        let ts_base = seqfill!(Timestamp, &mut v_1[..], now);

        let mut v_2 = vec![Timestamp::from(0); record_count_2];
        seqfill!(Timestamp, &mut v_2[..], ts_base);

        let b1_c2 = seqfill!(vec u8, record_count_1);
        let b1_c3 = seqfill!(vec u32, record_count_1);

        let data_1 = hashmap! {
            2 => Fragment::from(b1_c2.clone()),
            3 => Fragment::from(b1_c3.clone()),
        };

        let b2_c2 = seqfill!(vec u8, record_count_2);
        let b2_c3 = seqfill!(vec u32, record_count_2);

        let data_2 = hashmap! {
            2 => Fragment::from(b2_c2.clone()),
            3 => Fragment::from(b2_c3.clone()),
        };

        let expected = vec![
            hashmap! {
                0 => Fragment::from(v_1[..MAX_RECORDS].to_vec()),
                2 => Fragment::from(b1_c2[..MAX_RECORDS].to_vec()),
                3 => Fragment::from(b1_c3[..MAX_RECORDS].to_vec()),
            },
            hashmap! {
                0 => Fragment::from(merge_iter!(
                        into Vec<Timestamp>,
                        v_1[MAX_RECORDS..].iter().cloned(),
                        v_2[..MAX_RECORDS - 100].iter().cloned(),
                )),
                2 => Fragment::from(merge_iter!(
                        into Vec<u8>,
                        b1_c2[MAX_RECORDS..].iter().cloned(),
                        b2_c2[..MAX_RECORDS - 100].iter().cloned(),
                )),
                3 => Fragment::from(merge_iter!(
                        into Vec<u32>,
                        b1_c3[MAX_RECORDS..].iter().cloned(),
                        b2_c3[..MAX_RECORDS - 100].iter().cloned(),
                )),
            },
            hashmap! {
                0 => Fragment::from(merge_iter!(
                        into Vec<Timestamp>,
                        v_2[MAX_RECORDS - 100..].iter().cloned(),
                )),
                2 => Fragment::from(merge_iter!(
                        into Vec<u8>,
                        b2_c2[MAX_RECORDS - 100..].iter().cloned(),
                )),
                3 => Fragment::from(merge_iter!(
                        into Vec<u32>,
                        b2_c3[MAX_RECORDS - 100..].iter().cloned(),
                )),
            },
        ];

        append_test_impl!(
            hashmap! {
                0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                2 => Column::new(Memmap(BlockTy::U8Dense), "col1"),
                3 => Column::new(Memmap(BlockTy::U32Dense), "col2"),
            },
            now,
            expected,
            [
                v_1.into(),
                record_count_1,
                hashmap! {
                    2 => (record_count_1, 0, 0),
                    3 => (record_count_1, 0, 0),
                },
                data_1
            ],
            [
                v_2.into(),
                record_count_2,
                hashmap! {
                    2 => (record_count_2, 0, 0),
                    3 => (record_count_2, 0, 0),
                },
                data_2
            ]
        );
    }

    #[test]
    fn u32_10k_columns() {
        use block::BlockType as BlockTy;

        let now = <Timestamp as Default>::default();

        let record_count = 100;
        let column_count = 10_000;

        let mut v = vec![Timestamp::from(0); record_count];
        seqfill!(Timestamp, &mut v[..], now);

        let mut columns = hashmap! {
            0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
            1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
        };

        let mut data = hashmap!{};
        let mut expected = hashmap! {
            0 => Fragment::from(v.clone()),
        };
        let mut counts = hashmap!{};

        for idx in 2..column_count {
            columns.insert(
                idx,
                Column::new(Memmap(BlockTy::U32Dense), &format!("col{}", idx)),
            );

            let d = seqfill!(vec u32, record_count);

            data.insert(idx, Fragment::from(d.clone()));

            expected.insert(idx, Fragment::from(d));

            counts.insert(idx, (record_count, 0, 0));
        }

        append_test_impl!(
            columns,
            now,
            vec![expected],
            [v.into(), record_count, counts, data],
        );
    }
}

mod sparse {
    use super::*;
    use ty::block::BlockStorage::Memmap;

    #[test]
    fn current_only() {
        let now = <Timestamp as Default>::default();

        let record_count = 100;
        let sparse_count_2 = 25;
        let sparse_step_2 = 4;
        let sparse_count_3 = 14;
        let sparse_step_3 = 7;

        let mut v = vec![Timestamp::from(0); record_count];
        seqfill!(Timestamp, &mut v[..], now);

        let data = hashmap! {
            2 => Fragment::from((seqfill!(vec u8, sparse_count_2),
                    seqfill!(vec u32, sparse_count_2, 0, sparse_step_2)
                )),
            3 => Fragment::from((seqfill!(vec u32, sparse_count_3),
                    seqfill!(vec u32, sparse_count_3, 0, sparse_step_3)
                )),
        };

        let mut expected = data.clone();

        expected.insert(0, Fragment::from(v.clone()));

        append_test_impl!(
            hashmap! {
                0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                2 => Column::new(Memmap(BlockTy::U8Sparse), "col1"),
                3 => Column::new(Memmap(BlockTy::U32Sparse), "col2"),
            },
            now,
            vec![expected],
            [
                v.into(),
                record_count,
                hashmap! {
                    2 => (sparse_count_2, sparse_step_2, 0),
                    3 => (sparse_count_3, sparse_step_3, 0),
                },
                data
            ]
        );
    }

    #[test]
    fn consecutive_two() {
        let now = <Timestamp as Default>::default();

        let record_count = 100;
        let sparse_count_2 = 25;
        let sparse_step_2 = 4;
        let sparse_count_3 = 14;
        let sparse_step_3 = 7;

        let mut v_1 = vec![Timestamp::from(0); record_count];
        let ts_start = seqfill!(Timestamp, &mut v_1[..], now);

        let data_1 = hashmap! {
            2 => Fragment::from((seqfill!(vec u8, sparse_count_2),
                    seqfill!(vec u32, sparse_count_2, 0, sparse_step_2)
                )),
            3 => Fragment::from((seqfill!(vec u32, sparse_count_3),
                    seqfill!(vec u32, sparse_count_3, 0, sparse_step_3)
                )),
        };

        let mut v_2 = vec![Timestamp::from(0); record_count];
        seqfill!(Timestamp, &mut v_2[..], ts_start);

        let data_2 = hashmap! {
            2 => Fragment::from((seqfill!(vec u8, sparse_count_2),
                    seqfill!(vec u32, sparse_count_2, 0, sparse_step_2)
                )),
            3 => Fragment::from((seqfill!(vec u32, sparse_count_3),
                    seqfill!(vec u32, sparse_count_3, 0, sparse_step_3)
                )),
        };

        let expected = hashmap! {
            0 => Fragment::from(
                v_1.clone().into_iter().chain(v_2.clone().into_iter()).collect::<Vec<_>>()
            ),
            2 => Fragment::from(({
                        let mut v = seqfill!(vec u8, sparse_count_2);
                        let mut vc = v.clone();
                        v.append(&mut vc);
                        v
                    },
                    seqfill!(vec u32, sparse_count_2 * 2, 0, sparse_step_2)
                )),
            3 => Fragment::from(({
                        let mut v = seqfill!(vec u32, sparse_count_3);
                        let mut vc = v.clone();
                        v.append(&mut vc);
                        v
                    },
                    seqfill!(vec u32, sparse_count_3 * 2, 0, sparse_step_3)
                )),
        };

        append_test_impl!(
            hashmap! {
                0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                2 => Column::new(Memmap(BlockTy::U8Sparse), "col1"),
                3 => Column::new(Memmap(BlockTy::U32Sparse), "col2"),
            },
            now,
            vec![expected],
            [
                v_1.into(),
                record_count,
                hashmap! {
                    2 => (sparse_count_2, sparse_step_2, 0),
                    3 => (sparse_count_3, sparse_step_3, 0),
                },
                data_1
            ],
            [
                v_2.into(),
                record_count,
                hashmap! {
                    2 => (sparse_count_2, sparse_step_2, 0),
                    3 => (sparse_count_3, sparse_step_3, 0),
                },
                data_2
            ]
        );
    }

    #[test]
    fn single_full() {
        let now = <Timestamp as Default>::default();

        let record_count = MAX_RECORDS;
        let sparse_count_2 = MAX_RECORDS / 2;
        let sparse_step_2 = 2;

        let mut v = vec![Timestamp::from(0); record_count];
        seqfill!(Timestamp, &mut v[..], now);

        let data = hashmap! {
            2 => Fragment::from((seqfill!(vec u8, sparse_count_2),
                    seqfill!(vec u32, sparse_count_2, 0, sparse_step_2)
                )),
        };

        let mut expected = data.clone();

        expected.insert(0, Fragment::from(v.clone()));

        append_test_impl!(
            hashmap! {
                0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                2 => Column::new(Memmap(BlockTy::U8Sparse), "col1"),
            },
            now,
            vec![hashmap!{}, expected],
            [
                v.into(),
                record_count,
                hashmap! {
                    2 => (sparse_count_2, sparse_step_2, 0),
                },
                data
            ]
        );
    }
}
