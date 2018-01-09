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

mod catalog;
mod column;
mod partition;
mod partition_group;
mod partition_meta;

pub use self::catalog::Catalog;
pub use self::column::Column;
pub(crate) use self::partition::{Partition, PartitionId};
pub(crate) use self::partition_meta::PartitionMeta;
pub(crate) use self::partition_group::PartitionGroup;

pub(crate) type PartitionMap<'part> = HashMap<PartitionMeta, Partition<'part>>;
pub(crate) type PartitionGroupMap<'pg> = HashMap<SourceId, PartitionGroup<'pg>>;
pub type ColumnMap = HashMap<ColumnId, Column>;

#[cfg(test)]
mod tests {
    use super::*;
    use storage::manager::RootManager;
    use hyena_test::random::timestamp::RandomTimestampGen;
    use params::BLOCK_SIZE;

    // until const fn stabilizes we have to use this hack
    // see https://github.com/rust-lang/rust/issues/24111

    // make sure that size_of::<Timestamp>() == 8
    assert_eq_size!(timestamp_size_check; u64, Timestamp);

    const TIMESTAMP_SIZE: usize = 8; // should be `size_of::<Timestamp>()`
    const MAX_RECORDS: usize = BLOCK_SIZE / TIMESTAMP_SIZE;

    pub(super) fn create_random_partitions(pg: &mut PartitionGroup,
                                           im_count: usize,
                                           mut_count:usize) {
        let pts = RandomTimestampGen::pairs::<u64>(im_count + mut_count);

        let (imparts, mutparts): (Vec<_>, Vec<_>) = pts.iter()
            .map(|&(ref lo, ref hi)| {
                let mut part = pg.create_partition(*lo)
                    .with_context(|_| "Unable to create partition")
                    .unwrap();

                part.set_ts(None, Some(*hi))
                    .with_context(|_| "Failed to set timestamp on partition")
                    .unwrap();

                part
            })
            .enumerate()
            .partition(|&(idx, _)| idx < im_count);

        pg.immutable_partitions = imparts
            .into_iter()
            .map(|(_, part)| (PartitionMeta::from(&part), part))
            .collect();
        pg.mutable_partitions = locked!(rw mutparts.into_iter().map(|(_, part)| part).collect());
    }

    #[macro_use]
    mod append {
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
                use ty::block::BlockType::Memmap;

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
                use ty::block::BlockType::Memmap;

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
                use ty::block::BlockType::Memmap;

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
                use ty::block::BlockType::Memmap;

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
            use ty::block::BlockType::Memmap;

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
            use ty::block::BlockType::Memmap;

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
    }

    mod scan {
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
                    use block::BlockType as BlockTy;
                    use ty::block::BlockType::Memmap;
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
                            0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                            1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                            2 => Column::new(Memmap(BlockTy::U8Dense), "col1"),
                            3 => Column::new(Memmap(BlockTy::U16Dense), "col2"),
                            4 => Column::new(Memmap(BlockTy::U32Dense), "col3"),
                            5 => Column::new(Memmap(BlockTy::U64Dense), "col4"),
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
                    use block::BlockType as BlockTy;
                    use ty::block::BlockType::Memmap;
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
                            0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                            1 => Column::new(Memmap(BlockTy::U32Dense), "source"),
                            2 => Column::new(Memmap(BlockTy::U8Sparse), "col1"),
                            3 => Column::new(Memmap(BlockTy::U16Sparse), "col2"),
                            4 => Column::new(Memmap(BlockTy::U32Sparse), "col3"),
                            5 => Column::new(Memmap(BlockTy::U64Sparse), "col4"),
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
                            filters.insert($idx, vec![$( $value )+]);
                        )+

                        let scan = Scan::new(
                            filters,
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
                            filters.insert($idx, vec![$( $value )+]);
                        )+

                        let scan = Scan::new(
                            filters,
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
                use block::BlockType as BlockTy;
                use self::TyBlockType::Memmap;
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
                            0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                            1 => Column::new(Memmap(BlockTy::U32Dense), "source_id"),
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
                        hashmap! {
                            0 => vec![ScanFilter::U64(ScanFilterOp::Gt(0))] // a.k.a. full scan
                        },
                        None,
                        None,
                        Some(parts),
                        None,
                    );

                    let result = cat.scan(&scan).with_context(|_| "scan failed").unwrap();

                    assert_eq!(expected, result);
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
            use self::TyBlockType::Memmap;

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
                                vec![$( $dense_data:tt )*]
                            )),* $(,)*],
                            sparse [ $( $sparse_idx:expr => (
                                vec![$( $sparse_data:tt )*],
                                vec![$( $sparse_data_idx:tt )*]
                            )),* $(,)*]
                        ]
                    ),* $(,)* ]
                ) => {{
                    use block::BlockType as BlockTy;

                    let now = $ts;

                    let mut v = vec![Timestamp::from(0); $length];
                    seqfill!(Timestamp, &mut v[..], now);

                    // ts column is awlays present
                    let mut schema = hashmap! {
                        0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                    };

                    // adjust schema first

                    $(
                        schema.insert($dense_schema_idx,
                            Column::new(Memmap(BlockTy::$dense_ty), $dense_name));
                    )*

                    $(
                        schema.insert($sparse_schema_idx,
                            Column::new(Memmap(BlockTy::$sparse_ty), $sparse_name));

                    )*

                    let td = append_test_impl!(
                        schema,
                        now,
                        $(
                        [
                            v.clone().into(),
                            {
                                let mut data = hashmap! {};
                            $(
                                data.insert($dense_idx,
                                    Fragment::from(vec![$($dense_data)*]));
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
                    hashmap! {
                        0 => vec![ScanFilter::U64(ScanFilterOp::Lt(4))]
                    },
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
                    hashmap! {
                        1 => vec![ScanFilter::U8(ScanFilterOp::GtEq(4))]
                    },
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
                    hashmap! {
                        0 => vec![ScanFilter::U64(ScanFilterOp::In(hashset![2, 5, 10]))]
                    },
                    None,
                    None,
                    None,
                    None,
                );

                let result = catalog.scan(&scan).with_context(|_| "scan failed").unwrap();

                assert_eq!(expected, result);
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
                        hashmap! {
                            1 => vec![ScanFilter::U32(ScanFilterOp::Lt(3))],
                            3 => vec![ScanFilter::U16(ScanFilterOp::Gt(30))],
                        },
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
                        hashmap! {
                            2 => vec![ScanFilter::U8(ScanFilterOp::Lt(4))],
                            4 => vec![ScanFilter::U16(ScanFilterOp::Gt(15))],
                        },
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
                        hashmap! {
                            1 => vec![ScanFilter::U32(ScanFilterOp::Gt(4))],
                            4 => vec![ScanFilter::U16(ScanFilterOp::Lt(15))],
                        },
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
                        hashmap! {
                            0 => vec![ScanFilter::U64(ScanFilterOp::Lt(4))]
                        },
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
                        hashmap! {
                            0 => vec![ScanFilter::U64(ScanFilterOp::Lt(4))]
                        },
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
                            hashmap! {
                                0 => vec![ScanFilter::U64(ScanFilterOp::Lt(4))]
                            },
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
                            hashmap! {
                                0 => vec![ScanFilter::U64(ScanFilterOp::Lt(4))]
                            },
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
                            hashmap! {
                                0 => vec![ScanFilter::U64(ScanFilterOp::Lt(4))]
                            },
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
                            hashmap! {
                                $idx => vec![ScanFilterOp::Lt($value).into()]
                            },
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
                            hashmap! {
                                $idx => vec![ScanFilterOp::Lt($value).into()]
                            },
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
    }
}
