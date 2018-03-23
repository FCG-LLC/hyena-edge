use error::*;
use hyena_common::ty::Timestamp;
use block::SparseIndex;
use storage::manager::PartitionManager;
use std::collections::hash_map::HashMap;
use std::collections::vec_deque::VecDeque;
use std::path::{Path, PathBuf};
use std::iter::FromIterator;
use std::default::Default;
use std::sync::RwLock;
use params::{SourceId, PARTITION_GROUP_METADATA};
use mutator::append::Append;
use scanner::{Scan, ScanResult};
use ty::block::{BlockStorageMap, BlockStorageMapType};
use ty::ColumnId;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::iter::{once, repeat};
use ty::fragment::FragmentRef;

use super::partition::Partition;
use super::partition_meta::PartitionMeta;
use super::PartitionMap;
use super::catalog::Catalog;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PartitionGroup<'pg> {
    // TODO: we should consider changing this to something more universal
    // and less coupled with our specific schema perhaps
    source_id: SourceId,

    #[serde(skip)]
    pub(crate) immutable_partitions: PartitionMap<'pg>,

    #[serde(skip)]
    pub(crate) mutable_partitions: RwLock<VecDeque<Partition<'pg>>>,

    #[serde(skip)]
    data_root: PathBuf,
}

impl<'pg> PartitionGroup<'pg> {
    pub(super) fn new<P: AsRef<Path>>(root: P, source_id: SourceId) -> Result<PartitionGroup<'pg>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(PARTITION_GROUP_METADATA);

        if meta.exists() {
            bail!("Partition group metadata already exists {:?}", meta);
        }

        Ok(PartitionGroup {
            source_id,
            immutable_partitions: Default::default(),
            mutable_partitions: Default::default(),
            data_root: root,
        })
    }

    pub fn with_data<P: AsRef<Path>>(root: P) -> Result<PartitionGroup<'pg>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(PARTITION_GROUP_METADATA);

        PartitionGroup::deserialize(&meta, &root)
    }

    pub(super) fn flush(&self) -> Result<()> {
        // TODO: add dirty flag
        let meta = self.data_root.join(PARTITION_GROUP_METADATA);

        for p in self.immutable_partitions.values() {
            p.flush()?
        }

        for p in self.mutable_partitions.read().unwrap().iter() {
            p.flush()?
        }

        PartitionGroup::serialize(self, &meta)
    }

    /// Append data
    pub(super) fn append(&self, catalog: &Catalog, data: &Append) -> Result<usize> {

        // append with no data is a no-op
        if data.is_empty() {
            return Ok(0)
        }

        // get types of all columns used in this append

        let mut colindices = vec![0, 1]; // ts and source_id
        colindices.extend(data.data.keys());

        let columns = catalog.as_ref();

        let typemap: BlockStorageMap = colindices
            .iter()
            .map(|colidx| if let Some(col) = columns.get(colidx) {
                Ok((*colidx, **col))
            } else {
                bail!("column {} not found", colidx)
            })
            .collect::<Result<BlockStorageMapType>>()
            .with_context(|_| err_msg("failed to prepare all columns"))?
            .into();

        // calculate the size of the append (in records)
        let fragcount = data.len();

        let (emptycap, currentcap) = {
            let partitions = acquire!(read carry self.mutable_partitions);

            // empty partition capacity for this append
            //
            // the row count we can store depends on a specific sparse columns being present
            // in the append data, so it varies between appends
            let emptycap = catalog.space_for_blocks(&colindices);

            // the remaining capacity of the current partition
            // calculated for this specific append
            //
            // the current partition is the partition that is open for writing
            // i.e. the upcoming append operation will add some data to it
            //
            // current is empty -> return emptycap
            // current is full -> return 0
            // current doesn't exist -> return 0
            // current has space -> return space
            let currentcap = partitions
                .back()
                .map(|part| part
                    .space_for_blocks(&colindices)
                    .unwrap_or_else(|| emptycap))
                .unwrap_or_default();

            (
                emptycap,
                currentcap,
            )
        };

        // check if we can fit the data within current partition
        // or additional ones are needed

        // curfrags - the number of fragments we can write to the current partition
        // reqparts - required number of partitions to create
        let (curfrags, reqparts) = if fragcount > currentcap {
            let emptyfrags = fragcount - currentcap;

            (
                currentcap,
                (emptyfrags / emptycap) + if emptyfrags % emptycap != 0 { 1 } else { 0 }
            )
        } else {
            (fragcount, 0)
        };

        // current_is_full - is the current partition full, i.e. should we attempt to write to it
        let current_is_full = curfrags == 0;

        // split append data between partitions

        let (ts_idx, fragments, offsets) = Self::split_append(&data, curfrags, reqparts, emptycap);

        // create partition pool

        let partitions = acquire!(write carry self.mutable_partitions);

        let curidx = partitions.len();

        let newparts = ts_idx
            .iter()
            .skip(if current_is_full { 0 } else { 1 })
            .map(|ts| self.create_partition(**ts))
            .collect::<Result<Vec<_>>>()
            .with_context(|_| "Unable to create partition for writing")?;

        partitions.extend(newparts);

        // write data

        for ((partition, fragment), offset) in partitions
            .iter_mut()
            .skip(if current_is_full { curidx } else { curidx.saturating_sub(1) })
            .zip(fragments.iter())
            .zip(offsets.iter())
        {
            partition
                .append(&typemap, &fragment, *offset as SparseIndex)
                .with_context(|_| "partition append failed")
                .unwrap();
        }

        // todo: this is not very accurate, as the actual write could have failed
        Ok(fragcount)
    }

    /// Prepare source slices for append
    ///
    /// `current_fragments` is a free space in the current partition
    /// `required_partitions` is a number of partitions that will be created
    /// `empty_capacity` is a capacity of an empty partition for this particular `data`
    ///
    /// Returns (timestamp slices, fragments with blocks for each partition, offsets)
    #[inline]
    fn split_append(
        data: &Append,
        current_fragments: usize,
        required_partitions: usize,
        empty_capacity: usize,
    ) -> (
        Vec<&Timestamp>,
        Vec<HashMap<ColumnId, FragmentRef>>,
        Vec<usize>,
    ) {
        if required_partitions == 0 {
            assert!(current_fragments >= data.ts.len());

            (
                data.ts.as_slice().iter().take(1).collect(),
                vec![
                    once((0, FragmentRef::from(&data.ts)))
                        .chain(
                            data.data
                                .iter()
                                .map(|(col_id, frag)| (*col_id, FragmentRef::from(frag))),
                        )
                        .collect::<HashMap<_, _>>(),
                ],
                vec![0],
            )
        } else {
            let mut fragments = Vec::with_capacity(required_partitions);

            let (_, ts_1, mut frag_1, mut ts_idx, offsets, _) = once(current_fragments)
                    .filter(|c| *c != 0)
                    // subtract one from the fragment count
                    // as we will deal with the remainder in a later step
                    .chain(repeat(empty_capacity).take(required_partitions.saturating_sub(1)))
                    .fold(
                        (
                            // fragments
                            &mut fragments,
                            // ts_data
                            data.ts.as_slice(),
                            // frag_data
                            data.data
                                .iter()
                                .map(|(col_id, frag)| (*col_id, FragmentRef::from(frag)))
                                .collect::<HashMap<_, _>>(),
                            // ts_index
                            vec![],
                            // offsets
                            vec![0],
                            // previous mid
                            // this holds split point from the previous iteration
                            // this value is used to offset sparse indexes
                            // as they are not recalculated after the split
                            0,
                        ),
                        |store, mid| {

                            let (
                                fragments,
                                ts_data,
                                frag_data,
                                mut ts_idx,
                                mut offsets,
                                previous_mid
                            ) = store;

                            let offset_mid = mid.saturating_add(previous_mid);

                            // when dealing with sparse blocks we don't know beforehand which
                            // partition a sparse entry will belong to
                            // but as our splitting algorithm calculates the capacity with an
                            // assumption that sparse blocks are in fact dense, the "overflow" of
                            // sparse data shouldn't happen

                            let (ts_0, ts_1) = ts_data.split_at(mid);
                            let (mut frag_0, frag_1) = frag_data.iter().fold(
                                (HashMap::new(), HashMap::new()),
                                |acc, (col_id, frag)| {
                                    let (mut hm_0, mut hm_1) = acc;

                                    if frag.is_sparse() {
                                        // this uses unsafe conversion usize -> u32
                                        // in reality we shouldn't ever use mid > u32::MAX
                                        // it's still worth to consider adding some check

                                        // add offset only for sparse columns
                                        // as sparse indexes are not recalculated after the split

                                        let (frag_0, frag_1) = frag
                                            .split_at_idx(offset_mid as SparseIndex)
                                            .with_context(|_| "unable to split sparse fragment")
                                            .unwrap();

                                        hm_0.insert(*col_id, frag_0);
                                        hm_1.insert(*col_id, frag_1);
                                    } else {
                                        let (frag_0, frag_1) = frag.split_at(mid);

                                        hm_0.insert(*col_id, frag_0);
                                        hm_1.insert(*col_id, frag_1);
                                    }

                                    (hm_0, hm_1)
                                },
                            );

                            // as we did split, ts_0 cannot be empty
                            ts_idx.push(
                                *(&ts_0[..]
                                    .first()
                                    .ok_or_else(|| err_msg("ts_0 was empty, this shouldn't happen"))
                                    .unwrap()),
                            );

                            frag_0.insert(0, FragmentRef::from(&ts_0[..]));
                            fragments.push(frag_0);

                            // this is the offset that is used to recalculate sparse index value
                            // for a given fragment split
                            // so we have to adjust it the same way we did for sparse fragment
                            // split, using previous_mid
                            offsets.push(offset_mid);

                            (fragments, ts_1, frag_1, ts_idx, offsets, offset_mid)
                        },
                    );

            // push the remainder, if any

            if !ts_1.is_empty() {
                ts_idx.push(*(&ts_1[..].first().unwrap()));
                frag_1.insert(0, FragmentRef::from(&ts_1[..]));
                fragments.push(frag_1);
            }

            (ts_idx, fragments, offsets)
        }
    }

    pub fn scan(&self, scan: &Scan) -> Result<ScanResult> {
        // only filters and projection for now
        // all partitions
        // full ts range

        let partitions = acquire!(read carry self.mutable_partitions);

        partitions
            .par_iter()
            .filter(|partition| if let Some(ref scan_partitions) = scan.partitions {
                // todo: benchmark Partition::get_id() -> &PartitionId
                scan_partitions.contains(&partition.get_id())
            } else {
                true
            })
            .map(|partition| {
                partition
                    .scan(&scan)
                    .with_context(|_| "partition scan failed")
                    .ok()
            })
            .reduce(
                || None,
                |a, b| if a.is_none() {
                    b
                } else if b.is_none() {
                    a
                } else {
                    let mut a = a.unwrap();
                    let b = b.unwrap();
                    a.merge(b).unwrap();
                    Some(a)
                },
            )
            .ok_or_else(|| err_msg("partition scan failed"))
    }

    pub(super) fn create_partition<'part, TS>(&self, ts: TS) -> Result<Partition<'part>>
    where
        TS: Into<Timestamp> + Clone + Copy,
    {
        let part_id = Partition::gen_id();

        let part_root = PartitionManager::new(&self.data_root, part_id, ts)
            .with_context(|_| "Unable to create partition data path")?;

        Partition::new(part_root, part_id, ts)
    }

    fn prepare_partitions<P, I>(root: P, ids: I) -> Result<PartitionMap<'pg>>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = PartitionMeta>,
    {
        ids.into_iter()
            .map(|part_meta| {
                let path = PartitionManager::new(&root, part_meta.id, part_meta.ts_min)
                    .with_context(|_| {
                        format!(
                            "Unable to obtain data directory for partition {}",
                            &*part_meta
                        )
                    })?;

                let partition = Partition::with_data(path)
                    .with_context(|_| format!("Unable to read partition {:?}", &*part_meta))?;

                Ok((part_meta, partition))
            })
            .collect()
    }

    fn prepare_mut_partitions<P, I>(root: P, ids: I) -> Result<VecDeque<Partition<'pg>>>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = PartitionMeta>,
    {
        ids.into_iter()
            .map(|part_meta| {
                let path = PartitionManager::new(&root, part_meta.id, part_meta.ts_min)
                    .with_context(|_| {
                        format!(
                            "Unable to obtain data directory for partition {}",
                            &*part_meta
                        )
                    })?;

                let partition = Partition::with_data(path)
                    .with_context(|_| format!("Unable to read partition {:?}", &*part_meta))?;

                Ok(partition)
            })
            .collect()
    }

    fn serialize<P: AsRef<Path>>(pg: &PartitionGroup<'pg>, meta: P) -> Result<()> {
        let meta = meta.as_ref();

        let im_partition_metas = Vec::from_iter(pg.immutable_partitions.keys());
        let mut_partition_metas = acquire!(write carry pg.mutable_partitions)
            .iter()
            .map(PartitionMeta::from)
            .collect::<Vec<_>>();

        let data = (pg, im_partition_metas, mut_partition_metas);

        serialize!(file meta, &data)
            .with_context(|_| "Failed to serialize partition group metadata")
            .map_err(|e| e.into())
    }

    fn deserialize<P: AsRef<Path>, R: AsRef<Path>>(
        meta: P,
        root: R,
    ) -> Result<PartitionGroup<'pg>> {
        let meta = meta.as_ref();

        if !meta.exists() {
            bail!("Cannot find partition group metadata {:?}", meta);
        }

        let (mut pg, im_partition_metas, mut_partition_metas): (
            PartitionGroup,
            Vec<PartitionMeta>,
            Vec<PartitionMeta>,
        ) = deserialize!(file meta)
            .with_context(|_| "Failed to read catalog metadata")?;

        pg.immutable_partitions = PartitionGroup::prepare_partitions(&root, im_partition_metas)
            .with_context(|_| "Failed to read immutable partitions data")?;

        pg.mutable_partitions = locked!(rw PartitionGroup::prepare_mut_partitions(
            &root, mut_partition_metas)
            .with_context(|_| "Failed to read mutable partitions data")?);

        pg.data_root = root.as_ref().to_path_buf();

        Ok(pg)
    }
}

impl<'pg> Drop for PartitionGroup<'pg> {
    fn drop(&mut self) {
        self.flush()
            .with_context(|_| "Failed to flush data during drop")
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastore::tests::create_random_partitions;

    #[test]
    fn new() {
        let root = tempdir!();
        let source_id = 5;

        let pg = PartitionGroup::new(&root, source_id)
            .with_context(|_| "Unable to create partition group")
            .unwrap();

        assert!(pg.immutable_partitions.is_empty());
        assert!(acquire!(read pg.mutable_partitions).is_empty());
        assert_eq!(pg.source_id, source_id);
        assert_eq!(pg.data_root.as_path(), root.as_ref());
    }

    #[test]
    fn with_data() {
        let root = tempdir!();
        let source_id = 5;
        let im_part_count = 8;
        let mut_part_count = 2;

        {
            let mut pg = PartitionGroup::new(&root, source_id)
                .with_context(|_| "Unable to create partition group")
                .unwrap();

            create_random_partitions(&mut pg, im_part_count, mut_part_count);
        }

        let pg = PartitionGroup::with_data(&root)
            .with_context(|_| "Failed to open partition group")
            .unwrap();

        assert!(pg.immutable_partitions.len() == im_part_count);
        assert!(acquire!(read pg.mutable_partitions).len() == mut_part_count);
        assert_eq!(pg.source_id, source_id);
        assert_eq!(pg.data_root.as_path(), root.as_ref());
    }

    mod split {
        use super::*;
        use ty::fragment::{Fragment, TimestampFragment};

        mod sparse {
            use super::*;

            fn split_test(
                curfrags: usize,
                reqparts: usize,
                emptycap: usize,
                count: usize,
                expected: Vec<HashMap<ColumnId, Fragment>>,
            ) {
                assert!(count >= 1);

                let data = Append {
                    ts: TimestampFragment::from(
                        (0..count)
                            .map(|v| Timestamp::from(v as u64))
                            .collect::<Vec<_>>(),
                    ),
                    source_id: 1,
                    data: hashmap! {
                        2 => Fragment::from({
                            let sparse_count = count / 2;
                            (
                                (0..sparse_count).map(|v| v as u8).collect::<Vec<u8>>(),
                                (0..sparse_count)
                                    .map(|idx| (idx * 2) as SparseIndex)
                                    .collect::<Vec<SparseIndex>>(),
                            )
                        }),
                        3 => Fragment::from({
                            let sparse_count = count / 3;
                            (
                                (0..sparse_count).map(|v| v as u32).collect::<Vec<u32>>(),
                                (0..sparse_count)
                                    .map(|idx| (idx * 3 + 1) as SparseIndex)
                                    .collect::<Vec<SparseIndex>>(),
                            )
                        }),
                    },
                };

                let (ts_idx, fragments, offsets) =
                    PartitionGroup::split_append(&data, curfrags, reqparts, emptycap);

                // asserts
                assert_eq!(expected.len(), ts_idx.len());
                assert_eq!(ts_idx.len(), fragments.len());
                assert_eq!(ts_idx.len(), offsets.len());

                for (expected, actual) in expected.iter().zip(fragments.iter()) {
                    assert_eq!(expected.len(), actual.len());
                }

                for ((expected_frag, actual_frag), offset) in
                    expected.iter().zip(&fragments).zip(&offsets)
                {
                    for (column, expected) in expected_frag.iter() {
                        let actual = actual_frag.get(column).expect("expected column not found");

                        assert_eq!(expected.len(), actual.len());

                        expected
                            .iter()
                            .zip(actual.iter())
                            .map(|((expidx, expval), (actidx, actval))| {
                                let actidx = if expected.is_sparse() {
                                    // offset the value
                                    actidx
                                        .checked_sub(*offset)
                                        .expect("offset overflow occured")
                                } else {
                                    actidx
                                };

                                ((expidx, expval), (actidx, actval))
                            })
                            .for_each(|(expected, actual)| assert_eq!(expected, actual));
                    }
                }
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_empty_current_nonfull() {
                let generated_length = 4;

                let curfrags = 8;
                let reqparts = 0;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3]),
                        2 => Fragment::from((
                            vec![0_u8, 1],
                            vec![0_u32, 2],
                        )),
                        3 => Fragment::from((
                            vec![0_u32],
                            vec![1_u32],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_empty_current_full() {
                let generated_length = 8;

                let curfrags = 8;
                let reqparts = 0;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3, 4, 5, 6, 7]),
                        2 => Fragment::from((
                            vec![0_u8, 1, 2, 3],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![0_u32, 1],
                            vec![1_u32, 4],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_nonfull_current_nonfull() {
                let generated_length = 2;

                let curfrags = 4;
                let reqparts = 0;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1]),
                        2 => Fragment::from((
                            vec![0_u8],
                            vec![0_u32],
                        )),
                        3 => Fragment::from((Vec::<u32>::new(), Vec::<SparseIndex>::new())),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_nonfull_current_full() {
                let generated_length = 4;

                let curfrags = 4;
                let reqparts = 0;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3]),
                        2 => Fragment::from((
                            vec![0_u8, 1],
                            vec![0_u32, 2],
                        )),
                        3 => Fragment::from((
                            vec![0_u32],
                            vec![1_u32],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            // |   8    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   9    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   10   | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   11   | 11 |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   1    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   2    | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   3    | 11 |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_empty_new_nonfull() {
                let generated_length = 12;

                let curfrags = 8;
                let reqparts = 1;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3, 4, 5, 6, 7]),
                        2 => Fragment::from((
                            vec![0_u8, 1, 2, 3],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![0_u32, 1, 2],
                            vec![1_u32, 4, 7],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![8_u64, 9, 10, 11]),
                        2 => Fragment::from((
                            vec![4_u8, 5],
                            vec![0_u32, 2],
                        )),
                        3 => Fragment::from((
                            vec![3_u32],
                            vec![2_u32],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            // |   8    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   9    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   10   | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   11   | 11 |          |           |
            // +--------+----+----------+-----------+
            // |   12   | 12 | 6        |           |
            // +--------+----+----------+-----------+
            // |   13   | 13 |          | 4         |
            // +--------+----+----------+-----------+
            // |   14   | 14 | 7        |           |
            // +--------+----+----------+-----------+
            // |   15   | 15 |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   1    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   2    | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   3    | 11 |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 12 | 6        |           |
            // +--------+----+----------+-----------+
            // |   5    | 13 |          | 4         |
            // +--------+----+----------+-----------+
            // |   6    | 14 | 7        |           |
            // +--------+----+----------+-----------+
            // |   7    | 15 |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_empty_new_full() {
                let generated_length = 16;

                let curfrags = 8;
                let reqparts = 1;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3, 4, 5, 6, 7]),
                        2 => Fragment::from((
                            vec![0_u8, 1, 2, 3],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![0_u32, 1, 2],
                            vec![1_u32, 4, 7],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![8_u64, 9, 10, 11, 12, 13, 14, 15]),
                        2 => Fragment::from((
                            vec![4_u8, 5, 6, 7],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![3_u32, 4],
                            vec![2_u32, 5],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   1    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   2    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   3    | 7  |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_nonfull_new_nonfull() {
                let generated_length = 8;

                let curfrags = 4;
                let reqparts = 1;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3]),
                        2 => Fragment::from((
                            vec![0_u8, 1],
                            vec![0_u32, 2],
                        )),
                        3 => Fragment::from((
                            vec![0_u32],
                             vec![1_u32],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![4_u64, 5, 6, 7]),
                        2 => Fragment::from((
                            vec![2_u8, 3],
                            vec![0_u32, 2],
                        )),
                        3 => Fragment::from((
                            vec![1_u32],
                            vec![0_u32],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            // |   8    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   9    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   10   | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   11   | 11 |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   1    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   2    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   3    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            // |   4    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   5    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   7    | 11 |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_nonfull_new_full() {
                let generated_length = 12;

                let curfrags = 4;
                let reqparts = 1;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3]),
                        2 => Fragment::from((
                            vec![0_u8, 1],
                            vec![0_u32, 2],
                        )),
                        3 => Fragment::from((
                            vec![0_u32],
                             vec![1_u32],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![4_u64, 5, 6, 7, 8, 9, 10, 11]),
                        2 => Fragment::from((
                            vec![2_u8, 3, 4, 5],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![1_u32, 2, 3],
                            vec![0_u32, 3, 6],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_full_new_nonfull() {
                let generated_length = 4;

                let curfrags = 0;
                let reqparts = 1;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3]),
                        2 => Fragment::from((
                            vec![0_u8, 1],
                            vec![0_u32, 2],
                        )),
                        3 => Fragment::from((
                            vec![0_u32],
                             vec![1_u32],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_full_new_full() {
                let generated_length = 8;

                let curfrags = 0;
                let reqparts = 1;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3, 4, 5, 6, 7]),
                        2 => Fragment::from((
                            vec![0_u8, 1, 2, 3],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![0_u32, 1],
                             vec![1_u32, 4],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            // |   8    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   9    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   10   | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   11   | 11 |          |           |
            // +--------+----+----------+-----------+
            // |   12   | 12 | 6        |           |
            // +--------+----+----------+-----------+
            // |   13   | 13 |          | 4         |
            // +--------+----+----------+-----------+
            // |   14   | 14 | 7        |           |
            // +--------+----+----------+-----------+
            // |   15   | 15 |          |           |
            // +--------+----+----------+-----------+
            // |   16   | 16 | 8        | 5         |
            // +--------+----+----------+-----------+
            // |   17   | 17 |          |           |
            // +--------+----+----------+-----------+
            // |   18   | 18 | 9        |           |
            // +--------+----+----------+-----------+
            // |   19   | 19 |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   1    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   2    | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   3    | 11 |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 12 | 6        |           |
            // +--------+----+----------+-----------+
            // |   5    | 13 |          | 4         |
            // +--------+----+----------+-----------+
            // |   6    | 14 | 7        |           |
            // +--------+----+----------+-----------+
            // |   7    | 15 |          |           |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 16 | 8        | 5         |
            // +--------+----+----------+-----------+
            // |   1    | 17 |          |           |
            // +--------+----+----------+-----------+
            // |   2    | 18 | 9        |           |
            // +--------+----+----------+-----------+
            // |   3    | 19 |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_empty_new_two_nonfull() {
                let generated_length = 20;

                let curfrags = 8;
                let reqparts = 2;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3, 4, 5, 6, 7]),
                        2 => Fragment::from((
                            vec![0_u8, 1, 2, 3],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![0_u32, 1, 2],
                             vec![1_u32, 4, 7],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![8_u64, 9, 10, 11, 12, 13, 14, 15]),
                        2 => Fragment::from((
                            vec![4_u8, 5, 6, 7],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![3_u32, 4],
                            vec![2_u32, 5],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![16_u64, 17, 18, 19]),
                        2 => Fragment::from((
                            vec![8_u8, 9],
                            vec![0_u32, 2],
                        )),
                        3 => Fragment::from((
                            vec![5_u32],
                            vec![0_u32],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            // |   8    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   9    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   10   | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   11   | 11 |          |           |
            // +--------+----+----------+-----------+
            // |   12   | 12 | 6        |           |
            // +--------+----+----------+-----------+
            // |   13   | 13 |          | 4         |
            // +--------+----+----------+-----------+
            // |   14   | 14 | 7        |           |
            // +--------+----+----------+-----------+
            // |   15   | 15 |          |           |
            // +--------+----+----------+-----------+
            // |   16   | 16 | 8        | 5         |
            // +--------+----+----------+-----------+
            // |   17   | 17 |          |           |
            // +--------+----+----------+-----------+
            // |   18   | 18 | 9        |           |
            // +--------+----+----------+-----------+
            // |   19   | 19 |          | 6         |
            // +--------+----+----------+-----------+
            // |   20   | 20 | 10       |           |
            // +--------+----+----------+-----------+
            // |   21   | 21 |          |           |
            // +--------+----+----------+-----------+
            // |   22   | 22 | 11       | 7         |
            // +--------+----+----------+-----------+
            // |   23   | 23 |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   1    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   2    | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   3    | 11 |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 12 | 6        |           |
            // +--------+----+----------+-----------+
            // |   5    | 13 |          | 4         |
            // +--------+----+----------+-----------+
            // |   6    | 14 | 7        |           |
            // +--------+----+----------+-----------+
            // |   7    | 15 |          |           |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 16 | 8        | 5         |
            // +--------+----+----------+-----------+
            // |   1    | 17 |          |           |
            // +--------+----+----------+-----------+
            // |   2    | 18 | 9        |           |
            // +--------+----+----------+-----------+
            // |   3    | 19 |          | 6         |
            // +--------+----+----------+-----------+
            // |   4    | 20 | 10       |           |
            // +--------+----+----------+-----------+
            // |   5    | 21 |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 22 | 11       | 7         |
            // +--------+----+----------+-----------+
            // |   7    | 23 |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_empty_new_two_full() {
                let generated_length = 24;

                let curfrags = 8;
                let reqparts = 2;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3, 4, 5, 6, 7]),
                        2 => Fragment::from((
                            vec![0_u8, 1, 2, 3],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![0_u32, 1, 2],
                             vec![1_u32, 4, 7],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![8_u64, 9, 10, 11, 12, 13, 14, 15]),
                        2 => Fragment::from((
                            vec![4_u8, 5, 6, 7],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![3_u32, 4],
                            vec![2_u32, 5],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![16_u64, 17, 18, 19, 20, 21, 22, 23]),
                        2 => Fragment::from((
                            vec![8_u8, 9, 10, 11],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![5_u32, 6, 7],
                            vec![0_u32, 3, 6],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            // |   8    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   9    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   10   | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   11   | 11 |          |           |
            // +--------+----+----------+-----------+
            // |   12   | 12 | 6        |           |
            // +--------+----+----------+-----------+
            // |   13   | 13 |          | 4         |
            // +--------+----+----------+-----------+
            // |   14   | 14 | 7        |           |
            // +--------+----+----------+-----------+
            // |   15   | 15 |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   1    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   2    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   3    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            // |   4    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   5    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   7    | 11 |          |           |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 12 | 6        |           |
            // +--------+----+----------+-----------+
            // |   1    | 13 |          | 4         |
            // +--------+----+----------+-----------+
            // |   2    | 14 | 7        |           |
            // +--------+----+----------+-----------+
            // |   3    | 15 |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_nonfull_new_two_nonfull() {
                let generated_length = 16;

                let curfrags = 4;
                let reqparts = 2;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3]),
                        2 => Fragment::from((
                            vec![0_u8, 1],
                            vec![0_u32, 2],
                        )),
                        3 => Fragment::from((
                            vec![0_u32],
                             vec![1_u32],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![4_u64, 5, 6, 7, 8, 9, 10, 11]),
                        2 => Fragment::from((
                            vec![2_u8, 3, 4, 5],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![1_u32, 2, 3],
                            vec![0_u32, 3, 6],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![12_u64, 13, 14, 15]),
                        2 => Fragment::from((
                            vec![6_u8, 7],
                            vec![0_u32, 2],
                        )),
                        3 => Fragment::from((
                            vec![4_u32],
                            vec![1_u32],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            // |   8    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   9    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   10   | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   11   | 11 |          |           |
            // +--------+----+----------+-----------+
            // |   12   | 12 | 6        |           |
            // +--------+----+----------+-----------+
            // |   13   | 13 |          | 4         |
            // +--------+----+----------+-----------+
            // |   14   | 14 | 7        |           |
            // +--------+----+----------+-----------+
            // |   15   | 15 |          |           |
            // +--------+----+----------+-----------+
            // |   16   | 16 | 8        | 5         |
            // +--------+----+----------+-----------+
            // |   17   | 17 |          |           |
            // +--------+----+----------+-----------+
            // |   18   | 18 | 9        |           |
            // +--------+----+----------+-----------+
            // |   19   | 19 |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   1    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   2    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   3    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            // |   4    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   5    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   7    | 11 |          |           |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 12 | 6        |           |
            // +--------+----+----------+-----------+
            // |   1    | 13 |          | 4         |
            // +--------+----+----------+-----------+
            // |   2    | 14 | 7        |           |
            // +--------+----+----------+-----------+
            // |   3    | 15 |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 16 | 8        | 5         |
            // +--------+----+----------+-----------+
            // |   5    | 17 |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 18 | 9        |           |
            // +--------+----+----------+-----------+
            // |   7    | 19 |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_nonfull_new_two_full() {
                let generated_length = 20;

                let curfrags = 4;
                let reqparts = 2;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3]),
                        2 => Fragment::from((
                            vec![0_u8, 1],
                            vec![0_u32, 2],
                        )),
                        3 => Fragment::from((
                            vec![0_u32],
                             vec![1_u32],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![4_u64, 5, 6, 7, 8, 9, 10, 11]),
                        2 => Fragment::from((
                            vec![2_u8, 3, 4, 5],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![1_u32, 2, 3],
                            vec![0_u32, 3, 6],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![12_u64, 13, 14, 15, 16, 17, 18, 19]),
                        2 => Fragment::from((
                            vec![6_u8, 7, 8, 9],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![4_u32, 5],
                            vec![1_u32, 4],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            // |   8    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   9    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   10   | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   11   | 11 |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   1    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   2    | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   3    | 11 |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_full_new_two_nonfull() {
                let generated_length = 12;

                let curfrags = 0;
                let reqparts = 2;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3, 4, 5, 6, 7]),
                        2 => Fragment::from((
                            vec![0_u8, 1, 2, 3],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![0_u32, 1, 2],
                             vec![1_u32, 4, 7],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![8_u64, 9, 10, 11]),
                        2 => Fragment::from((
                            vec![4_u8, 5],
                            vec![0_u32, 2],
                        )),
                        3 => Fragment::from((
                            vec![3_u32],
                            vec![2_u32],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }

            // # Input
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            // |   8    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   9    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   10   | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   11   | 11 |          |           |
            // +--------+----+----------+-----------+
            // |   12   | 12 | 6        |           |
            // +--------+----+----------+-----------+
            // |   13   | 13 |          | 4         |
            // +--------+----+----------+-----------+
            // |   14   | 14 | 7        |           |
            // +--------+----+----------+-----------+
            // |   15   | 15 |          |           |
            // +--------+----+----------+-----------+
            //
            // # Expected
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 0  | 0        |           |
            // +--------+----+----------+-----------+
            // |   1    | 1  |          | 0         |
            // +--------+----+----------+-----------+
            // |   2    | 2  | 1        |           |
            // +--------+----+----------+-----------+
            // |   3    | 3  |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 4  | 2        | 1         |
            // +--------+----+----------+-----------+
            // |   5    | 5  |          |           |
            // +--------+----+----------+-----------+
            // |   6    | 6  | 3        |           |
            // +--------+----+----------+-----------+
            // |   7    | 7  |          | 2         |
            // +--------+----+----------+-----------+
            //
            // +--------+----+----------+-----------+
            // | rowidx | ts | u8sparse | u32sparse |
            // +--------+----+----------+-----------+
            // |   0    | 8  | 4        |           |
            // +--------+----+----------+-----------+
            // |   1    | 9  |          |           |
            // +--------+----+----------+-----------+
            // |   2    | 10 | 5        | 3         |
            // +--------+----+----------+-----------+
            // |   3    | 11 |          |           |
            // +--------+----+----------+-----------+
            // |   4    | 12 | 6        |           |
            // +--------+----+----------+-----------+
            // |   5    | 13 |          | 4         |
            // +--------+----+----------+-----------+
            // |   6    | 14 | 7        |           |
            // +--------+----+----------+-----------+
            // |   7    | 15 |          |           |
            // +--------+----+----------+-----------+

            #[test]
            fn current_full_new_two_full() {
                let generated_length = 16;

                let curfrags = 0;
                let reqparts = 2;
                let emptycap = 8;

                let expected = vec![
                    hashmap! {
                        0 => Fragment::from(vec![0_u64, 1, 2, 3, 4, 5, 6, 7]),
                        2 => Fragment::from((
                            vec![0_u8, 1, 2, 3],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![0_u32, 1, 2],
                             vec![1_u32, 4, 7],
                        )),
                    },
                    hashmap! {
                        0 => Fragment::from(vec![8_u64, 9, 10, 11, 12, 13, 14, 15]),
                        2 => Fragment::from((
                            vec![4_u8, 5, 6, 7],
                            vec![0_u32, 2, 4, 6],
                        )),
                        3 => Fragment::from((
                            vec![3_u32, 4],
                            vec![2_u32, 5],
                        )),
                    },
                ];

                split_test(curfrags, reqparts, emptycap, generated_length, expected);
            }
        }
    }
}
