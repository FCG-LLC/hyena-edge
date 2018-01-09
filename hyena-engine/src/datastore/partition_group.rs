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

use super::partition::{Partition, PartitionId};
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

    pub(super) fn append(&self, catalog: &Catalog, data: &Append) -> Result<usize> {
        use std::iter::{once, repeat};
        use ty::fragment::FragmentRef;

        // calculate the size of the append (in records)

        let mut colindices = vec![0, 1]; // ts and source_id
        colindices.extend(data.data.keys());

        let columns = catalog.as_ref();

        let typemap: BlockTypeMap = colindices
            .iter()
            .map(|colidx| if let Some(col) = columns.get(colidx) {
                Ok((*colidx, **col))
            } else {
                bail!("column {} not found", colidx)
            })
            .collect::<Result<BlockTypeMapTy>>()
            .with_context(|_| err_msg("failed to prepare all columns"))?
            .into();

        let fragcount = data.len();

        let (emptycap, currentcap) = {
            let partitions = acquire!(read carry self.mutable_partitions);
            // current partition capacity
            let curpart = partitions
                .back()
                .ok_or_else(|| err_msg("Mutable partitions pool is empty"))?;

            // empty partition capacity for this append
            let emptycap = catalog.space_for_blocks(&colindices);
            let currentcap = curpart.space_for_blocks(&colindices);

            (
                emptycap,
                // check if current partition exceeded its capacity or is simply uninitialized
                if currentcap == 0 && curpart.is_empty() {
                    emptycap
                } else {
                    currentcap
                },
            )
        };

        // check if we can fit the data within current partition
        // or additional ones are needed

        let (curfrags, reqparts, current_is_full) = if fragcount > currentcap {
            let emptyfrags = fragcount - currentcap;
            let lastfull = emptyfrags % emptycap == 0;

            (
                currentcap,
                (emptyfrags / emptycap + if lastfull { 0 } else { 1 }) - 1,
                lastfull,
            )
        } else {
            (fragcount, 0, fragcount == currentcap)
        };

        // prepare source slices

        let mut fragments = Vec::with_capacity(reqparts + 1);

        let (fragments, ts_1, mut frag_1, mut ts_idx, offsets) = once(curfrags)
            .filter(|c| *c != 0)
            .chain(repeat(emptycap).take(reqparts))
            .fold(
                (
                    &mut fragments,
                    data.ts.as_slice(),
                    data.data
                        .iter()
                        .map(|(col_id, frag)| (*col_id, FragmentRef::from(frag)))
                        .collect::<HashMap<_, _>>(),
                    vec![],
                    vec![0],
                ),
                |store, mid| {

                    let (fragments, ts_data, frag_data, mut ts_idx, mut offsets) = store;

                    // when dealing with sparse blocks we don't know beforehand which
                    // partition a sparse entry will belong to
                    // but as our splitting algorithm calculates the capacity with an assumption
                    // that sparse blocks are in fact dense, the "overflow" of sparse data
                    // shouldn't happen

                    let (ts_0, ts_1) = ts_data.split_at(mid);
                    let (mut frag_0, frag_1) = frag_data.iter().fold(
                        (HashMap::new(), HashMap::new()),
                        |acc, (col_id, frag)| {
                            let (mut hm_0, mut hm_1) = acc;

                            if frag.is_sparse() {
                                // this uses unsafe conversion usize -> u32
                                // in reality we shouldn't ever use mid > u32::MAX
                                // it's still worth to consider adding some check
                                let (frag_0, frag_1) = frag.split_at_idx(mid as SparseIndex)
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
                    offsets.push(mid);

                    (fragments, ts_1, frag_1, ts_idx, offsets)
                },
            );

        if !ts_1.is_empty() {
            ts_idx.push(*(&ts_1[..].first().unwrap()));
            frag_1.insert(0, FragmentRef::from(&ts_1[..]));
            fragments.push(frag_1);
        }

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
            .skip(curidx - if current_is_full { 0 } else { 1 })
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
    use storage::manager::RootManager;
    use hyena_test::random::timestamp::RandomTimestampGen;
    use params::BLOCK_SIZE;
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
}
