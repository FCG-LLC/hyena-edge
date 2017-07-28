use error::*;
use ty::{BlockType, Timestamp};
use partition::{Partition, PartitionId};
use storage::manager::{PartitionGroupManager, PartitionManager};
use std::collections::hash_map::HashMap;
use std::collections::vec_deque::VecDeque;
use std::path::{Path, PathBuf};
use std::iter::FromIterator;
use std::fmt::{Debug, Display};
use std::default::Default;
use std::hash::Hash;
use std::ops::Deref;
use serde::{Deserialize, Serialize};
use params::{SourceId, CATALOG_METADATA, PARTITION_GROUP_METADATA};


pub(crate) type PartitionMap<'part> = HashMap<PartitionMeta, Partition<'part>>;
pub(crate) type PartitionGroupMap<'pg> = HashMap<SourceId, PartitionGroup<'pg>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Catalog<'cat> {
    columns: Vec<Column>,

    groups: PartitionGroupMap<'cat>,

    #[serde(skip)]
    data_root: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PartitionGroup<'pg> {
    // TODO: we should consider changing this to something more universal
    // and less coupled with our specific schema perhaps
    source_id: SourceId,

    #[serde(skip)]
    immutable_partitions: PartitionMap<'pg>,

    #[serde(skip)]
    mutable_partitions: VecDeque<Partition<'pg>>,

    #[serde(skip)]
    data_root: PathBuf,
}

impl<'pg> PartitionGroup<'pg> {
    fn new<P: AsRef<Path>>(root: P, source_id: SourceId) -> Result<PartitionGroup<'pg>> {
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

    fn flush(&self) -> Result<()> {
        // TODO: add dirty flag
        let meta = self.data_root.join(PARTITION_GROUP_METADATA);

        PartitionGroup::serialize(self, &meta)
    }

    fn create_partition<'part, TS>(&self, ts: TS) -> Result<Partition<'part>>
    where
        TS: Into<Timestamp> + Clone + Copy,
    {
        let part_id = Partition::gen_id();

        let part_root = PartitionManager::new(&self.data_root, part_id, ts)
            .chain_err(|| "Unable to create partition data path")?;

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
                    .chain_err(|| {
                        format!(
                            "Unable to obtain data directory for partition {}",
                            &*part_meta
                        )
                    })?;

                let partition = Partition::with_data(path)
                    .chain_err(|| format!("Unable to read partition {:?}", &*part_meta))?;

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
                    .chain_err(|| {
                        format!(
                            "Unable to obtain data directory for partition {}",
                            &*part_meta
                        )
                    })?;

                let partition = Partition::with_data(path)
                    .chain_err(|| format!("Unable to read partition {:?}", &*part_meta))?;

                Ok(partition)
            })
            .collect()
    }

    fn serialize<P: AsRef<Path>>(pg: &PartitionGroup<'pg>, meta: P) -> Result<()> {
        let meta = meta.as_ref();

        let im_partition_metas = Vec::from_iter(pg.immutable_partitions.keys());
        let mut_partition_metas = pg.mutable_partitions
            .iter()
            .map(PartitionMeta::from)
            .collect::<Vec<_>>();

        let data = (pg, im_partition_metas, mut_partition_metas);

        serialize!(file meta, &data).chain_err(|| "Failed to serialize partition group metadata")
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
            .chain_err(|| "Failed to read catalog metadata")?;

        pg.immutable_partitions = PartitionGroup::prepare_partitions(&root, im_partition_metas)
            .chain_err(|| "Failed to read immutable partitions data")?;

        pg.mutable_partitions = PartitionGroup::prepare_mut_partitions(&root, mut_partition_metas)
            .chain_err(|| "Failed to read mutable partitions data")?;

        pg.data_root = root.as_ref().to_path_buf();

        Ok(pg)
    }
}

impl<'pg> Drop for PartitionGroup<'pg> {
    fn drop(&mut self) {
        self.flush()
            .chain_err(|| "Failed to flush data during drop")
            .unwrap();
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    ty: BlockType,
    name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Hash)]
pub(crate) struct PartitionMeta {
    id: PartitionId,

    ts_min: Timestamp,
    ts_max: Timestamp,
}

impl PartitionMeta {
    fn new<TS>(id: PartitionId, ts_min: TS, ts_max: TS) -> PartitionMeta
    where
        Timestamp: From<TS>,
    {
        PartitionMeta {
            id,
            ts_min: ts_min.into(),
            ts_max: ts_max.into(),
        }
    }
}

impl<'a, 'part> From<&'a Partition<'part>> for PartitionMeta {
    fn from(partition: &'a Partition<'part>) -> PartitionMeta {
        let (ts_min, ts_max) = partition.get_ts();

        PartitionMeta {
            id: partition.get_id(),
            ts_min,
            ts_max,
        }
    }
}

impl Deref for PartitionMeta {
    type Target = PartitionId;

    fn deref(&self) -> &Self::Target {
        &self.id
    }
}

impl<'cat> Catalog<'cat> {
    fn new<P: AsRef<Path>>(root: P) -> Result<Catalog<'cat>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(CATALOG_METADATA);

        if meta.exists() {
            bail!("Catalog metadata already exists {:?}", meta);
        }

        Ok(Catalog {
            columns: Default::default(),
            groups: Default::default(),
            data_root: root,
        })
    }

    pub fn with_data<P: AsRef<Path>>(root: P) -> Result<Catalog<'cat>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(CATALOG_METADATA);

        Catalog::deserialize(&meta, &root)
    }

    fn flush(&self) -> Result<()> {
        // TODO: add dirty flag
        let meta = self.data_root.join(CATALOG_METADATA);

        Catalog::serialize(self, &meta)
    }

    fn create_partition<'part, TS>(
        &mut self,
        source_id: SourceId,
        ts: TS,
    ) -> Result<Partition<'part>>
    where
        TS: Into<Timestamp> + Clone + Copy,
    {
        let pg = self.ensure_group(source_id)
            .chain_err(|| {
                format!("Unable to retrieve partition group for {}", source_id)
            })?;
        pg.create_partition(ts)
    }

    pub(crate) fn ensure_group(
        &mut self,
        source_id: SourceId,
    ) -> Result<&mut PartitionGroup<'cat>> {
        let root = self.data_root.clone();

        Ok(self.groups.entry(source_id).or_insert_with(|| {
            // this shouldn't fail in general

            let root = PartitionGroupManager::new(root, source_id)
                .chain_err(|| "Failed to create group manager")
                .unwrap();

            PartitionGroup::new(&root, source_id)
                .chain_err(|| "Unable to create partition group")
                .unwrap()
        }))
    }

    fn prepare_partition_groups<P, I>(root: P, ids: I) -> Result<PartitionGroupMap<'cat>>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = SourceId>,
    {
        ids.into_iter()
            .map(|source_id| {
                let path = PartitionGroupManager::new(&root, source_id).chain_err(|| {
                    format!(
                        "Unable to obtain data directory for partition group {}",
                        source_id
                    )
                })?;

                let partition_group = PartitionGroup::with_data(path)
                    .chain_err(|| format!("Unable to read partition group {:?}", source_id))?;

                Ok((source_id, partition_group))
            })
            .collect()
    }


    fn serialize<P: AsRef<Path>>(catalog: &Catalog<'cat>, meta: P) -> Result<()> {
        let meta = meta.as_ref();

        let group_metas = Vec::from_iter(catalog.groups.keys());

        let data = (catalog, group_metas);

        serialize!(file meta, &data).chain_err(|| "Failed to serialize catalog metadata")
    }

    fn deserialize<P: AsRef<Path>, R: AsRef<Path>>(meta: P, root: R) -> Result<Catalog<'cat>> {
        let meta = meta.as_ref();

        if !meta.exists() {
            bail!("Cannot find catalog metadata {:?}", meta);
        }

        let (mut catalog, group_metas): (Catalog, Vec<SourceId>) = deserialize!(file meta)
            .chain_err(|| "Failed to read catalog metadata")?;

        catalog.groups = Catalog::prepare_partition_groups(&root, group_metas)
            .chain_err(|| "Failed to read partition data")?;

        catalog.data_root = root.as_ref().to_path_buf();

        Ok(catalog)
    }
}

impl<'cat> Drop for Catalog<'cat> {
    fn drop(&mut self) {
        self.flush()
            .chain_err(|| "Failed to flush data during drop")
            .unwrap();
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use helpers::random::timestamp::{RandomTimestamp, RandomTimestampGen};

    fn create_random_partitions(pg: &mut PartitionGroup, im_count: usize, mut_count: usize) {
        let pts = RandomTimestampGen::pairs::<u64>(im_count + mut_count);

        let (imparts, mutparts): (Vec<_>, Vec<_>) = pts.iter()
            .map(|&(ref lo, ref hi)| {
                let mut part = pg.create_partition(*lo)
                    .chain_err(|| "Unable to create partition")
                    .unwrap();

                part.set_ts(None, Some(*hi))
                    .chain_err(|| "Failed to set timestamp on partition")
                    .unwrap();

                part
            })
            .enumerate()
            .partition(|&(idx, _)| idx < im_count);

        pg.immutable_partitions = imparts
            .into_iter()
            .map(|(_, part)| (PartitionMeta::from(&part), part))
            .collect();
        pg.mutable_partitions = mutparts.into_iter().map(|(_, part)| part).collect();
    }

    mod partition_meta {
        use super::*;

        #[test]
        fn deref() {
            let id = Partition::gen_id();
            let ts_min = RandomTimestampGen::random::<u64>();
            let ts_max = RandomTimestampGen::random_from(ts_min);

            let pm = PartitionMeta::new(id, ts_min, ts_max);

            assert_eq!(*pm, id);
        }
    }

    mod partition_group {
        use super::*;

        #[test]
        fn new() {
            let root = tempdir!();
            let source_id = 5;

            let pg = PartitionGroup::new(&root, source_id)
                .chain_err(|| "Unable to create partition group")
                .unwrap();

            assert!(pg.immutable_partitions.is_empty());
            assert!(pg.mutable_partitions.is_empty());
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
                    .chain_err(|| "Unable to create partition group")
                    .unwrap();

                create_random_partitions(&mut pg, im_part_count, mut_part_count);
            }

            let pg = PartitionGroup::with_data(&root)
                .chain_err(|| "Failed to open partition group")
                .unwrap();

            assert!(pg.immutable_partitions.len() == im_part_count);
            assert!(pg.mutable_partitions.len() == mut_part_count);
            assert_eq!(pg.source_id, source_id);
            assert_eq!(pg.data_root.as_path(), root.as_ref());
        }
    }

    mod catalog {
        use super::*;

        #[test]
        fn new() {
            let source_ids = [1, 5, 7];
            let im_part_count = 8;
            let mut_part_count = 2;

            let root = tempdir!();

            let mut cat = Catalog::new(&root)
                .chain_err(|| "Unable to create catalog")
                .unwrap();

            for source_id in &source_ids {

                let pg = cat.ensure_group(*source_id)
                    .chain_err(|| "Unable to retrieve partition group")
                    .unwrap();

                create_random_partitions(pg, im_part_count, mut_part_count);
            }
        }
    }
}
