use error::*;
use ty::{BlockType, ColumnId, Timestamp};
use partition::{Partition, PartitionId};
use storage::manager::{PartitionGroupManager, PartitionManager};
use std::collections::hash_map::HashMap;
use std::collections::vec_deque::VecDeque;
use std::path::{Path, PathBuf};
use std::iter::FromIterator;
use std::fmt::{Debug, Display, Error as FmtError, Formatter};
use std::default::Default;
use std::hash::Hash;
use std::ops::Deref;
use std::result::Result as StdResult;
use std::sync::{RwLock, RwLockWriteGuard};
use serde::{Deserialize, Serialize};
use params::{SourceId, CATALOG_METADATA, PARTITION_GROUP_METADATA};
use mutator::append::Append;
use ty::block::{BlockTypeMap, BlockTypeMapTy};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator,
                  ParallelIterator};


pub(crate) type PartitionMap<'part> = HashMap<PartitionMeta, Partition<'part>>;
pub(crate) type PartitionGroupMap<'pg> = HashMap<SourceId, PartitionGroup<'pg>>;
pub type ColumnMap = HashMap<ColumnId, Column>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Catalog<'cat> {
    columns: ColumnMap,

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
    mutable_partitions: RwLock<VecDeque<Partition<'pg>>>,

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

    fn append(&self, catalog: &Catalog, data: &Append) -> Result<usize> {
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
            .chain_err(|| "failed to prepare all columns")?
            .into();

        let fragcount = data.len();

        let (emptycap, currentcap) = {
            let mut partitions = acquire!(read carry self.mutable_partitions);
            // current partition capacity
            let curpart = partitions
                .back()
                .ok_or_else(|| "Mutable partitions pool is empty")?;

            // empty partition capacity for this append
            let emptycap = catalog.space_for_blocks(&colindices);
            let currentcap = curpart.space_for_blocks(&colindices);

            (emptycap, currentcap)
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

        let (mut fragments, ts_1, mut frag_1, mut ts_idx) = once(curfrags)
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
                ),
                |store, mid| {

                    let (mut fragments, ts_data, frag_data, mut ts_idx) = store;

                    let (ts_0, ts_1) = ts_data.split_at(mid);
                    let (mut frag_0, frag_1) = frag_data.iter().fold(
                        (HashMap::new(), HashMap::new()),
                        |acc, (col_id, frag)| {
                            let (mut hm_0, mut hm_1) = acc;
                            let (frag_0, frag_1) = frag.split_at(mid);

                            hm_0.insert(*col_id, frag_0);
                            hm_1.insert(*col_id, frag_1);

                            (hm_0, hm_1)
                        },
                    );

                    // as we did split, ts_0 cannot be empty
                    ts_idx.push(
                        *(&ts_0[..]
                            .first()
                            .ok_or_else(|| "ts_0 was empty, this shouldn't happen")
                            .unwrap()),
                    );
                    frag_0.insert(0, FragmentRef::from(&ts_0[..]));
                    fragments.push(frag_0);

                    (fragments, ts_1, frag_1, ts_idx)
                },
            );

        if !ts_1.is_empty() {
            ts_idx.push(*(&ts_1[..].first().unwrap()));
            frag_1.insert(0, FragmentRef::from(&ts_1[..]));
            fragments.push(frag_1);
        }

        // create partition pool

        let mut partitions = acquire!(write carry self.mutable_partitions);

        let curidx = partitions.len();

        let newparts = ts_idx
            .iter()
            .skip(if current_is_full { 0 } else { 1 })
            .map(|ts| self.create_partition(**ts))
            .collect::<Result<Vec<_>>>()
            .chain_err(|| "Unable to create partition for writing")?;

        partitions.extend(newparts);

        // write data

        for (mut partition, fragment) in partitions
            .iter_mut()
            .skip(curidx - if current_is_full { 0 } else { 1 })
            .zip(fragments.iter())
        {
            partition
                .append(&typemap, &fragment)
                .chain_err(|| "partition append failed")
                .unwrap();
        }

        Ok(0)
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
        let mut_partition_metas = acquire!(write carry pg.mutable_partitions)
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

        pg.mutable_partitions = locked!(rw PartitionGroup::prepare_mut_partitions(
            &root, mut_partition_metas)
            .chain_err(|| "Failed to read mutable partitions data")?);

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

impl Column {
    pub fn new(ty: BlockType, name: &str) -> Column {
        Column {
            ty,
            name: name.to_owned(),
        }
    }
}

impl Deref for Column {
    type Target = BlockType;

    fn deref(&self) -> &Self::Target {
        &self.ty
    }
}

impl Display for Column {
    fn fmt(&self, fmt: &mut Formatter) -> StdResult<(), FmtError> {
        write!(fmt, "{}", self.name)
    }
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

    pub fn append(&self, data: &Append) -> Result<usize> {
        if data.is_empty() {
            bail!("Provided Append contains no data");
        }

        // dispatch to proper PartitionGroup
        if let Some(pg) = self.groups.get(&data.source_id) {
            pg.append(&self, &data)
        } else {
            bail!("No PartitionGroup found for source_id = {}", data.source_id);
        }
    }

    fn flush(&self) -> Result<()> {
        // TODO: add dirty flag
        let meta = self.data_root.join(CATALOG_METADATA);

        Catalog::serialize(self, &meta)
    }

    pub fn ensure_columns(&mut self, type_map: ColumnMap) -> Result<()> {
        self.columns.extend(type_map);

        Ok(())
    }

    /// Calculate an empty partition's capacity for given column set
    fn space_for_blocks(&self, indices: &[usize]) -> usize {
        use params::BLOCK_SIZE;

        indices.iter()
            .filter_map(|col_id| {
                if let Some(column) = self.columns.get(col_id) {
                    Some(BLOCK_SIZE / column.size_of())
                } else {
                    None
                }
            })
            .min()
            // the default shouldn't ever happen, as there always should be ts block
            // but in case it happens, this will return 0
            // which in turn will cause new partition to be used
            .unwrap_or_default()
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

impl<'cat> AsRef<ColumnMap> for Catalog<'cat> {
    fn as_ref(&self) -> &ColumnMap {
        &self.columns
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
        pg.mutable_partitions = locked!(rw mutparts.into_iter().map(|(_, part)| part).collect());
    }

    mod append {
        use super::*;
        use params::BLOCK_SIZE;
        use std::mem::size_of;

        macro_rules! append_test_impl {
            (init append $record_count: expr) => {{
                let mut init = append_test_impl!(init);
                append_test_impl!(append init.0, init.1, init.2, $record_count)
            }};

            (init $columns: expr, $ts_min: expr) => {{
                let ts_min = $ts_min;

                let source_ids = [1, 5, 7];

                let root = tempdir!(persistent);

                let mut cat = Catalog::new(&root)
                    .chain_err(|| "Unable to create catalog")
                    .unwrap();

                let columns = $columns;

                cat.ensure_columns(
                    columns.into(),
                ).unwrap();

                for source_id in &source_ids {

                    let pg = cat.ensure_group(*source_id)
                        .chain_err(|| "Unable to retrieve partition group")
                        .unwrap();

                    let mut part = pg.create_partition(ts_min)
                        .chain_err(|| "Unable to create partition")
                        .unwrap();

                    let mut vp = VecDeque::new();
                    vp.push_front(part);

                    pg.mutable_partitions = locked!(rw vp);
                }

                (root, cat, ts_min)
            }};

            (init $columns: expr) => {{
                use chrono::prelude::*;

                let ts_min: Timestamp = RandomTimestampGen::random_from(Utc::now().naive_local());
                append_test_impl!(init $columns, ts_min)
            }};

            (init) => {{
                use ty::block::mmap::BlockType as BlockTy;

                let columns = hashmap! {
                    0 => Column::new(BlockTy::U64Dense.into(), "ts"),
                    1 => Column::new(BlockTy::U32Dense.into(), "source"),
                    2 => Column::new(BlockTy::U8Dense.into(), "col1"),
                    3 => Column::new(BlockTy::U32Dense.into(), "col2"),
                };

                append_test_impl!(init columns)
            }};

            (append $root:expr,
                    $cat: expr,
                    $ts_min: expr,
                    $record_count: expr) => {
                append_test_impl!(rand append $root, $cat, $ts_min, $record_count,
                    2 => u8,
                    3 => u32);
            };

            (gen append $root:expr,
                         $cat: expr,
                         $ts: expr,
                         $data: expr) => {{
                let root = &$root;
                let mut cat = &mut $cat;
                let ts = $ts;
                let data = $data;

                // append
                let append = Append {
                    ts,
                    source_id: 1,
                    data,
                };

                cat.append(&append).expect("unable to append fragment");
            }};

            (rand append $root:expr,
                         $cat: expr,
                         $ts_min: expr,
                         $record_count: expr,
                         $( $idx: expr => $ty: ty ),+ $(,)*) => {{
                let ts_min = $ts_min;
                let record_count = $record_count;

                let ts = RandomTimestampGen::iter_range_from(ts_min)
                        .take(record_count)
                        .collect::<Vec<Timestamp>>()
                        .into();

                let data = hashmap! {
                    $(
                    $idx => random!(gen $ty, record_count).into(),
                    )+
                };

                append_test_impl!(gen append $root, $cat, ts, data)
            }};

            (seq append $root:expr,
                        $cat: expr,
                        $ts_min: expr,
                        $record_count: expr,
                        $start: expr,
                        $( $idx: expr => $ty: ty ),+ $(,)*) => {{

                let ts_min = $ts_min;
                let record_count = $record_count;

                let mut v = vec![Timestamp::from(0); record_count];

                let ts = seqfill!(Timestamp, &mut v[..], ts_min);
                let ts = v.into();

                let data = hashmap! {
                    $(
                    $idx => seqfill!(vec $ty, record_count, $start).into(),
                    )+
                };

                append_test_impl!(gen append $root, $cat, ts, data)
            }};
        }

        #[cfg(all(feature = "nightly", test))]
        mod benches {
            use test::Bencher;
            use super::*;

            #[bench]
            fn tiny(b: &mut Bencher) {
                use ty::block::mmap::BlockType as BlockTy;

                let record_count = 1;

                let max_records = BLOCK_SIZE / size_of::<Timestamp>();

                let columns = hashmap! {
                    0 => Column::new(BlockTy::U64Dense.into(), "ts"),
                    1 => Column::new(BlockTy::U32Dense.into(), "source"),
                    2 => Column::new(BlockTy::U8Dense.into(), "col1"),
                    3 => Column::new(BlockTy::U32Dense.into(), "col2"),
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

                let mut cat = init.1;

                b.iter(|| cat.append(&append).expect("unable to append fragment"));
            }

            #[bench]
            fn small(b: &mut Bencher) {
                use ty::block::mmap::BlockType as BlockTy;

                let record_count = 100;

                let max_records = BLOCK_SIZE / size_of::<Timestamp>();

                let columns = hashmap! {
                    0 => Column::new(BlockTy::U64Dense.into(), "ts"),
                    1 => Column::new(BlockTy::U32Dense.into(), "source"),
                    2 => Column::new(BlockTy::U8Dense.into(), "col1"),
                    3 => Column::new(BlockTy::U32Dense.into(), "col2"),
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

                let mut cat = init.1;

                b.iter(|| cat.append(&append).expect("unable to append fragment"));
            }

            #[bench]
            fn lots_columns(b: &mut Bencher) {
                use ty::block::mmap::BlockType as BlockTy;

                let record_count = 100;
                let column_count = 10000;

                let max_records = BLOCK_SIZE / size_of::<Timestamp>();

                let mut columns = hashmap! {
                    0 => Column::new(BlockTy::U64Dense.into(), "ts"),
                    1 => Column::new(BlockTy::U32Dense.into(), "source"),
                };

                let mut data = hashmap!{};

                for idx in 2..column_count {
                    columns.insert(
                        idx,
                        Column::new(BlockTy::U32Dense.into(), &format!("col{}", idx)),
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

                let mut cat = init.1;

                b.iter(|| cat.append(&append).expect("unable to append fragment"));
            }

            #[bench]
            fn big_data(b: &mut Bencher) {
                use ty::block::mmap::BlockType as BlockTy;

                let max_records = BLOCK_SIZE / size_of::<Timestamp>();
                let record_count = max_records;

                let columns = hashmap! {
                    0 => Column::new(BlockTy::U64Dense.into(), "ts"),
                    1 => Column::new(BlockTy::U32Dense.into(), "source"),
                    2 => Column::new(BlockTy::U8Dense.into(), "col1"),
                    3 => Column::new(BlockTy::U32Dense.into(), "col2"),
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

                let mut cat = init.1;

                b.iter(|| cat.append(&append).expect("unable to append fragment"));
            }
        }

        #[test]
        fn current_only() {
            append_test_impl!(init append 100);
        }

        #[test]
        fn current_almost_full() {
            let max_records = BLOCK_SIZE / size_of::<Timestamp>();

            append_test_impl!(init append max_records - 1);
        }

        #[test]
        fn current_full() {
            let max_records = BLOCK_SIZE / size_of::<Timestamp>();

            append_test_impl!(init append max_records);
        }

        #[test]
        fn two() {
            let max_records = BLOCK_SIZE / size_of::<Timestamp>();

            append_test_impl!(init append max_records + 100);
        }

        #[test]
        fn two_full() {
            let max_records = BLOCK_SIZE / size_of::<Timestamp>();

            append_test_impl!(init append max_records * 2);
        }

        #[test]
        fn consecutive_small() {
            let mut init = append_test_impl!(init);

            append_test_impl!(append init.0, init.1, init.2, 100);
            append_test_impl!(append init.0, init.1, init.2, 100);
        }

        #[test]
        fn consecutive_two() {
            let max_records = BLOCK_SIZE / size_of::<Timestamp>();
            let mut init = append_test_impl!(init);

            append_test_impl!(append init.0, init.1, init.2, max_records + 100);
            append_test_impl!(append init.0, init.1, init.2, 100);
        }

        #[test]
        fn consecutive_two_full() {
            let max_records = BLOCK_SIZE / size_of::<Timestamp>();
            let mut init = append_test_impl!(init);

            append_test_impl!(append init.0, init.1, init.2, max_records);
            append_test_impl!(append init.0, init.1, init.2, max_records);
        }

        #[test]
        fn consecutive_two_overflow() {
            let max_records = BLOCK_SIZE / size_of::<Timestamp>();
            let mut init = append_test_impl!(init);

            append_test_impl!(append init.0, init.1, init.2, max_records + 100);
            append_test_impl!(append init.0, init.1, init.2, max_records + 100);
        }

        #[test]
        fn lots_columns() {
            use ty::block::mmap::BlockType as BlockTy;

            let record_count = 100;
            let column_count = 10000;

            let max_records = BLOCK_SIZE / size_of::<Timestamp>();

            let mut columns = hashmap! {
                0 => Column::new(BlockTy::U64Dense.into(), "ts"),
                1 => Column::new(BlockTy::U32Dense.into(), "source"),
            };

            let mut data = hashmap!{};

            for idx in 2..column_count {
                columns.insert(
                    idx,
                    Column::new(BlockTy::U32Dense.into(), &format!("col{}", idx)),
                );
                data.insert(idx, random!(gen u32, record_count).into());
            }

            let mut init = append_test_impl!(init columns);

            let ts = RandomTimestampGen::iter_range_from(init.2)
                .take(record_count)
                .collect::<Vec<Timestamp>>()
                .into();

            append_test_impl!(gen append init.0, init.1, ts, data);
        }

        mod seq {
            use super::*;

            #[test]
            fn current_only() {
                use ty::block::mmap::BlockType as BlockTy;
                let now = <Timestamp as Default>::default();

                let columns = hashmap! {
                    0 => Column::new(BlockTy::U64Dense.into(), "ts"),
                    1 => Column::new(BlockTy::U32Dense.into(), "source"),
                    2 => Column::new(BlockTy::U8Dense.into(), "col1"),
                    3 => Column::new(BlockTy::U32Dense.into(), "col2"),
                };

                let mut init = append_test_impl!(init columns, now);

                append_test_impl!(seq append
                    init.0,
                    init.1,
                    init.2,
                    100,
                    now,
                    2 => u8,
                    3 => u32,
                );
            }
        }
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
                    .chain_err(|| "Unable to create partition group")
                    .unwrap();

                create_random_partitions(&mut pg, im_part_count, mut_part_count);
            }

            let pg = PartitionGroup::with_data(&root)
                .chain_err(|| "Failed to open partition group")
                .unwrap();

            assert!(pg.immutable_partitions.len() == im_part_count);
            assert!(acquire!(read pg.mutable_partitions).len() == mut_part_count);
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
