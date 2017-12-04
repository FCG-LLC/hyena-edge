use error::*;
use ty::{BlockType as TyBlockType, ColumnId, Timestamp};
use block::{BlockType, SparseIndex};
use partition::{Partition, PartitionId};
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
use ty::timestamp::MIN_TIMESTAMP;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

pub(crate) type PartitionMap<'part> = HashMap<PartitionMeta, Partition<'part>>;
pub(crate) type PartitionGroupMap<'pg> = HashMap<SourceId, PartitionGroup<'pg>>;
pub type ColumnMap = HashMap<ColumnId, Column>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Catalog<'cat> {
    pub(crate) columns: ColumnMap,

    pub(crate) groups: PartitionGroupMap<'cat>,

    #[serde(skip)]
    pub(crate) data_root: PathBuf,
}

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

        for p in self.immutable_partitions.values() {
            p.flush()?
        }

        for p in self.mutable_partitions.read().unwrap().iter() {
            p.flush()?
        }

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
            let partitions = acquire!(read carry self.mutable_partitions);
            // current partition capacity
            let curpart = partitions
                .back()
                .ok_or_else(|| "Mutable partitions pool is empty")?;

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
                                    .chain_err(|| "unable to split sparse fragment")
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
                            .ok_or_else(|| "ts_0 was empty, this shouldn't happen")
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
            .chain_err(|| "Unable to create partition for writing")?;

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
                .chain_err(|| "partition append failed")
                .unwrap();
        }

        Ok(0)
    }

    pub fn scan(&self, scan: &Scan) -> Result<ScanResult> {
        // only filters and projection for now
        // all partitions
        // full ts range

        let partitions = acquire!(read carry self.mutable_partitions);

        partitions
            .par_iter()
            .map(|partition| {
                partition
                    .scan(&scan)
                    .chain_err(|| "partition scan failed")
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
            .ok_or_else(|| "partition scan failed".into())
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
    pub(crate) ty: TyBlockType,
    pub(crate) name: String,
}

impl Column {
    pub fn new(ty: TyBlockType, name: &str) -> Column {
        Column {
            ty,
            name: name.to_owned(),
        }
    }
}

impl Deref for Column {
    type Target = TyBlockType;

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
    #[allow(unused)]
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
    pub(crate) fn new<P: AsRef<Path>>(root: P) -> Result<Catalog<'cat>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(CATALOG_METADATA);

        if meta.exists() {
            bail!("Catalog metadata already exists {:?}", meta);
        }

        let mut catalog = Catalog {
            columns: Default::default(),
            groups: Default::default(),
            data_root: root,
        };

        catalog.ensure_default_columns()?;

        Ok(catalog)
    }

    fn ensure_default_columns(&mut self) -> Result<()> {
        let ts_column = Column::new(TyBlockType::Memory(BlockType::U64Dense), "timestamp");
        let source_column = Column::new(TyBlockType::Memory(BlockType::I32Dense), "source_id"); // will be String some day
        let mut map = HashMap::new();
        map.insert(0, ts_column);
        map.insert(1, source_column);

        self.ensure_columns(map)
    }

    pub fn with_data<P: AsRef<Path>>(root: P) -> Result<Catalog<'cat>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(CATALOG_METADATA);

        Catalog::deserialize(&meta, &root)
    }

    pub fn open_or_create<P: AsRef<Path> + Clone>(root: P) -> Catalog<'cat> {
        match Catalog::with_data(root.clone()) {
            Ok(catalog) => catalog,
            Err(e) => {
                debug!("Can't open data_dir: {:?}", e);
                Catalog::new(root).unwrap()
            }
        }
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

    pub fn scan(&self, scan: &Scan) -> Result<ScanResult> {
        let res = self.groups
            .par_iter()
            .filter(|&(pgid, _)| *pgid == 1)
            .map(|(_, pg)| pg.scan(&scan))
            .collect::<Result<Vec<ScanResult>>>();

        res.map(|mut v| v.pop().unwrap())
    }

    pub fn flush(&self) -> Result<()> {
        // TODO: add dirty flag
        let meta = self.data_root.join(CATALOG_METADATA);

        for pg in self.groups.values() {
            pg.flush()?
        }

        Catalog::serialize(self, &meta)
    }

    pub fn ensure_columns(&mut self, type_map: ColumnMap) -> Result<()> {
        self.columns.extend(type_map);

        Ok(())
    }

    // Adds the column to the catalog. It verifies that catalog does not already contain:
    // a) column with the given id, or
    // b) column with the given name.
    // This function takes all-or-nothing approach: either all columns can are added, or none gets added.
    pub fn add_columns(&mut self, column_map: ColumnMap) -> Result<()> {
        for (id, column) in column_map.iter() {
            info!("Adding column {}:{:?} with id {}", column.name, column.ty, id);
            if self.columns.contains_key(id) {
                bail!(ErrorKind::ColumnIdAlreadyExists(*id));
            }
            if self.columns.values().any(|col| col.name == column.name) {
                bail!(ErrorKind::ColumnNameAlreadyExists(column.name.clone()));
            }
        }

        Ok(self.columns.extend(column_map))
    }

    pub fn next_id(&self) -> usize {
        let default = 0;
        *self.columns.keys().max().unwrap_or(&default) + 1
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

    #[allow(unused)]
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

    #[allow(unused)]
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

            let mut pg = PartitionGroup::new(&root, source_id)
                .chain_err(|| "Unable to create partition group")
                .unwrap();
            Catalog::create_single_partition(&mut pg);
            pg.flush().unwrap();
            pg
        }))
    }

    fn create_single_partition(pg: &mut PartitionGroup) {
        let part = pg.create_partition(MIN_TIMESTAMP)
            .chain_err(|| "Unable to create partition")
            .unwrap();

        let mut vp = VecDeque::new();
        vp.push_front(part);

        pg.mutable_partitions = locked!(rw vp);
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
    use storage::manager::RootManager;
    use helpers::random::timestamp::RandomTimestampGen;
    use params::BLOCK_SIZE;

    // until const fn stabilizes we have to use this hack
    // see https://github.com/rust-lang/rust/issues/24111

    // make sure that size_of::<Timestamp>() == 8
    assert_eq_size!(timestamp_size_check; u64, Timestamp);

    const TIMESTAMP_SIZE: usize = 8; // should be `size_of::<Timestamp>()`
    const MAX_RECORDS: usize = BLOCK_SIZE / TIMESTAMP_SIZE;

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

    #[macro_use]
    mod append {
        use super::*;
        use ty::fragment::Fragment;

        macro_rules! append_test_impl {
            (init $columns: expr) => {
                append_test_impl!(init $columns, <Timestamp as Default>::default())

            };

            (init $columns: expr, $ts_min: expr) => {{
                let ts_min = $ts_min;

                let source_ids = [1, 5, 7];

                let root = tempdir!();

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

                    let part = pg.create_partition(ts_min)
                        .chain_err(|| "Unable to create partition")
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
                ]),+ $(,)*) => {{

                let columns = $schema;

                let now = $now;

                let init = append_test_impl!(init columns.clone(), now);
                let cat = init.1;

                $(

                let data = $data;

                let append = Append {
                    ts: $ts,
                    source_id: 1,
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
                use helpers::tempfile::tempdir_tools::TempDirExt;
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
                    .chain_err(|| "unable to instantiate RootManager")
                    .unwrap();

                let pg_root = PartitionGroupManager::new(&root, 1)
                    .chain_err(|| "unable to instantiate PartitionGroupManager")
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
                        .chain_err(|| "unable to instantiate PartitionManager")
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
                                    .chain_err(|| "unable to read block data")
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
                                    .chain_err(|| "unable to read block data")
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
                                    .chain_err(|| "unable to read index data")
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

    mod partition_meta {
        use super::*;
        use helpers::random::timestamp::{RandomTimestamp, RandomTimestampGen};

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
                        .chain_err(|| "unable to open catalog")
                        .unwrap();

                    (td, cat, now)
                }};

                (init sparse) => {
                    scan_test_impl!(init sparse count MAX_RECORDS - 4, 4);
                };

                (init sparse count $count: expr, $sparse_ratio: expr) => {{
                    use block::BlockType as BlockTy;
                    use ty::block::BlockType::Memmap;

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
                        .chain_err(|| "unable to open catalog")
                        .unwrap();

                    (td, cat, now)
                }};

                (dense simple $( $name: ident, $idx: expr, $value: expr),+ $(,)*) => {
                    scan_test_impl!(dense long MAX_RECORDS - 1, $( $name, $idx, $value, )+ );
                };

                (dense long $count: expr, $( $name: ident, $idx: expr, $value: expr ),+ $(,)*) =>
                {
                    $(
                    #[test]
                    fn $name() {
                        use ty::fragment::Fragment;
                        use scanner::ScanResult;

                        let (_td, catalog, now) = scan_test_impl!(init count $count);

                        let record_count = $count;

                        let scan = Scan::new(
                            hashmap! {
                                $idx => vec![ScanFilterOp::Lt($value).into()]
                            },
                            None,
                            None,
                            None,
                            None,
                        );

                        let result = catalog.scan(&scan).chain_err(|| "scan failed").unwrap();

                        fn u8_filter<T: Ord>(data: Vec<T>, val: u8) -> Vec<T> {
                            let v = data.into_iter()
                                .enumerate()
                                .filter_map(|(idx, v)| if (idx as u8) < val {
                                    Some(v)
                                }  else {
                                    None
                                })
                                .collect::<Vec<_>>();
                            v
                        }

                        let mut v = vec![Timestamp::from(0); record_count];
                        seqfill!(Timestamp, &mut v[..], now);
                        let v = u8_filter(v, 100);

                        let expected = ScanResult::from(hashmap! {
                            0 => Some(Fragment::from(v)),
                            1 => Some(Fragment::from(Vec::<u32>::new())),
                            2 => Some(Fragment::from(u8_filter(
                                seqfill!(vec u8, record_count),100))),
                            3 => Some(Fragment::from(u8_filter(
                                seqfill!(vec u16 as u8, record_count), 100))),
                            4 => Some(Fragment::from(u8_filter(
                                seqfill!(vec u32 as u8, record_count), 100))),
                            5 => Some(Fragment::from(u8_filter(
                                seqfill!(vec u64 as u8, record_count), 100))),
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

                        let scan = Scan::new(
                            hashmap! {
                                $idx => vec![ScanFilterOp::Lt($value).into()]
                            },
                            None,
                            None,
                            None,
                            None,
                        );

                        let result = catalog.scan(&scan).chain_err(|| "scan failed").unwrap();

                        // Help generate ts data for the test expectation
                        //
                        // This helper filters out every 4th record and additionally checks the
                        // accopanying value of the record
                        fn u8_filter<T: Ord>(data: Vec<T>, val: u8) -> Vec<T> {
                            let v = data.into_iter()
                                .scan(($sparse_ratio - 1, 0_u8),
                                    |&mut (ref mut pos, ref mut idx), v| {
                                    *pos += 1;
                                    if *pos > ($sparse_ratio - 1) {
                                        *pos = 0;

                                        if {
                                            let t = *idx < val;
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
                                .collect::<Vec<_>>();
                            v
                        }

                        // Help generate sparse data for the test expectation
                        //
                        // This helper filters out generated values from a sparse data
                        // while translating resulting rowidx values
                        // so the resulting sparse column looks like a dense one (no nulls)
                        fn u8_sparse_filter<T>(data: Vec<T>, index: Vec<u32>, val: u8)
                            -> (Vec<T>, Vec<u32>)
                            where T: Ord + PartialEq + From<u8>
                            {
                                data.into_iter()
                                    .zip(index.into_iter())
                                    .scan(0_usize, |projected_idx, (v, _)| {
                                        if {
                                            v < T::from(val)
                                        } {
                                            Some(Some((v, {
                                                let t = *projected_idx;
                                                *projected_idx += 1;
                                                t as u32
                                            })))
                                        } else {
                                            Some(None)
                                        }
                                    })
                                    .filter_map(|element| element)
                                    .collect::<Vec<_>>()
                                    .into_iter().unzip()
                        }

                        let mut v = vec![Timestamp::from(0); dense_count];
                        seqfill!(Timestamp, &mut v[..], now);
                        let v = u8_filter(v, $value as u8);

                        let expected = ScanResult::from(hashmap! {
                            0 => Some(Fragment::from(v)),
                            1 => Some(Fragment::from(Vec::<u32>::new())),
                            2 => Some(Fragment::from(u8_sparse_filter(
                                seqfill!(vec u8 as u8, record_count),
                                seqfill!(vec u32, record_count, 0, sparse_ratio),
                                $value as u8,
                            ))),
                            3 => Some(Fragment::from(u8_sparse_filter(
                                seqfill!(vec u16 as u8, record_count),
                                seqfill!(vec u32, record_count, 0, sparse_ratio),
                                $value as u8,
                            ))),
                            4 => Some(Fragment::from(u8_sparse_filter(
                                seqfill!(vec u32 as u8, record_count),
                                seqfill!(vec u32, record_count, 0, sparse_ratio),
                                $value as u8,
                            ))),
                            5 => Some(Fragment::from(u8_sparse_filter(
                                seqfill!(vec u64 as u8, record_count),
                                seqfill!(vec u32, record_count, 0, sparse_ratio),
                                $value as u8,
                            ))),
                        });

                        assert_eq!(result, expected);
                    }
                    )+
                }
            }

            scan_test_impl!(dense simple
                simple_u8, 2, 100_u8,
                simple_u16, 3, 100_u16,
                simple_u32, 4, 100_u32,
                simple_u64, 5, 100_u64,);

            scan_test_impl!(sparse simple
                simple_sparse_u8, 2, 100_u8,
                simple_sparse_u16, 3, 100_u16,
                simple_sparse_u32, 4, 100_u32,
                simple_sparse_u64, 5, 100_u64,);

            scan_test_impl!(dense long MAX_RECORDS * 2 - 1,
                long_u8, 2, 100_u8,
                long_u16, 3, 100_u16,
                long_u32, 4, 100_u32,
                long_u64, 5, 100_u64,);

            scan_test_impl!(sparse long MAX_RECORDS * 2 - 4, 4,
                long_sparse_u8, 2, 100_u8,
                long_sparse_u16, 3, 100_u16,
                long_sparse_u32, 4, 100_u32,
                long_sparse_u64, 5, 100_u64,);
        }

        /// The tests that use manually crafted blocks
        ///
        /// The 'light' suite
        mod minimal {
            use super::*;
            use ty::fragment::Fragment;
            use scanner::ScanFilter;
            use self::TyBlockType::Memmap;

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
                ) => {{
                    use block::BlockType as BlockTy;

                    let now = $ts;

                    let mut v = vec![Timestamp::from(0); $length];
                    seqfill!(Timestamp, &mut v[..], now);

                    // ts column is awlays present
                    let mut schema = hashmap! {
                        0 => Column::new(Memmap(BlockTy::U64Dense), "ts"),
                    };

                    let mut data = hashmap! {};

                    $(
                        schema.insert($dense_idx,
                            Column::new(Memmap(BlockTy::$dense_ty), $dense_name));

                        data.insert($dense_idx,
                            Fragment::from(vec![$($dense_data)*]));
                    )*

                    $(
                        schema.insert($sparse_idx,
                            Column::new(Memmap(BlockTy::$sparse_ty), $sparse_name));

                        data.insert($sparse_idx,
                            Fragment::from((vec![$($sparse_data)*], vec![$($sparse_data_idx)*])));
                    )*

                    let td = append_test_impl!(
                        schema,
                        now,
                        [
                            v.into(),
                            data
                        ]
                    );

                    let cat = Catalog::with_data(&td)
                        .chain_err(|| "unable to open catalog")
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

                let result = catalog.scan(&scan).chain_err(|| "scan failed").unwrap();

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

                let result = catalog.scan(&scan).chain_err(|| "scan failed").unwrap();

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

                let result = catalog.scan(&scan).chain_err(|| "scan failed").unwrap();

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

                    let result = catalog.scan(&scan).chain_err(|| "scan failed").unwrap();

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

                    let result = catalog.scan(&scan).chain_err(|| "scan failed").unwrap();

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

                    let result = catalog.scan(&scan).chain_err(|| "scan failed").unwrap();

                    assert_eq!(expected, result);
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
                        let (_, catalog, _) = scan_test_impl!(init);

                        let scan = Scan::new(
                            hashmap! {
                                $idx => vec![ScanFilterOp::Lt($value).into()]
                            },
                            None,
                            None,
                            None,
                            None,
                        );

                        b.iter(|| catalog.scan(&scan).chain_err(|| "scan failed").unwrap());
                    }
                    )+
                };

                (sparse simple $( $name: ident, $idx: expr, $value: expr ),+ $(,)*) => {
                    $(
                    #[bench]
                    fn $name(b: &mut Bencher) {
                        let (_, catalog, _) = scan_test_impl!(init sparse);

                        let scan = Scan::new(
                            hashmap! {
                                $idx => vec![ScanFilterOp::Lt($value).into()]
                            },
                            None,
                            None,
                            None,
                            None,
                        );

                        b.iter(|| catalog.scan(&scan).chain_err(|| "scan failed").unwrap());
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
