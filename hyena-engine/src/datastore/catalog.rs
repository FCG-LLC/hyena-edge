use error::*;
use ty::{BlockStorage, ColumnIndexStorageMap, ColumnId};
use block::BlockType;
use storage::manager::PartitionGroupManager;
use hyena_common::collections::HashMap;
use std::path::{Path, PathBuf};
use std::iter::FromIterator;
use std::default::Default;
use params::{SourceId, CATALOG_METADATA, TIMESTAMP_COLUMN};
use mutator::append::Append;
use scanner::{Scan, ScanResult};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use super::{PartitionGroupMap, ColumnMap};
use super::column::Column;
use super::partition_group::PartitionGroup;

#[derive(Debug, Serialize, Deserialize)]
pub struct Catalog<'cat> {
    pub(crate) colmap: ColumnMap,
    pub(crate) groups: PartitionGroupMap<'cat>,
    pub(crate) indexes: ColumnIndexStorageMap,

    #[serde(skip)]
    pub(crate) data_root: PathBuf,
}

impl<'cat> Catalog<'cat> {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Catalog<'cat>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(CATALOG_METADATA);

        if meta.exists() {
            bail!("Catalog metadata already exists {:?}", meta);
        }

        let mut catalog = Catalog {
            colmap: Default::default(),
            groups: Default::default(),
            indexes: Default::default(),
            data_root: root,
        };

        catalog.ensure_default_columns()?;

        Ok(catalog)
    }

    fn ensure_default_columns(&mut self) -> Result<()> {
        let ts_column = Column::new(BlockStorage::Memmap(BlockType::U64Dense), "timestamp");
        let source_column = Column::new(BlockStorage::Memory(BlockType::I32Dense), "source_id");
        let mut map = HashMap::new();
        map.insert(TIMESTAMP_COLUMN, ts_column);
        map.insert(1, source_column);

        self.ensure_columns(map)
    }

    pub fn with_data<P: AsRef<Path>>(root: P) -> Result<Catalog<'cat>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(CATALOG_METADATA);

        Catalog::deserialize(&meta, &root)
    }

    pub fn open_or_create<P: AsRef<Path>>(root: P) -> Result<Catalog<'cat>> {
        Catalog::with_data(root.as_ref()).or_else(|_| Catalog::new(root.as_ref()))
    }

    pub fn columns(&self) -> &ColumnMap {
        &self.colmap
    }

    #[cfg(feature = "validate_append")]
    fn validate_append(&self, data: &Append) -> bool {
        let ts_len = data.ts.len();

        data.data.iter()
            .all(|(_col, fragment)| {

                // check fragment length for dense blocks
                if !fragment.is_sparse() {
                    if ts_len != fragment.len() {
                        error!("Dense append fragment has different length than ts");

                        return false;
                    }

                } else {
                    if ts_len > fragment.len() {
                        error!("Sparse append fragment longer than ts");

                        return false;
                    }

                    if fragment.iter().any(|(idx, _)| idx >= ts_len) {
                        error!("Sparse append fragment has index greater than ts length");

                        return false;
                    }
                }

                true
            })
    }

    pub fn append(&self, data: &Append) -> Result<usize> {
        if data.is_empty() {
            bail!("Provided Append contains no data");
        }

        #[cfg(feature = "validate_append")]
        {
            if !self.validate_append(&data) {
                bail!("Provided Append is not consistent");
            }
        }

        // dispatch to proper PartitionGroup
        if let Some(pg) = self.groups.get(&data.source_id) {
            pg.append(&self, &data)
        } else {
            bail!("No PartitionGroup found for source_id = {}", data.source_id);
        }
    }

    pub fn scan(&self, scan: &Scan) -> Result<ScanResult> {

        let all_groups = if scan.groups.is_some() {
            None
        } else {
            Some(self.groups.keys().cloned().collect::<Vec<_>>())
        };

        if scan.groups.is_some() {
            scan.groups.as_ref().unwrap()
        } else {
            all_groups.as_ref().unwrap()
        }
        .par_iter()
        .filter_map(|pgid| self.groups.get(pgid))
        .map(|pg| pg.scan(&scan))
        // todo: this would potentially be better with some short-circuiting combinator instead
        // need to bench with collect_into()
        .reduce(|| Ok(ScanResult::merge_identity()), |a, b| {
            let mut a = a?;
            let b = b?;

            a.merge(b)?;

            Ok(a)
        })
    }

    pub fn flush(&self) -> Result<()> {
        // TODO: add dirty flag
        let meta = self.data_root.join(CATALOG_METADATA);

        for pg in self.groups.values() {
            pg.flush()?
        }

        Catalog::serialize(self, &meta)
    }

    /// Extend internal column map without any sanitization checks.
    ///
    /// This function uses `std::iter::Extend` internally,
    /// so it allows redefinition of a column type.
    /// Use this feature with great caution.
    pub(crate) fn ensure_columns(&mut self, type_map: ColumnMap) -> Result<()> {
        self.colmap.extend(type_map);

        Ok(())
    }

    /// Adds a column to the catalog. It verifies that catalog does not already contain:
    /// a) column with the given id, or
    /// b) column with the given name.
    /// This function takes all-or-nothing approach:
    /// either all columns are added, or no changes are applied.
    pub fn add_columns(&mut self, column_map: ColumnMap) -> Result<()> {
        for (id, column) in column_map.iter() {
            info!("Adding column {}:{:?} with id {}", column.name, column.ty, id);

            if self.colmap.contains_key(id) {
                bail!("Column Id already exists {}", *id);
            }
            if self.colmap.values().any(|col| col.name == column.name) {
                bail!("Column Name already exists '{}'", column.name);
            }
        }

        self.ensure_columns(column_map)
    }

    /// Extend internal index map without any sanitization checks.
    ///
    /// This function uses `std::iter::Extend` internally,
    /// so it allows redefinition of a index type.
    /// Also, the index' support for a given column is not checked.
    /// Use this feature with great caution.
    pub(crate) fn ensure_indexes(&mut self, index_map: ColumnIndexStorageMap) -> Result<()> {
        self.indexes.extend(&*index_map);

        Ok(())
    }

    /// Adds index to the catalog. Verifies that catalog does not already contain:
    /// a) index for a column with the given id, or
    /// b) index for a column with the given name.
    /// This function takes all-or-nothing approach:
    /// either all indexes are added, or no changes are applied.
    pub fn add_indexes(&mut self, index_map: ColumnIndexStorageMap) -> Result<()> {
        for (id, index) in index_map.iter() {
            let column = self.colmap.get(id)
                .ok_or_else(|| err_msg(format!("column not found {}", id)))?;

            info!("Adding index {:?} for column {}[{}]:{:?}",
                index, column.name, id, column.ty);

            if self.indexes.contains_key(id) {
                bail!("Index already exists {}", *id);
            }
        }

        self.ensure_indexes(index_map)
    }
    /// Fetch the first non-occupied column index
    ///
    /// todo: rethink this approach (max() every time)
    pub fn next_id(&self) -> usize {
        let default = 0;
        *self.colmap.keys().max().unwrap_or(&default) + 1
    }

    /// Calculate an empty partition's capacity for given column set
    pub(super) fn space_for_blocks<'iter>(&self, indices: impl Iterator<Item = &'iter ColumnId>)
    -> usize
    {
        use params::BLOCK_SIZE;

        indices
            .filter_map(|col_id| {
                if let Some(column) = self.colmap.get(col_id) {
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

    pub(crate) fn ensure_group(
        &mut self,
        source_id: SourceId,
    ) -> Result<&mut PartitionGroup<'cat>> {
        let data_root = <_ as AsRef<Path>>::as_ref(&self.data_root);

        Ok(self.groups.entry(source_id).or_insert_with(|| {
            // this shouldn't fail in general

            let root = PartitionGroupManager::new(data_root, source_id)
                .with_context(|_| "Failed to create group manager")
                .unwrap();

            let pg = PartitionGroup::new(&root, source_id)
                .with_context(|_| "Unable to create partition group")
                .unwrap();

            pg.flush().unwrap();

            pg
        }))
    }

    /// Add new partition group with given source id

    pub fn add_partition_group(&mut self, source_id: SourceId) -> Result<()> {
        let _ = self.ensure_group(source_id)?;

        Ok(())
    }

    fn prepare_partition_groups<P, I>(root: P, ids: I) -> Result<PartitionGroupMap<'cat>>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = SourceId>,
    {
        ids.into_iter()
            .map(|source_id| {
                let path = PartitionGroupManager::new(&root, source_id).with_context(|_| {
                    format!(
                        "Unable to obtain data directory for partition group {}",
                        source_id
                    )
                })?;

                let partition_group = PartitionGroup::with_data(path)
                    .with_context(|_| format!("Unable to read partition group {:?}", source_id))?;

                Ok((source_id, partition_group))
            })
            .collect()
    }


    fn serialize<P: AsRef<Path>>(catalog: &Catalog<'cat>, meta: P) -> Result<()> {
        let meta = meta.as_ref();

        let group_metas = Vec::from_iter(catalog.groups.keys());

        let data = (catalog, group_metas);

        serialize!(file meta, &data)
            .with_context(|_| "Failed to serialize catalog metadata")
            .map_err(|e| e.into())
    }

    fn deserialize<P: AsRef<Path>, R: AsRef<Path>>(meta: P, root: R) -> Result<Catalog<'cat>> {
        let meta = meta.as_ref();

        if !meta.exists() {
            bail!("Cannot find catalog metadata {:?}", meta);
        }

        let (mut catalog, group_metas): (Catalog, Vec<SourceId>) = deserialize!(file meta)
            .with_context(|_| "Failed to read catalog metadata")?;

        catalog.groups = Catalog::prepare_partition_groups(&root, group_metas)
            .with_context(|_| "Failed to read partition data")?;

        catalog.data_root = root.as_ref().to_path_buf();

        Ok(catalog)
    }
}

impl<'cat> Drop for Catalog<'cat> {
    fn drop(&mut self) {
        self.flush()
            .with_context(|_| "Failed to flush data during drop")
            .unwrap();
    }
}

impl<'cat> AsRef<ColumnMap> for Catalog<'cat> {
    fn as_ref(&self) -> &ColumnMap {
        &self.colmap
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datastore::tests::create_random_partitions;

    #[test]
    fn new() {
        let source_ids = [1, 5, 7];
        let im_part_count = 8;
        let mut_part_count = 2;

        let root = tempdir!();

        let mut cat = Catalog::new(&root)
            .with_context(|_| "Unable to create catalog")
            .unwrap();

        for source_id in &source_ids {

            let pg = cat.ensure_group(*source_id)
                .with_context(|_| "Unable to retrieve partition group")
                .unwrap();

            create_random_partitions(pg, im_part_count, mut_part_count);
        }
    }

    #[test]
    fn add_partition_group_idempotence() {
        let root = tempdir!();

        let mut cat = Catalog::new(&root)
            .with_context(|_| "Unable to create catalog")
            .unwrap();

        const PG_ID: SourceId = 10;

        cat.add_partition_group(PG_ID).unwrap();
        cat.add_partition_group(PG_ID).unwrap();

        assert_eq!(cat.groups.len(), 1);
        assert_eq!(cat.groups.iter().nth(0).expect("partition group not found").0, &PG_ID);
    }
}
