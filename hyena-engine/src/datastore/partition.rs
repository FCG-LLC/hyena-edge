use crate::error::*;
use hyena_common::ty::Uuid;

use crate::block::{BlockData, BufferHead, SparseIndex};
use crate::ty::{BlockHeads, BlockHeadMap, BlockMap, BlockStorage, BlockStorageMap,
ColumnId, RowId, ColumnIndexMap, ColumnIndexStorage, ColumnIndexStorageMap};
use hyena_common::ty::Timestamp;
use std::path::{Path, PathBuf};
use std::cmp::{max, min};
use hyena_common::collections::{HashMap, HashSet};
#[cfg(feature = "mmap")]
use rayon::prelude::*;
use crate::params::{PARTITION_METADATA, TIMESTAMP_COLUMN};
use std::sync::RwLock;
use std::iter::FromIterator;
use crate::ty::fragment::{Fragment, TimestampFragmentRef};
use crate::mutator::BlockRefData;
use crate::scanner::{Scan, ScanFilterApply, ScanResult, ScanFilters, ScanTsRange, BloomFilterValues};


pub(crate) type PartitionId = Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct Partition<'part> {
    pub(crate) id: PartitionId,
    // TODO: do we really need ts_* here?
    pub(crate) ts_min: Timestamp,
    pub(crate) ts_max: Timestamp,

    #[serde(skip)]
    blocks: BlockMap<'part>,
    #[serde(skip)]
    indexes: ColumnIndexMap<'part>,
    #[serde(skip)]
    data_root: PathBuf,
}

impl<'part> Partition<'part> {
    pub fn new<P: AsRef<Path>, TS: Into<Timestamp>>(
        root: P,
        id: PartitionId,
        ts: TS,
    ) -> Result<Partition<'part>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(PARTITION_METADATA);

        if meta.exists() {
            bail!("Partition metadata already exists {:?}", meta);
        }

        let ts = ts.into();

        Ok(Partition {
            id,
            ts_min: ts,
            ts_max: ts,
            blocks: Default::default(),
            indexes: Default::default(),
            data_root: root,
        })
    }

    pub fn with_data<P: AsRef<Path>>(root: P) -> Result<Partition<'part>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(PARTITION_METADATA);

        Partition::deserialize(&meta, &root)
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        if !self.blocks.is_empty() {
            if let Some(block) = self.blocks.get(&TIMESTAMP_COLUMN) {
                let b = acquire!(read block);
                return b.len();
            }
        }

        0
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty() || self.len() == 0
    }

    pub(crate) fn append<'frag>(
        &mut self,
        blockmap: &BlockStorageMap,
        indexmap: &ColumnIndexStorageMap,
        frags: &BlockRefData<'frag>,
        offset: SparseIndex,
    ) -> Result<usize> {
        self.ensure_blocks(&blockmap)
            .with_context(|_| "Unable to create block map")
            .unwrap();

        self.ensure_column_indexes(&indexmap)
            .with_context(|_| "Unable to create index map")
            .unwrap();

        // ts range update
        let (ts_min, ts_max) =
            self.compare_ts(frags.get(&TIMESTAMP_COLUMN).expect("assumed ts fragment not found"));

        let written = {
            let ops = frags.iter().filter_map(|(ref blk_idx, ref frag)| {
                if let Some(block) = self.blocks.get(blk_idx) {
                    let idx = self.indexes.get(blk_idx);

                    trace!("writing block {} with frag len {} {}",
                        blk_idx, (*frag).len(), idx.map_or("", |_ | "and column index"));
                    Some((block, frags.get(blk_idx).unwrap(), idx))
                } else {
                    None
                }
            });

            let mut written: usize = 0;

            for (ref mut block, ref data, colidx) in ops {
                let b = acquire!(write block);
                let mut colidx = colidx.map(|ref mut lock| acquire!(raw write lock));

                let r = map_fragment!(mut map ref b, *data, blk, frg, fidx, {
                    // dense block handler

                    // destination bounds checking intentionally left out in release builds
                    let slen = frg.len();

                    {
                        let blkslice = blk.as_mut_slice_append();

                        debug_assert!(slen <= blkslice.len(),
                            "bounds check failed for dense block");

                        &blkslice[..slen].copy_from_slice(&frg[..]);
                    }

                    blk.set_written(slen).with_context(|_| "set_written failed")?;

                    slen
                }, {
                    // sparse block handler
                    // destination bounds checking intentionally left out in release builds
                    let slen = frg.len();

                    let block_offset = if let Some(offset) = blk.as_index_slice().last() {
                        *offset + 1
                    } else {
                        0
                    };

                    {
                        let (blkindex, blkslice) = blk.as_mut_indexed_slice_append();

                        debug_assert!(slen <= blkslice.len(),
                            "bounds check failed for sparse block");

                        &mut blkslice[..slen].copy_from_slice(&frg[..]);
                        &mut blkindex[..slen].copy_from_slice(&fidx[..]);

                        // adjust the offset
                        &mut blkindex[..slen].par_iter_mut().for_each(|idx| {
                            *idx = *idx - offset + block_offset;
                        });
                    }

                    blk.set_written(slen).with_context(|_| "set_written failed")?;

                    slen
                }, {
                    // dense string handler

                    // check if the column is indexed

                    frg
                        .iter()
                        .map(|value| {
                            if let Some(ref mut colidx) = colidx {
                                colidx.append_value(&value);
                            }

                            blk.append_string(value)
                        })
                        .sum::<Result<usize>>()
                        .with_context(|_| "string block append failed")?
                });

                written = written.saturating_add(r.with_context(|_| "map_fragment failed")?);
            }

            written
        };

        // update ts
        if let Some(ts_min) = ts_min {
            self.ts_min = ts_min;
        }

        if let Some(ts_max) = ts_max {
            self.ts_max = ts_max;
        }

        // todo: fix this (should be slen)
        Ok(written)
    }

    pub(crate) fn scan(&self, scan: &Scan) -> Result<ScanResult> {
        // only filters and projection for now
        // full ts range

        let rowids = if let Some(ref filters) = scan.filters {
            let rowids = self.filter(
                filters,
                scan.bloom_filters.as_ref(),
                scan.or_clauses_count)?;

            // this is superfluous if we're dealing with dense blocks only
            // as it doesn't matter if rowids are sorted then
            // but it still should be cheaper than converting for each sparse block separately

            let mut rowids = Vec::from_iter(rowids.into_iter());

            rowids.par_sort_unstable();

            Some(rowids)
        } else {
            None
        };

        let materialized = self.materialize(&scan.projection, rowids.as_ref().map(AsRef::as_ref));

        materialized.map(|m| m.into())
    }

    /// Apply filters to given columns (blocks)
    ///
    /// # Arguments
    ///
    /// * `filters` - a `ScanFilters` object
    ///
    /// # Returns
    ///
    /// `HashSet` with matching `RowId`s

    #[inline]
    fn filter(&self,
        filters: &ScanFilters,
        column_filters: Option<&BloomFilterValues>,
        or_clauses_count: usize
    ) -> Result<HashSet<usize>> {
        let result = filters
            .par_iter()
            .fold(|| vec![Some(HashSet::new()); or_clauses_count],
                |mut acc, (block_id, or_filters)| {
                if let Some(block) = self.blocks.get(block_id) {
                    let block = acquire!(read block);
                    let index = self.indexes.get(block_id).map(|ref lock| acquire!(raw read lock));

                    map_block!(map block, blk, {
                        blk.as_slice().iter()
                            .enumerate()
                            .for_each(|(rowid, val)| {
                                or_filters
                                    .iter()
                                    .zip(acc.iter_mut())
                                    .for_each(|(and_filters, result)| {
                                        if let Some(ref mut result) = *result {
                                            if and_filters.iter().all(|f| f.apply(val)) {
                                                result.insert(rowid);
                                            }
                                        } else {
                                            unreachable!()
                                        }
                                    });
                            });

                    }, {
                        let (idx, data) = blk.as_indexed_slice();

                        data.iter()
                            .enumerate()
                            .for_each(|(rowid, val)| {
                                or_filters
                                    .iter()
                                    .zip(acc.iter_mut())
                                    .for_each(|(and_filters, result)| {
                                        if let Some(ref mut result) = *result {
                                            if and_filters.iter().all(|f| f.apply(val)) {
                                                // this is a safe cast u32 -> usize
                                                result.insert(idx[rowid] as usize);
                                            }
                                        } else {
                                            unreachable!()
                                        }
                                    });
                            });
                    }, {
                        // pooled

                        if let Some(ref index) = index {
                            blk
                                .iter()
                                .enumerate()
                                .zip(index.iter())
                                .filter_map(|((rowid, value), bloom)| {
                                    // bloom

                                    if let Some(filters) = column_filters {
                                        if let Some(filters) = filters.get(&block_id) {
                                            if filters
                                                .iter()
                                                .any(|tested| bloom.contains(*tested)) {
                                                Some((rowid, value))
                                            } else {
                                                None
                                            }
                                        } else {
                                            Some((rowid, value))
                                        }
                                    } else {
                                        Some((rowid, value))
                                    }
                                })
                                .for_each(|(rowid, val)| {
                                    or_filters
                                        .iter()
                                        .zip(acc.iter_mut())
                                        .for_each(|(and_filters, result)| {
                                            if let Some(ref mut result) = *result {
                                                if and_filters.iter().all(|f| f.apply(&val)) {
                                                    result.insert(rowid);
                                                }
                                            } else {
                                                unreachable!()
                                            }
                                        });
                                });
                        } else {
                            blk
                                .iter()
                                .enumerate()
                                .for_each(|(rowid, val)| {
                                    or_filters
                                        .iter()
                                        .zip(acc.iter_mut())
                                        .for_each(|(and_filters, result)| {
                                            if let Some(ref mut result) = *result {
                                                if and_filters.iter().all(|f| f.apply(&val)) {
                                                    result.insert(rowid);
                                                }
                                            } else {
                                                unreachable!()
                                            }
                                        });
                                });
                        }
                    })
                }

                acc
            })
            .reduce(
                || vec![None; or_clauses_count],
                |a, b| {
                    a.into_iter().zip(b.into_iter())
                        .map(|(a, b)| {
                            if a.is_none() {
                                b
                            } else if b.is_none() {
                                a
                            } else {
                                Some(a.unwrap().intersection(&b.unwrap()).cloned().collect())
                            }
                        })
                        .collect()
                },
            );

            if result.iter().any(|result| result.is_none()) {
                Err(err_msg("scan error: an empty filter variant was provided"))
            } else {
                Ok(result.into_iter()
                    .fold(HashSet::new(), |a, b| {
                        a.union(&b.unwrap()).cloned().collect()
                    }))
            }
    }

    /// Materialize scan results with given projection
    ///
    /// # Arguments
    ///
    /// * `projection` - an optional `Vec` of `ColumnId` or None for all columns
    /// * `rowids` - an Option of slice of row indexes, needs to be sorted in ascending order!
    ///              when set to None, materialize all rowids
    ///
    /// # Returns
    ///
    /// `HashMap` with results placed in `Fragment`s

    #[inline]
    fn materialize(&self, projection: &Option<Vec<ColumnId>>, rowids: Option<&[RowId]>)
        -> Result<HashMap<ColumnId, Option<Fragment>>> {

        let all_columns = if projection.is_some() {
            None
        } else {
            Some(self.blocks.keys().cloned().collect::<Vec<_>>())
        };

        if projection.is_some() {
            projection.as_ref().unwrap()
        } else {
            all_columns.as_ref().unwrap()
        }.par_iter()
            .map(|block_id| {
                if let Some(block) = self.blocks.get(block_id) {
                    let block = acquire!(read block);

                    Ok((
                        *block_id,
                        Some(map_block!(map block, blk, {

                        let bs = blk.as_slice();

                        Fragment::from(if bs.is_empty() {
                                Vec::new()
                            } else {
                                if let Some(rowids) = rowids {
                                    rowids.iter()
                                        .map(|rowid| bs[*rowid])
                                        .collect::<Vec<_>>()
                                } else {
                                    // materialize full block

                                    bs.to_vec()
                                }
                            })
                        }, {
                            use crate::ty::{SparseIter, SparseIterator};

                            let (index, data) = blk.as_indexed_slice();

                            // map rowid (source block rowid) to target_rowid
                            // (target block rowid), which is an index in the rowids vec
                            // so essentially a rowid of the resulting rowset
                            //
                            // for a block defined like this:
                            //
                            // +--------+----+------+------+
                            // | rowid  | ts | col1 | col2 |
                            // +--------+----+------+------+
                            // |   0    | 1  |      | 10   |
                            // +--------+----+------+------+
                            // |   1    | 2  | 1    | 20   |
                            // +--------+----+------+------+
                            // |   2    | 3  |      |      |
                            // +--------+----+------+------+
                            // |   3    | 4  | 2    |      |
                            // +--------+----+------+------+
                            // |   4    | 5  |      |      |
                            // +--------+----+------+------+
                            //
                            // with rowids = [1, 2, 3] and all columns materialize
                            //
                            // we would get the result like this:
                            // (origid included only to illustrate the transformation)
                            //
                            // +--------+--------+----+------+------+
                            // | rowid  | origid | ts | col1 | col2 |
                            // +--------+--------+----+------+------+
                            // |   0    |   1    | 2  | 1    | 20   |
                            // +--------+--------+----+------+------+
                            // |   1    |   2    | 3  |      |      |
                            // +--------+--------+----+------+------+
                            // |   2    |   3    | 4  | 2    |      |
                            // +--------+--------+----+------+------+
                            //
                            // which translates to the following for sparse col1:
                            //
                            // +--------+--------+------+
                            // | rowid  | origid | col1 |
                            // +--------+--------+------+
                            // |   0    |   1    | 1    |
                            // +--------+--------+------+
                            // |   1    |   2    |      |
                            // +--------+--------+------+
                            // |   2    |   3    | 2    |
                            // +--------+--------+------+
                            //
                            // (1, 0), (2, 2)
                            //
                            //
                            // rowid is an index of an element in rowids slice
                            // origid is a rowid from original data set
                            // every matching sparse row will have its index replaced by new rowid
                            // to properly translate to the result Fragment

                            let mut iter = SparseIter::new(data, index.into_iter().cloned());

                            let (d, i): (Vec<_>, Vec<SparseIndex>) = if let Some(rowids) = rowids {
                                rowids
                                    .iter()
                                    .enumerate()
                                    .filter_map(|(target_rowid, rowid)| {
                                        // `as` shouldn't misbehave here, as we assume that
                                        // both `rowid` and `target_rowid` are <= u32::MAX
                                        // nevertheless it would be better to have this migrated to
                                        // `TryFrom` as soon as it stabilizes

                                        // using assert! here gives ~20%-30% performance decrease
                                        debug_assert!(*rowid <= ::std::u32::MAX as usize);
                                        debug_assert!(target_rowid <= ::std::u32::MAX as usize);

                                        iter
                                            .next_to(*rowid as SparseIndex)
                                            .and_then(|v| v)
                                            .map(|(v, _)| (*v, target_rowid as SparseIndex))
                                    })
                                    .unzip()
                            } else {
                                iter.unzip()
                            };

                            Fragment::from((d, i))
                        }, {
                            // pooled
                            Fragment::from(if blk.is_empty() {
                                    Vec::new()
                                } else {
                                    if let Some(rowids) = rowids {
                                        rowids.iter()
                                            .map(|rowid| String::from(&blk[*rowid]))
                                            .collect::<Vec<_>>()
                                    } else {
                                        // materialize full block

                                        blk
                                            .iter()
                                            .map(String::from)
                                            .collect::<Vec<_>>()
                                    }
                                })
                        }))
                    ))
                } else {
                    // if this is a sparse block, then it's a valid case
                    // so we'll leave the decision whether scan failed or not here
                    // to our caller (who has access to the catalog)

                    Ok((*block_id, None))
                }
            })
            .collect()
    }

    #[inline]
    pub(crate) fn is_within_ts_range(&self, range: &ScanTsRange) -> bool {
        range.overlaps_with(self.ts_min, self.ts_max)
    }

    #[inline]
    fn compare_ts<'frag, 'tsfrag, T>(&self, ts_frag: &'frag T)
        -> (Option<Timestamp>, Option<Timestamp>)
        where TimestampFragmentRef<'tsfrag>: From<&'frag T> {

        TimestampFragmentRef::from(ts_frag)
            .iter()
            .fold((None, None), |mut acc, ts| {
                if *ts < acc.0.unwrap_or(self.ts_min) {
                    acc.0 = Some(*ts);
                }

                if *ts > acc.1.unwrap_or(self.ts_max) {
                    acc.1 = Some(*ts);
                }

                acc
            })
    }

    #[allow(unused)]
    pub(crate) fn scan_ts(&self) -> Result<(Timestamp, Timestamp)> {
        let ts_block = self.blocks
            .get(&0)
            .ok_or_else(|| err_msg("Failed to get timestamp block"))?;

        block_apply!(expect physical U64Dense, ts_block, block, pb, {
            let (ts_min, ts_max) = pb.as_slice().iter().fold((None, None), |mut acc, &x| {
                acc.0 = Some(if let Some(cur) = acc.0 {
                    min(cur, x)
                } else {
                    x
                });

                acc.1 = Some(if let Some(cur) = acc.1 {
                    max(cur, x)
                } else {
                    x
                });

                acc
            });

            Ok((ts_min.ok_or_else(|| err_msg("Failed to get min timestamp"))?.into(),
                ts_max.ok_or_else(|| err_msg("Failed to get max timestamp"))?.into()))
        })
    }

    pub fn get_id(&self) -> PartitionId {
        self.id
    }

    pub(crate) fn get_ts(&self) -> (Timestamp, Timestamp) {
        (self.ts_min, self.ts_max)
    }

    #[allow(unused)]
    pub(crate) fn set_ts<TS>(&mut self, ts_min: Option<TS>, ts_max: Option<TS>) -> Result<()>
    where
        Timestamp: From<TS>,
    {
        let cmin = ts_min.map(Timestamp::from).unwrap_or(self.ts_min);
        let cmax = ts_max.map(Timestamp::from).unwrap_or(self.ts_max);

        if !(cmin <= cmax) {
            bail!("ts_min ({:?}) should be <= ts_max ({:?})", cmin, cmax)
        } else {
            self.ts_min = cmin;
            self.ts_max = cmax;

            Ok(())
        }
    }

    #[allow(unused)]
    pub(crate) fn update_meta(&mut self) -> Result<()> {
        // TODO: handle ts_min changes -> partition path

        let (ts_min, ts_max) = self.scan_ts()
            .with_context(|_| "Unable to perform ts scan for metadata update")?;

        if ts_min != self.ts_min {
            warn!(
                "Partition min ts changed {} -> {} [{}]",
                self.ts_min,
                ts_min,
                self.id
            );
        }

        self.ts_min = ts_min;
        self.ts_max = ts_max;

        Ok(())
    }

    /// Calculate how many records containing given indices will fit into this partition
    /// or return None, meaning that this partition doesn't contain any blocks
    /// in which case the caller should resort to `Catalog::space_for_blocks`
    pub(crate) fn space_for_blocks(&self, indices: &[ColumnId]) -> Option<usize> {
        indices.iter()
            .filter_map(|block_id| {
                if let Some(block) = self.blocks.get(block_id) {
                    let b = acquire!(read block);
                    Some(b.size() - b.len())
                } else {
                    None
                }
            })
            .min()
    }

    #[allow(unused)]
    pub(crate) fn get_blocks(&self) -> &BlockMap<'part> {
        &self.blocks
    }

    #[allow(unused)]
    pub(crate) fn mut_blocks(&mut self) -> &mut BlockMap<'part> {
        &mut self.blocks
    }

    #[inline]
    pub fn ensure_blocks(&mut self, storage_map: &BlockStorageMap) -> Result<()> {
        let fmap = storage_map
            .iter()
            .filter_map(|(block_id, block_type)| {
                if self.blocks.contains_key(block_id) {
                    None
                } else {
                    Some((*block_id, *block_type))
                }
            })
            .collect::<HashMap<_, _>>().into();

        let blocks = Partition::prepare_blocks(&self.data_root, &fmap)
            .with_context(|_| "Unable to create block map")?;

        self.blocks.extend(blocks);

        Ok(())
    }

    #[inline]
    pub fn ensure_column_indexes(&mut self, storage_map: &ColumnIndexStorageMap) -> Result<()> {
        let fmap = storage_map
            .iter()
            .filter_map(|(block_id, index_type)| {
                if self.indexes.contains_key(block_id) {
                    None
                } else {
                    Some((*block_id, *index_type))
                }
            })
            .collect::<HashMap<_, _>>().into();

        let blocks = Partition::prepare_indexes(&self.data_root, &fmap)
            .with_context(|_| "Unable to create block map")?;

        self.indexes.extend(blocks);

        Ok(())
    }

    #[inline]
    pub fn gen_id() -> PartitionId {
        Uuid::new()
    }

    // TODO: to be benched
    fn prepare_blocks<'i, P>(
        root: P,
        storage_map: &'i BlockStorageMap,
    ) -> Result<BlockMap<'part>>
    where
        P: AsRef<Path> + Sync,
//         BT: IntoParallelRefIterator<'i, Item = (&'i BlockId, &'i BlockType)> + 'i,
//         &'i BT: IntoIterator<Item = (BlockId, BlockType)>,
    {

        storage_map
            .iter()
            .map(|(block_id, block_type)| match *block_type {
                BlockStorage::Memory(bty) => {
                    use crate::ty::block::memory::Block;

                    Block::create(bty)
                        .map(|block| (*block_id, locked!(rw block.into())))
                        .with_context(|_| "Unable to create in-memory block")
                        .map_err(|e| e.into())
                }
                BlockStorage::Memmap(bty) => {
                    use crate::ty::block::mmap::Block;

                    Block::create(&root, bty, *block_id)
                        .map(|block| (*block_id, locked!(rw block.into())))
                        .with_context(|_| "Unable to create mmap block")
                        .map_err(|e| e.into())
                }
            })
            .collect()
    }

    // TODO: to be benched
    fn prepare_indexes<'i, P>(
        root: P,
        storage_map: &'i ColumnIndexStorageMap,
    ) -> Result<ColumnIndexMap<'part>>
    where
        P: AsRef<Path> + Sync,
//         BT: IntoParallelRefIterator<'i, Item = (&'i BlockId, &'i BlockType)> + 'i,
//         &'i BT: IntoIterator<Item = (BlockId, BlockType)>,
    {

        storage_map
            .iter()
            .map(|(block_id, index_type)| match *index_type {
                ColumnIndexStorage::Memory(colidx) => {
                    use crate::ty::index::memory::ColumnIndex;

                    ColumnIndex::create(colidx)
                        .map(|block| (*block_id, locked!(rw block.into())))
                        .with_context(|_| "Unable to create in-memory column index block")
                        .map_err(|e| e.into())
                }
                ColumnIndexStorage::Memmap(colidx) => {
                    use crate::ty::index::mmap::ColumnIndex;

                    ColumnIndex::create(&root, colidx, *block_id)
                        .map(|block| (*block_id, locked!(rw block.into())))
                        .with_context(|_| "Unable to create mmap column index block")
                        .map_err(|e| e.into())
                }
            })
            .collect()
    }

    pub(crate) fn flush(&self) -> Result<()> {
        // TODO: add dirty flag
        let meta = self.data_root.join(PARTITION_METADATA);

        Partition::serialize(self, &meta)
    }

    fn serialize<P: AsRef<Path>>(partition: &Partition<'part>, meta: P) -> Result<()> {
        let meta = meta.as_ref();

        let blocks: BlockStorageMap = (&partition.blocks).into();
        let indexes: ColumnIndexStorageMap = (&partition.indexes).into();
        let heads = partition
            .blocks
            .iter()
            .map(|(blockid, block)| {
                (
                    *blockid,
                    block_apply!(map physical block, blk, pb, {
                    BlockHeads {
                        head: pb.head(),
                        pool_head: pb.pool_head(),
                    }
                }),
                )
            })
            .collect::<BlockHeadMap>();

        let data = (partition, blocks, indexes, heads);

        serialize!(file meta, &data)
            .with_context(|_| "Failed to serialize partition metadata")
            .map_err(|e| e.into())
    }

    fn deserialize<P: AsRef<Path>, R: AsRef<Path> + Sync>(
        meta: P,
        root: R,
    ) -> Result<Partition<'part>> {
        let meta = meta.as_ref();

        if !meta.exists() {
            bail!("Cannot find partition metadata {:?}", meta);
        }

        let (mut partition, blocks, indexes, heads):
            (Partition, BlockStorageMap, ColumnIndexStorageMap,  BlockHeadMap) =
            deserialize!(file meta)
                .with_context(|_| "Failed to read partition metadata")?;

        partition.blocks = Partition::prepare_blocks(&root, &blocks)
            .with_context(|_| "Failed to read block data")?;

        partition.indexes = Partition::prepare_indexes(&root, &indexes)
            .with_context(|_| "Failed to read column index block data")?;

        let partid = partition.id;

        for (blockid, block) in &mut partition.blocks {
            block_apply!(mut map physical block, blk, pb, {
                let head = heads.get(blockid)
                    .ok_or_else(||
                        format_err!("Unable to read block head ptr of partition {}", partid))?;
                *(pb.mut_head()) = head.head;
                if let Some(head) = head.pool_head {
                    pb.set_pool_head(head);
                }

                // check for this block's index
                if let Some(index) = partition.indexes.get(&blockid) {
                    let index = acquire!(write index);
                    index.set_head(head.head)
                        .with_context(|_|
                            format_err!("Unable to set column index block head ptr of partition {}",
                                partid))?;
                }
            })
        }

        partition.data_root = root.as_ref().to_path_buf();

        Ok(partition)
    }
}


impl<'part> Drop for Partition<'part> {
    fn drop(&mut self) {
        self.flush()
            .with_context(|_| "Failed to flush data during drop")
            .unwrap();
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use hyena_common::collections::HashMap;
    use crate::ty::block::{Block, BlockStorage, BlockId};
    use std::fmt::Debug;
    use hyena_test::random::timestamp::{RandomTimestamp, RandomTimestampGen};
    use crate::block::BlockData;
    use crate::ty::fragment::Fragment;


    macro_rules! block_test_impl_mem {
        ($base: ident, $($variant: ident),* $(,)*) => {{
            use self::BlockStorage::Memory;
            let mut idx = 0;

            hashmap! {
                $(
                    {idx += 1; idx} => Memory($base::$variant),
                )*
            }
        }};
    }

    macro_rules! block_test_impl_mmap {
        ($base: ident, $($variant: ident),* $(,)*) => {{
            use self::BlockStorage::Memmap;
            let mut idx = 0;

            hashmap! {
                $(
                    {idx += 1; idx} => Memmap($base::$variant),
                )*
            }
        }};
    }

    macro_rules! block_test_impl {
        (mem $base: ident) => {
            map_block_type_variants!(block_test_impl_mem, $base)
        };

        (mmap $base: ident) => {
            map_block_type_variants!(block_test_impl_mmap, $base)
        };
    }

    macro_rules! block_map {
        (mem {$($key:expr => $value:expr),+}) => {{
            hashmap! {
                $(
                    $key => BlockStorage::Memory($value),
                 )+
            }
        }};

        (mmap {$($key:expr => $value:expr),+}) => {{
            hashmap! {
                $(
                    $key => BlockStorage::Memmap($value),
                 )+
            }
        }}
    }

    macro_rules! block_write_test_impl {
        (read $ops: expr) => {
                $ops.as_slice().par_iter().for_each(
                    |&(ref block, ref data)| {
                        let b = acquire!(read block);

                        map_fragment!(map owned b, *data, _blk, _frg, _fidx, {
                            // destination bounds checking intentionally left out
                            assert_eq!(_blk.len(), _frg.len());
                            assert_eq!(_blk.as_slice(), _frg.as_slice());
                        }, {}, {}).unwrap()
                    },
                );
        };
    }

    fn block_write_test_impl(short_map: &BlockStorageMap, long_map: &BlockStorageMap) {
        let count = 100;

        let root = tempdir!();

        let ts = RandomTimestampGen::random::<u64>();

        let mut part = Partition::new(&root, Partition::gen_id(), ts)
            .with_context(|_| "Failed to create partition")
            .unwrap();

        part.ensure_blocks(short_map)
            .with_context(|_| "Unable to create block map")
            .unwrap();

        // write some data

        let frags = vec![
            Fragment::from(random!(gen u64, count)),
            Fragment::from(random!(gen u32, count)),
        ];

        {
            let mut ops = vec![
                (part.blocks.get(&0).unwrap(), frags.get(0).unwrap()),
                (part.blocks.get(&1).unwrap(), frags.get(1).unwrap()),
            ];

            ops.as_mut_slice().par_iter_mut().for_each(
                |&mut (ref mut block, ref data)| {
                    let b = acquire!(write block);

                    map_fragment!(mut map owned b, *data, _blk, _frg, _fidx, {
                        // destination bounds checking intentionally left out
                        let slen = _frg.len();

                        _blk.as_mut_slice_append()[..slen].copy_from_slice(&_frg[..]);
                        _blk.set_written(count).unwrap();

                    }, {}, {}).unwrap()
                },
                );
        }

        // verify the data is there

        {
            let ops = vec![
                (part.blocks.get(&0).unwrap(), frags.get(0).unwrap()),
                (part.blocks.get(&1).unwrap(), frags.get(1).unwrap()),
            ];

            block_write_test_impl!(read ops);
        }

        part.ensure_blocks(long_map)
            .with_context(|_| "Unable to create block map")
            .unwrap();

        // verify the data is still there

        {
            let ops = vec![
                (part.blocks.get(&0).unwrap(), frags.get(0).unwrap()),
                (part.blocks.get(&1).unwrap(), frags.get(1).unwrap()),
            ];

            block_write_test_impl!(read ops);
        }
    }

    fn ts_init<'part, P>(
        root: P,
        ts_ty: BlockStorage,
        count: usize,
    ) -> (Partition<'part>, Timestamp, Timestamp)
    where
        P: AsRef<Path>,
    {
        let ts = RandomTimestampGen::iter().take(count).collect::<Vec<_>>();

        let ts_min = ts.iter()
            .min()
            .ok_or_else(|| "Failed to get minimal timestamp")
            .unwrap();

        let ts_max = ts.iter()
            .max()
            .ok_or_else(|| "Failed to get maximal timestamp")
            .unwrap();

        let mut part = Partition::new(&root, Partition::gen_id(), *ts_min)
            .with_context(|_| "Failed to create partition")
            .unwrap();

        part.ensure_blocks(&(hashmap! { 0 => ts_ty }).into())
            .with_context(|_| "Failed to create blocks")
            .unwrap();

        {
            let ts_block = part.mut_blocks()
                .get_mut(&0)
                .ok_or_else(|| "Failed to get ts block")
                .unwrap();

            block_apply!(mut expect physical U64Dense, ts_block, block, pb, {
                {
                    let buf = pb.as_mut_slice_append();
                    assert!(buf.len() >= ts.len());

                    &mut buf[..count].copy_from_slice(&ts[..]);

                    assert_eq!(&buf[..count], &ts[..]);
                }

                pb.set_written(count)
                    .with_context(|_| "Unable to move head ptr")
                    .unwrap();
            });
        }

        (part, (*ts_min).into(), (*ts_max).into())
    }

    fn scan_ts(ts_ty: BlockStorage) {

        let root = tempdir!();

        let (part, ts_min, ts_max) = ts_init(&root, ts_ty, 100);

        let (ret_ts_min, ret_ts_max) = part.scan_ts()
            .with_context(|_| "Failed to scan timestamps")
            .unwrap();

        assert_eq!(ret_ts_min, ts_min);
        assert_eq!(ret_ts_max, ts_max);
    }

    fn meta_ts(ts_ty: BlockStorage) {
        use crate::ty::fragment::FragmentRef;

        let root = tempdir!();
        let row_count = 100;

        let mut part = Partition::new(&root, Partition::gen_id(), 0)
            .with_context(|_| "Failed to create partition")
            .unwrap();

        let ts = RandomTimestampGen::iter().take(row_count).collect::<Vec<_>>();

        let (ts_min, ts_max) = ts.iter().fold((0, 0), |(ts_min, ts_max), ts| {
            (
                min(*ts, ts_min),
                max(*ts, ts_max),
            )
        });

        let blocks = hashmap! {
            0 => ts_ty,
        }.into();

        let frags = hashmap! {
            0 => Fragment::from(ts),
        };

        let indexes = hashmap! {}.into();

        let refs = frags.iter()
            .map(|(blk, frag)| (*blk, FragmentRef::from(frag)))
            .collect();

        part.append(&blocks, &indexes, &refs, 0).expect("Partition append failed");

        assert_eq!(part.ts_min, Timestamp::from(ts_min));
        assert_eq!(part.ts_max, Timestamp::from(ts_max));
    }

    fn update_meta(ts_ty: BlockStorage) {

        let root = tempdir!();
        let (mut part, ts_min, ts_max) = ts_init(&root, ts_ty, 100);

        part.update_meta()
            .with_context(|_| "Unable to update metadata")
            .unwrap();

        assert_eq!(part.ts_min, ts_min);
        assert_eq!(part.ts_max, ts_max);
    }

    // This macro performs a dense block write
    // sparse blocks will silently do nothing
    macro_rules! block_write {
        ($part: expr, $blockmap: expr, $frags: expr) => {{
            let blockmap = $blockmap;
            let part = $part;
            let frags = $frags;

            part.ensure_blocks(&blockmap)
                .with_context(|_| "Unable to create block map")
                .unwrap();

            {
                let mut ops = frags.iter()
                    .filter_map(|(ref blk_idx, ref _frag)| {
                        if let Some(block) = part.blocks.get(blk_idx) {
                            Some((block, frags.get(blk_idx).unwrap()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                ops.as_mut_slice().par_iter_mut().for_each(
                    |&mut (ref mut block, ref data)| {
                        let b = acquire!(write block);

                        map_fragment!(mut map owned b, *data, _blk, _frg, _fidx, {
                            // destination bounds checking intentionally left out
                            let slen = _frg.len();

                            _blk.as_mut_slice_append()[..slen].copy_from_slice(&_frg[..]);
                            _blk.set_written(slen).unwrap();

                        }, {
                            // sparse writes are intentionally not implemented
                        }, {
                            // pooled dense writes are intentionally not implemented
                        }).unwrap()
                    },
                );
            }
        }};
    }

    mod scan {
        use super::*;

//     +--------+----+------+------+
//     | rowidx | ts | col1 | col2 |
//     +--------+----+------+------+
//     |   0    | 1  |      | 10   |
//     +--------+----+------+------+
//     |   1    | 2  | 1    | 20   |
//     +--------+----+------+------+
//     |   2    | 3  |      |      |
//     +--------+----+------+------+
//     |   3    | 4  | 2    |      |
//     +--------+----+------+------+
//     |   4    | 5  |      |      |
//     +--------+----+------+------+
//     |   5    | 6  | 3    | 30   |
//     +--------+----+------+------+
//     |   6    | 7  |      |      |
//     +--------+----+------+------+
//     |   7    | 8  |      |      |
//     +--------+----+------+------+
//     |   8    | 9  | 4    | 7    |
//     +--------+----+------+------+
//     |   9    | 10 |      | 8    |
//     +--------+----+------+------+

        macro_rules! scan_test_init {
            () => {{
                use crate::block::BlockType;
                use self::BlockStorage::Memory;
                use crate::ty::fragment::FragmentRef;

                let root = tempdir!();

                let ts = 0;

                let mut part = Partition::new(&root, Partition::gen_id(), ts)
                    .with_context(|_| "Failed to create partition")
                    .unwrap();

                let blocks = hashmap! {
                    0 => Memory(BlockType::U64Dense),
                    1 => Memory(BlockType::U8Sparse),
                    2 => Memory(BlockType::U16Sparse),
                }.into();

                let frags = hashmap! {
                    0 => Fragment::from(vec![1_u64, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
                    1 => Fragment::from((vec![
                        1_u8, 2, 3, 4,
                    ], vec![
                        1_u32, 3, 5, 8,
                    ])),
                    2 => Fragment::from((vec![
                        10_u16, 20, 30, 7, 8,
                    ], vec![
                        0_u32, 1, 5, 8, 9,
                    ])),
                };

                let indexes = hashmap! {}.into();

                let refs = frags.iter()
                    .map(|(blk, frag)| (*blk, FragmentRef::from(frag)))
                    .collect();

                part.append(&blocks, &indexes, &refs, 0).expect("Partition append failed");

                (root, part, ts)
            }};
        }

        mod filter {
            use super::*;
            use crate::scanner::{ScanFilter, ScanFilterOp};

            mod and {
                use super::*;

                // ts < 4 && ts > 1

                #[test]
                fn single_column() {
                    let (_root, part, _) = scan_test_init!();

                    let expected = hashset!{1, 2};

                    let filters = hashmap! {
                            0 => vec![
                                vec![
                                    ScanFilter::U64(ScanFilterOp::Lt(4)),
                                    ScanFilter::U64(ScanFilterOp::Gt(1))
                                ]
                            ]
                        };

                    let result = part.filter(&filters, None, Scan::count_or_clauses(&filters))
                        .expect("filter failed");

                    assert_eq!(expected, result);
                }

                // ts < 4 && ts > 1 && source < 2

                #[test]
                fn multiple_columns() {
                    let (_root, part, _) = scan_test_init!();

                    let expected = hashset!{1};

                    let filters = hashmap! {
                            0 => vec![
                                vec![
                                    ScanFilter::U64(ScanFilterOp::Lt(4)),
                                    ScanFilter::U64(ScanFilterOp::Gt(1))
                                ]
                            ],
                            1 => vec![
                                vec![
                                    ScanFilter::U8(ScanFilterOp::Lt(2))
                                ]
                            ]
                        };

                    let result = part.filter(&filters, None, Scan::count_or_clauses(&filters))
                        .expect("filter failed");

                    assert_eq!(expected, result);
                }
            }


            mod or {
                use super::*;

                // (ts < 4 && ts > 1) || (ts > 7 && ts < 9)

                #[test]
                fn single_column() {
                    let (_root, part, _) = scan_test_init!();

                    let expected = hashset!{1, 2, 7};

                    let filters = hashmap! {
                            0 => vec![
                                vec![
                                    ScanFilter::U64(ScanFilterOp::Lt(4)),
                                    ScanFilter::U64(ScanFilterOp::Gt(1))
                                ],
                                vec![
                                    ScanFilter::U64(ScanFilterOp::Gt(7)),
                                    ScanFilter::U64(ScanFilterOp::Lt(9))
                                ]
                            ]
                        };

                    let result = part.filter(&filters, None, Scan::count_or_clauses(&filters))
                        .expect("filter failed");

                    assert_eq!(expected, result);
                }

                // (ts < 4 && ts > 1 && source < 2) || (ts > 5 && source > 3 && source <= 4)

                #[test]
                fn multiple_columns() {
                    let (_root, part, _) = scan_test_init!();

                    let expected = hashset!{1, 8};

                    let filters = hashmap! {
                            0 => vec![
                                vec![
                                    ScanFilter::U64(ScanFilterOp::Lt(4)),
                                    ScanFilter::U64(ScanFilterOp::Gt(1))
                                ],
                                vec![
                                    ScanFilter::U64(ScanFilterOp::Gt(5))
                                ]
                            ],
                            1 => vec![
                                vec![
                                    ScanFilter::U8(ScanFilterOp::Lt(2))
                                ],
                                vec![
                                    ScanFilter::U8(ScanFilterOp::Gt(3)),
                                    ScanFilter::U8(ScanFilterOp::LtEq(4))
                                ]
                            ]
                        };

                    let result = part.filter(&filters, None, Scan::count_or_clauses(&filters))
                        .expect("filter failed");

                    assert_eq!(expected, result);
                }
            }

            #[cfg(all(feature = "nightly", test))]
            mod benches {
                use test::{Bencher, black_box};
                use scanner::{ScanFilter, ScanFilterOp, Scan};
                use super::*;

                mod and {
                    use super::*;

                    #[bench]
                    fn single_column_one_filter(b: &mut Bencher) {
                        let (_root, part, _) = scan_test_init!();

                        let filters = hashmap! {
                            0 => vec![vec![
                                ScanFilter::U64(ScanFilterOp::Lt(4)),
                            ]],
                        };

                        b.iter(|| {
                            let result = part
                                .filter(&filters, None, Scan::count_or_clauses(&filters))
                                .expect("Filter failed");
                            black_box(&result);
                        })
                    }

                    #[bench]
                    fn single_column_two_filters(b: &mut Bencher) {
                        let (_root, part, _) = scan_test_init!();

                        let filters = hashmap! {
                            0 => vec![vec![
                                ScanFilter::U64(ScanFilterOp::Lt(4)),
                                ScanFilter::U64(ScanFilterOp::Gt(1)),
                            ]],
                        };

                        b.iter(|| {
                            let result = part
                                .filter(&filters, None, Scan::count_or_clauses(&filters))
                                .expect("Filter failed");
                            black_box(&result);
                        })
                    }

                    #[bench]
                    fn multi_columns_one_filter(b: &mut Bencher) {
                        let (_root, part, _) = scan_test_init!();

                        let filters = hashmap! {
                            0 => vec![vec![
                                ScanFilter::U64(ScanFilterOp::Lt(4)),
                            ]],
                            1 => vec![vec![
                                ScanFilter::U8(ScanFilterOp::Gt(2)),
                            ]],
                        };

                        b.iter(|| {
                            let result = part
                                .filter(&filters, None, Scan::count_or_clauses(&filters))
                                .expect("Filter failed");
                            black_box(&result);
                        })
                    }

                    #[bench]
                    fn multi_columns_two_filters(b: &mut Bencher) {
                        let (_root, part, _) = scan_test_init!();

                        let filters = hashmap! {
                            0 => vec![vec![
                                ScanFilter::U64(ScanFilterOp::Lt(4)),
                                ScanFilter::U64(ScanFilterOp::Gt(1)),
                            ]],
                            1 => vec![vec![
                                ScanFilter::U8(ScanFilterOp::Gt(2)),
                                ScanFilter::U8(ScanFilterOp::Lt(4)),
                            ]],
                        };

                        b.iter(|| {
                            let result = part
                                .filter(&filters, None, Scan::count_or_clauses(&filters))
                                .expect("Filter failed");
                            black_box(&result);
                        })
                    }
                }
            }
        }

        mod materialize {
            use super::*;

            #[test]
            fn continuous() {
                let (_root, part, _) = scan_test_init!();

                let rowids = vec![1_usize, 2, 3];

                let result = part.materialize(&None, Some(&rowids[..]))
                    .expect("Materialize failed");

                let expected = hashmap! {
                    0 => Some(Fragment::from(vec![2_u64, 3, 4])),
                    1 => Some(Fragment::from((vec![1_u8, 2], vec![0_u32, 2]))),
                    2 => Some(Fragment::from((vec![20_u16], vec![0_u32]))),
                };

                assert_eq!(expected, result);
            }

            #[test]
            fn non_continuous() {
                let (_root, part, _) = scan_test_init!();

                let rowids = vec![0_usize, 1, 3, 8];

                let result = part.materialize(&None, Some(&rowids[..]))
                    .expect("Materialize failed");

                let expected = hashmap! {
                    0 => Some(Fragment::from(vec![1_u64, 2, 4, 9])),
                    1 => Some(Fragment::from((vec![1_u8, 2, 4], vec![1_u32, 2, 3]))),
                    2 => Some(Fragment::from((vec![10_u16, 20, 7], vec![0_u32, 1, 3]))),
                };

                assert_eq!(expected, result);
            }

            #[cfg(all(feature = "nightly", test))]
            mod benches {
                use test::{Bencher, black_box};
                use super::*;

                #[bench]
                fn continuous(b: &mut Bencher) {
                    let (_root, part, _) = scan_test_init!();

                    let rowids = vec![1_usize, 2, 3];

                    b.iter(|| {
                        let result = part
                            .materialize(&None, Some(&rowids[..]))
                            .expect("Materialize failed");
                        black_box(&result);
                    })
                }

                #[bench]
                fn non_continuous(b: &mut Bencher) {
                    let (_root, part, _) = scan_test_init!();

                    let rowids = vec![0_usize, 1, 3, 8];

                    b.iter(|| {
                        let result = part
                            .materialize(&None, Some(&rowids[..]))
                            .expect("Materialize failed");
                        black_box(&result);
                    });
                }
            }
        }
    }

    #[test]
    fn space_for_blocks() {
        use rayon::iter::IntoParallelRefMutIterator;
        use crate::block::{BlockType, BlockData};
        use std::mem::size_of;
        use crate::params::BLOCK_SIZE;
        use std::iter::FromIterator;
        use self::BlockStorage::Memory;

        // reserve 20 records on timestamp
        let count = BLOCK_SIZE / size_of::<u64>() - 20;

        let root = tempdir!();

        let ts = RandomTimestampGen::random::<u64>();

        let mut part = Partition::new(&root, Partition::gen_id(), ts)
            .with_context(|_| "Failed to create partition")
            .unwrap();

        let blocks = hashmap! {
            0 => Memory(BlockType::U64Dense),
            1 => Memory(BlockType::U32Dense),
        }.into();

        let frags = hashmap! {
            0 => Fragment::from(random!(gen u64, count)),
            1 => Fragment::from(random!(gen u32, count)),
        };

        block_write!(&mut part, blocks, frags);

        let blocks = hashmap! {
            2 => Memory(BlockType::U64Sparse),
        }.into();

        let frags = hashmap! {
            2 => Fragment::from((random!(gen u64, count), random!(gen u32, count))),
        };

        block_write!(&mut part, blocks, frags);

        let blocks = hashmap! {
            0 => Memory(BlockType::U64Dense),
            1 => Memory(BlockType::U32Dense),
            2 => Memory(BlockType::U64Sparse),
        };

        assert_eq!(
            part.space_for_blocks(Vec::from_iter(blocks.keys().cloned()).as_slice()).unwrap(),
            20
        );
    }

    mod ts {
        use super::*;
        use crate::ty::fragment::FragmentRef;
        use crate::block::BlockType;
        use self::BlockStorage::Memory;

        macro_rules! ts_test_init {
            ($ts_init: expr, $( $ts_frag: expr ),+ $(,)*) => {{
                let root = tempdir!();

                let mut part = Partition::new(&root, Partition::gen_id(), $ts_init)
                    .with_context(|_| "Failed to create partition")
                    .unwrap();

                let blocks = hashmap! {
                    0 => Memory(BlockType::U64Dense),
                }.into();

                $(
                    let ts: Vec<u64> = $ts_frag;

                    let frags = hashmap! {
                        0 => Fragment::from(ts),
                    };

                    let indexes = hashmap! {}.into();

                    let refs = frags.iter()
                        .map(|(blk, frag)| (*blk, FragmentRef::from(frag)))
                        .collect();

                    part.append(&blocks, &indexes, &refs, 0).expect("Partition append failed");
                )+

                (root, part)
            }};
        }

        #[test]
        fn single_append() {
            let (_td, part) = ts_test_init!(0, vec![0, 1, 2, 3, 4, 5]);

            assert_eq!(part.ts_min, Timestamp::from(0));
            assert_eq!(part.ts_max, Timestamp::from(5));
        }

        #[test]
        fn multiple_appends() {
            let (_td, part) = ts_test_init!(
                1,
                vec![1, 2, 3, 4, 5],
                vec![10, 20, 30],
                vec![1, 5, 45],
                vec![0, 7, 5],
            );

            assert_eq!(part.ts_min, Timestamp::from(0));
            assert_eq!(part.ts_max, Timestamp::from(45));
        }

        #[test]
        fn get_ts() {
            let root = tempdir!();

            let &(ts_min, ts_max) = &RandomTimestampGen::pairs(1)[..][0];

            let mut part = Partition::new(&root, Partition::gen_id(), ts_min)
                .with_context(|_| "Failed to create partition")
                .unwrap();

            part.ts_max = ts_max;

            let part = part;

            let (ret_ts_min, ret_ts_max) = part.get_ts();

            assert_eq!(ret_ts_min, ts_min);
            assert_eq!(ret_ts_max, ts_max);
        }

        #[test]
        fn set_ts() {
            let root = tempdir!();

            let &(ts_min, ts_max) = &RandomTimestampGen::pairs(1)[..][0];
            let start_ts = RandomTimestampGen::random_from(ts_max);

            assert_ne!(ts_min, start_ts);

            let mut part = Partition::new(&root, Partition::gen_id(), start_ts)
                .with_context(|_| "Failed to create partition")
                .unwrap();

            part.set_ts(Some(ts_min), Some(ts_max))
                .with_context(|_| "Unable to set timestamps")
                .unwrap();

            assert_eq!(part.ts_min, ts_min);
            assert_eq!(part.ts_max, ts_max);
        }
    }

    mod serialize {
        use super::*;


        pub(super) fn blocks<'block, T>(type_map: HashMap<BlockId, T>)
        where
            T: Debug + Clone,
            Block<'block>: PartialEq<T>,
            BlockStorageMap: From<HashMap<BlockId, T>>,
        {
            let root = tempdir!();
            let ts = <Timestamp as Default>::default();

            let mut part = Partition::new(&root, Partition::gen_id(), *ts)
                .with_context(|_| "Failed to create partition")
                .unwrap();

            part.ensure_blocks(&(type_map.clone().into()))
                .with_context(|_| "Failed to create blocks")
                .unwrap();

            part.flush().with_context(|_| "Partition flush failed").unwrap();

            assert_eq!(part.ts_min, ts);
            assert_eq!(part.ts_max, ts);
            assert_eq!(part.blocks.len(), type_map.len());
            for (block_id, block_type) in &type_map {
                assert_eq!(*acquire!(raw read part.blocks[block_id]), *block_type);
            }
        }

        pub(super) fn heads<'part, 'block, P>(
            root: P,
            type_map: HashMap<BlockId, BlockStorage>,
            ts_ty: BlockStorage,
            count: usize,
        ) where
            'part: 'block,
            P: AsRef<Path>,
        {
            let (mut part, _, _) = ts_init(&root, ts_ty, count);

            part.ensure_blocks(&(type_map.into()))
                .with_context(|_| "Failed to create blocks")
                .unwrap();

            for (blockid, block) in part.mut_blocks() {
                if *blockid != 0 {
                    block_apply!(mut map physical block, blk, pb, {
                        pb.set_written(count)
                            .with_context(|_| "Unable to move head ptr")
                            .unwrap();
                    });
                }
            }

            part.flush().with_context(|_| "Partition flush failed").unwrap();
        }

        #[test]
        fn meta() {
            let root = tempdir!();
            let ts = <Timestamp as Default>::default();

            {
                Partition::new(&root, Partition::gen_id(), *ts)
                    .with_context(|_| "Failed to create partition")
                    .unwrap();
            }

            let meta = root.as_ref().join(PARTITION_METADATA);

            assert!(meta.exists());
        }
    }

    mod deserialize {
        use super::*;


        pub(super) fn blocks<'block, T>(type_map: HashMap<BlockId, T>)
        where
            T: Debug + Clone,
            Block<'block>: PartialEq<T>,
            BlockStorageMap: From<HashMap<BlockId, T>>,
        {
            let root = tempdir!();
            let ts = <Timestamp as Default>::default();

            {
                let mut part = Partition::new(&root, Partition::gen_id(), *ts)
                    .with_context(|_| "Failed to create partition")
                    .unwrap();

                part.ensure_blocks(&(type_map.clone().into()))
                    .with_context(|_| "Failed to create blocks")
                    .unwrap();
            }

            let part = Partition::with_data(&root)
                .with_context(|_| "Failed to read partition data")
                .unwrap();

            assert_eq!(part.blocks.len(), type_map.len());
            for (block_id, block_type) in &type_map {
                assert_eq!(*acquire!(raw read part.blocks[block_id]), *block_type);
            }
        }

        pub(super) fn heads<'part, 'block, P>(
            root: P,
            type_map: HashMap<BlockId, BlockStorage>,
            ts_ty: BlockStorage,
            count: usize,
        ) where
            'part: 'block,
            P: AsRef<Path>,
        {
            super::serialize::heads(&root, type_map.clone(), ts_ty, count);

            let part = Partition::with_data(&root)
                .with_context(|_| "Failed to read partition data")
                .unwrap();

            for (_blockid, block) in &part.blocks {
                block_apply!(map physical block, blk, pb, {
                    assert_eq!(pb.len(), count);
                });
            }
        }

        #[test]
        fn meta() {
            let root = tempdir!();
            let ts = <Timestamp as Default>::default();

            let id = {
                let part = Partition::new(&root, Partition::gen_id(), *ts)
                    .with_context(|_| "Failed to create partition")
                    .unwrap();

                part.id
            };

            let part = Partition::with_data(&root)
                .with_context(|_| "Failed to read partition data")
                .unwrap();

            assert_eq!(part.id, id);
            assert_eq!(part.ts_min, ts);
            assert_eq!(part.ts_max, ts);
        }
    }

    mod memory {
        use super::*;
        use crate::block::BlockType;
        use self::BlockStorage::Memory;

        #[test]
        fn scan_ts() {
            super::scan_ts(Memory(BlockType::U64Dense));
        }

        #[test]
        fn meta_ts() {
            super::meta_ts(Memory(BlockType::U64Dense));
        }

        #[test]
        fn update_meta() {
            super::update_meta(Memory(BlockType::U64Dense));
        }

        #[test]
        fn ensure_blocks() {
            let short_map = block_map!(mem {
                0usize => BlockType::U64Dense,
                1usize => BlockType::U32Dense
            }).into();
            let long_map = block_map!(mem {
                0 => BlockType::U64Dense,
                1 => BlockType::U32Dense,
                2 => BlockType::U64Dense,
                3 => BlockType::U32Dense
            }).into();

            block_write_test_impl(&short_map, &long_map);
        }

        mod serialize {
            use super::*;
            use self::BlockStorage::Memory;

            #[test]
            fn blocks() {
                super::super::serialize::blocks(block_test_impl!(mem BlockType));
            }

            #[test]
            fn heads() {
                let root = tempdir!();

                super::super::serialize::heads(
                    &root,
                    block_test_impl!(mem BlockType),
                    Memory(BlockType::U64Dense),
                    100,
                );
            }
        }

        mod deserialize {
            use super::*;
            use self::BlockStorage::Memory;

            #[test]
            fn blocks() {
                super::super::deserialize::blocks(block_test_impl!(mem BlockType));
            }

            #[test]
            fn heads() {
                let root = tempdir!();

                super::super::deserialize::heads(
                    &root,
                    block_test_impl!(mem BlockType),
                    Memory(BlockType::U64Dense),
                    100,
                );
            }
        }
    }


    #[cfg(feature = "mmap")]
    mod mmap {
        use super::*;
        use crate::block::BlockType;
        use self::BlockStorage::Memmap;

        #[test]
        fn scan_ts() {
            super::scan_ts(Memmap(BlockType::U64Dense));
        }

        #[test]
        fn meta_ts() {
            super::meta_ts(Memmap(BlockType::U64Dense));
        }

        #[test]
        fn update_meta() {
            super::update_meta(Memmap(BlockType::U64Dense));
        }

        #[test]
        fn ensure_blocks() {
            let short_map = block_map!(mmap {
                0 => BlockType::U64Dense,
                1 => BlockType::U32Dense
            }).into();
            let long_map = block_map!(mmap {
                0 => BlockType::U64Dense,
                1 => BlockType::U32Dense,
                2 => BlockType::U64Dense,
                3 => BlockType::U32Dense
            }).into();

            block_write_test_impl(&short_map, &long_map);
        }

        mod serialize {
            use super::*;
            use self::BlockStorage::Memmap;

            #[test]
            fn blocks() {
                super::super::serialize::blocks(block_test_impl!(mmap BlockType));
            }

            #[test]
            fn heads() {
                let root = tempdir!();

                super::super::serialize::heads(
                    &root,
                    block_test_impl!(mmap BlockType),
                    Memmap(BlockType::U64Dense),
                    100,
                );
            }
        }

        mod deserialize {
            use super::*;
            use self::BlockStorage::Memmap;

            #[test]
            fn blocks() {
                super::super::deserialize::blocks(block_test_impl!(mmap BlockType));
            }

            #[test]
            fn heads() {
                let root = tempdir!();

                super::super::deserialize::heads(
                    &root,
                    block_test_impl!(mmap BlockType),
                    Memmap(BlockType::U64Dense),
                    100,
                );
            }
        }
    }
}
