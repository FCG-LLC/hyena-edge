use error::*;
use uuid::Uuid;

use block::{BlockData, BufferHead, SparseIndex};
use ty::{Block, BlockHeadMap, BlockId, BlockMap, BlockType as TyBlockType, BlockTypeMap, Timestamp};
use std::path::{Path, PathBuf};
use std::cmp::{max, min};
use block::BlockType;
use std::collections::HashMap;
#[cfg(feature = "mmap")]
use rayon::prelude::*;
use params::PARTITION_METADATA;
use std::sync::RwLock;
use ty::fragment::FragmentRef;
use mutator::BlockRefData;


pub(crate) type PartitionId = Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct Partition<'part> {
    id: PartitionId,
    // TODO: do we really need ts_* here?
    ts_min: Timestamp,
    ts_max: Timestamp,

    #[serde(skip)]
    blocks: BlockMap<'part>,
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
            data_root: root,
        })
    }

    pub fn with_data<P: AsRef<Path>>(root: P) -> Result<Partition<'part>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(PARTITION_METADATA);

        Partition::deserialize(&meta, &root)
    }

    pub fn len(&self) -> usize {
        if !self.blocks.is_empty() {
            if let Some(block) = self.blocks.get(&0) {
                let b = acquire!(read block);
                return b.len();
            }
        }

        0
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty() || self.len() != 0
    }

    pub(crate) fn append<'frag>(
        &mut self,
        blockmap: &BlockTypeMap,
        frags: &BlockRefData<'frag>,
        offset: SparseIndex,
    ) -> Result<usize> {

        self.ensure_blocks(&blockmap)
            .chain_err(|| "Unable to create block map")
            .unwrap();
        let mut ops = frags.iter().filter_map(|(ref blk_idx, ref frag)| {
            if let Some(block) = self.blocks.get(blk_idx) {
                trace!("writing block {} with frag len {}", blk_idx, (*frag).len());
                Some((block, frags.get(blk_idx).unwrap()))
            } else {
                None
            }
        });

        for (ref mut block, ref data) in ops {
            let mut b = acquire!(write block);
            let r = map_fragment!(mut map ref b, *data, blk, frg, fidx, {
                // dense block handler

                // destination bounds checking intentionally left out
                let slen = frg.len();

                {
                    let mut blkslice = blk.as_mut_slice_append();
                    &mut blkslice[..slen].copy_from_slice(&frg[..]);
                }

                blk.set_written(slen).unwrap();
            }, {
                // sparse block handler

                // destination bounds checking intentionally left out
                let slen = frg.len();

                let block_offset = if let Some(offset) = blk.as_index_slice().last() {
                    *offset + 1
                } else {
                    0
                };

                {
                    let (mut blkindex, mut blkslice) = blk.as_mut_indexed_slice_append();

                    &mut blkslice[..slen].copy_from_slice(&frg[..]);
                    &mut blkindex[..slen].copy_from_slice(&fidx[..]);

                    // adjust the offset
                    &mut blkindex[..slen].par_iter_mut().for_each(|idx| {
                        *idx = *idx - offset + block_offset;
                    });
                }

                blk.set_written(slen).unwrap();
            });

            r.unwrap()
        }


        // todo: fix this (should be slen)
        Ok(42)
    }

    pub(crate) fn scan_ts(&self) -> Result<(Timestamp, Timestamp)> {
        let ts_block = self.blocks
            .get(&0)
            .ok_or_else(|| "Failed to get timestamp block")?;

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

            Ok((ts_min.ok_or_else(|| "Failed to get min timestamp")?.into(),
                ts_max.ok_or_else(|| "Failed to get max timestamp")?.into()))
        })
    }

    pub fn get_id(&self) -> PartitionId {
        self.id
    }

    pub(crate) fn get_ts(&self) -> (Timestamp, Timestamp) {
        (self.ts_min, self.ts_max)
    }
    #[must_use]
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

    pub(crate) fn update_meta(&mut self) -> Result<()> {
        // TODO: handle ts_min changes -> partition path

        let (ts_min, ts_max) = self.scan_ts()
            .chain_err(|| "Unable to perform ts scan for metadata update")?;

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

    pub fn space_for_blocks(&self, indices: &[usize]) -> usize {
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
            // the default shouldn't ever happen, as there always should be ts block
            // but in case it happens, this will return 0
            // which in turn will cause new partition to be used
            .unwrap_or_default()
    }

    pub(crate) fn get_blocks(&self) -> &BlockMap<'part> {
        &self.blocks
    }

    pub(crate) fn mut_blocks(&mut self) -> &mut BlockMap<'part> {
        &mut self.blocks
    }

    #[inline]
    pub fn ensure_blocks(&mut self, type_map: &BlockTypeMap) -> Result<()> {
        let fmap = type_map
            .iter()
            .filter_map(|(block_id, block_type)| {
                if self.blocks.contains_key(block_id) {
                    None
                } else {
                    Some((*block_id, *block_type))
                }
            })
            .collect::<HashMap<_, _>>();

        let blocks = Partition::prepare_blocks(&self.data_root, &fmap)
            .chain_err(|| "Unable to create block map")?;

        self.blocks.extend(blocks);

        Ok(())
    }

    #[inline]
    pub fn gen_id() -> PartitionId {
        Uuid::new_v4()
    }

    // TODO: to be benched
    fn prepare_blocks<'i, P>(
        root: P,
        type_map: &'i HashMap<BlockId, TyBlockType>,
    ) -> Result<BlockMap<'part>>
    where
        P: AsRef<Path> + Sync,
    {
        type_map
            .iter()
            .map(|(block_id, block_type)| match *block_type {
                TyBlockType::Memory(bty) => {
                    use ty::block::memory::Block;

                    Block::create(bty)
                        .map(|block| (*block_id, locked!(rw block.into())))
                        .chain_err(|| "Unable to create in-memory block")
                }
                TyBlockType::Memmap(bty) => {
                    use ty::block::mmap::Block;

                    Block::create(&root, bty, *block_id)
                        .map(|block| (*block_id, locked!(rw block.into())))
                        .chain_err(|| "Unable to create mmap block")
                }
            })
            .collect()
    }

    fn flush(&self) -> Result<()> {
        // TODO: add dirty flag
        let meta = self.data_root.join(PARTITION_METADATA);

        Partition::serialize(self, &meta)
    }

    fn serialize<P: AsRef<Path>>(partition: &Partition<'part>, meta: P) -> Result<()> {
        let meta = meta.as_ref();

        let blocks: BlockTypeMap = (&partition.blocks).into();
        let heads = partition
            .blocks
            .iter()
            .map(|(blockid, block)| {
                (
                    *blockid,
                    block_apply!(map physical block, blk, pb, {
                    pb.head()
                }),
                )
            })
            .collect::<BlockHeadMap>();

        let data = (partition, blocks, heads);

        serialize!(file meta, &data).chain_err(|| "Failed to serialize partition metadata")
    }

    fn deserialize<P: AsRef<Path>, R: AsRef<Path> + Sync>(
        meta: P,
        root: R,
    ) -> Result<Partition<'part>> {
        let meta = meta.as_ref();

        if !meta.exists() {
            bail!("Cannot find partition metadata {:?}", meta);
        }

        let (mut partition, blocks, heads): (Partition, BlockTypeMap, BlockHeadMap) =
            deserialize!(file meta)
                .chain_err(|| "Failed to read partition metadata")?;

        partition.blocks = Partition::prepare_blocks(&root, &*blocks)
            .chain_err(|| "Failed to read block data")?;

        let partid = partition.id;

        for (blockid, block) in &mut partition.blocks {
            block_apply!(mut map physical block, blk, pb, {
                let head = heads.get(blockid)
                    .ok_or_else(||
                        format!("Unable to read block head ptr of partition {}", partid))?;
                *(pb.mut_head()) = *head;
            })
        }

        partition.data_root = root.as_ref().to_path_buf();

        Ok(partition)
    }
}


impl<'part> Drop for Partition<'part> {
    fn drop(&mut self) {
        self.flush()
            .chain_err(|| "Failed to flush data during drop")
            .unwrap();
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::HashMap;
    use ty::BlockId;
    use std::fmt::Debug;
    use helpers::random::timestamp::{RandomTimestamp, RandomTimestampGen};
    use block::BlockData;
    use ty::fragment::Fragment;


    macro_rules! block_test_impl {
        ($base: ident, $($variant: ident),* $(,)*) => {{
            let mut idx = 0;

            hashmap! {
                $(
                    {idx += 1; idx} => $base::$variant,
                )*
            }
        }};

        ($base: ident) => {
            map_block_type_variants!(block_test_impl, $base)
        };
    }

    macro_rules! block_write_test_impl {
        ($block_ty: ident) => {{
            use rayon::iter::{IntoParallelIterator, IntoParallelRefMutIterator};
            use block::BlockData;

            let count = 100;

            let root = tempdir!();

            let ts = RandomTimestampGen::random::<u64>();

            let mut part = Partition::new(&root, Partition::gen_id(), ts)
                .chain_err(|| "Failed to create partition")
                .unwrap();

            let blockmap = hashmap! {
                0 => $block_ty::U64Dense,
                1 => $block_ty::U32Dense,
            }.into();

            part.ensure_blocks(&blockmap)
                .chain_err(|| "Unable to create block map")
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
                        let mut b = acquire!(write block);

                        map_fragment!(mut map owned b, *data, blk, frg, _fidx, {
                            // destination bounds checking intentionally left out
                            let slen = frg.len();

                            blk.as_mut_slice_append()[..slen].copy_from_slice(&frg[..]);
                            blk.set_written(count).unwrap();

                        }, {}).unwrap()
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

            let blockmap = hashmap! {
                0 => $block_ty::U64Dense,
                1 => $block_ty::U32Dense,
                2 => $block_ty::U64Dense,
                3 => $block_ty::U32Dense,
            }.into();

            part.ensure_blocks(&blockmap)
                .chain_err(|| "Unable to create block map")
                .unwrap();

            // verify the data is still there

            {
                let ops = vec![
                    (part.blocks.get(&0).unwrap(), frags.get(0).unwrap()),
                    (part.blocks.get(&1).unwrap(), frags.get(1).unwrap()),
                ];

                block_write_test_impl!(read ops);
            }
        }};

        (read $ops: expr) => {
                $ops.as_slice().par_iter().for_each(
                    |&(ref block, ref data)| {
                        let b = acquire!(read block);

                        map_fragment!(map owned b, *data, blk, frg, _fidx, {
                            // destination bounds checking intentionally left out
                            assert_eq!(blk.len(), frg.len());
                            assert_eq!(blk.as_slice(), frg.as_slice());
                        }, {}).unwrap()
                    },
                );
        };
    }

    fn ts_init<'part, P, T>(
        root: P,
        ts_ty: T,
        count: usize,
    ) -> (Partition<'part>, Timestamp, Timestamp)
    where
        T: Debug + Clone,
        BlockType: From<T>,
        BlockTypeMap: From<HashMap<BlockId, T>>,
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
            .chain_err(|| "Failed to create partition")
            .unwrap();

        part.ensure_blocks(&(hashmap! { 0 => ts_ty }).into())
            .chain_err(|| "Failed to create blocks")
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
                    .chain_err(|| "Unable to move head ptr")
                    .unwrap();
            });
        }

        (part, (*ts_min).into(), (*ts_max).into())
    }

    fn scan_ts<T>(ts_ty: T)
    where
        T: Debug + Clone + Copy,
        BlockType: From<T>,
        BlockTypeMap: From<HashMap<BlockId, T>>,
    {

        let root = tempdir!();

        let (part, ts_min, ts_max) = ts_init(&root, ts_ty, 100);

        let (ret_ts_min, ret_ts_max) = part.scan_ts()
            .chain_err(|| "Failed to scan timestamps")
            .unwrap();

        assert_eq!(ret_ts_min, ts_min);
        assert_eq!(ret_ts_max, ts_max);
    }

    fn update_meta<T>(ts_ty: T)
    where
        T: Debug + Clone + Copy,
        BlockType: From<T>,
        BlockTypeMap: From<HashMap<BlockId, T>>,
    {

        let root = tempdir!();

        let (mut part, ts_min, ts_max) = ts_init(&root, ts_ty, 100);

        part.update_meta()
            .chain_err(|| "Unable to update metadata")
            .unwrap();

        assert_eq!(part.ts_min, ts_min);
        assert_eq!(part.ts_max, ts_max);
    }

    macro_rules! block_write {
        ($part: expr, $blockmap: expr, $frags: expr) => {{
            let blockmap = $blockmap;
            let mut part = $part;
            let frags = $frags;

            part.ensure_blocks(&blockmap)
                .chain_err(|| "Unable to create block map")
                .unwrap();

            {
                let mut ops = frags.iter()
                    .filter_map(|(ref blk_idx, ref frag)| {
                        if let Some(block) = part.blocks.get(blk_idx) {
                            Some((block, frags.get(blk_idx).unwrap()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                ops.as_mut_slice().par_iter_mut().for_each(
                    |&mut (ref mut block, ref data)| {
                        let mut b = acquire!(write block);

                        map_fragment!(mut map owned b, *data, blk, frg, _fidx, {
                            // destination bounds checking intentionally left out
                            let slen = frg.len();

                            blk.as_mut_slice_append()[..slen].copy_from_slice(&frg[..]);
                            blk.set_written(slen).unwrap();

                        }, {}).unwrap()
                    },
                );
            }
        }};
    }

    #[test]
    fn space_for_blocks() {
        use rayon::iter::{IntoParallelIterator, IntoParallelRefMutIterator};
        use block::BlockData;
        use std::mem::size_of;
        use params::BLOCK_SIZE;
        use std::iter::FromIterator;


        // reserve 20 records on timestamp
        let count = BLOCK_SIZE / size_of::<u64>() - 20;

        let root = tempdir!();

        let ts = RandomTimestampGen::random::<u64>();

        let mut part = Partition::new(&root, Partition::gen_id(), ts)
            .chain_err(|| "Failed to create partition")
            .unwrap();

        let blocks = hashmap! {
            0 => BlockType::U64Dense,
            1 => BlockType::U32Dense,
        }.into();

        let frags = hashmap! {
            0 => Fragment::from(random!(gen u64, count)),
            1 => Fragment::from(random!(gen u32, count)),
        };

        block_write!(&mut part, blocks, frags);

        let blocks = hashmap! {
            2 => BlockType::U64Sparse,
        }.into();

        let frags = hashmap! {
            2 => Fragment::from((random!(gen u64, count), random!(gen u32, count))),
        };

        block_write!(&mut part, blocks, frags);

        let blocks = hashmap! {
            0 => BlockType::U64Dense,
            1 => BlockType::U32Dense,
            2 => BlockType::U64Dense,
        };

        assert_eq!(
            part.space_for_blocks(Vec::from_iter(blocks.keys().cloned()).as_slice()),
            20
        );
    }

    #[test]
    fn get_ts() {
        let root = tempdir!();

        let &(ts_min, ts_max) = &RandomTimestampGen::pairs(1)[..][0];

        let mut part = Partition::new(&root, Partition::gen_id(), ts_min)
            .chain_err(|| "Failed to create partition")
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
            .chain_err(|| "Failed to create partition")
            .unwrap();

        part.set_ts(Some(ts_min), Some(ts_max))
            .chain_err(|| "Unable to set timestamps")
            .unwrap();

        assert_eq!(part.ts_min, ts_min);
        assert_eq!(part.ts_max, ts_max);
    }

    mod serialize {
        use super::*;


        pub(super) fn blocks<'block, T>(type_map: HashMap<BlockId, T>)
        where
            T: Debug + Clone,
            Block<'block>: PartialEq<T>,
            BlockType: From<T>,
            BlockTypeMap: From<HashMap<BlockId, T>>,
        {
            let root = tempdir!();
            let ts = <Timestamp as Default>::default();

            let mut part = Partition::new(&root, Partition::gen_id(), *ts)
                .chain_err(|| "Failed to create partition")
                .unwrap();

            part.ensure_blocks(&(type_map.clone().into()))
                .chain_err(|| "Failed to create blocks")
                .unwrap();

            part.flush().chain_err(|| "Partition flush failed").unwrap();

            assert_eq!(part.ts_min, ts);
            assert_eq!(part.ts_max, ts);
            assert_eq!(part.blocks.len(), type_map.len());
            for (block_id, block_type) in &type_map {
                assert_eq!(*acquire!(raw read part.blocks[block_id]), *block_type);
            }
        }

        pub(super) fn heads<'part, 'block, P, T>(
            root: P,
            type_map: HashMap<BlockId, T>,
            ts_ty: T,
            count: usize,
        ) where
            'part: 'block,
            T: Debug + Clone + Copy,
            Block<'block>: PartialEq<T>,
            BlockType: From<T>,
            BlockTypeMap: From<HashMap<BlockId, T>>,
            P: AsRef<Path>,
        {
            let (mut part, _, _) = ts_init(&root, ts_ty, count);

            part.ensure_blocks(&(type_map.into()))
                .chain_err(|| "Failed to create blocks")
                .unwrap();

            for (blockid, block) in part.mut_blocks() {
                if *blockid != 0 {
                    block_apply!(mut map physical block, blk, pb, {
                        pb.set_written(count)
                            .chain_err(|| "Unable to move head ptr")
                            .unwrap();
                    });
                }
            }

            part.flush().chain_err(|| "Partition flush failed").unwrap();
        }

        #[test]
        fn meta() {
            let root = tempdir!();
            let ts = <Timestamp as Default>::default();

            {
                Partition::new(&root, Partition::gen_id(), *ts)
                    .chain_err(|| "Failed to create partition")
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
            BlockType: From<T>,
            BlockTypeMap: From<HashMap<BlockId, T>>,
        {
            let root = tempdir!();
            let ts = <Timestamp as Default>::default();

            {
                let mut part = Partition::new(&root, Partition::gen_id(), *ts)
                    .chain_err(|| "Failed to create partition")
                    .unwrap();

                part.ensure_blocks(&(type_map.clone().into()))
                    .chain_err(|| "Failed to create blocks")
                    .unwrap();
            }

            let part = Partition::with_data(&root)
                .chain_err(|| "Failed to read partition data")
                .unwrap();

            assert_eq!(part.blocks.len(), type_map.len());
            for (block_id, block_type) in &type_map {
                assert_eq!(*acquire!(raw read part.blocks[block_id]), *block_type);
            }
        }

        pub(super) fn heads<'part, 'block, P, T>(
            root: P,
            type_map: HashMap<BlockId, T>,
            ts_ty: T,
            count: usize,
        ) where
            'part: 'block,
            T: Debug + Clone + Copy,
            Block<'block>: PartialEq<T>,
            BlockType: From<T>,
            BlockTypeMap: From<HashMap<BlockId, T>>,
            P: AsRef<Path>,
        {
            super::serialize::heads(&root, type_map.clone(), ts_ty, count);

            let part = Partition::with_data(&root)
                .chain_err(|| "Failed to read partition data")
                .unwrap();

            for (blockid, block) in &part.blocks {
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
                    .chain_err(|| "Failed to create partition")
                    .unwrap();

                part.id
            };

            let part = Partition::with_data(&root)
                .chain_err(|| "Failed to read partition data")
                .unwrap();

            assert_eq!(part.id, id);
            assert_eq!(part.ts_min, ts);
            assert_eq!(part.ts_max, ts);
        }
    }

    mod memory {
        use super::*;

        #[test]
        fn scan_ts() {
            super::scan_ts(BlockType::U64Dense);
        }

        #[test]
        fn update_meta() {
            super::update_meta(BlockType::U64Dense);
        }

        #[test]
        fn ensure_blocks() {
            block_write_test_impl!(BlockType);
        }

        mod serialize {
            use super::*;

            #[test]
            fn blocks() {
                super::super::serialize::blocks(block_test_impl!(BlockType));
            }

            #[test]
            fn heads() {
                let root = tempdir!();

                super::super::serialize::heads(
                    &root,
                    block_test_impl!(BlockType),
                    BlockType::U64Dense,
                    100,
                );
            }
        }

        mod deserialize {
            use super::*;

            #[test]
            fn blocks() {
                super::super::deserialize::blocks(block_test_impl!(BlockType));
            }

            #[test]
            fn heads() {
                let root = tempdir!();

                super::super::deserialize::heads(
                    &root,
                    block_test_impl!(BlockType),
                    BlockType::U64Dense,
                    100,
                );
            }
        }
    }


    #[cfg(feature = "mmap")]
    mod mmap {
        use super::*;

        #[test]
        fn scan_ts() {
            super::scan_ts(BlockType::U64Dense);
        }

        #[test]
        fn update_meta() {
            super::update_meta(BlockType::U64Dense);
        }

        #[test]
        fn ensure_blocks() {
            block_write_test_impl!(BlockType);
        }

        mod serialize {
            use super::*;

            #[test]
            fn blocks() {
                super::super::serialize::blocks(block_test_impl!(BlockType));
            }

            #[test]
            fn heads() {
                let root = tempdir!();

                super::super::serialize::heads(
                    &root,
                    block_test_impl!(BlockType),
                    BlockType::U64Dense,
                    100,
                );
            }
        }

        mod deserialize {
            use super::*;

            #[test]
            fn blocks() {
                super::super::deserialize::blocks(block_test_impl!(BlockType));
            }

            #[test]
            fn heads() {
                let root = tempdir!();

                super::super::deserialize::heads(
                    &root,
                    block_test_impl!(BlockType),
                    BlockType::U64Dense,
                    100,
                );
            }
        }
    }
}
